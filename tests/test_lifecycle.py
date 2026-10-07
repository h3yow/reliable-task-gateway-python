"""任务生命周期测试：超时终止、主动取消、取消与完成撞车、重启后语义保持。"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time

import pytest

from app.breaker import CircuitBreaker
from app.downstream import NonRetryableError, RetryableError, RetryPolicy
from app.models import Task, TaskStatus
from app.service import (CancelOutcome, SubmitOutcome, TaskGateway,
                         TaskNotFoundError)
from app.store import TaskStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    force=True,
)
logger = logging.getLogger("test.lifecycle")


class ScriptedClient:
    """按脚本行为的假下游客户端，线程安全地记录调用次数。"""

    def __init__(self, behavior) -> None:
        self._behavior = behavior
        self._lock = threading.Lock()
        self.calls = 0

    def call(self, task_id: str, payload: str) -> str:
        with self._lock:
            self.calls += 1
            n = self.calls
        return self._behavior(n, task_id, payload)


class BlockingClient:
    """进入下游调用后阻塞，直到测试放行（模拟在途调用）。"""

    def __init__(self) -> None:
        self.entered = threading.Event()
        self.release = threading.Event()
        self.calls = 0
        self._lock = threading.Lock()

    def call(self, task_id: str, payload: str) -> str:
        with self._lock:
            self.calls += 1
        self.entered.set()
        self.release.wait(timeout=5)
        return f"result:{payload}"


@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "gateway.db")


def make_gateway(db_path, client, *, max_attempts=3, threshold=3,
                 recovery_timeout=30.0, gw_clock=None):
    store = TaskStore(db_path)
    breaker = CircuitBreaker(failure_threshold=threshold,
                             recovery_timeout=recovery_timeout)
    retry = RetryPolicy(max_attempts=max_attempts, base_delay=0.001,
                        sleep=lambda s: None)
    gw = TaskGateway(store, client, breaker, retry,
                     clock=gw_clock or time.time)
    return gw, store


# ---------- 1. 超时终止 ----------

def test_timeout_after_slow_call_finalizes_without_retrigger(db_path):
    """下游调用在时限后才返回：按 TIMED_OUT 收尾，且不再次触发下游。"""
    clock = [1000.0]

    def slow(n, tid, p):
        clock[0] += 10.0  # 调用耗时 10s，超过 5s 时限
        return "late-result"

    client = ScriptedClient(slow)
    gw, store = make_gateway(db_path, client, gw_clock=lambda: clock[0])

    task, outcome = gw.submit("to-key-1", "p", timeout_seconds=5.0)
    assert outcome is SubmitOutcome.CREATED
    assert task.status is TaskStatus.TIMED_OUT, "超时必须收尾为 TIMED_OUT"
    assert task.error.startswith("TIMEOUT:"), task.error
    assert task.result is None, "超时收尾不得记入迟到结果"
    assert task.status not in (TaskStatus.FAILED, TaskStatus.CIRCUIT_REJECTED), \
        "超时要与失败/熔断拒绝区分"
    assert client.calls == 1

    # 超时收尾后重复提交：幂等命中，不得重新触发下游（不产生重复副作用）
    dup, dup_outcome = gw.submit("to-key-1", "p", timeout_seconds=5.0)
    assert dup_outcome is SubmitOutcome.DUPLICATE
    assert dup.status is TaskStatus.TIMED_OUT
    assert client.calls == 1, "超时收尾后下游不得被重新触发"
    logger.info("判定依据: key=to-key-1 status=%s error=%s result=%s "
                "downstream_calls=%d（超时终态与 FAILED/CIRCUIT_REJECTED 区分，"
                "重复提交未再触发下游）",
                dup.status.value, dup.error, dup.result, client.calls)
    store.close()


def test_timeout_during_retry_backoff_stops_retrying(db_path):
    """重试退避期间到点：停止重试并收尾为 TIMED_OUT，而非 RETRY_EXHAUSTED。"""
    clock = [1000.0]

    def flaky(n, tid, p):
        clock[0] += 10.0  # 每次调用耗时 10s
        raise RetryableError("downstream 5xx")

    client = ScriptedClient(flaky)
    gw, store = make_gateway(db_path, client, max_attempts=5,
                             gw_clock=lambda: clock[0])

    task, _ = gw.submit("to-key-2", "p", timeout_seconds=15.0)
    assert task.status is TaskStatus.TIMED_OUT
    assert task.error.startswith("TIMEOUT:"), task.error
    assert task.attempts < 5, "到点后不得继续重试到耗尽"
    assert client.calls == task.attempts
    logger.info("判定依据: key=to-key-2 status=%s attempts=%d/%d error=%s "
                "（退避期间到点，按超时收尾而非重试耗尽）",
                task.status.value, task.attempts, 5, task.error)
    store.close()


def test_submit_without_timeout_behaves_as_before(db_path):
    """不带时限的老提交方式：deadline_at 为 NULL，行为与旧版完全一致。"""
    client = ScriptedClient(lambda n, tid, p: f"result:{p}")
    gw, store = make_gateway(db_path, client)

    task, outcome = gw.submit("to-key-3", "p")
    assert outcome is SubmitOutcome.CREATED
    assert task.status is TaskStatus.SUCCEEDED
    assert task.result == "result:p"
    assert task.deadline_at is None, "不带时限的提交不得设置截止时间"
    assert task.cancel_requested is False
    logger.info("判定依据: key=to-key-3 status=%s result=%s deadline_at=%s "
                "（老提交方式行为不变）",
                task.status.value, task.result, task.deadline_at)
    store.close()


# ---------- 2. 主动取消 ----------

def test_cancel_inflight_task_idempotent_and_no_retrigger(db_path):
    """取消在途任务：取消幂等，在途调用返回后不得覆盖取消、不得重试。"""
    client = BlockingClient()
    gw, store = make_gateway(db_path, client)

    holder: dict = {}
    t = threading.Thread(
        target=lambda: holder.setdefault(
            "r", gw.submit("cx-key-1", "p", timeout_seconds=None)),
        daemon=True)
    t.start()
    assert client.entered.wait(timeout=5), "下游调用应已进入"

    # 第一次取消：生效
    task1, outcome1 = gw.cancel("cx-key-1")
    assert outcome1 is CancelOutcome.CANCELLED
    assert task1.status is TaskStatus.CANCELLED
    assert task1.error.startswith("CANCELLED:"), task1.error

    # 重复取消：不报错，结果一致
    task2, outcome2 = gw.cancel("cx-key-1")
    assert outcome2 is CancelOutcome.CANCELLED
    assert task2.task_id == task1.task_id
    assert task2.status is TaskStatus.CANCELLED

    # 放行在途调用：执行线程不得把 CANCELLED 覆盖为 SUCCEEDED，也不得重试
    client.release.set()
    t.join(timeout=5)
    final = gw.get("cx-key-1")
    assert final.status is TaskStatus.CANCELLED, "在途调用返回后不得覆盖取消终态"
    assert final.result is None, "取消后迟到结果不得落库"
    assert client.calls == 1, "取消后不得重试、不得再次触发下游"
    logger.info("判定依据: key=cx-key-1 两次取消 outcome=%s/%s 终态=%s "
                "result=%s downstream_calls=%d（取消幂等，在途调用未造成重复副作用）",
                outcome1.value, outcome2.value, final.status.value,
                final.result, client.calls)
    store.close()


def test_cancel_terminal_task_reports_actual_state(db_path):
    """任务已成功/已失败后再取消：如实返回现有终态，不覆盖、不假装成功。"""
    client = ScriptedClient(lambda n, tid, p: f"result:{p}")
    gw, store = make_gateway(db_path, client)

    ok, _ = gw.submit("cx-key-2", "p")
    assert ok.status is TaskStatus.SUCCEEDED
    task, outcome = gw.cancel("cx-key-2")
    assert outcome is CancelOutcome.ALREADY_TERMINAL
    assert task.status is TaskStatus.SUCCEEDED, "不得把已有成功结果盖掉"
    assert task.result == "result:p", "既有结果不得丢失"
    assert task.cancel_requested is False, "终态任务不得被置取消标记"
    ok_outcome, ok_status = outcome.value, task.status.value

    def bad(n, tid, p):
        raise NonRetryableError("invalid payload")

    client2 = ScriptedClient(bad)
    gw2, store2 = make_gateway(db_path, client2)
    failed, _ = gw2.submit("cx-key-3", "p")
    assert failed.status is TaskStatus.FAILED
    task, outcome = gw2.cancel("cx-key-3")
    assert outcome is CancelOutcome.ALREADY_TERMINAL
    assert task.status is TaskStatus.FAILED
    assert task.error.startswith("NON_RETRYABLE:"), task.error

    with pytest.raises(TaskNotFoundError):
        gw2.cancel("cx-key-absent")
    logger.info("判定依据: 已成功任务取消 -> %s/%s result 保留=%s; "
                "已失败任务取消 -> %s/%s error 保留; 不存在的键 -> TaskNotFoundError",
                ok_outcome, ok_status, "result:p",
                CancelOutcome.ALREADY_TERMINAL.value, TaskStatus.FAILED.value)
    store.close()
    store2.close()


def test_cancel_vs_completion_race_single_terminal_winner(db_path):
    """取消与正常完成撞车：终态二选一、唯一确定，无结果丢失/状态回退/双终态。"""
    winners: dict[str, int] = {"CANCELLED": 0, "SUCCEEDED": 0}
    for i in range(30):
        about_to_return = threading.Event()

        def racing(n, tid, p, ev=about_to_return):
            ev.set()
            time.sleep(0.001)  # 给取消线程一个撞车窗口
            return "ok"

        client = ScriptedClient(racing)
        gw, store = make_gateway(db_path, client)
        key = f"race-key-{i}"
        done = threading.Event()
        t = threading.Thread(target=lambda: (gw.submit(key, "p"), done.set()),
                             daemon=True)
        t.start()
        assert about_to_return.wait(timeout=5)
        cancel_task, cancel_outcome = gw.cancel(key)
        done.wait(timeout=5)
        t.join(timeout=5)

        final = gw.get(key)
        assert final.status in (TaskStatus.CANCELLED, TaskStatus.SUCCEEDED), \
            f"撞车后必须是唯一终态之一，实际={final.status.value}"
        if final.status is TaskStatus.CANCELLED:
            winners["CANCELLED"] += 1
            assert final.result is None, "取消胜出时不得落下迟到结果"
            assert final.error.startswith("CANCELLED:")
        else:
            winners["SUCCEEDED"] += 1
            assert final.result == "ok", "完成胜出时结果不得丢失"
            assert cancel_outcome is CancelOutcome.ALREADY_TERMINAL
        assert client.calls == 1, "撞车后不得重新触发下游"
        # 撞车后再取消/再读，状态不得回退
        again, _ = gw.cancel(key)
        assert again.status is final.status
        store.close()
    logger.info("判定依据: 30 次撞车实验终态分布=%s；每次均为唯一终态、"
                "下游仅调用 1 次、撞车后状态不回退", winners)


# ---------- 3. 重启后语义保持 ----------

def _insert_running_task(store: TaskStore, key: str, **kwargs) -> Task:
    """直接落库一个 RUNNING 任务（模拟进程在执行中途崩溃后的残留状态）。"""
    import hashlib
    task = Task(task_id=f"tid-{key}", idempotency_key=key,
                payload_hash=hashlib.sha256(b"p").hexdigest(),
                payload="p", status=TaskStatus.RUNNING, attempts=1, **kwargs)
    stored, created = store.insert_if_absent(task)
    assert created
    return stored


def test_restart_deadline_keeps_original_time(db_path):
    """重启前定下的时限：重启后按原截止时间继续计算超时。"""
    clock = [1000.0]
    client = ScriptedClient(lambda n, tid, p: "ok")
    gw, store = make_gateway(db_path, client, gw_clock=lambda: clock[0])
    _insert_running_task(store, "rs-key-1", deadline_at=1010.0)
    store.close()  # 模拟进程崩溃/重启

    # 重启：未到截止时间，清扫不动它
    clock[0] = 1005.0
    gw2, store2 = make_gateway(db_path, client, gw_clock=lambda: clock[0])
    assert gw2.sweep() == []
    assert gw2.get("rs-key-1").status is TaskStatus.RUNNING

    # 时间推进到原截止时间之后：按原 deadline 收尾为 TIMED_OUT
    clock[0] = 1015.0
    finalized = gw2.sweep()
    assert [t.idempotency_key for t in finalized] == ["rs-key-1"]
    timed_out = gw2.get("rs-key-1")
    assert timed_out.status is TaskStatus.TIMED_OUT
    assert timed_out.error.startswith("TIMEOUT:")
    assert client.calls == 0, "超时收尾不得触发下游调用"

    # 再次重启：已收尾任务不得被改回
    store2.close()
    gw3, store3 = make_gateway(db_path, client, gw_clock=lambda: clock[0])
    assert gw3.sweep() == []
    assert gw3.get("rs-key-1").status is TaskStatus.TIMED_OUT
    # 重启后重复提交：幂等命中，不触发下游
    dup, outcome = gw3.submit("rs-key-1", "p", timeout_seconds=10.0)
    assert outcome is SubmitOutcome.DUPLICATE
    assert dup.status is TaskStatus.TIMED_OUT
    assert client.calls == 0
    logger.info("判定依据: key=rs-key-1 重启后 clock=1005 未超时不动；"
                "clock=1015 超过原 deadline=1010 -> %s；再次重启后仍为 %s，"
                "重复提交 outcome=%s downstream_calls=%d",
                timed_out.status.value, gw3.get("rs-key-1").status.value,
                outcome.value, client.calls)
    store3.close()


def test_restart_cancel_request_stays_effective(db_path):
    """重启前发起的取消：取消意图已持久化，重启后清扫继续完成取消。"""
    client = ScriptedClient(lambda n, tid, p: "ok")
    gw, store = make_gateway(db_path, client)
    task = _insert_running_task(store, "rs-key-2")
    # 取消意图落库，但进程在做状态迁移前崩溃
    store.request_cancel(task.task_id)
    store.close()

    gw2, store2 = make_gateway(db_path, client)
    finalized = gw2.sweep()
    assert [t.idempotency_key for t in finalized] == ["rs-key-2"]
    cancelled = gw2.get("rs-key-2")
    assert cancelled.status is TaskStatus.CANCELLED
    assert cancelled.cancel_requested is True
    assert client.calls == 0

    # 再次重启：取消终态保持，不可改回；再取消仍幂等
    store2.close()
    gw3, store3 = make_gateway(db_path, client)
    assert gw3.sweep() == []
    again, outcome = gw3.cancel("rs-key-2")
    assert outcome is CancelOutcome.CANCELLED
    assert again.status is TaskStatus.CANCELLED
    logger.info("判定依据: key=rs-key-2 重启后清扫落实取消 -> %s "
                "(cancel_requested=%s)；再次重启后仍为 %s，重复取消 outcome=%s",
                cancelled.status.value, cancelled.cancel_requested,
                gw3.get("rs-key-2").status.value, outcome.value)
    store3.close()


def test_restart_terminal_states_not_reverted(db_path):
    """超时/取消收尾的任务重启后不得被改回其他状态。"""
    clock = [1000.0]
    client = ScriptedClient(lambda n, tid, p: "ok")
    gw, store = make_gateway(db_path, client, gw_clock=lambda: clock[0])
    _insert_running_task(store, "rs-key-3", deadline_at=900.0)  # 已过期
    _insert_running_task(store, "rs-key-4", cancel_requested=True)
    gw.sweep()
    assert gw.get("rs-key-3").status is TaskStatus.TIMED_OUT
    assert gw.get("rs-key-4").status is TaskStatus.CANCELLED
    store.close()

    clock[0] = 9999.0
    gw2, store2 = make_gateway(db_path, client, gw_clock=lambda: clock[0])
    assert gw2.sweep() == [], "终态任务不得被清扫改动"
    assert gw2.get("rs-key-3").status is TaskStatus.TIMED_OUT
    assert gw2.get("rs-key-4").status is TaskStatus.CANCELLED
    from app.models import IllegalTransitionError
    with pytest.raises(IllegalTransitionError):
        store2.transition("tid-rs-key-3", TaskStatus.TIMED_OUT, TaskStatus.RUNNING)
    with pytest.raises(IllegalTransitionError):
        store2.transition("tid-rs-key-4", TaskStatus.CANCELLED, TaskStatus.RUNNING)
    logger.info("判定依据: 重启后 rs-key-3=%s rs-key-4=%s 保持不变，"
                "TIMED_OUT/CANCELLED -> RUNNING 迁移被拒绝",
                gw2.get("rs-key-3").status.value, gw2.get("rs-key-4").status.value)
    store2.close()


# ---------- 4. 升级兼容：老库表迁移 ----------

def test_old_schema_db_migrated_and_unlimited(db_path):
    """升级前已存下的任务：自动补列，老任务无时限、无取消标记，行为不变。"""
    # 手工建一个旧版（无 deadline_at / cancel_requested 列）的库
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE tasks ("
        " task_id TEXT PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE,"
        " payload_hash TEXT NOT NULL, payload TEXT NOT NULL, status TEXT NOT NULL,"
        " attempts INTEGER NOT NULL DEFAULT 0, result TEXT, error TEXT,"
        " version INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL,"
        " updated_at REAL NOT NULL)")
    conn.execute(
        "INSERT INTO tasks VALUES ('old-tid-1','old-key-1','h','p','SUCCEEDED',"
        " 1,'old-result',NULL,3,100.0,100.0)")
    conn.execute(
        "INSERT INTO tasks VALUES ('old-tid-2','old-key-2','h','p','RUNNING',"
        " 1,NULL,NULL,4,100.0,100.0)")
    conn.commit()
    conn.close()

    # 打开即迁移：补列后老数据可读，deadline_at 为 NULL（不限时）
    client = ScriptedClient(lambda n, tid, p: "ok")
    gw, store = make_gateway(db_path, client)
    done = gw.get("old-key-1")
    assert done.status is TaskStatus.SUCCEEDED
    assert done.result == "old-result", "老任务结果不得丢失"
    assert done.deadline_at is None
    assert done.cancel_requested is False

    running = gw.get("old-key-2")
    assert running.status is TaskStatus.RUNNING
    assert running.deadline_at is None
    assert gw.sweep() == [], "无时限的老任务不得被超时清扫"

    # 老任务可正常取消（新能力对老数据可用）
    task, outcome = gw.cancel("old-key-2")
    assert outcome is CancelOutcome.CANCELLED
    assert task.status is TaskStatus.CANCELLED
    logger.info("判定依据: 老库迁移后 old-key-1=%s result=%s deadline_at=%s; "
                "old-key-2(RUNNING,无时限) 不被超时清扫，可取消 -> %s",
                done.status.value, done.result, done.deadline_at,
                task.status.value)
    store.close()
