"""任务生命周期测试：超时终止、幂等取消、取消与完成撞车、重启后语义保持。"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time

import pytest

from app.breaker import CircuitBreaker
from app.downstream import NonRetryableError, RetryableError, RetryPolicy
from app.models import IllegalTransitionError, Task, TaskStatus
from app.service import CancelOutcome, SubmitOutcome, TaskGateway
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


@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "gateway.db")


def make_gateway(db_path, client, *, max_attempts=3, threshold=3, clock=None,
                 retry_sleep=lambda s: None):
    store = TaskStore(db_path)
    breaker = CircuitBreaker(failure_threshold=threshold, recovery_timeout=30.0)
    retry = RetryPolicy(max_attempts=max_attempts, base_delay=0.001, sleep=retry_sleep)
    return TaskGateway(store, client, breaker, retry,
                       clock=clock or time.time), store


# ---------- 1. 超时终止 ----------

def test_timeout_finalizes_to_distinct_terminal_state(db_path):
    """场景：下游迟迟不返回，清扫器到点把任务收尾为 TIMED_OUT，
    且与 FAILED / CIRCUIT_REJECTED 可明确区分。"""
    now = [1000.0]
    release = threading.Event()
    client = ScriptedClient(lambda n, tid, p: (release.wait(5), "late-result")[1])
    gw, store = make_gateway(db_path, client, clock=lambda: now[0])

    # 后台提交（下游阻塞中），时限 10 秒
    holder = {}
    t = threading.Thread(
        target=lambda: holder.update(task=gw.submit("to-key-1", "p",
                                                    timeout_seconds=10.0)[0]))
    t.start()
    while gw.get("to-key-1") is None or gw.get("to-key-1").status is not TaskStatus.RUNNING:
        time.sleep(0.005)

    # 到点前清扫：不应收尾
    now[0] = 1005.0
    assert gw.sweep_timeouts() == []
    assert gw.get("to-key-1").status is TaskStatus.RUNNING

    # 到点后清扫：收尾为 TIMED_OUT，原因前缀 TIMEOUT: 可区分
    now[0] = 1001.0 + 10.0  # deadline = 1000 + 10 = 1010
    finalized = gw.sweep_timeouts()
    assert len(finalized) == 1
    assert finalized[0].status is TaskStatus.TIMED_OUT
    assert finalized[0].error.startswith("TIMEOUT:"), finalized[0].error
    assert not finalized[0].error.startswith(("RETRY_EXHAUSTED", "CIRCUIT_OPEN",
                                              "NON_RETRYABLE"))
    logger.info("判定依据: key=to-key-1 超时收尾 status=%s error=%s "
                "（TIMEOUT: 前缀与 RETRY_EXHAUSTED/CIRCUIT_OPEN/NON_RETRYABLE 可区分）",
                finalized[0].status.value, finalized[0].error)

    # 在途下游调用随后返回：终态不得被覆盖，且不得再次触发下游
    release.set()
    t.join(timeout=5)
    task = holder["task"]
    assert task.status is TaskStatus.TIMED_OUT, "超时收尾后提交方看到的仍是 TIMED_OUT"
    assert task.result is None, "迟到的下游结果不得覆盖超时终态"
    assert client.calls == 1, "已发出的下游调用不得因超时被重新触发"
    logger.info("判定依据: 在途调用返回后 status=%s result=%r downstream_calls=%d "
                "（终态唯一、无重复副作用）", task.status.value, task.result, client.calls)
    store.close()


def test_timeout_during_retry_backoff_stops_further_attempts(db_path):
    """场景：重试退避期间到达时限，不再派发新的下游尝试，直接收尾 TIMED_OUT。"""
    now = [2000.0]
    client = ScriptedClient(lambda n, tid, p: (_ for _ in ()).throw(
        RetryableError("downstream 5xx")))

    def advance_clock(seconds):
        now[0] += 1.0  # 退避等待期间墙钟前进 1s，越过 0.002s 的时限

    gw, store = make_gateway(db_path, client, max_attempts=5,
                             clock=lambda: now[0], retry_sleep=advance_clock)
    task, _ = gw.submit("to-key-2", "p", timeout_seconds=0.002)
    assert task.status is TaskStatus.TIMED_OUT
    assert task.error.startswith("TIMEOUT:")
    assert client.calls == 1, "到点后不得再派发新的下游尝试"
    assert task.attempts == 1, "未派发的尝试不得计入 attempts"
    logger.info("判定依据: key=to-key-2 退避期间到点 status=%s attempts=%d "
                "downstream_calls=%d（未发生第 2 次尝试）",
                task.status.value, task.attempts, client.calls)
    store.close()


def test_submit_without_timeout_unchanged(db_path):
    """场景：不带时限的老提交方式行为完全不变：无 deadline、正常成功、
    清扫器永不触碰。"""
    now = [3000.0]
    client = ScriptedClient(lambda n, tid, p: f"result:{p}")
    gw, store = make_gateway(db_path, client, clock=lambda: now[0])
    task, outcome = gw.submit("to-key-3", "payload")
    assert outcome is SubmitOutcome.CREATED
    assert task.status is TaskStatus.SUCCEEDED
    assert task.result == "result:payload"
    assert task.deadline_at is None, "无时限任务不得有截止时间"
    now[0] += 10_000  # 时间大幅前进，清扫器不得影响无时限任务
    assert gw.sweep_timeouts() == []
    assert gw.get("to-key-3").status is TaskStatus.SUCCEEDED
    logger.info("判定依据: key=to-key-3 deadline_at=None status=%s result=%s "
                "时钟前进 10000s 后清扫结果为空（老提交方式行为不变）",
                task.status.value, task.result)
    store.close()


# ---------- 2. 幂等取消 ----------

def test_cancel_is_idempotent_and_blocks_late_result(db_path):
    """场景：取消 RUNNING 任务 -> CANCELLED；重复取消结果一致；
    在途下游调用返回后终态不被覆盖、不重新触发。"""
    release = threading.Event()
    client = ScriptedClient(lambda n, tid, p: (release.wait(5), "late")[1])
    gw, store = make_gateway(db_path, client)

    done = threading.Event()
    t = threading.Thread(target=lambda: (gw.submit("cx-key-1", "p"), done.set()))
    t.start()
    while gw.get("cx-key-1") is None or gw.get("cx-key-1").status is not TaskStatus.RUNNING:
        time.sleep(0.005)

    task1, outcome1 = gw.cancel("cx-key-1")
    assert outcome1 is CancelOutcome.CANCELLED
    assert task1.status is TaskStatus.CANCELLED
    assert task1.error.startswith("CANCELLED:")

    # 重复取消：幂等，结果一致，不报错
    task2, outcome2 = gw.cancel("cx-key-1")
    task3, outcome3 = gw.cancel("cx-key-1")
    assert outcome2 is CancelOutcome.CANCELLED and outcome3 is CancelOutcome.CANCELLED
    assert task2.status is TaskStatus.CANCELLED and task3.status is TaskStatus.CANCELLED
    assert task2.task_id == task1.task_id and task2.version == task1.version, \
        "重复取消不得产生新的状态变更"

    # 在途调用返回：终态保持 CANCELLED，下游只被调用一次
    release.set()
    done.wait(timeout=5)
    t.join(timeout=5)
    final = gw.get("cx-key-1")
    assert final.status is TaskStatus.CANCELLED
    assert final.result is None, "迟到的结果不得覆盖取消终态"
    assert client.calls == 1
    logger.info("判定依据: key=cx-key-1 三次取消 outcome 均为 %s、version 一致(%d)；"
                "在途调用返回后 status=%s result=%r downstream_calls=%d",
                outcome1.value, task1.version, final.status.value, final.result,
                client.calls)
    store.close()


def test_cancel_terminal_task_reports_actual_state(db_path):
    """场景：任务已成功/已失败后再取消：明确告知真实终态，不覆盖已有结果。"""
    client = ScriptedClient(lambda n, tid, p: f"ok:{p}")
    gw, store = make_gateway(db_path, client)
    task, _ = gw.submit("cx-key-2", "p")
    assert task.status is TaskStatus.SUCCEEDED

    cancelled, outcome = gw.cancel("cx-key-2")
    assert outcome is CancelOutcome.ALREADY_FINISHED
    assert cancelled.status is TaskStatus.SUCCEEDED, "取消不得把成功改为取消"
    assert cancelled.result == "ok:p", "取消不得覆盖已有结果"

    # 失败任务同理
    def bad(n, tid, p):
        raise NonRetryableError("invalid")

    client2 = ScriptedClient(bad)
    gw2, store2 = make_gateway(db_path, client2)
    failed, _ = gw2.submit("cx-key-3", "p")
    assert failed.status is TaskStatus.FAILED
    cancelled2, outcome2 = gw2.cancel("cx-key-3")
    assert outcome2 is CancelOutcome.ALREADY_FINISHED
    assert cancelled2.status is TaskStatus.FAILED
    assert cancelled2.error.startswith("NON_RETRYABLE:"), "失败原因不得被抹掉"
    logger.info("判定依据: 已成功任务取消 -> outcome=%s status=%s result=%r；"
                "已失败任务取消 -> outcome=%s status=%s error=%s（真实终态如实告知）",
                outcome.value, cancelled.status.value, cancelled.result,
                outcome2.value, cancelled2.status.value, cancelled2.error)
    store.close()
    store2.close()


def test_cancel_unknown_key_returns_none(db_path):
    client = ScriptedClient(lambda n, tid, p: "ok")
    gw, store = make_gateway(db_path, client)
    assert gw.cancel("no-such-key") is None
    logger.info("判定依据: 取消不存在的键返回 None（路由层映射 404）")
    store.close()


# ---------- 3. 取消与完成撞车 ----------

def test_cancel_races_with_completion_exactly_one_terminal_state(db_path):
    """场景：取消与正常完成在时间上撞车，最终状态二选一、唯一确定，
    不出现结果丢失、状态回退或两个终态都算数。"""
    rounds = 50
    for i in range(rounds):
        key = f"race-key-{i}"
        client = ScriptedClient(lambda n, tid, p: f"done:{p}")
        gw, store = make_gateway(db_path, client)
        barrier = threading.Barrier(2)
        outcome_holder = {}

        def do_submit():
            barrier.wait()
            gw.submit(key, "p")

        def do_cancel():
            barrier.wait()
            # 取消可能先于提交落库到达（返回 None），自旋到任务存在为止
            r = gw.cancel(key)
            while r is None:
                r = gw.cancel(key)
            outcome_holder["result"] = r

        ts = threading.Thread(target=do_submit)
        tc = threading.Thread(target=do_cancel)
        ts.start()
        tc.start()
        ts.join()
        tc.join()

        final = gw.get(key)
        # 终态唯一：要么成功要么取消，且结果与状态自洽
        assert final.status in (TaskStatus.SUCCEEDED, TaskStatus.CANCELLED), \
            f"第 {i} 轮出现意外状态 {final.status}"
        if final.status is TaskStatus.SUCCEEDED:
            assert final.result == "done:p", "成功终态必须携带结果"
            _, outcome = outcome_holder["result"]
            assert outcome is CancelOutcome.ALREADY_FINISHED
        else:
            assert final.result is None, "取消终态不得携带迟到的结果"
        # 撞车后状态不可再变：重复取消/重复提交都不得改变终态
        again, _ = gw.cancel(key)
        assert again.status == final.status
        dup, dup_outcome = gw.submit(key, "p")
        assert dup_outcome is SubmitOutcome.DUPLICATE
        assert dup.status == final.status
        assert client.calls <= 1, "下游不得被重复触发"
        store.close()
    logger.info("判定依据: %d 轮取消/完成撞车，每轮终态唯一且自洽"
                "（SUCCEEDED 必带结果、CANCELLED 必无结果），"
                "撞车后重复取消/重复提交均不改变终态，下游调用<=1 次", rounds)


# ---------- 4. 重启后语义保持 ----------

def test_restart_continues_original_deadline(db_path):
    """场景：重启前定下的时限，重启后按原绝对时间继续计算超时。"""
    now = [5000.0]
    client = ScriptedClient(lambda n, tid, p: "ok")
    gw, store = make_gateway(db_path, client, clock=lambda: now[0])

    # 直接落库一个 RUNNING 任务（模拟重启前执行到一半进程退出），时限 60s
    task = Task(task_id="t-restart-1", idempotency_key="rs-key-1",
                payload_hash="h", payload="p", status=TaskStatus.RUNNING,
                attempts=1, deadline_at=now[0] + 60.0)
    store.insert_if_absent(task)
    store.close()

    # 模拟进程重启：重开同一数据库，注入同一时钟
    gw2, store2 = make_gateway(db_path, client, clock=lambda: now[0])
    restored = gw2.get("rs-key-1")
    assert restored.deadline_at == 5060.0, "重启后截止时间必须按原值保留"

    now[0] = 5030.0  # 重启后未到点：不收尾
    assert gw2.sweep_timeouts() == []
    now[0] = 5060.0  # 按原时间到点：收尾
    finalized = gw2.sweep_timeouts()
    assert len(finalized) == 1 and finalized[0].status is TaskStatus.TIMED_OUT
    assert client.calls == 0, "重启恢复的超时收尾不得触发任何下游调用"
    logger.info("判定依据: key=rs-key-1 重启后 deadline_at=%s 保留；"
                "时钟到 5030 不收尾、到 5060 收尾为 %s；downstream_calls=%d",
                restored.deadline_at, finalized[0].status.value, client.calls)
    store2.close()


def test_restart_preserves_cancellation(db_path):
    """场景：重启前发起的取消，重启后继续生效；取消终态不可被改回。"""
    release = threading.Event()
    client = ScriptedClient(lambda n, tid, p: (release.wait(5), "late")[1])
    gw, store = make_gateway(db_path, client)

    done = threading.Event()
    t = threading.Thread(target=lambda: (gw.submit("rs-key-2", "p"), done.set()))
    t.start()
    while gw.get("rs-key-2") is None or gw.get("rs-key-2").status is not TaskStatus.RUNNING:
        time.sleep(0.005)
    cancelled, outcome = gw.cancel("rs-key-2")
    assert outcome is CancelOutcome.CANCELLED
    release.set()
    done.wait(timeout=5)
    t.join(timeout=5)
    store.close()

    # 重启：取消终态保持，重复取消仍幂等，重复提交不再触发下游
    gw2, store2 = make_gateway(db_path, client)
    restored = gw2.get("rs-key-2")
    assert restored.status is TaskStatus.CANCELLED, "重启后取消终态不得回退"
    again, outcome2 = gw2.cancel("rs-key-2")
    assert outcome2 is CancelOutcome.CANCELLED and again.status is TaskStatus.CANCELLED
    calls_before = client.calls
    dup, dup_outcome = gw2.submit("rs-key-2", "p")
    assert dup_outcome is SubmitOutcome.DUPLICATE
    assert dup.status is TaskStatus.CANCELLED
    assert client.calls == calls_before, "重启后重复提交不得再次调用下游"
    with pytest.raises(IllegalTransitionError):
        store2.transition(restored.task_id, TaskStatus.CANCELLED, TaskStatus.RUNNING)
    logger.info("判定依据: key=rs-key-2 重启后 status=%s 保持；重复取消 outcome=%s；"
                "重复提交 outcome=%s 且 downstream_calls 未增加(%d)；"
                "CANCELLED->RUNNING 迁移被拒绝",
                restored.status.value, outcome2.value, dup_outcome.value, client.calls)
    store2.close()


def test_restart_preserves_timeout_terminal(db_path):
    """场景：超时收尾的任务重启后不得被改回其他状态。"""
    now = [7000.0]
    client = ScriptedClient(lambda n, tid, p: "ok")
    gw, store = make_gateway(db_path, client, clock=lambda: now[0])
    task = Task(task_id="t-restart-3", idempotency_key="rs-key-3",
                payload_hash="h", payload="p", status=TaskStatus.RUNNING,
                attempts=1, deadline_at=now[0])
    store.insert_if_absent(task)
    finalized = gw.sweep_timeouts()
    assert finalized[0].status is TaskStatus.TIMED_OUT
    store.close()

    gw2, store2 = make_gateway(db_path, client, clock=lambda: now[0])
    restored = gw2.get("rs-key-3")
    assert restored.status is TaskStatus.TIMED_OUT
    with pytest.raises(IllegalTransitionError):
        store2.transition(restored.task_id, TaskStatus.TIMED_OUT, TaskStatus.RUNNING)
    cancelled, outcome = gw2.cancel("rs-key-3")
    assert outcome is CancelOutcome.ALREADY_FINISHED
    assert cancelled.status is TaskStatus.TIMED_OUT, "取消不得覆盖超时终态"
    logger.info("判定依据: key=rs-key-3 重启后 TIMED_OUT 保持；"
                "TIMED_OUT->RUNNING 被拒绝；取消返回 outcome=%s 且不覆盖终态",
                outcome.value)
    store2.close()


# ---------- 5. 升级兼容 ----------

def test_old_schema_database_migrates_and_behaves_as_before(db_path):
    """场景：升级前建的老库（无 deadline_at 列）打开后自动迁移，
    老任务无时限、行为与升级前一致。"""
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE tasks (task_id TEXT PRIMARY KEY,"
        " idempotency_key TEXT NOT NULL UNIQUE, payload_hash TEXT NOT NULL,"
        " payload TEXT NOT NULL, status TEXT NOT NULL,"
        " attempts INTEGER NOT NULL DEFAULT 0, result TEXT, error TEXT,"
        " version INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL,"
        " updated_at REAL NOT NULL)")
    conn.execute(
        "INSERT INTO tasks VALUES ('old-t1','old-key-1','h','p','SUCCEEDED',"
        "1,'old-result',NULL,3,1.0,1.0)")
    conn.commit()
    conn.close()

    client = ScriptedClient(lambda n, tid, p: "new")
    gw, store = make_gateway(db_path, client)
    old = gw.get("old-key-1")
    assert old is not None and old.status is TaskStatus.SUCCEEDED
    assert old.result == "old-result", "老任务结果不得丢失"
    assert old.deadline_at is None, "老任务迁移后无时限"
    assert gw.sweep_timeouts() == [], "老任务不参与超时判定"
    # 老库上新的带时限提交正常工作
    task, _ = gw.submit("old-key-2", "p", timeout_seconds=60.0)
    assert task.status is TaskStatus.SUCCEEDED
    assert task.deadline_at is not None
    logger.info("判定依据: 老库迁移后 old-key-1 status=%s result=%s deadline_at=%s；"
                "新提交 old-key-2 deadline_at=%s（兼容范围验证通过）",
                old.status.value, old.result, old.deadline_at, task.deadline_at)
    store.close()
