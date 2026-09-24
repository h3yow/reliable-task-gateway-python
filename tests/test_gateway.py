"""任务网关可靠性测试：并发重复提交、冲突载荷、重启恢复、熔断转换。"""
from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.breaker import BreakerState, CircuitBreaker
from app.downstream import NonRetryableError, RetryableError, RetryPolicy
from app.models import TaskStatus
from app.service import SubmitOutcome, TaskGateway
from app.store import PayloadConflictError, TaskStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    force=True,
)
logger = logging.getLogger("test.gateway")


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


def make_gateway(db_path, client, *, max_attempts=3, threshold=3,
                 recovery_timeout=30.0, clock=None):
    store = TaskStore(db_path)
    breaker = CircuitBreaker(failure_threshold=threshold,
                             recovery_timeout=recovery_timeout,
                             clock=clock or (lambda: __import__("time").monotonic()))
    retry = RetryPolicy(max_attempts=max_attempts, base_delay=0.001,
                        sleep=lambda s: None)
    return TaskGateway(store, client, breaker, retry), store


# ---------- 1. 并发重复提交 ----------

def test_concurrent_duplicate_submit_single_execution(db_path):
    client = ScriptedClient(lambda n, tid, p: f"result:{p}")
    gw, store = make_gateway(db_path, client)

    barrier = threading.Barrier(16)

    def submit(_):
        barrier.wait()
        return gw.submit("biz-key-1", "payload-1")

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(submit, range(16)))

    task_ids = {t.task_id for t, _ in results}
    outcomes = [o for _, o in results]
    assert len(task_ids) == 1, "并发重复提交必须只产生一个任务"
    assert outcomes.count(SubmitOutcome.CREATED) == 1
    assert outcomes.count(SubmitOutcome.DUPLICATE) == 15
    assert client.calls == 1, "下游必须只被调用一次"
    final = {t.result for t, _ in results}
    assert final == {"result:payload-1"}, "所有提交方必须拿到与首次一致的结果"
    logger.info("判定依据: task_ids=%s outcomes CREATED=%d DUPLICATE=%d "
                "downstream_calls=%d results=%s",
                task_ids, outcomes.count(SubmitOutcome.CREATED),
                outcomes.count(SubmitOutcome.DUPLICATE), client.calls, final)
    store.close()


# ---------- 2. 冲突载荷 ----------

def test_conflicting_payload_rejected_and_distinguishable(db_path):
    client = ScriptedClient(lambda n, tid, p: f"result:{p}")
    gw, store = make_gateway(db_path, client)

    task, outcome = gw.submit("biz-key-2", "payload-A")
    assert outcome is SubmitOutcome.CREATED

    dup, dup_outcome = gw.submit("biz-key-2", "payload-A")
    assert dup_outcome is SubmitOutcome.DUPLICATE
    assert dup.task_id == task.task_id and dup.result == task.result

    with pytest.raises(PayloadConflictError) as exc_info:
        gw.submit("biz-key-2", "payload-B")
    logger.info("判定依据: key=biz-key-2 重复提交 outcome=%s result=%s; "
                "冲突载荷拒绝原因=%s（与重复提交可区分）",
                dup_outcome.value, dup.result, exc_info.value)
    store.close()


# ---------- 3. 重启恢复 ----------

def test_restart_preserves_state_and_result(db_path):
    client = ScriptedClient(lambda n, tid, p: f"result:{p}")
    gw, store = make_gateway(db_path, client)
    task, _ = gw.submit("biz-key-3", "payload-C")
    assert task.status is TaskStatus.SUCCEEDED
    store.close()

    # 模拟进程重启：重新打开同一数据库文件
    store2 = TaskStore(db_path)
    gw2, _ = make_gateway(db_path, client)
    restored = gw2.get("biz-key-3")
    assert restored is not None
    assert restored.task_id == task.task_id
    assert restored.status is TaskStatus.SUCCEEDED, "重启后状态不得回退"
    assert restored.result == "result:payload-C", "重启后结果不得丢失"

    # 重启后重复提交仍返回首次结果，且不再次调用下游
    calls_before = client.calls
    dup, outcome = gw2.submit("biz-key-3", "payload-C")
    assert outcome is SubmitOutcome.DUPLICATE
    assert dup.result == task.result
    assert client.calls == calls_before
    logger.info("判定依据: key=biz-key-3 重启后 status=%s result=%s "
                "downstream_calls 未增加(%d)", restored.status.value,
                restored.result, client.calls)
    store2.close()


def test_terminal_state_immutable_across_restart(db_path):
    client = ScriptedClient(lambda n, tid, p: "ok")
    gw, store = make_gateway(db_path, client)
    task, _ = gw.submit("biz-key-4", "payload-D")
    store.close()

    store2 = TaskStore(db_path)
    from app.models import IllegalTransitionError
    with pytest.raises(IllegalTransitionError):
        store2.transition(task.task_id, TaskStatus.SUCCEEDED, TaskStatus.RUNNING)
    logger.info("判定依据: key=biz-key-4 终态 %s 不可变更（重启后仍生效）",
                TaskStatus.SUCCEEDED.value)
    store2.close()


# ---------- 4. 熔断状态转换 ----------

def test_circuit_breaker_opens_and_rejects_with_distinct_reason(db_path):
    def always_fail(n, tid, p):
        raise RetryableError("downstream 5xx")

    client = ScriptedClient(always_fail)
    gw, store = make_gateway(db_path, client, max_attempts=1, threshold=3)

    # 连续 3 个任务失败 -> 熔断器打开
    for i in range(3):
        t, _ = gw.submit(f"cb-key-{i}", "p")
        assert t.status is TaskStatus.FAILED
        assert t.error.startswith("RETRY_EXHAUSTED"), t.error
    assert gw._breaker.state is BreakerState.OPEN

    # 第 4 个任务：熔断拒绝，原因可区分
    t, _ = gw.submit("cb-key-3", "p")
    assert t.status is TaskStatus.CIRCUIT_REJECTED
    assert t.error.startswith("CIRCUIT_OPEN"), t.error
    assert client.calls == 3, "熔断后不得再调用下游"
    logger.info("判定依据: 连续失败 3 次后熔断器状态=%s; key=cb-key-3 "
                "拒绝原因=%s（与 RETRY_EXHAUSTED 可区分）; downstream_calls=%d",
                gw._breaker.state.value, t.error, client.calls)
    store.close()


def test_circuit_breaker_half_open_probe_recovers(db_path):
    now = [0.0]
    state = {"failing": True}

    def flaky(n, tid, p):
        if state["failing"]:
            raise RetryableError("downstream 5xx")
        return "recovered"

    client = ScriptedClient(flaky)
    gw, store = make_gateway(db_path, client, max_attempts=1, threshold=2,
                             recovery_timeout=10.0, clock=lambda: now[0])

    for i in range(2):
        gw.submit(f"probe-key-{i}", "p")
    assert gw._breaker.state is BreakerState.OPEN

    # 未过恢复超时：直接拒绝，不放大流量
    t, _ = gw.submit("probe-key-2", "p")
    assert t.status is TaskStatus.CIRCUIT_REJECTED
    calls_before = client.calls

    # 过了恢复超时且下游恢复：单个探测成功 -> 熔断器关闭
    now[0] = 11.0
    state["failing"] = False
    t, _ = gw.submit("probe-key-3", "p")
    assert t.status is TaskStatus.SUCCEEDED and t.result == "recovered"
    assert gw._breaker.state is BreakerState.CLOSED
    assert client.calls == calls_before + 1, "恢复期只允许一个探测请求"
    logger.info("判定依据: 恢复探测后熔断器状态=%s; key=probe-key-3 status=%s; "
                "探测期间下游调用仅增加 %d 次（未放大流量）",
                gw._breaker.state.value, t.status.value,
                client.calls - calls_before)
    store.close()


def test_non_retryable_failure_distinct_reason(db_path):
    def bad_request(n, tid, p):
        raise NonRetryableError("invalid payload")

    client = ScriptedClient(bad_request)
    gw, store = make_gateway(db_path, client, max_attempts=3)
    t, _ = gw.submit("nr-key-1", "p")
    assert t.status is TaskStatus.FAILED
    assert t.error.startswith("NON_RETRYABLE"), t.error
    assert t.attempts == 1, "不可重试失败不得重试"
    logger.info("判定依据: key=nr-key-1 status=%s attempts=%d 原因=%s",
                t.status.value, t.attempts, t.error)
    store.close()
