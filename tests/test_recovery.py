import logging

from app.breaker import CircuitBreaker
from app.downstream import RetryableError
from app.models import TaskStatus
from app.service import TaskGateway
from app.store import TaskStore

from .conftest import ScriptedDownstream

logger = logging.getLogger("test.recovery")


def test_restart_recovers_inflight_and_preserves_terminal(tmp_path):
    """进程重启：RUNNING 回退重跑，终态结果不丢失不回退。"""
    db = str(tmp_path / "tasks.db")

    # --- 第一次“进程”：一个任务成功，一个任务在 RUNNING 时“崩溃” ---
    store1 = TaskStore(db)
    ok_downstream = ScriptedDownstream()
    gw1 = TaskGateway(store1, ok_downstream, CircuitBreaker(), retry_backoff=0.001)
    done = gw1.submit("k-done", {"v": 1})
    assert done.status == TaskStatus.SUCCEEDED

    # 手工构造一个“崩溃时仍在 RUNNING”的任务
    from app.models import Task
    from app.service import payload_hash
    inflight = Task(task_id="t-inflight", idempotency_key="k-inflight",
                    payload_hash=payload_hash({"v": 2}), payload={"v": 2})
    store1.insert_if_absent(inflight)
    store1.transition("t-inflight", 0, TaskStatus.RUNNING, increment_attempts=True)
    store1.close()  # 模拟进程退出（崩溃）

    # --- 第二次“进程”：重开同一 DB，恢复 ---
    store2 = TaskStore(db)
    recovered_downstream = ScriptedDownstream()
    gw2 = TaskGateway(store2, recovered_downstream, CircuitBreaker(),
                      retry_backoff=0.001)
    executed = gw2.recover()

    # 在途任务被重新驱动至成功
    t = store2.get_by_key("k-inflight")
    assert t.status == TaskStatus.SUCCEEDED
    assert t.result == {"echo": {"v": 2}}
    assert [x.task_id for x in executed] == ["t-inflight"]
    # 终态任务未被重跑、结果未回退
    t_done = store2.get_by_key("k-done")
    assert t_done.status == TaskStatus.SUCCEEDED
    assert t_done.result == done.result
    assert ok_downstream.calls == 1 and recovered_downstream.calls == 1
    logger.info("key=k-inflight recovered RUNNING->PENDING->SUCCEEDED; "
                "key=k-done terminal preserved result=%s", t_done.result)
    store2.close()


def test_restart_preserves_failure_reason(tmp_path):
    """重启后失败任务的状态与失败原因不丢失。"""
    db = str(tmp_path / "tasks.db")
    store1 = TaskStore(db)
    failing = ScriptedDownstream([RetryableError("down")] * 10)
    gw1 = TaskGateway(store1, failing, CircuitBreaker(failure_threshold=100),
                      max_retries=2, retry_backoff=0.001)
    t1 = gw1.submit("k-err", {"v": 1})
    assert t1.status == TaskStatus.FAILED
    assert t1.error.startswith("retry_exhausted:")
    store1.close()

    store2 = TaskStore(db)
    t2 = store2.get_by_key("k-err")
    assert t2.status == TaskStatus.FAILED
    assert t2.error == t1.error
    # recover 不会重跑终态任务
    gw2 = TaskGateway(store2, ScriptedDownstream(), CircuitBreaker())
    assert gw2.recover() == []
    assert store2.get_by_key("k-err").status == TaskStatus.FAILED
    logger.info("key=k-err terminal FAILED preserved across restart reason=%s",
                t2.error)
    store2.close()
