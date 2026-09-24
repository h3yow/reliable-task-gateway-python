import logging
import threading

import pytest

from app.models import TaskStatus
from app.store import PayloadConflictError

from .conftest import ScriptedDownstream

logger = logging.getLogger("test.idempotency")


def test_concurrent_duplicate_submit_single_execution(make_gateway):
    """并发重复提交：只产生一次执行，所有提交方拿到一致结果。"""
    downstream = ScriptedDownstream()
    gw = make_gateway(downstream)
    results, errors = [], []

    def worker():
        try:
            results.append(gw.submit("order-42", {"amount": 100}))
        except Exception as e:  # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert len(results) == 16
    first = results[0]
    for r in results:
        assert r.task_id == first.task_id
        assert r.status == TaskStatus.SUCCEEDED
        assert r.result == first.result
    assert downstream.calls == 1, "重复/并发提交不得产生第二次下游调用"
    logger.info("key=order-42 concurrent=16 task_id=%s status=%s calls=%d",
                first.task_id, first.status.value, downstream.calls)


def test_sequential_duplicate_returns_first_result(make_gateway):
    downstream = ScriptedDownstream()
    gw = make_gateway(downstream)
    t1 = gw.submit("k1", {"v": 1})
    t2 = gw.submit("k1", {"v": 1})
    assert t1.task_id == t2.task_id
    assert t1.status == t2.status == TaskStatus.SUCCEEDED
    assert t1.result == t2.result
    assert downstream.calls == 1


def test_conflicting_payload_rejected_and_distinguishable(make_gateway, caplog):
    """冲突载荷被拒绝，且原因可与重复提交区分。"""
    downstream = ScriptedDownstream()
    gw = make_gateway(downstream)
    t1 = gw.submit("k-conflict", {"v": 1})
    assert t1.status == TaskStatus.SUCCEEDED

    with caplog.at_level(logging.INFO, logger="gateway.service"):
        with pytest.raises(PayloadConflictError) as exc_info:
            gw.submit("k-conflict", {"v": 2})
    # 拒绝原因与“重复提交”路径不同：抛异常而非返回首次结果
    assert "different payload" in str(exc_info.value)
    # 冲突提交不触发新的下游调用
    assert downstream.calls == 1
    # 原任务状态未被污染
    assert gw.store.get_by_key("k-conflict").status == TaskStatus.SUCCEEDED
    logger.info("key=k-conflict conflict rejected reason=%s", exc_info.value)


def test_failed_task_same_key_replay_returns_same_failure(make_gateway):
    """失败任务的重复提交同样幂等：返回首次的失败结果而非重新执行。"""
    from app.downstream import NonRetryableError
    downstream = ScriptedDownstream([NonRetryableError("bad request")])
    gw = make_gateway(downstream)
    t1 = gw.submit("k-fail", {"v": 1})
    assert t1.status == TaskStatus.FAILED
    t2 = gw.submit("k-fail", {"v": 1})
    assert t2.task_id == t1.task_id
    assert t2.status == TaskStatus.FAILED
    assert t2.error == t1.error
    assert downstream.calls == 1
