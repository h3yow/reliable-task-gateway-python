import logging

from app.breaker import BreakerState
from app.downstream import NonRetryableError, RetryableError
from app.models import TaskStatus

from .conftest import ScriptedDownstream

logger = logging.getLogger("test.retry_breaker")


def test_retryable_failure_retries_then_succeeds(make_gateway):
    """可重试失败：按策略重试，最终成功。"""
    downstream = ScriptedDownstream(
        [RetryableError("timeout"), RetryableError("503")])
    gw = make_gateway(downstream, max_retries=3)
    t = gw.submit("k-retry", {"v": 1})
    assert t.status == TaskStatus.SUCCEEDED
    assert downstream.calls == 3
    logger.info("key=k-retry attempts=%d final=%s", downstream.calls, t.status.value)


def test_retry_exhausted_fails_with_distinguishable_reason(make_gateway):
    downstream = ScriptedDownstream([RetryableError("down")] * 10)
    gw = make_gateway(downstream, max_retries=2, failure_threshold=100)
    t = gw.submit("k-exhaust", {"v": 1})
    assert t.status == TaskStatus.FAILED
    assert t.error.startswith("retry_exhausted:")
    assert downstream.calls == 2
    logger.info("key=k-exhaust reason=%s", t.error)


def test_non_retryable_fails_immediately_without_breaker_trip(make_gateway):
    """不可重试失败：不重试、不计入熔断。"""
    downstream = ScriptedDownstream([NonRetryableError("invalid")] * 10)
    gw = make_gateway(downstream, max_retries=5, failure_threshold=1)
    t = gw.submit("k-nonretry", {"v": 1})
    assert t.status == TaskStatus.FAILED
    assert t.error.startswith("non_retryable:")
    assert downstream.calls == 1, "不可重试错误不得重试"
    assert gw.breaker.state is BreakerState.CLOSED, "业务错误不得计入熔断"
    logger.info("key=k-nonretry reason=%s breaker=%s", t.error, gw.breaker.state.value)


def test_breaker_opens_after_consecutive_failures_and_rejects(make_gateway):
    """连续可重试失败 -> 熔断 OPEN，后续提交以可区分原因拒绝且不再调用下游。"""
    downstream = ScriptedDownstream([RetryableError("down")] * 100)
    gw = make_gateway(downstream, max_retries=1, failure_threshold=2)

    t1 = gw.submit("k-b1", {"v": 1})
    assert t1.error.startswith("retry_exhausted:")
    assert gw.breaker.state is BreakerState.CLOSED  # 第 1 次未达阈值
    t2 = gw.submit("k-b2", {"v": 2})
    assert t2.error.startswith("retry_exhausted:")
    assert gw.breaker.state is BreakerState.OPEN  # 第 2 次达到阈值

    calls_before = downstream.calls
    t3 = gw.submit("k-b3", {"v": 3})
    assert t3.status == TaskStatus.FAILED
    assert t3.error.startswith("circuit_open:"), "熔断拒绝原因须可区分"
    assert downstream.calls == calls_before, "OPEN 期间不得调用下游"
    logger.info("key=k-b3 rejected reason=%s breaker=%s",
                t3.error, gw.breaker.state.value)


def test_breaker_half_open_probe_no_amplification(store):
    """恢复探测：HALF_OPEN 只放行一个探测，成功则闭合，失败则重新打开。"""
    from app.breaker import CircuitBreaker
    from app.service import TaskGateway

    now = [0.0]
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=10.0,
                             clock=lambda: now[0])
    downstream = ScriptedDownstream([RetryableError("down")])
    gw = TaskGateway(store, downstream, breaker, max_retries=1,
                     retry_backoff=0.001)

    t1 = gw.submit("k-p1", {"v": 1})
    assert breaker.state is BreakerState.OPEN

    # 冷却期内：拒绝且不调用下游
    now[0] += 5.0
    t2 = gw.submit("k-p2", {"v": 2})
    assert t2.error.startswith("circuit_open:")
    assert downstream.calls == 1

    # 冷却结束：探测成功 -> CLOSED
    now[0] += 10.0
    assert breaker.state is BreakerState.HALF_OPEN
    t3 = gw.submit("k-p3", {"v": 3})
    assert t3.status == TaskStatus.SUCCEEDED
    assert breaker.state is BreakerState.CLOSED
    assert downstream.calls == 2, "探测只放行一次调用，不放大流量"
    logger.info("key=k-p3 probe succeeded breaker=%s calls=%d",
                breaker.state.value, downstream.calls)


def test_breaker_probe_failure_reopens(store):
    from app.breaker import CircuitBreaker
    from app.service import TaskGateway

    now = [0.0]
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=10.0,
                             clock=lambda: now[0])
    downstream = ScriptedDownstream([RetryableError("down")] * 10)
    gw = TaskGateway(store, downstream, breaker, max_retries=1,
                     retry_backoff=0.001)

    gw.submit("k-q1", {"v": 1})
    assert breaker.state is BreakerState.OPEN
    now[0] += 10.0
    t = gw.submit("k-q2", {"v": 2})  # 探测仍失败
    assert t.error.startswith("retry_exhausted:")
    assert breaker.state is BreakerState.OPEN, "探测失败须重新打开熔断"
    # 重新打开后冷却重新计时
    now[0] += 5.0
    t3 = gw.submit("k-q3", {"v": 3})
    assert t3.error.startswith("circuit_open:")
    logger.info("key=k-q2 probe failed -> breaker reopened")
