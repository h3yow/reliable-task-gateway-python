"""下游调用熔断器：CLOSED -> OPEN -> HALF_OPEN -> CLOSED。"""
from __future__ import annotations

import enum
import logging
import threading
import time

logger = logging.getLogger("gateway.breaker")


class BreakerState(str, enum.Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitOpenError(Exception):
    """熔断器处于 OPEN 状态，拒绝调用。"""


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 3, recovery_timeout: float = 30.0,
                 clock=time.monotonic) -> None:
        self._threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._clock = clock
        self._lock = threading.Lock()
        self._state = BreakerState.CLOSED
        self._consecutive_failures = 0
        self._opened_at = 0.0
        self._probe_in_flight = False

    @property
    def state(self) -> BreakerState:
        with self._lock:
            return self._state

    def before_call(self) -> bool:
        """调用前检查。OPEN 未超时抛 CircuitOpenError；
        HALF_OPEN 仅放行一个探测请求（返回 True 表示本请求是探测）。"""
        with self._lock:
            if self._state is BreakerState.OPEN:
                if self._clock() - self._opened_at < self._recovery_timeout:
                    logger.info("breaker OPEN，拒绝调用（恢复超时未到）")
                    raise CircuitOpenError("circuit breaker is OPEN")
                self._state = BreakerState.HALF_OPEN
                self._probe_in_flight = False
                logger.info("breaker OPEN -> HALF_OPEN（进入恢复探测）")
            if self._state is BreakerState.HALF_OPEN:
                if self._probe_in_flight:
                    logger.info("breaker HALF_OPEN，探测进行中，拒绝并发请求以防流量放大")
                    raise CircuitOpenError("circuit breaker is HALF_OPEN, probe in flight")
                self._probe_in_flight = True
                return True
            return False

    def on_success(self) -> None:
        with self._lock:
            if self._state is not BreakerState.CLOSED:
                logger.info("breaker %s -> CLOSED（调用成功）", self._state.value)
            self._state = BreakerState.CLOSED
            self._consecutive_failures = 0
            self._probe_in_flight = False

    def on_failure(self) -> None:
        with self._lock:
            self._consecutive_failures += 1
            if self._state is BreakerState.HALF_OPEN:
                self._state = BreakerState.OPEN
                self._opened_at = self._clock()
                self._probe_in_flight = False
                logger.warning("breaker HALF_OPEN -> OPEN（探测失败，连续失败=%d）",
                               self._consecutive_failures)
            elif (self._state is BreakerState.CLOSED
                  and self._consecutive_failures >= self._threshold):
                self._state = BreakerState.OPEN
                self._opened_at = self._clock()
                logger.warning("breaker CLOSED -> OPEN（连续失败=%d 达到阈值=%d）",
                               self._consecutive_failures, self._threshold)
