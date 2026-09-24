"""熔断器：CLOSED -> OPEN -> HALF_OPEN -> CLOSED。

- CLOSED：正常放行；连续失败达到阈值切换 OPEN；
- OPEN：直接拒绝（CircuitOpenError），冷却 recovery_timeout 后进入 HALF_OPEN；
- HALF_OPEN：仅放行一个探测请求（不放大流量），成功则 CLOSED，失败则重新 OPEN。
"""
from __future__ import annotations

import enum
import threading
import time


class BreakerState(str, enum.Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitOpenError(Exception):
    """熔断器处于 OPEN 状态，拒绝调用。"""


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 3, recovery_timeout: float = 30.0,
                 clock=time.monotonic) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._clock = clock
        self._lock = threading.Lock()
        self._state = BreakerState.CLOSED
        self._consecutive_failures = 0
        self._opened_at = 0.0
        self._probe_in_flight = False

    @property
    def state(self) -> BreakerState:
        with self._lock:
            self._maybe_half_open()
            return self._state

    def _maybe_half_open(self) -> None:
        if (self._state is BreakerState.OPEN
                and self._clock() - self._opened_at >= self.recovery_timeout):
            self._state = BreakerState.HALF_OPEN
            self._probe_in_flight = False

    def before_call(self) -> None:
        """调用前检查；OPEN 未冷却或探测已在进行则抛 CircuitOpenError。"""
        with self._lock:
            self._maybe_half_open()
            if self._state is BreakerState.OPEN:
                raise CircuitOpenError(
                    f"circuit OPEN, retry after "
                    f"{self.recovery_timeout - (self._clock() - self._opened_at):.3f}s"
                )
            if self._state is BreakerState.HALF_OPEN:
                if self._probe_in_flight:
                    raise CircuitOpenError("half-open probe already in flight")
                self._probe_in_flight = True

    def on_success(self) -> None:
        with self._lock:
            self._state = BreakerState.CLOSED
            self._consecutive_failures = 0
            self._probe_in_flight = False

    def on_failure(self) -> None:
        with self._lock:
            self._consecutive_failures += 1
            if self._state is BreakerState.HALF_OPEN:
                # 探测失败：重新打开并重新计时
                self._state = BreakerState.OPEN
                self._opened_at = self._clock()
                self._probe_in_flight = False
            elif self._consecutive_failures >= self.failure_threshold:
                self._state = BreakerState.OPEN
                self._opened_at = self._clock()
