"""下游调用熔断器：CLOSED -> OPEN -> HALF_OPEN -> CLOSED。"""
from __future__ import annotations

import enum
import threading


class BreakerState(str, enum.Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitOpenError(Exception):
    """熔断器处于 OPEN 状态，拒绝调用。"""


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 3, recovery_timeout: float = 30.0,
                 clock=...) -> None:
        self._threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._clock = clock
        self._lock = threading.Lock()
        raise NotImplementedError

    @property
    def state(self) -> BreakerState:
        raise NotImplementedError

    def before_call(self) -> bool:
        """调用前检查。OPEN 未超时抛 CircuitOpenError；
        HALF_OPEN 仅放行一个探测请求（返回 True 表示本请求是探测）。"""
        raise NotImplementedError

    def on_success(self) -> None:
        raise NotImplementedError

    def on_failure(self) -> None:
        raise NotImplementedError
