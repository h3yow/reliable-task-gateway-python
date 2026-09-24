"""下游调用抽象、错误分类与重试策略。"""
from __future__ import annotations

from typing import Protocol


class DownstreamError(Exception):
    """下游调用失败基类。"""


class RetryableError(DownstreamError):
    """可重试失败（如超时、5xx、连接错误）。"""


class NonRetryableError(DownstreamError):
    """不可重试失败（如 4xx 业务校验失败），直接终态失败。"""


class DownstreamClient(Protocol):
    def call(self, task_id: str, payload: str) -> str:
        """执行下游调用，成功返回结果字符串。"""
        ...


class RetryPolicy:
    def __init__(self, max_attempts: int = 3, base_delay: float = 0.01,
                 sleep=...) -> None:
        self._max_attempts = max_attempts
        self._base_delay = base_delay
        self._sleep = sleep

    @property
    def max_attempts(self) -> int:
        return self._max_attempts

    def wait(self, attempt: int) -> None:
        """第 attempt 次失败后的退避等待（指数退避）。"""
        raise NotImplementedError
