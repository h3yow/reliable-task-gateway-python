"""下游调用抽象与错误分类（骨架）。"""
from __future__ import annotations

from typing import Protocol


class DownstreamError(Exception):
    """下游调用失败基类。"""


class RetryableError(DownstreamError):
    """可重试失败（超时、5xx、连接错误等）。"""


class NonRetryableError(DownstreamError):
    """不可重试失败（4xx 业务校验等），直接置任务失败。"""


class DownstreamClient(Protocol):
    def call(self, payload: dict) -> dict:
        """同步调用下游，成功返回结果 dict。"""
        ...
