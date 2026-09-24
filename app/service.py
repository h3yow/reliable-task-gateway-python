"""任务网关服务：幂等提交、状态机驱动执行、重试与熔断。"""
from __future__ import annotations

import enum

from app.breaker import CircuitBreaker
from app.downstream import DownstreamClient, RetryPolicy
from app.models import Task
from app.store import TaskStore


class SubmitOutcome(str, enum.Enum):
    CREATED = "CREATED"        # 首次提交
    DUPLICATE = "DUPLICATE"    # 重复提交（幂等命中，返回首次结果）


class TaskGateway:
    def __init__(self, store: TaskStore, client: DownstreamClient,
                 breaker: CircuitBreaker, retry: RetryPolicy) -> None:
        self._store = store
        self._client = client
        self._breaker = breaker
        self._retry = retry

    def submit(self, idempotency_key: str, payload: str) -> tuple[Task, SubmitOutcome]:
        """幂等提交：同键同载荷返回首次结果；同键不同载荷抛 PayloadConflictError。"""
        raise NotImplementedError

    def get(self, idempotency_key: str) -> Task | None:
        raise NotImplementedError

    def run(self, task: Task) -> Task:
        """执行单个任务：状态机迁移 + 重试 + 熔断。"""
        raise NotImplementedError
