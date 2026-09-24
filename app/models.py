"""任务领域模型与状态机定义。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class TaskStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"          # 不可重试失败（终态）
    CIRCUIT_REJECTED = "CIRCUIT_REJECTED"  # 熔断拒绝（终态）


TERMINAL_STATES: frozenset[TaskStatus] = frozenset(
    {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CIRCUIT_REJECTED}
)

# 合法状态迁移表
ALLOWED_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.PENDING: frozenset({TaskStatus.RUNNING, TaskStatus.CIRCUIT_REJECTED}),
    TaskStatus.RUNNING: frozenset(
        {TaskStatus.PENDING, TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CIRCUIT_REJECTED}
    ),
    TaskStatus.SUCCEEDED: frozenset(),
    TaskStatus.FAILED: frozenset(),
    TaskStatus.CIRCUIT_REJECTED: frozenset(),
}


class IllegalTransitionError(Exception):
    """非法状态迁移。"""


def check_transition(src: TaskStatus, dst: TaskStatus) -> None:
    """校验状态迁移是否合法，非法则抛出 IllegalTransitionError。"""
    raise NotImplementedError


@dataclass
class Task:
    task_id: str
    idempotency_key: str
    payload_hash: str
    payload: str
    status: TaskStatus = TaskStatus.PENDING
    attempts: int = 0
    result: str | None = None
    error: str | None = None
    version: int = 0
    created_at: float = 0.0
    updated_at: float = 0.0
