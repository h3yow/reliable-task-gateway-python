"""任务领域模型与状态机定义（骨架）。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class TaskStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


#: 合法状态迁移表；终态（SUCCEEDED/FAILED）不可再迁移。
ALLOWED_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.PENDING: frozenset({TaskStatus.RUNNING, TaskStatus.FAILED}),
    TaskStatus.RUNNING: frozenset({TaskStatus.SUCCEEDED, TaskStatus.FAILED}),
    TaskStatus.SUCCEEDED: frozenset(),
    TaskStatus.FAILED: frozenset(),
}


class IllegalTransitionError(Exception):
    """状态机收到非法迁移时抛出。"""


def check_transition(current: TaskStatus, target: TaskStatus) -> None:
    """校验迁移是否合法，非法则抛 IllegalTransitionError。"""
    if target not in ALLOWED_TRANSITIONS[current]:
        raise IllegalTransitionError(
            f"illegal transition: {current.value} -> {target.value}"
        )


@dataclass
class Task:
    task_id: str
    idempotency_key: str
    payload_hash: str
    payload: dict
    status: TaskStatus = TaskStatus.PENDING
    result: dict | None = None
    error: str | None = None
    attempts: int = 0
    version: int = 0
    created_at: float = 0.0
    updated_at: float = 0.0
