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
    TIMED_OUT = "TIMED_OUT"    # 超过最长执行时限（终态）
    CANCELLED = "CANCELLED"    # 调用方主动取消（终态）


TERMINAL_STATES: frozenset[TaskStatus] = frozenset(
    {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CIRCUIT_REJECTED,
     TaskStatus.TIMED_OUT, TaskStatus.CANCELLED}
)

# 合法状态迁移表
ALLOWED_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.PENDING: frozenset(
        {TaskStatus.RUNNING, TaskStatus.CIRCUIT_REJECTED,
         TaskStatus.TIMED_OUT, TaskStatus.CANCELLED}
    ),
    TaskStatus.RUNNING: frozenset(
        # RUNNING -> RUNNING 为自环，仅用于重试时递增 attempts
        {TaskStatus.RUNNING, TaskStatus.PENDING, TaskStatus.SUCCEEDED,
         TaskStatus.FAILED, TaskStatus.CIRCUIT_REJECTED,
         TaskStatus.TIMED_OUT, TaskStatus.CANCELLED}
    ),
    TaskStatus.SUCCEEDED: frozenset(),
    TaskStatus.FAILED: frozenset(),
    TaskStatus.CIRCUIT_REJECTED: frozenset(),
    TaskStatus.TIMED_OUT: frozenset(),
    TaskStatus.CANCELLED: frozenset(),
}


class IllegalTransitionError(Exception):
    """非法状态迁移。"""


def check_transition(src: TaskStatus, dst: TaskStatus) -> None:
    """校验状态迁移是否合法，非法则抛出 IllegalTransitionError。"""
    if dst not in ALLOWED_TRANSITIONS[src]:
        raise IllegalTransitionError(f"illegal transition: {src.value} -> {dst.value}")


@dataclass
class Task:
    task_id: str
    idempotency_key: str
    payload_hash: str
    payload: str
    status: TaskStatus = TaskStatus.PENDING
    attempts: int = 0
    # 绝对截止时间（epoch 秒）；None 表示无时限（老提交方式，行为不变）
    deadline_at: float | None = None
    result: str | None = None
    error: str | None = None
    version: int = 0
    created_at: float = 0.0
    updated_at: float = 0.0
