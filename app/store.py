"""基于 SQLite 的任务本地持久化存储。

- 幂等键唯一约束保证重复/并发提交只产生一条记录；
- 状态迁移通过条件 UPDATE（乐观并发控制）保证不丢更新；
- 进程重启后数据保留在 SQLite 文件中，不丢失、不回退。
"""
from __future__ import annotations

import sqlite3

from app.models import Task, TaskStatus


class PayloadConflictError(Exception):
    """同一幂等键提交了不同载荷。"""

    def __init__(self, idempotency_key: str) -> None:
        super().__init__(f"payload conflict for idempotency key: {idempotency_key}")
        self.idempotency_key = idempotency_key


class TaskStore:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError

    def insert_if_absent(self, task: Task) -> tuple[Task, bool]:
        """按幂等键插入任务。

        返回 (任务, 是否新创建)。键已存在时：
        - 载荷一致 -> 返回已存在任务与 False（重复提交，返回首次结果）；
        - 载荷不一致 -> 抛 PayloadConflictError。
        """
        raise NotImplementedError

    def get_by_key(self, idempotency_key: str) -> Task | None:
        raise NotImplementedError

    def transition(self, task_id: str, expected: TaskStatus, target: TaskStatus,
                   *, result: str | None = None, error: str | None = None,
                   inc_attempt: bool = False) -> Task:
        """条件迁移：仅当当前状态等于 expected 时迁移到 target。

        迁移合法性由 models.check_transition 校验；条件不满足时抛
        IllegalTransitionError，保证并发更新不丢更新、终态不可变更。
        """
        raise NotImplementedError
