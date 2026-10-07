"""基于 SQLite 的任务本地持久化存储。

- 幂等键唯一约束保证重复/并发提交只产生一条记录；
- 状态迁移通过条件 UPDATE（乐观并发控制）保证不丢更新；
- 进程重启后数据保留在 SQLite 文件中，不丢失、不回退。
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time

from app.models import IllegalTransitionError, Task, TaskStatus, check_transition

logger = logging.getLogger("gateway.store")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    payload_hash TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    deadline_at REAL,
    result TEXT,
    error TEXT,
    version INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
)
"""

# 升级兼容：老库（无 deadline_at 列）打开时自动补列，老任务 deadline_at 为 NULL
# （无时限，行为与升级前完全一致）。
_MIGRATIONS = (
    "ALTER TABLE tasks ADD COLUMN deadline_at REAL",
)


class PayloadConflictError(Exception):
    """同一幂等键提交了不同载荷。"""

    def __init__(self, idempotency_key: str) -> None:
        super().__init__(f"payload conflict for idempotency key: {idempotency_key}")
        self.idempotency_key = idempotency_key


class TaskStore:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        with self._conn:
            self._conn.execute(_SCHEMA)
            existing = {row[1] for row in self._conn.execute("PRAGMA table_info(tasks)")}
            for stmt in _MIGRATIONS:
                # 形如 "ALTER TABLE tasks ADD COLUMN deadline_at REAL"
                column = stmt.split("ADD COLUMN")[1].split()[0]
                if column not in existing:
                    logger.info("store.migrate 补充缺失列: %s", column)
                    self._conn.execute(stmt)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @staticmethod
    def _row_to_task(row: sqlite3.Row) -> Task:
        return Task(
            task_id=row["task_id"],
            idempotency_key=row["idempotency_key"],
            payload_hash=row["payload_hash"],
            payload=row["payload"],
            status=TaskStatus(row["status"]),
            attempts=row["attempts"],
            deadline_at=row["deadline_at"],
            result=row["result"],
            error=row["error"],
            version=row["version"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def insert_if_absent(self, task: Task) -> tuple[Task, bool]:
        """按幂等键插入任务。

        返回 (任务, 是否新创建)。键已存在时：
        - 载荷一致 -> 返回已存在任务与 False（重复提交，返回首次结果）；
        - 载荷不一致 -> 抛 PayloadConflictError。
        """
        now = time.time()
        task.created_at = task.updated_at = now
        with self._lock, self._conn:
            try:
                self._conn.execute(
                    "INSERT INTO tasks (task_id, idempotency_key, payload_hash, payload,"
                    " status, attempts, deadline_at, result, error, version,"
                    " created_at, updated_at)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (task.task_id, task.idempotency_key, task.payload_hash, task.payload,
                     task.status.value, task.attempts, task.deadline_at, task.result,
                     task.error, task.version, task.created_at, task.updated_at),
                )
                logger.info("store.insert key=%s task_id=%s status=%s (新建)",
                            task.idempotency_key, task.task_id, task.status.value)
                return task, True
            except sqlite3.IntegrityError:
                row = self._conn.execute(
                    "SELECT * FROM tasks WHERE idempotency_key=?",
                    (task.idempotency_key,),
                ).fetchone()
                existing = self._row_to_task(row)
                if existing.payload_hash != task.payload_hash:
                    logger.warning(
                        "store.insert key=%s 载荷冲突: 已有hash=%s 新hash=%s -> 拒绝",
                        task.idempotency_key, existing.payload_hash, task.payload_hash)
                    raise PayloadConflictError(task.idempotency_key)
                logger.info("store.insert key=%s 幂等命中，返回已有任务 task_id=%s status=%s",
                            task.idempotency_key, existing.task_id, existing.status.value)
                return existing, False

    def get_by_key(self, idempotency_key: str) -> Task | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE idempotency_key=?", (idempotency_key,)
            ).fetchone()
        return self._row_to_task(row) if row else None

    def find_expired(self, now: float) -> list[Task]:
        """查找已超时时限、但仍处于非终态的任务（deadline_at 为 NULL 的无时限任务
        不参与超时判定）。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM tasks WHERE deadline_at IS NOT NULL AND deadline_at <= ?"
                " AND status IN (?, ?)",
                (now, TaskStatus.PENDING.value, TaskStatus.RUNNING.value),
            ).fetchall()
        return [self._row_to_task(r) for r in rows]

    def transition(self, task_id: str, expected: TaskStatus, target: TaskStatus,
                   *, result: str | None = None, error: str | None = None,
                   inc_attempt: bool = False) -> Task:
        """条件迁移：仅当当前状态等于 expected 时迁移到 target。

        迁移合法性由 models.check_transition 校验；条件不满足时抛
        IllegalTransitionError，保证并发更新不丢更新、终态不可变更。
        """
        check_transition(expected, target)
        now = time.time()
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE tasks SET status=?, result=COALESCE(?, result),"
                " error=COALESCE(?, error),"
                " attempts=attempts+?, version=version+1, updated_at=?"
                " WHERE task_id=? AND status=?",
                (target.value, result, error, 1 if inc_attempt else 0,
                 now, task_id, expected.value),
            )
            if cur.rowcount == 0:
                row = self._conn.execute(
                    "SELECT status FROM tasks WHERE task_id=?", (task_id,)
                ).fetchone()
                actual = TaskStatus(row["status"]) if row else None
                logger.warning(
                    "store.transition task_id=%s 迁移失败: 期望=%s 实际=%s 目标=%s",
                    task_id, expected.value, actual, target.value)
                raise IllegalTransitionError(
                    f"task {task_id}: expected {expected.value}, actual "
                    f"{actual.value if actual else 'MISSING'}, target {target.value}")
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE task_id=?", (task_id,)
            ).fetchone()
            task = self._row_to_task(row)
            logger.info(
                "store.transition task_id=%s key=%s 状态迁移 %s -> %s "
                "(attempts=%d, version=%d, result=%r, error=%r)",
                task.task_id, task.idempotency_key, expected.value, target.value,
                task.attempts, task.version, task.result, task.error)
            return task
