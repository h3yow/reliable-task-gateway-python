"""SQLite 本地持久化存储：幂等插入 + 乐观锁状态迁移。

并发安全策略：
- 所有写操作在 ``BEGIN IMMEDIATE`` 事务内完成，SQLite 级串行化，不丢更新；
- 状态迁移携带版本号（乐观锁），版本不匹配抛 ConcurrentUpdateError；
- 状态机校验在事务内基于最新行执行，校验失败抛 IllegalTransitionError。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time

from app.models import Task, TaskStatus, check_transition

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    payload_hash TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    result TEXT,
    error TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    version INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
"""


class PayloadConflictError(Exception):
    """同一幂等键提交了不同载荷。"""


class ConcurrentUpdateError(Exception):
    """乐观锁冲突，需重试。"""


class TaskStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        with self._conn:
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        self._conn.close()

    @staticmethod
    def _row_to_task(row: sqlite3.Row) -> Task:
        return Task(
            task_id=row["task_id"],
            idempotency_key=row["idempotency_key"],
            payload_hash=row["payload_hash"],
            payload=json.loads(row["payload"]),
            status=TaskStatus(row["status"]),
            result=json.loads(row["result"]) if row["result"] is not None else None,
            error=row["error"],
            attempts=row["attempts"],
            version=row["version"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def insert_if_absent(self, task: Task) -> tuple[Task, bool]:
        """按幂等键插入；已存在则返回 (已存在任务, False)，载荷不一致抛 PayloadConflictError。"""
        now = time.time()
        task.created_at = task.created_at or now
        task.updated_at = now
        with self._lock, self._conn:
            try:
                self._conn.execute(
                    "INSERT INTO tasks (task_id, idempotency_key, payload_hash, payload,"
                    " status, result, error, attempts, version, created_at, updated_at)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (task.task_id, task.idempotency_key, task.payload_hash,
                     json.dumps(task.payload, sort_keys=True), task.status.value,
                     None, None, task.attempts, task.version,
                     task.created_at, task.updated_at),
                )
                return task, True
            except sqlite3.IntegrityError:
                row = self._conn.execute(
                    "SELECT * FROM tasks WHERE idempotency_key = ?",
                    (task.idempotency_key,),
                ).fetchone()
                existing = self._row_to_task(row)
                if existing.payload_hash != task.payload_hash:
                    raise PayloadConflictError(
                        f"idempotency key {task.idempotency_key!r} already used "
                        f"with a different payload"
                    )
                return existing, False

    def get_by_key(self, idempotency_key: str) -> Task | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
        return self._row_to_task(row) if row else None

    def get(self, task_id: str) -> Task | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
        return self._row_to_task(row) if row else None

    def transition(self, task_id: str, expect_version: int, target: TaskStatus,
                   *, result: dict | None = None, error: str | None = None,
                   increment_attempts: bool = False) -> Task:
        """带版本号与状态机校验的迁移；版本不匹配抛 ConcurrentUpdateError。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
                ).fetchone()
                if row is None:
                    raise KeyError(f"task {task_id!r} not found")
                current = self._row_to_task(row)
                if current.version != expect_version:
                    raise ConcurrentUpdateError(
                        f"task {task_id}: version {current.version} != expected {expect_version}"
                    )
                check_transition(current.status, target)
                cur = self._conn.execute(
                    "UPDATE tasks SET status=?, result=?, error=?,"
                    " attempts=attempts+?, version=version+1, updated_at=?"
                    " WHERE task_id=? AND version=?",
                    (target.value,
                     json.dumps(result, sort_keys=True) if result is not None else None,
                     error, 1 if increment_attempts else 0, time.time(),
                     task_id, expect_version),
                )
                if cur.rowcount != 1:
                    raise ConcurrentUpdateError(f"task {task_id}: lost update race")
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
        return self.get(task_id)  # type: ignore[return-value]

    def list_by_status(self, status: TaskStatus) -> list[Task]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM tasks WHERE status = ? ORDER BY created_at",
                (status.value,),
            ).fetchall()
        return [self._row_to_task(r) for r in rows]

    def recover_inflight(self) -> list[Task]:
        """重启恢复：将残留 RUNNING 任务回退为 PENDING 并返回。"""
        with self._lock, self._conn:
            rows = self._conn.execute(
                "SELECT * FROM tasks WHERE status = ?", (TaskStatus.RUNNING.value,)
            ).fetchall()
            tasks = [self._row_to_task(r) for r in rows]
            for t in tasks:
                self._conn.execute(
                    "UPDATE tasks SET status=?, version=version+1, updated_at=?"
                    " WHERE task_id=? AND version=?",
                    (TaskStatus.PENDING.value, time.time(), t.task_id, t.version),
                )
        return tasks
