"""任务网关服务：幂等提交 + 状态机 + 重试 + 熔断编排。

失败原因（Task.error 前缀）可互相区分：
- ``non_retryable:``  下游返回不可重试错误，立即失败；
- ``retry_exhausted:`` 可重试错误在 max_retries 内未恢复；
- ``circuit_open:``   熔断器 OPEN，本次未调用下游。
"""
from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
import uuid

from app.breaker import CircuitBreaker, CircuitOpenError
from app.downstream import DownstreamClient, NonRetryableError, RetryableError
from app.models import Task, TaskStatus
from app.store import TaskStore

logger = logging.getLogger("gateway.service")

TERMINAL = (TaskStatus.SUCCEEDED, TaskStatus.FAILED)


class CircuitOpenRejection(Exception):
    """熔断拒绝（可与其它失败区分）。"""


def payload_hash(payload: dict) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


class TaskGateway:
    def __init__(self, store: TaskStore, downstream: DownstreamClient,
                 breaker: CircuitBreaker, max_retries: int = 3,
                 retry_backoff: float = 0.05, wait_timeout: float = 30.0,
                 sleeper=time.sleep) -> None:
        self.store = store
        self.downstream = downstream
        self.breaker = breaker
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.wait_timeout = wait_timeout
        self._sleeper = sleeper

    # ------------------------------------------------------------------ API
    def submit(self, idempotency_key: str, payload: dict) -> Task:
        """幂等提交：重复提交返回首次结果，冲突载荷抛 PayloadConflictError。"""
        task = Task(
            task_id=uuid.uuid4().hex,
            idempotency_key=idempotency_key,
            payload_hash=payload_hash(payload),
            payload=payload,
        )
        task, created = self.store.insert_if_absent(task)
        if not created:
            logger.info("key=%s duplicate submit -> existing task=%s status=%s",
                        idempotency_key, task.task_id, task.status.value)
            return self._await_terminal(task)
        logger.info("key=%s accepted new task=%s", idempotency_key, task.task_id)
        return self._execute(task)

    def recover(self) -> list[Task]:
        """进程重启后恢复在途任务：RUNNING 回退 PENDING，再驱动全部 PENDING。"""
        requeued = self.store.recover_inflight()
        for t in requeued:
            logger.info("key=%s recovered inflight task=%s RUNNING->PENDING",
                        t.idempotency_key, t.task_id)
        pending = self.store.list_by_status(TaskStatus.PENDING)
        done = []
        for t in pending:
            done.append(self._execute(t))
        return done

    # ------------------------------------------------------------- internal
    def _await_terminal(self, task: Task) -> Task:
        """并发重复提交时等待首个执行到达终态，保证返回结果一致。"""
        deadline = time.monotonic() + self.wait_timeout
        current = task
        while current.status not in TERMINAL and time.monotonic() < deadline:
            self._sleeper(0.005)
            current = self.store.get(task.task_id) or current
        return current

    def _transition(self, task: Task, target: TaskStatus, *, reason: str,
                    result: dict | None = None, error: str | None = None,
                    increment_attempts: bool = False) -> Task:
        logger.info("key=%s task=%s transition %s->%s reason=%s",
                    task.idempotency_key, task.task_id,
                    task.status.value, target.value, reason)
        return self.store.transition(
            task.task_id, task.version, target,
            result=result, error=error, increment_attempts=increment_attempts,
        )

    def _execute(self, task: Task) -> Task:
        task = self._transition(task, TaskStatus.RUNNING,
                                reason="execute", increment_attempts=True)
        last_error: str = ""
        for attempt in range(1, self.max_retries + 1):
            try:
                self.breaker.before_call()
            except CircuitOpenError as e:
                last_error = f"circuit_open: {e}"
                logger.info("key=%s task=%s rejected by breaker: %s",
                            task.idempotency_key, task.task_id, e)
                break
            try:
                result = self.downstream.call(task.payload)
            except NonRetryableError as e:
                # 业务错误不计入熔断，直接失败
                last_error = f"non_retryable: {e}"
                logger.info("key=%s task=%s attempt=%d non-retryable: %s",
                            task.idempotency_key, task.task_id, attempt, e)
                break
            except RetryableError as e:
                self.breaker.on_failure()
                last_error = f"retry_exhausted: {e}"
                logger.info("key=%s task=%s attempt=%d retryable failure: %s",
                            task.idempotency_key, task.task_id, attempt, e)
                if attempt < self.max_retries:
                    self._sleeper(self.retry_backoff * attempt)
                continue
            else:
                self.breaker.on_success()
                return self._transition(task, TaskStatus.SUCCEEDED,
                                        reason="downstream ok", result=result)
        return self._transition(task, TaskStatus.FAILED,
                                reason=last_error, error=last_error)
