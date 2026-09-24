"""任务网关服务：幂等提交、状态机驱动执行、重试与熔断。"""
from __future__ import annotations

import enum
import hashlib
import logging
import threading
import uuid

from app.breaker import CircuitBreaker, CircuitOpenError
from app.downstream import (DownstreamClient, NonRetryableError, RetryableError,
                            RetryPolicy)
from app.models import IllegalTransitionError, Task, TaskStatus
from app.store import TaskStore

logger = logging.getLogger("gateway.service")


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
        self._done_events: dict[str, threading.Event] = {}
        self._events_lock = threading.Lock()

    # ---------------- 提交 ----------------

    def submit(self, idempotency_key: str, payload: str) -> tuple[Task, SubmitOutcome]:
        """幂等提交：同键同载荷返回首次结果；同键不同载荷抛 PayloadConflictError。"""
        task = Task(
            task_id=str(uuid.uuid4()),
            idempotency_key=idempotency_key,
            payload_hash=hashlib.sha256(payload.encode()).hexdigest(),
            payload=payload,
        )
        stored, created = self._store.insert_if_absent(task)
        if created:
            logger.info("submit key=%s task_id=%s 首次提交，开始执行",
                        idempotency_key, stored.task_id)
            done = threading.Event()
            with self._events_lock:
                self._done_events[idempotency_key] = done
            try:
                return self.run(stored), SubmitOutcome.CREATED
            finally:
                done.set()
        # 重复提交：等待首次执行结束，返回与首次一致的结果
        logger.info("submit key=%s 重复提交（幂等命中 task_id=%s），等待首次结果",
                    idempotency_key, stored.task_id)
        with self._events_lock:
            done = self._done_events.get(idempotency_key)
        if done is not None:
            done.wait(timeout=30.0)
        final = self._store.get_by_key(idempotency_key)
        assert final is not None
        return final, SubmitOutcome.DUPLICATE

    def get(self, idempotency_key: str) -> Task | None:
        return self._store.get_by_key(idempotency_key)

    # ---------------- 执行 ----------------

    def run(self, task: Task) -> Task:
        """执行单个任务：状态机迁移 + 重试 + 熔断。"""
        try:
            task = self._store.transition(
                task.task_id, TaskStatus.PENDING, TaskStatus.RUNNING, inc_attempt=True)
        except IllegalTransitionError:
            # 已被其他执行者推进（如重启恢复场景），直接返回当前状态
            current = self._store.get_by_key(task.idempotency_key)
            assert current is not None
            logger.info("run key=%s 任务已处于 %s，跳过重复执行",
                        task.idempotency_key, current.status.value)
            return current

        for attempt in range(1, self._retry.max_attempts + 1):
            try:
                self._breaker.before_call()
            except CircuitOpenError:
                reason = "CIRCUIT_OPEN: downstream circuit breaker is open"
                logger.warning("run key=%s 熔断拒绝: %s", task.idempotency_key, reason)
                return self._store.transition(
                    task.task_id, TaskStatus.RUNNING, TaskStatus.CIRCUIT_REJECTED,
                    error=reason)
            try:
                result = self._client.call(task.task_id, task.payload)
            except RetryableError as exc:
                self._breaker.on_failure()
                logger.warning(
                    "run key=%s 第 %d 次尝试可重试失败: %s",
                    task.idempotency_key, attempt, exc)
                if attempt < self._retry.max_attempts:
                    self._retry.wait(attempt)
                    task = self._store.transition(
                        task.task_id, TaskStatus.RUNNING, TaskStatus.RUNNING,
                        inc_attempt=True)
                    continue
                reason = f"RETRY_EXHAUSTED: {exc}"
                logger.warning("run key=%s 重试耗尽: %s", task.idempotency_key, reason)
                return self._store.transition(
                    task.task_id, TaskStatus.RUNNING, TaskStatus.FAILED, error=reason)
            except NonRetryableError as exc:
                reason = f"NON_RETRYABLE: {exc}"
                logger.warning("run key=%s 不可重试失败: %s",
                               task.idempotency_key, reason)
                return self._store.transition(
                    task.task_id, TaskStatus.RUNNING, TaskStatus.FAILED, error=reason)
            else:
                self._breaker.on_success()
                logger.info("run key=%s 第 %d 次尝试成功", task.idempotency_key, attempt)
                return self._store.transition(
                    task.task_id, TaskStatus.RUNNING, TaskStatus.SUCCEEDED,
                    result=result)
        raise AssertionError("unreachable")
