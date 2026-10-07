"""任务网关服务：幂等提交、状态机驱动执行、重试与熔断、超时与取消。"""
from __future__ import annotations

import enum
import hashlib
import logging
import threading
import time
import uuid

from app.breaker import CircuitBreaker, CircuitOpenError
from app.downstream import (DownstreamClient, NonRetryableError, RetryableError,
                            RetryPolicy)
from app.models import (TERMINAL_STATES, IllegalTransitionError, Task,
                        TaskStatus)
from app.store import TaskStore

logger = logging.getLogger("gateway.service")


class SubmitOutcome(str, enum.Enum):
    CREATED = "CREATED"        # 首次提交
    DUPLICATE = "DUPLICATE"    # 重复提交（幂等命中，返回首次结果）


class CancelOutcome(str, enum.Enum):
    CANCELLED = "CANCELLED"                # 取消生效（本次或此前取消），终态为 CANCELLED
    ALREADY_TERMINAL = "ALREADY_TERMINAL"  # 任务已收尾为其他终态，返回真实状态


class TaskNotFoundError(Exception):
    """按幂等键未找到任务。"""

    def __init__(self, idempotency_key: str) -> None:
        super().__init__(f"task not found for idempotency key: {idempotency_key}")
        self.idempotency_key = idempotency_key


class TaskGateway:
    def __init__(self, store: TaskStore, client: DownstreamClient,
                 breaker: CircuitBreaker, retry: RetryPolicy,
                 clock=time.time) -> None:
        self._store = store
        self._client = client
        self._breaker = breaker
        self._retry = retry
        self._clock = clock
        self._done_events: dict[str, threading.Event] = {}
        self._events_lock = threading.Lock()

    # ---------------- 提交 ----------------

    def submit(self, idempotency_key: str, payload: str,
               timeout_seconds: float | None = None
               ) -> tuple[Task, SubmitOutcome]:
        """幂等提交：同键同载荷返回首次结果；同键不同载荷抛 PayloadConflictError。

        timeout_seconds 给定时，任务的最长执行时限以绝对截止时刻
        （deadline_at = 提交时刻 + 时限）持久化，重启后按原时间继续计算；
        不给定时与旧行为完全一致（deadline_at 为 NULL，永不超时）。
        重复提交以首次提交的时限为准。
        """
        deadline_at = (None if timeout_seconds is None
                       else self._clock() + timeout_seconds)
        task = Task(
            task_id=str(uuid.uuid4()),
            idempotency_key=idempotency_key,
            payload_hash=hashlib.sha256(payload.encode()).hexdigest(),
            payload=payload,
            deadline_at=deadline_at,
        )
        stored, created = self._store.insert_if_absent(task)
        if created:
            logger.info("submit key=%s task_id=%s 首次提交（deadline_at=%s），开始执行",
                        idempotency_key, stored.task_id, stored.deadline_at)
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

    # ---------------- 取消 ----------------

    def cancel(self, idempotency_key: str) -> tuple[Task, CancelOutcome]:
        """主动取消未收尾任务，可重复调用。

        - 未收尾：先持久化取消意图（重启后仍生效），再条件迁移到 CANCELLED；
        - 已取消：幂等返回 CANCELLED（结果一致）；
        - 已为其他终态：不覆盖既有结果，返回真实状态 + ALREADY_TERMINAL；
        - 与正常完成撞车：条件迁移保证唯一终态、先到者赢，
          取消方落败时返回实际终态。
        """
        task = self._store.get_by_key(idempotency_key)
        if task is None:
            raise TaskNotFoundError(idempotency_key)
        if task.status in TERMINAL_STATES:
            outcome = (CancelOutcome.CANCELLED
                       if task.status is TaskStatus.CANCELLED
                       else CancelOutcome.ALREADY_TERMINAL)
            logger.info("cancel key=%s 任务已收尾（%s），不覆盖既有结果 -> %s",
                        idempotency_key, task.status.value, outcome.value)
            return task, outcome
        # 先落库取消意图：即使进程在迁移前崩溃，重启后 sweep 仍会完成取消
        self._store.request_cancel(task.task_id)
        try:
            cancelled = self._store.transition(
                task.task_id, task.status, TaskStatus.CANCELLED,
                error="CANCELLED: cancelled by caller")
            logger.info("cancel key=%s 取消生效: %s -> CANCELLED",
                        idempotency_key, task.status.value)
            return cancelled, CancelOutcome.CANCELLED
        except IllegalTransitionError:
            # 撞车：任务被并发推进（完成/超时/另一次取消），以实际终态为准
            current = self._store.get_by_key(idempotency_key)
            assert current is not None
            outcome = (CancelOutcome.CANCELLED
                       if current.status is TaskStatus.CANCELLED
                       else CancelOutcome.ALREADY_TERMINAL)
            logger.info("cancel key=%s 与并发收尾撞车，实际终态=%s -> %s",
                        idempotency_key, current.status.value, outcome.value)
            return current, outcome

    # ---------------- 超时/取消清扫 ----------------

    def sweep(self) -> list[Task]:
        """清扫未收尾任务：落实已持久化的取消意图、收尾已超时的任务。

        进程重启后调用，保证重启前定下的时限按原截止时间继续生效、
        重启前发起的取消继续生效。已收尾任务不受影响。
        """
        now = self._clock()
        finalized: list[Task] = []
        for task in self._store.list_unfinished():
            if task.cancel_requested:
                finalized.append(self._finalize_cancelled(task))
            elif task.deadline_at is not None and now >= task.deadline_at:
                finalized.append(self._finalize_timed_out(task))
        if finalized:
            logger.info("sweep 收尾 %d 个任务: %s", len(finalized),
                        [(t.idempotency_key, t.status.value) for t in finalized])
        return finalized

    # ---------------- 执行 ----------------

    def run(self, task: Task) -> Task:
        """执行单个任务：状态机迁移 + 重试 + 熔断 + 超时/取消检查。"""
        try:
            task = self._store.transition(
                task.task_id, TaskStatus.PENDING, TaskStatus.RUNNING,
                inc_attempt=True)
        except IllegalTransitionError:
            # 已被其他执行者推进（如重启恢复、取消、超时），直接返回当前状态
            current = self._store.get_by_key(task.idempotency_key)
            assert current is not None
            logger.info("run key=%s 任务已处于 %s，跳过重复执行",
                        task.idempotency_key, current.status.value)
            return current

        for attempt in range(1, self._retry.max_attempts + 1):
            # 每次尝试前重读任务：确认未被取消、未超时、未被外部收尾，
            # 避免在取消/超时之后再次触发下游调用（不产生重复副作用）
            task = self._store.get_by_id(task.task_id) or task
            if task.status is not TaskStatus.RUNNING:
                logger.info("run key=%s 任务已被外部收尾为 %s，停止执行",
                            task.idempotency_key, task.status.value)
                return task
            if task.cancel_requested:
                return self._finalize_cancelled(task)
            if self._deadline_exceeded(task):
                return self._finalize_timed_out(task)
            try:
                self._breaker.before_call()
            except CircuitOpenError:
                reason = "CIRCUIT_OPEN: downstream circuit breaker is open"
                logger.warning("run key=%s 熔断拒绝: %s", task.idempotency_key, reason)
                return self._safe_transition(
                    task.task_id, TaskStatus.RUNNING, TaskStatus.CIRCUIT_REJECTED,
                    error=reason)
            try:
                result = self._client.call(task.task_id, task.payload)
            except RetryableError as exc:
                self._breaker.on_failure()
                logger.warning(
                    "run key=%s 第 %d 次尝试可重试失败: %s",
                    task.idempotency_key, attempt, exc)
                if self._deadline_exceeded(task):
                    return self._finalize_timed_out(task)
                if attempt < self._retry.max_attempts:
                    # 退避等待不超过剩余时限，到点后在循环顶部收尾为 TIMED_OUT
                    cap = (None if task.deadline_at is None
                           else task.deadline_at - self._clock())
                    self._retry.wait(attempt, cap=cap)
                    try:
                        task = self._store.transition(
                            task.task_id, TaskStatus.RUNNING, TaskStatus.RUNNING,
                            inc_attempt=True)
                    except IllegalTransitionError:
                        current = self._store.get_by_id(task.task_id)
                        assert current is not None
                        logger.info(
                            "run key=%s 重试自环未命中，任务已被并发收尾为 %s",
                            task.idempotency_key, current.status.value)
                        return current
                    continue
                reason = f"RETRY_EXHAUSTED: {exc}"
                logger.warning("run key=%s 重试耗尽: %s", task.idempotency_key, reason)
                return self._safe_transition(
                    task.task_id, TaskStatus.RUNNING, TaskStatus.FAILED, error=reason)
            except NonRetryableError as exc:
                reason = f"NON_RETRYABLE: {exc}"
                logger.warning("run key=%s 不可重试失败: %s",
                               task.idempotency_key, reason)
                return self._safe_transition(
                    task.task_id, TaskStatus.RUNNING, TaskStatus.FAILED, error=reason)
            else:
                self._breaker.on_success()
                if self._deadline_exceeded(task):
                    # 下游调用在时限后才返回：按超时收尾，不记成功、不再重试
                    return self._finalize_timed_out(task)
                logger.info("run key=%s 第 %d 次尝试成功", task.idempotency_key, attempt)
                return self._safe_transition(
                    task.task_id, TaskStatus.RUNNING, TaskStatus.SUCCEEDED,
                    result=result)
        raise AssertionError("unreachable")

    # ---------------- 内部 ----------------

    def _deadline_exceeded(self, task: Task) -> bool:
        return (task.deadline_at is not None
                and self._clock() >= task.deadline_at)

    def _finalize_timed_out(self, task: Task) -> Task:
        reason = f"TIMEOUT: deadline {task.deadline_at} exceeded"
        logger.warning("run/sweep key=%s 超时收尾: %s",
                       task.idempotency_key, reason)
        return self._safe_transition(
            task.task_id, task.status, TaskStatus.TIMED_OUT, error=reason)

    def _finalize_cancelled(self, task: Task) -> Task:
        reason = "CANCELLED: cancelled by caller"
        logger.info("run/sweep key=%s 取消收尾", task.idempotency_key)
        return self._safe_transition(
            task.task_id, task.status, TaskStatus.CANCELLED, error=reason)

    def _safe_transition(self, task_id: str, expected: TaskStatus,
                         target: TaskStatus, **kwargs) -> Task:
        """条件迁移；若任务已被并发收尾（取消/超时/完成先到），返回实际状态。

        保证取消/超时与正常完成撞车时终态唯一、先到者赢，
        不出现结果丢失、状态回退或双终态。
        """
        try:
            return self._store.transition(task_id, expected, target, **kwargs)
        except IllegalTransitionError:
            current = self._store.get_by_id(task_id)
            assert current is not None
            logger.info(
                "task_id=%s 迁移 %s -> %s 未命中，并发收尾先到，当前状态=%s",
                task_id, expected.value, target.value, current.status.value)
            return current
