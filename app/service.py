"""任务网关服务：幂等提交、状态机执行、重试与熔断编排、超时与取消。"""
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
from app.models import IllegalTransitionError, Task, TaskStatus
from app.store import TaskStore

logger = logging.getLogger("gateway.service")


class SubmitOutcome(str, enum.Enum):
    CREATED = "CREATED"        # 首次提交
    DUPLICATE = "DUPLICATE"    # 重复提交（幂等命中，返回首次结果）


class CancelOutcome(str, enum.Enum):
    CANCELLED = "CANCELLED"                # 已取消（本次取消或此前已取消，幂等一致）
    ALREADY_FINISHED = "ALREADY_FINISHED"  # 任务已达其他终态，取消未生效，返回真实状态


class TaskGateway:
    def __init__(self, store: TaskStore, client: DownstreamClient,
                 breaker: CircuitBreaker, retry: RetryPolicy,
                 clock=time.time) -> None:
        self._store = store
        self._client = client
        self._breaker = breaker
        self._retry = retry
        self._clock = clock  # 墙钟，用于截止时间判定（可注入便于测试）
        self._done_events: dict[str, threading.Event] = {}
        self._events_lock = threading.Lock()
        self._sweeper_lock = threading.Lock()
        self._sweeper_thread: threading.Thread | None = None
        self._sweeper_stop = threading.Event()

    # ---------------- 提交 ----------------

    def submit(self, idempotency_key: str, payload: str,
               timeout_seconds: float | None = None) -> tuple[Task, SubmitOutcome]:
        """幂等提交：同键同载荷返回首次结果；同键不同载荷抛 PayloadConflictError。

        timeout_seconds 为可选最长执行时限（秒）；到点未完成的任务被收尾为
        终态 TIMED_OUT。不传时 deadline_at 为 NULL，行为与老的提交方式完全一致。
        截止时间为绝对时间戳并持久化，进程重启后按原时间继续计算。
        """
        deadline_at = None if timeout_seconds is None else self._clock() + timeout_seconds
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

    def cancel(self, idempotency_key: str) -> tuple[Task, CancelOutcome] | None:
        """主动取消未完成任务。幂等：重复取消结果一致（CANCELLED）。

        - 任务已达其他终态（成功/失败/熔断拒绝/超时）：不覆盖已有结果，
          返回 (真实当前任务, ALREADY_FINISHED)；
        - 取消与并发完成/超时撞车：条件 UPDATE 保证唯一胜者，
          取消失败方返回真实当前状态；
        - 任务不存在返回 None。
        """
        task = self._store.get_by_key(idempotency_key)
        if task is None:
            return None
        if task.status is TaskStatus.CANCELLED:
            logger.info("cancel key=%s 幂等命中：任务已处于 CANCELLED", idempotency_key)
            return task, CancelOutcome.CANCELLED
        if task.status in _TERMINAL:
            logger.info("cancel key=%s 任务已达终态 %s，取消不生效、不覆盖结果",
                        idempotency_key, task.status.value)
            return task, CancelOutcome.ALREADY_FINISHED
        try:
            cancelled = self._store.transition(
                task.task_id, task.status, TaskStatus.CANCELLED,
                error="CANCELLED: cancelled by caller")
            logger.info("cancel key=%s 状态迁移 %s -> CANCELLED",
                        idempotency_key, task.status.value)
            return cancelled, CancelOutcome.CANCELLED
        except IllegalTransitionError:
            # 与并发完成/超时撞车且落败：返回真实当前状态，不假装取消成功
            current = self._store.get_by_key(idempotency_key)
            assert current is not None
            if current.status is TaskStatus.CANCELLED:
                return current, CancelOutcome.CANCELLED
            logger.info("cancel key=%s 与并发收尾撞车落败，当前状态=%s",
                        idempotency_key, current.status.value)
            return current, CancelOutcome.ALREADY_FINISHED

    # ---------------- 超时 ----------------

    def sweep_timeouts(self) -> list[Task]:
        """扫描并收尾所有已超时的非终态任务（PENDING/RUNNING -> TIMED_OUT）。

        与并发完成/取消的竞争由条件 UPDATE 仲裁，保证唯一终态；已真正发出的
        下游调用不会被重新触发（TIMED_OUT 为终态，执行循环不会再次派发）。
        """
        now = self._clock()
        finalized = []
        for task in self._store.find_expired(now):
            try:
                t = self._store.transition(
                    task.task_id, task.status, TaskStatus.TIMED_OUT,
                    error=f"TIMEOUT: deadline {task.deadline_at} exceeded")
                logger.warning("sweep key=%s 超时收尾 %s -> TIMED_OUT (deadline_at=%s)",
                               task.idempotency_key, task.status.value, task.deadline_at)
                finalized.append(t)
            except IllegalTransitionError:
                # 已被并发路径（完成/取消）推进为终态，条件更新保证唯一胜者
                logger.info("sweep key=%s 超时收尾与并发收尾撞车落败，保持已有终态",
                            task.idempotency_key)
        return finalized

    def start_timeout_sweeper(self, interval_seconds: float = 0.5) -> None:
        """启动后台超时清扫线程（幂等，重复调用不重复启动）。"""
        with self._sweeper_lock:
            if self._sweeper_thread is not None:
                return
            self._sweeper_stop.clear()

            def loop() -> None:
                while not self._sweeper_stop.wait(interval_seconds):
                    self.sweep_timeouts()

            self._sweeper_thread = threading.Thread(
                target=loop, name="timeout-sweeper", daemon=True)
            self._sweeper_thread.start()
            logger.info("超时清扫线程已启动（间隔 %.3fs）", interval_seconds)

    def stop_timeout_sweeper(self) -> None:
        with self._sweeper_lock:
            if self._sweeper_thread is None:
                return
            self._sweeper_stop.set()
            self._sweeper_thread.join(timeout=5.0)
            self._sweeper_thread = None

    # ---------------- 执行 ----------------

    def _current(self, task: Task) -> Task:
        current = self._store.get_by_key(task.idempotency_key)
        assert current is not None
        return current

    def _try_transition(self, task: Task, expected: TaskStatus,
                        target: TaskStatus, **kwargs) -> Task | None:
        """条件迁移；与并发取消/超时撞车落败时返回 None（调用方回读真实状态）。"""
        try:
            return self._store.transition(task.task_id, expected, target, **kwargs)
        except IllegalTransitionError:
            return None

    def _deadline_exceeded(self, task: Task) -> bool:
        return task.deadline_at is not None and self._clock() >= task.deadline_at

    def run(self, task: Task) -> Task:
        """执行单个任务：状态机迁移 + 重试 + 熔断 + 时限检查。"""
        try:
            task = self._store.transition(
                task.task_id, TaskStatus.PENDING, TaskStatus.RUNNING, inc_attempt=True)
        except IllegalTransitionError:
            # 已被其他执行者推进（如并发取消、重启恢复场景），直接返回当前状态
            current = self._current(task)
            logger.info("run key=%s 任务已处于 %s，跳过重复执行",
                        task.idempotency_key, current.status.value)
            return current

        for attempt in range(1, self._retry.max_attempts + 1):
            # 时限检查：到点不再派发新的下游调用，避免超时后产生新的副作用
            if self._deadline_exceeded(task):
                reason = f"TIMEOUT: deadline {task.deadline_at} exceeded"
                logger.warning("run key=%s 到达执行时限，收尾为 TIMED_OUT",
                               task.idempotency_key)
                finalized = self._try_transition(
                    task, TaskStatus.RUNNING, TaskStatus.TIMED_OUT, error=reason)
                return finalized if finalized is not None else self._current(task)
            try:
                self._breaker.before_call()
            except CircuitOpenError:
                reason = "CIRCUIT_OPEN: downstream circuit breaker is open"
                logger.warning("run key=%s 熔断拒绝: %s", task.idempotency_key, reason)
                finalized = self._try_transition(
                    task, TaskStatus.RUNNING, TaskStatus.CIRCUIT_REJECTED, error=reason)
                return finalized if finalized is not None else self._current(task)
            try:
                result = self._client.call(task.task_id, task.payload)
            except RetryableError as exc:
                self._breaker.on_failure()
                logger.warning(
                    "run key=%s 第 %d 次尝试可重试失败: %s",
                    task.idempotency_key, attempt, exc)
                if attempt < self._retry.max_attempts:
                    self._retry.wait(attempt)
                    # 退避结束后先到点：直接超时收尾，不再派发新的下游尝试
                    if self._deadline_exceeded(task):
                        reason = f"TIMEOUT: deadline {task.deadline_at} exceeded"
                        logger.warning("run key=%s 退避期间到达时限，收尾为 TIMED_OUT",
                                       task.idempotency_key)
                        finalized = self._try_transition(
                            task, TaskStatus.RUNNING, TaskStatus.TIMED_OUT, error=reason)
                        return finalized if finalized is not None else self._current(task)
                    # 重试自环；若期间被取消/超时收尾，则停止执行，
                    # 已发出的调用绝不因超时被再次触发
                    nxt = self._try_transition(
                        task, TaskStatus.RUNNING, TaskStatus.RUNNING, inc_attempt=True)
                    if nxt is None:
                        current = self._current(task)
                        logger.info("run key=%s 重试前任务已被并发收尾为 %s，停止执行",
                                    task.idempotency_key, current.status.value)
                        return current
                    task = nxt
                    continue
                reason = f"RETRY_EXHAUSTED: {exc}"
                logger.warning("run key=%s 重试耗尽: %s", task.idempotency_key, reason)
                finalized = self._try_transition(
                    task, TaskStatus.RUNNING, TaskStatus.FAILED, error=reason)
                return finalized if finalized is not None else self._current(task)
            except NonRetryableError as exc:
                reason = f"NON_RETRYABLE: {exc}"
                logger.warning("run key=%s 不可重试失败: %s",
                               task.idempotency_key, reason)
                finalized = self._try_transition(
                    task, TaskStatus.RUNNING, TaskStatus.FAILED, error=reason)
                return finalized if finalized is not None else self._current(task)
            else:
                self._breaker.on_success()
                logger.info("run key=%s 第 %d 次尝试成功", task.idempotency_key, attempt)
                finalized = self._try_transition(
                    task, TaskStatus.RUNNING, TaskStatus.SUCCEEDED, result=result)
                if finalized is None:
                    # 与取消/超时撞车落败：下游结果已拿到但任务已被收尾，
                    # 不覆盖终态，返回真实当前状态（唯一终态由条件更新仲裁）
                    current = self._current(task)
                    logger.info("run key=%s 成功收尾与并发收尾撞车落败，保持 %s",
                                task.idempotency_key, current.status.value)
                    return current
                return finalized
        raise AssertionError("unreachable")


_TERMINAL = frozenset(
    {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CIRCUIT_REJECTED,
     TaskStatus.TIMED_OUT}
)
