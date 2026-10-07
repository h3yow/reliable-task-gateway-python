"""可靠任务网关 HTTP 入口。"""
from __future__ import annotations

import logging
import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.breaker import CircuitBreaker
from app.downstream import RetryPolicy
from app.service import SubmitOutcome, TaskGateway, TaskNotFoundError
from app.store import PayloadConflictError, TaskStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

DB_PATH = os.environ.get("GATEWAY_DB", "gateway.db")

app = FastAPI(title="Reliable Task Gateway")


class EchoClient:
    """默认下游客户端：回显载荷（生产环境可替换为真实下游）。"""

    def call(self, task_id: str, payload: str) -> str:
        return f"echo:{payload}"


class SubmitRequest(BaseModel):
    idempotency_key: str
    payload: str
    # 可选：最长执行时限（秒）。不填则不限时，行为与旧版一致
    timeout_seconds: float | None = Field(default=None, gt=0)


def _task_view(task) -> dict:
    return {
        "task_id": task.task_id,
        "idempotency_key": task.idempotency_key,
        "status": task.status.value,
        "attempts": task.attempts,
        "result": task.result,
        "error": task.error,
        "deadline_at": task.deadline_at,
        "cancel_requested": task.cancel_requested,
    }


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.post("/tasks", status_code=201)
def submit_task(req: SubmitRequest):
    gateway: TaskGateway = app.state.gateway
    try:
        task, outcome = gateway.submit(req.idempotency_key, req.payload,
                                       timeout_seconds=req.timeout_seconds)
    except PayloadConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"reason": "PAYLOAD_CONFLICT", "message": str(exc)},
        )
    body = _task_view(task)
    body["outcome"] = outcome.value
    return body


@app.get("/tasks/{idempotency_key}")
def get_task(idempotency_key: str):
    gateway: TaskGateway = app.state.gateway
    task = gateway.get(idempotency_key)
    if task is None:
        raise HTTPException(status_code=404, detail={"reason": "NOT_FOUND"})
    return _task_view(task)


@app.post("/tasks/{idempotency_key}/cancel")
def cancel_task(idempotency_key: str):
    gateway: TaskGateway = app.state.gateway
    try:
        task, outcome = gateway.cancel(idempotency_key)
    except TaskNotFoundError:
        raise HTTPException(status_code=404, detail={"reason": "NOT_FOUND"})
    body = _task_view(task)
    body["cancel_outcome"] = outcome.value
    return body


@app.on_event("startup")
def _startup() -> None:
    store = TaskStore(DB_PATH)
    app.state.store = store
    app.state.gateway = TaskGateway(
        store=store,
        client=EchoClient(),
        breaker=CircuitBreaker(failure_threshold=3, recovery_timeout=30.0),
        retry=RetryPolicy(max_attempts=3, base_delay=0.1),
    )
    # 重启恢复：落实重启前持久化的取消意图、按原截止时间收尾超时任务
    app.state.gateway.sweep()


@app.on_event("shutdown")
def _shutdown() -> None:
    app.state.store.close()
