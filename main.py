"""可靠任务网关 HTTP 入口。"""
from __future__ import annotations

import logging
import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.breaker import CircuitBreaker
from app.downstream import RetryPolicy
from app.service import SubmitOutcome, TaskGateway
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
    timeout_seconds: float | None = None  # 可选最长执行时限；不传则行为同老版本


def _task_view(task) -> dict:
    return {
        "task_id": task.task_id,
        "idempotency_key": task.idempotency_key,
        "status": task.status.value,
        "attempts": task.attempts,
        "deadline_at": task.deadline_at,
        "result": task.result,
        "error": task.error,
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


@app.post("/tasks/{idempotency_key}/cancel")
def cancel_task(idempotency_key: str):
    gateway: TaskGateway = app.state.gateway
    result = gateway.cancel(idempotency_key)
    if result is None:
        raise HTTPException(status_code=404, detail={"reason": "NOT_FOUND"})
    task, outcome = result
    body = _task_view(task)
    body["cancel_outcome"] = outcome.value
    return body


@app.get("/tasks/{idempotency_key}")
def get_task(idempotency_key: str):
    gateway: TaskGateway = app.state.gateway
    task = gateway.get(idempotency_key)
    if task is None:
        raise HTTPException(status_code=404, detail={"reason": "NOT_FOUND"})
    return _task_view(task)


@app.on_event("startup")
def _startup() -> None:
    store = TaskStore(DB_PATH)
    app.state.store = store
    gateway = TaskGateway(
        store=store,
        client=EchoClient(),
        breaker=CircuitBreaker(failure_threshold=3, recovery_timeout=30.0),
        retry=RetryPolicy(max_attempts=3, base_delay=0.1),
    )
    gateway.start_timeout_sweeper(interval_seconds=0.5)
    app.state.gateway = gateway


@app.on_event("shutdown")
def _shutdown() -> None:
    app.state.gateway.stop_timeout_sweeper()
    app.state.store.close()
