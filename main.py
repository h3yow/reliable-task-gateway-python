"""可靠任务网关 HTTP 入口。"""
from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.breaker import CircuitBreaker, CircuitOpenError
from app.downstream import RetryPolicy
from app.service import SubmitOutcome, TaskGateway
from app.store import PayloadConflictError, TaskStore

DB_PATH = os.environ.get("GATEWAY_DB", "gateway.db")

app = FastAPI(title="Reliable Task Gateway")


class SubmitRequest(BaseModel):
    idempotency_key: str
    payload: str


def _task_view(task) -> dict:
    return {
        "task_id": task.task_id,
        "idempotency_key": task.idempotency_key,
        "status": task.status.value,
        "attempts": task.attempts,
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
        task, outcome = gateway.submit(req.idempotency_key, req.payload)
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


@app.on_event("startup")
def _startup() -> None:
    store = TaskStore(DB_PATH)
    from main import default_client  # 下游客户端，可替换
    app.state.store = store
    app.state.gateway = TaskGateway(
        store=store,
        client=default_client(),
        breaker=CircuitBreaker(),
        retry=RetryPolicy(),
    )


@app.on_event("shutdown")
def _shutdown() -> None:
    app.state.store.close()


def default_client():
    from app.downstream import NonRetryableError
    raise NonRetryableError("no downstream client configured")
