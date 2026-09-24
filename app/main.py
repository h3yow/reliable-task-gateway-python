"""HTTP 接口层：提交 / 查询任务。"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.breaker import CircuitBreaker
from app.downstream import NonRetryableError, RetryableError
from app.models import Task, TaskStatus
from app.service import TaskGateway
from app.store import PayloadConflictError, TaskStore


class SubmitRequest(BaseModel):
    idempotency_key: str
    payload: dict


class TaskResponse(BaseModel):
    task_id: str
    idempotency_key: str
    status: str
    result: dict | None
    error: str | None
    attempts: int

    @classmethod
    def of(cls, t: Task) -> "TaskResponse":
        return cls(task_id=t.task_id, idempotency_key=t.idempotency_key,
                   status=t.status.value, result=t.result, error=t.error,
                   attempts=t.attempts)


class HttpDownstream:
    """默认下游适配器：5xx/连接错误 -> 可重试，4xx -> 不可重试。"""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    def call(self, payload: dict) -> dict:
        import urllib.error
        import urllib.request

        req = urllib.request.Request(
            self.base_url, data=__import__("json").dumps(payload).encode(),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return __import__("json").loads(resp.read())
        except urllib.error.HTTPError as e:
            if 400 <= e.code < 500:
                raise NonRetryableError(f"downstream {e.code}") from e
            raise RetryableError(f"downstream {e.code}") from e
        except urllib.error.URLError as e:
            raise RetryableError(f"connection: {e.reason}") from e


def create_app(db_path: str | None = None,
               downstream=None,
               breaker: CircuitBreaker | None = None,
               max_retries: int = 3) -> FastAPI:
    db_path = db_path or os.environ.get("GATEWAY_DB", "gateway.db")
    downstream = downstream or HttpDownstream(
        os.environ.get("DOWNSTREAM_URL", "http://localhost:9999/tasks"))
    gateway = TaskGateway(
        TaskStore(db_path), downstream,
        breaker or CircuitBreaker(
            failure_threshold=int(os.environ.get("BREAKER_THRESHOLD", "3")),
            recovery_timeout=float(os.environ.get("BREAKER_RECOVERY", "30")),
        ),
        max_retries=max_retries,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        gateway.recover()  # 重启恢复在途任务
        yield

    app = FastAPI(title="reliable-task-gateway", lifespan=lifespan)
    app.state.gateway = gateway

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "breaker": gateway.breaker.state.value}

    @app.post("/tasks", response_model=TaskResponse, status_code=200)
    def submit(req: SubmitRequest) -> TaskResponse:
        try:
            task = gateway.submit(req.idempotency_key, req.payload)
        except PayloadConflictError as e:
            raise HTTPException(status_code=409, detail=str(e)) from e
        return TaskResponse.of(task)

    @app.get("/tasks/{idempotency_key}", response_model=TaskResponse)
    def get_task(idempotency_key: str) -> TaskResponse:
        task = gateway.store.get_by_key(idempotency_key)
        if task is None:
            raise HTTPException(status_code=404, detail="task not found")
        return TaskResponse.of(task)

    return app


app = create_app()
