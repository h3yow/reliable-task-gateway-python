import logging

from fastapi.testclient import TestClient

from app.downstream import RetryableError
from app.main import create_app

from .conftest import ScriptedDownstream

logger = logging.getLogger("test.http")


def _client(downstream, tmp_path, **kw):
    from app.breaker import CircuitBreaker
    breaker = CircuitBreaker(failure_threshold=kw.get("failure_threshold", 3),
                             recovery_timeout=kw.get("recovery_timeout", 30.0))
    app = create_app(db_path=str(tmp_path / "http.db"),
                     downstream=downstream, breaker=breaker,
                     max_retries=kw.get("max_retries", 3))
    return TestClient(app, raise_server_exceptions=False)


def test_submit_duplicate_and_conflict_over_http(tmp_path):
    downstream = ScriptedDownstream()
    with _client(downstream, tmp_path) as c:
        r1 = c.post("/tasks", json={"idempotency_key": "hk", "payload": {"a": 1}})
        assert r1.status_code == 200
        assert r1.json()["status"] == "SUCCEEDED"

        # 重复提交：200 + 与首次一致的响应体
        r2 = c.post("/tasks", json={"idempotency_key": "hk", "payload": {"a": 1}})
        assert r2.status_code == 200 and r2.json() == r1.json()

        # 冲突载荷：409，与重复提交明确区分
        r3 = c.post("/tasks", json={"idempotency_key": "hk", "payload": {"a": 2}})
        assert r3.status_code == 409
        assert "different payload" in r3.json()["detail"]

        # 查询接口返回同一任务
        assert c.get("/tasks/hk").json()["task_id"] == r1.json()["task_id"]
        assert c.get("/tasks/unknown").status_code == 404
        assert downstream.calls == 1
    logger.info("key=hk http duplicate=200 conflict=409 calls=%d", downstream.calls)


def test_failure_reasons_distinguishable_over_http(tmp_path):
    downstream = ScriptedDownstream([RetryableError("down")] * 100)
    with _client(downstream, tmp_path, failure_threshold=1, max_retries=1) as c:
        r1 = c.post("/tasks", json={"idempotency_key": "e1", "payload": {}})
        assert r1.json()["error"].startswith("retry_exhausted:")
        r2 = c.post("/tasks", json={"idempotency_key": "e2", "payload": {}})
        assert r2.json()["error"].startswith("circuit_open:")
        assert c.get("/health").json()["breaker"] == "OPEN"
    logger.info("http reasons distinguishable: retry_exhausted vs circuit_open")


def test_service_logs_key_transition_and_rationale(tmp_path, caplog):
    """单测日志须包含业务键、状态迁移与判定依据。"""
    downstream = ScriptedDownstream([RetryableError("boom")])
    with _client(downstream, tmp_path) as c, \
            caplog.at_level(logging.INFO, logger="gateway.service"):
        c.post("/tasks", json={"idempotency_key": "log-key", "payload": {}})

    text = caplog.text
    assert "log-key" in text, "日志须包含业务键"
    assert "PENDING->RUNNING" in text and "RUNNING->SUCCEEDED" in text, \
        "日志须包含状态迁移"
    assert "reason=" in text, "日志须包含判定依据"
    assert "retryable failure" in text
