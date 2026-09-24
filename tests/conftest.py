import logging

import pytest

from app.breaker import CircuitBreaker
from app.service import TaskGateway
from app.store import TaskStore


class ScriptedDownstream:
    """按脚本行为的下游替身：每次调用弹出下一个行为。"""

    def __init__(self, behaviors=()):
        self.behaviors = list(behaviors)
        self.calls = 0
        self.lock = __import__("threading").Lock()

    def call(self, payload: dict) -> dict:
        with self.lock:
            self.calls += 1
            behavior = self.behaviors.pop(0) if self.behaviors else True
        if isinstance(behavior, Exception):
            raise behavior
        return {"echo": payload} if behavior is True else behavior


@pytest.fixture()
def store(tmp_path):
    s = TaskStore(str(tmp_path / "tasks.db"))
    yield s
    s.close()


@pytest.fixture()
def make_gateway(store):
    def _make(downstream, *, max_retries=3, failure_threshold=3,
              recovery_timeout=30.0, clock=None):
        breaker = CircuitBreaker(failure_threshold, recovery_timeout,
                                 clock=clock) if clock else CircuitBreaker(
            failure_threshold, recovery_timeout)
        return TaskGateway(store, downstream, breaker,
                           max_retries=max_retries, retry_backoff=0.001)
    return _make


@pytest.fixture(autouse=True)
def _logcap():
    logging.getLogger("gateway.service").setLevel(logging.INFO)
