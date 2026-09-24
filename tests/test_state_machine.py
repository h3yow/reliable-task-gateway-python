import pytest

from app.models import IllegalTransitionError, TaskStatus, check_transition


def test_legal_transitions():
    check_transition(TaskStatus.PENDING, TaskStatus.RUNNING)
    check_transition(TaskStatus.PENDING, TaskStatus.FAILED)
    check_transition(TaskStatus.RUNNING, TaskStatus.SUCCEEDED)
    check_transition(TaskStatus.RUNNING, TaskStatus.FAILED)


@pytest.mark.parametrize("current,target", [
    (TaskStatus.PENDING, TaskStatus.SUCCEEDED),
    (TaskStatus.SUCCEEDED, TaskStatus.RUNNING),
    (TaskStatus.SUCCEEDED, TaskStatus.FAILED),
    (TaskStatus.FAILED, TaskStatus.PENDING),
    (TaskStatus.FAILED, TaskStatus.RUNNING),
])
def test_illegal_transitions_rejected(current, target, caplog):
    with caplog.at_level("INFO"):
        with pytest.raises(IllegalTransitionError):
            check_transition(current, target)
    print(f"rejected {current.value}->{target.value}: terminal/illegal transition")
