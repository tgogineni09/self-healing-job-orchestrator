"""Unit tests for the scheduling and self-healing engine (no third-party deps)."""
from datetime import datetime, timedelta

import pytest

from app.core import InMemoryStore, Job, Orchestrator, Status
from app.tasks import TASKS

T0 = datetime(2026, 1, 1, 12, 0, 0)


class RecordingMetrics:
    def __init__(self):
        self.finished = []
        self.retried = 0
        self.recovered = 0
        self.blocked = 0
        self.recover_times = []
        self.status_counts = {}

    def job_finished(self, result, duration_seconds):
        self.finished.append(result)

    def job_retried(self):
        self.retried += 1

    def job_recovered(self):
        self.recovered += 1

    def job_blocked(self):
        self.blocked += 1

    def time_to_recover(self, seconds):
        self.recover_times.append(seconds)

    def set_status_counts(self, counts):
        self.status_counts = counts


@pytest.fixture
def setup():
    store = InMemoryStore()
    metrics = RecordingMetrics()
    orch = Orchestrator(store, TASKS, metrics=metrics, backoff_base_seconds=2, stranded_timeout_seconds=300)
    return store, metrics, orch


def test_simple_job_succeeds(setup):
    store, metrics, orch = setup
    job = store.add(Job(name="hello", task="echo", params={"message": "hi"}, next_run_at=T0))
    result = orch.tick(T0)
    assert result["executed"] == 1
    assert store.get(job.id).status == Status.SUCCEEDED
    assert metrics.finished == ["success"]


def test_transient_failure_is_retried_with_exponential_backoff(setup):
    store, metrics, orch = setup
    job = store.add(Job(name="flaky", task="flaky", params={"fail_times": 2}, next_run_at=T0))

    orch.tick(T0)  # attempt 1 fails -> retry in 2s
    j = store.get(job.id)
    assert j.status == Status.RETRYING and j.attempts == 1
    assert j.next_run_at == T0 + timedelta(seconds=2)

    orch.tick(T0 + timedelta(seconds=1))  # not due yet
    assert store.get(job.id).attempts == 1

    orch.tick(T0 + timedelta(seconds=2))  # attempt 2 fails -> retry in 4s
    j = store.get(job.id)
    assert j.status == Status.RETRYING and j.attempts == 2
    assert j.next_run_at == T0 + timedelta(seconds=2 + 4)

    orch.tick(T0 + timedelta(seconds=6))  # attempt 3 succeeds
    j = store.get(job.id)
    assert j.status == Status.SUCCEEDED and j.attempts == 3
    assert metrics.retried == 2
    assert metrics.finished == ["failure", "failure", "success"]
    assert metrics.recover_times == [6.0]  # first failure at T0, success at T0+6s


def test_job_fails_permanently_after_max_retries(setup):
    store, metrics, orch = setup
    job = store.add(Job(name="bad", task="always_fail", max_retries=2, next_run_at=T0))
    now = T0
    for _ in range(10):
        orch.tick(now)
        now += timedelta(seconds=60)
    j = store.get(job.id)
    assert j.status == Status.FAILED
    assert j.attempts == 3  # 1 initial attempt + 2 retries
    assert "permanent failure" in j.last_error


def test_unknown_task_fails_immediately_without_retries(setup):
    store, _, orch = setup
    job = store.add(Job(name="x", task="does_not_exist", next_run_at=T0))
    orch.tick(T0)
    j = store.get(job.id)
    assert j.status == Status.FAILED and j.attempts == 1


def test_task_raising_keyerror_is_still_retried(setup):
    """KeyError is a LookupError subclass; it must not be mistaken for an unknown task."""
    store, _, _ = setup

    def broken(params, attempt):
        raise KeyError("missing field")

    orch = Orchestrator(store, {"broken": broken}, backoff_base_seconds=2)
    job = store.add(Job(name="k", task="broken", max_retries=1, next_run_at=T0))
    orch.tick(T0)
    assert store.get(job.id).status == Status.RETRYING


def test_dependency_runs_in_order_within_one_tick(setup):
    store, _, orch = setup
    parent = store.add(Job(name="parent", task="echo", next_run_at=T0))
    child = store.add(Job(name="child", task="echo", depends_on_id=parent.id, next_run_at=T0))
    orch.tick(T0)
    assert store.get(parent.id).status == Status.SUCCEEDED
    assert store.get(child.id).status == Status.SUCCEEDED


def test_child_waits_while_parent_is_retrying(setup):
    store, _, orch = setup
    parent = store.add(Job(name="parent", task="flaky", params={"fail_times": 1}, next_run_at=T0))
    child = store.add(Job(name="child", task="echo", depends_on_id=parent.id, next_run_at=T0))
    orch.tick(T0)
    assert store.get(parent.id).status == Status.RETRYING
    assert store.get(child.id).status == Status.PENDING  # still waiting, not blocked
    orch.tick(T0 + timedelta(seconds=2))
    assert store.get(parent.id).status == Status.SUCCEEDED
    assert store.get(child.id).status == Status.SUCCEEDED


def test_failed_dependency_blocks_children_then_manual_retry_unblocks(setup):
    store, metrics, orch = setup
    parent = store.add(Job(name="parent", task="always_fail", max_retries=0, next_run_at=T0))
    child = store.add(Job(name="child", task="echo", depends_on_id=parent.id, next_run_at=T0))
    grandchild = store.add(Job(name="grandchild", task="echo", depends_on_id=child.id, next_run_at=T0))

    orch.tick(T0)
    assert store.get(parent.id).status == Status.FAILED
    orch.tick(T0)
    assert store.get(child.id).status == Status.BLOCKED
    assert store.get(grandchild.id).status == Status.BLOCKED
    assert metrics.blocked == 2

    # Fix the root cause: swap in a task that succeeds, then retry the parent.
    p = store.get(parent.id)
    p.task = "echo"
    store.save(p)
    orch.retry_now(parent.id, now=T0)
    orch.tick(T0)  # parent succeeds, children are unblocked and run in the same tick
    orch.tick(T0)
    assert store.get(parent.id).status == Status.SUCCEEDED
    assert store.get(child.id).status == Status.SUCCEEDED
    assert store.get(grandchild.id).status == Status.SUCCEEDED


def test_stranded_running_job_is_recovered_and_rerun(setup):
    store, metrics, orch = setup
    job = store.add(Job(name="stuck", task="echo", next_run_at=T0))
    j = store.get(job.id)
    j.status = Status.RUNNING  # simulate a worker that crashed mid-run
    j.started_at = T0
    store.save(j)

    orch.tick(T0 + timedelta(seconds=100))  # inside the timeout: left alone
    assert store.get(job.id).status == Status.RUNNING

    result = orch.tick(T0 + timedelta(seconds=301))
    j = store.get(job.id)
    assert result["recovered"] == 1
    assert j.recovered_count == 1
    assert j.status == Status.SUCCEEDED  # recovered, then re-run in the same tick
    assert metrics.recovered == 1


def test_retry_now_only_applies_to_failed_or_blocked(setup):
    store, _, orch = setup
    job = store.add(Job(name="ok", task="echo", next_run_at=T0))
    orch.tick(T0)
    assert orch.retry_now(job.id).status == Status.SUCCEEDED  # unchanged
    assert orch.retry_now(999) is None


def test_status_counts_cover_every_status(setup):
    store, metrics, orch = setup
    store.add(Job(name="a", task="echo", next_run_at=T0))
    store.add(Job(name="b", task="always_fail", max_retries=0, next_run_at=T0))
    orch.tick(T0)
    counts = metrics.status_counts
    assert counts["SUCCEEDED"] == 1 and counts["FAILED"] == 1
    assert set(counts) == {s.value for s in Status}
