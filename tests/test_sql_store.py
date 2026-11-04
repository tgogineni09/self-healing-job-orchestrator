"""SQL store tests (skipped automatically if SQLAlchemy is not installed)."""
from datetime import datetime, timedelta

import pytest

pytest.importorskip("sqlalchemy")

from app.core import Job, Orchestrator, Status  # noqa: E402
from app.sql_store import SqlJobStore  # noqa: E402
from app.tasks import TASKS  # noqa: E402

T0 = datetime(2026, 1, 1, 12, 0, 0)


@pytest.fixture
def store():
    return SqlJobStore("sqlite:///:memory:")


def test_round_trip(store):
    job = store.add(Job(name="a", task="echo", params={"message": "hi"}, next_run_at=T0))
    loaded = store.get(job.id)
    assert loaded.name == "a" and loaded.params == {"message": "hi"}
    assert loaded.status == Status.PENDING


def test_list_filters_by_status(store):
    a = store.add(Job(name="a", task="echo", next_run_at=T0))
    store.add(Job(name="b", task="echo", next_run_at=T0))
    a.status = Status.SUCCEEDED
    store.save(a)
    assert [j.name for j in store.list([Status.SUCCEEDED])] == ["a"]
    assert [j.name for j in store.list([Status.PENDING])] == ["b"]
    assert len(store.list()) == 2


def test_orchestrator_retry_flow_on_sql_store(store):
    orch = Orchestrator(store, TASKS, backoff_base_seconds=2)
    job = store.add(Job(name="flaky", task="flaky", params={"fail_times": 1}, next_run_at=T0))
    orch.tick(T0)
    assert store.get(job.id).status == Status.RETRYING
    orch.tick(T0 + timedelta(seconds=2))
    assert store.get(job.id).status == Status.SUCCEEDED
