"""API tests (skipped automatically if FastAPI / Prometheus client are not installed)."""
import pytest

pytest.importorskip("fastapi")
pytest.importorskip("prometheus_client")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient  # noqa: E402

from app.core import InMemoryStore  # noqa: E402
from app.main import create_app  # noqa: E402


@pytest.fixture(params=["memory", "sql"])
def store(request):
    if request.param == "memory":
        return InMemoryStore()
    from app.sql_store import SqlJobStore

    return SqlJobStore("sqlite:///:memory:")


@pytest.fixture
def client(store):
    app = create_app(store=store, start_worker=False)
    with TestClient(app) as c:
        yield c


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_create_and_run_job(client):
    r = client.post("/jobs", json={"name": "hello", "task": "echo", "params": {"message": "hi"}})
    assert r.status_code == 201
    job_id = r.json()["id"]
    assert client.post("/admin/tick").json()["executed"] == 1
    assert client.get(f"/jobs/{job_id}").json()["status"] == "SUCCEEDED"


def test_unknown_task_and_missing_dependency_are_rejected(client):
    assert client.post("/jobs", json={"name": "x", "task": "nope"}).status_code == 422
    r = client.post("/jobs", json={"name": "x", "task": "echo", "depends_on_id": 99})
    assert r.status_code == 422


def test_get_missing_job_is_404(client):
    assert client.get("/jobs/12345").status_code == 404


def test_failed_job_can_be_retried_via_api(client):
    r = client.post("/jobs", json={"name": "bad", "task": "always_fail", "max_retries": 0})
    job_id = r.json()["id"]
    client.post("/admin/tick")
    assert client.get(f"/jobs/{job_id}").json()["status"] == "FAILED"
    assert client.post(f"/jobs/{job_id}/retry").json()["status"] == "PENDING"


def test_retrying_a_healthy_job_returns_409(client):
    job_id = client.post("/jobs", json={"name": "ok", "task": "echo"}).json()["id"]
    client.post("/admin/tick")
    assert client.post(f"/jobs/{job_id}/retry").status_code == 409


def test_transient_failure_is_scheduled_for_retry(client):
    job_id = client.post(
        "/jobs", json={"name": "flaky", "task": "flaky", "params": {"fail_times": 1}}
    ).json()["id"]
    client.post("/admin/tick")
    body = client.get(f"/jobs/{job_id}").json()
    assert body["status"] == "RETRYING"
    assert body["attempts"] == 1
    assert "transient failure" in body["last_error"]


def test_stranded_job_is_recovered_through_the_api(store):
    from datetime import timedelta

    from app.core import Status, utcnow

    app = create_app(store=store, start_worker=False)
    with TestClient(app) as client:
        job_id = client.post("/jobs", json={"name": "stuck", "task": "echo"}).json()["id"]
        job = store.get(job_id)
        job.status = Status.RUNNING  # simulate a worker that crashed mid-run
        job.started_at = utcnow() - timedelta(hours=1)
        store.save(job)

        assert client.post("/admin/tick").json()["recovered"] == 1
        body = client.get(f"/jobs/{job_id}").json()
        assert body["status"] == "SUCCEEDED" and body["recovered_count"] == 1
        assert "orchestrator_jobs_recovered_total 1.0" in client.get("/metrics").text


def test_failed_parent_blocks_child_through_the_api(client):
    parent = client.post("/jobs", json={"name": "p", "task": "always_fail", "max_retries": 0}).json()
    child = client.post("/jobs", json={"name": "c", "task": "echo", "depends_on_id": parent["id"]}).json()
    client.post("/admin/tick")  # parent fails
    client.post("/admin/tick")  # child is blocked
    assert client.get(f"/jobs/{child['id']}").json()["status"] == "BLOCKED"
    assert client.get("/stats").json()["BLOCKED"] == 1


def test_metrics_endpoint_exposes_orchestrator_metrics(client):
    client.post("/jobs", json={"name": "ok", "task": "echo"})
    client.post("/admin/tick")
    body = client.get("/metrics").text
    assert "orchestrator_jobs_executed_total" in body
    assert 'orchestrator_jobs_by_status{status="SUCCEEDED"} 1.0' in body
