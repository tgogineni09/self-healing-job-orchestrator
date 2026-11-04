"""FastAPI service: job API, background worker, health and Prometheus metrics."""
import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, Response
from pydantic import BaseModel, ConfigDict, Field

from app.core import Job, Orchestrator, Status
from app.metrics import PrometheusMetrics
from app.sql_store import SqlJobStore
from app.tasks import TASKS


class JobCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    task: str
    params: Dict[str, Any] = Field(default_factory=dict)
    max_retries: int = Field(default=3, ge=0, le=10)
    depends_on_id: Optional[int] = None


class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    task: str
    params: Dict[str, Any]
    status: Status
    attempts: int
    max_retries: int
    depends_on_id: Optional[int]
    next_run_at: datetime
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    first_failed_at: Optional[datetime]
    last_error: Optional[str]
    recovered_count: int
    created_at: datetime


def create_app(
    store=None,
    start_worker: Optional[bool] = None,
    tick_seconds: Optional[float] = None,
) -> FastAPI:
    store = store or SqlJobStore(os.getenv("DATABASE_URL", "sqlite:///./orchestrator.db"))
    metrics = PrometheusMetrics()
    orchestrator = Orchestrator(
        store,
        TASKS,
        metrics=metrics,
        backoff_base_seconds=float(os.getenv("BACKOFF_BASE_SECONDS", "2")),
        stranded_timeout_seconds=float(os.getenv("STRANDED_TIMEOUT_SECONDS", "300")),
    )
    if start_worker is None:
        start_worker = os.getenv("DISABLE_WORKER", "0") != "1"
    interval = tick_seconds or float(os.getenv("TICK_SECONDS", "2"))

    async def worker_loop() -> None:
        while True:
            await asyncio.to_thread(orchestrator.tick)
            await asyncio.sleep(interval)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        task = asyncio.create_task(worker_loop()) if start_worker else None
        try:
            yield
        finally:
            if task:
                task.cancel()

    app = FastAPI(title="Self-Healing Job Orchestrator", version="1.0.0", lifespan=lifespan)

    @app.get("/health")
    def health() -> Dict[str, str]:
        return {"status": "ok"}

    @app.post("/jobs", response_model=JobOut, status_code=201)
    def create_job(body: JobCreate) -> Job:
        if body.task not in TASKS:
            raise HTTPException(422, f"unknown task '{body.task}'. Available: {sorted(TASKS)}")
        if body.depends_on_id is not None and store.get(body.depends_on_id) is None:
            raise HTTPException(422, f"depends_on_id {body.depends_on_id} does not exist")
        # A dependency must already exist, so dependency cycles are impossible.
        return store.add(
            Job(
                name=body.name,
                task=body.task,
                params=body.params,
                max_retries=body.max_retries,
                depends_on_id=body.depends_on_id,
            )
        )

    @app.get("/jobs", response_model=List[JobOut])
    def list_jobs(status: Optional[Status] = Query(default=None)) -> List[Job]:
        return store.list([status] if status else None)

    @app.get("/jobs/{job_id}", response_model=JobOut)
    def get_job(job_id: int) -> Job:
        job = store.get(job_id)
        if job is None:
            raise HTTPException(404, "job not found")
        return job

    @app.post("/jobs/{job_id}/retry", response_model=JobOut)
    def retry_job(job_id: int) -> Job:
        job = orchestrator.retry_now(job_id)
        if job is None:
            raise HTTPException(404, "job not found")
        if job.status != Status.PENDING:
            raise HTTPException(409, f"only FAILED or BLOCKED jobs can be retried (is {job.status.value})")
        return job

    @app.get("/stats")
    def stats() -> Dict[str, int]:
        return orchestrator.status_counts()

    @app.post("/admin/tick")
    def tick() -> Dict[str, int]:
        """Run one scheduling pass immediately (handy for demos and tests)."""
        return orchestrator.tick()

    @app.get("/metrics")
    def prometheus_metrics() -> Response:
        return Response(metrics.render(), media_type=metrics.content_type)

    return app


app = create_app()
