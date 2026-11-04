"""Core scheduling and self-healing engine.

This module has no third-party dependencies, so the retry, recovery and
dependency logic can be unit tested without a web server or a database.
Storage (in-memory or SQL) and metrics (Prometheus or none) are plugged in.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Callable, Dict, Iterable, List, Optional, Protocol
import time


def utcnow() -> datetime:
    """Naive UTC timestamp (kept naive so SQLite and Postgres behave alike)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


class UnknownTaskError(Exception):
    """A job references a task name that is not registered."""


class Status(str, Enum):
    PENDING = "PENDING"        # waiting to run
    RUNNING = "RUNNING"        # picked up by the worker
    RETRYING = "RETRYING"      # failed, will run again after a backoff
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"          # retries exhausted
    BLOCKED = "BLOCKED"        # a job it depends on failed


@dataclass
class Job:
    name: str
    task: str
    id: Optional[int] = None
    params: dict = field(default_factory=dict)
    status: Status = Status.PENDING
    attempts: int = 0
    max_retries: int = 3
    depends_on_id: Optional[int] = None
    next_run_at: datetime = field(default_factory=utcnow)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    first_failed_at: Optional[datetime] = None
    last_error: Optional[str] = None
    recovered_count: int = 0
    created_at: datetime = field(default_factory=utcnow)


class JobStore(Protocol):
    def add(self, job: Job) -> Job: ...
    def get(self, job_id: int) -> Optional[Job]: ...
    def save(self, job: Job) -> None: ...
    def list(self, statuses: Optional[Iterable[Status]] = None) -> List[Job]: ...


class Metrics(Protocol):
    def job_finished(self, result: str, duration_seconds: float) -> None: ...
    def job_retried(self) -> None: ...
    def job_recovered(self) -> None: ...
    def job_blocked(self) -> None: ...
    def time_to_recover(self, seconds: float) -> None: ...
    def set_status_counts(self, counts: Dict[str, int]) -> None: ...


class NullMetrics:
    """Default metrics sink: does nothing."""

    def job_finished(self, result: str, duration_seconds: float) -> None: ...
    def job_retried(self) -> None: ...
    def job_recovered(self) -> None: ...
    def job_blocked(self) -> None: ...
    def time_to_recover(self, seconds: float) -> None: ...
    def set_status_counts(self, counts: Dict[str, int]) -> None: ...


class InMemoryStore:
    """Simple store used by tests and for quick local experiments."""

    def __init__(self) -> None:
        self._jobs: Dict[int, Job] = {}
        self._next_id = 1

    def add(self, job: Job) -> Job:
        job.id = self._next_id
        self._next_id += 1
        self._jobs[job.id] = job
        return job

    def get(self, job_id: int) -> Optional[Job]:
        return self._jobs.get(job_id)

    def save(self, job: Job) -> None:
        self._jobs[job.id] = job

    def list(self, statuses: Optional[Iterable[Status]] = None) -> List[Job]:
        wanted = set(statuses) if statuses else None
        jobs = [j for j in self._jobs.values() if wanted is None or j.status in wanted]
        return sorted(jobs, key=lambda j: j.id)


TaskFn = Callable[[dict, int], object]


class Orchestrator:
    """Runs due jobs and heals common failure modes on every tick.

    Each tick, in order:
      1. recover jobs stranded in RUNNING (e.g. the worker crashed mid-run)
      2. unblock jobs whose dependency is healthy again
      3. block jobs whose dependency failed permanently
      4. run every due job whose dependency has succeeded
    """

    def __init__(
        self,
        store: JobStore,
        tasks: Dict[str, TaskFn],
        metrics: Optional[Metrics] = None,
        backoff_base_seconds: float = 2.0,
        stranded_timeout_seconds: float = 300.0,
    ) -> None:
        self.store = store
        self.tasks = tasks
        self.metrics = metrics or NullMetrics()
        self.backoff_base_seconds = backoff_base_seconds
        self.stranded_timeout_seconds = stranded_timeout_seconds

    # ---- public API -------------------------------------------------
    def tick(self, now: Optional[datetime] = None) -> Dict[str, int]:
        now = now or utcnow()
        recovered = self._recover_stranded(now)
        unblocked = self._unblock(now)
        blocked = self._block_on_failed_dependencies()
        executed = 0
        for job in self.store.list([Status.PENDING, Status.RETRYING]):
            if job.next_run_at > now or not self._dependency_ready(job):
                continue
            self._run_job(job, now)
            executed += 1
        self.metrics.set_status_counts(self.status_counts())
        return {
            "executed": executed,
            "recovered": recovered,
            "unblocked": unblocked,
            "blocked": blocked,
        }

    def status_counts(self) -> Dict[str, int]:
        counts = {s.value: 0 for s in Status}
        for job in self.store.list():
            counts[job.status.value] += 1
        return counts

    def retry_now(self, job_id: int, now: Optional[datetime] = None) -> Optional[Job]:
        """Manually reset a FAILED or BLOCKED job so it runs again."""
        job = self.store.get(job_id)
        if job is None or job.status not in (Status.FAILED, Status.BLOCKED):
            return job
        job.status = Status.PENDING
        job.attempts = 0
        job.next_run_at = now or utcnow()
        job.finished_at = None
        job.last_error = None
        self.store.save(job)
        return job

    def backoff_seconds(self, attempt: int) -> float:
        """Exponential backoff: base, 2*base, 4*base, ..."""
        return self.backoff_base_seconds * (2 ** (attempt - 1))

    # ---- internals --------------------------------------------------
    def _dependency_ready(self, job: Job) -> bool:
        if job.depends_on_id is None:
            return True
        dep = self.store.get(job.depends_on_id)
        return dep is not None and dep.status == Status.SUCCEEDED

    def _run_job(self, job: Job, now: datetime) -> None:
        job.status = Status.RUNNING
        job.started_at = now
        job.attempts += 1
        self.store.save(job)

        started = time.perf_counter()
        try:
            fn = self.tasks.get(job.task)
            if fn is None:
                raise UnknownTaskError(f"unknown task '{job.task}'")
            fn(job.params, job.attempts)
        except Exception as exc:  # noqa: BLE001 - any task error is a job failure
            duration = time.perf_counter() - started
            job.last_error = f"{type(exc).__name__}: {exc}"[:500]
            job.first_failed_at = job.first_failed_at or now
            unknown_task = isinstance(exc, UnknownTaskError)  # retrying cannot fix this
            if job.attempts <= job.max_retries and not unknown_task:
                job.status = Status.RETRYING
                job.next_run_at = now + timedelta(seconds=self.backoff_seconds(job.attempts))
                self.metrics.job_retried()
            else:
                job.status = Status.FAILED
                job.finished_at = now
            self.metrics.job_finished("failure", duration)
        else:
            duration = time.perf_counter() - started
            job.status = Status.SUCCEEDED
            job.finished_at = now
            job.last_error = None
            if job.first_failed_at is not None:
                self.metrics.time_to_recover((now - job.first_failed_at).total_seconds())
            self.metrics.job_finished("success", duration)
        self.store.save(job)

    def _recover_stranded(self, now: datetime) -> int:
        cutoff = now - timedelta(seconds=self.stranded_timeout_seconds)
        count = 0
        for job in self.store.list([Status.RUNNING]):
            if job.started_at is not None and job.started_at <= cutoff:
                job.status = Status.PENDING
                job.next_run_at = now
                job.recovered_count += 1
                job.last_error = "recovered: job was stranded in RUNNING"
                self.store.save(job)
                self.metrics.job_recovered()
                count += 1
        return count

    def _block_on_failed_dependencies(self) -> int:
        count = 0
        for job in self.store.list([Status.PENDING, Status.RETRYING]):
            if job.depends_on_id is None:
                continue
            dep = self.store.get(job.depends_on_id)
            if dep is not None and dep.status in (Status.FAILED, Status.BLOCKED):
                job.status = Status.BLOCKED
                job.last_error = f"blocked: dependency job {dep.id} is {dep.status.value}"
                self.store.save(job)
                self.metrics.job_blocked()
                count += 1
        return count

    def _unblock(self, now: datetime) -> int:
        count = 0
        for job in self.store.list([Status.BLOCKED]):
            dep = self.store.get(job.depends_on_id) if job.depends_on_id else None
            if dep is not None and dep.status not in (Status.FAILED, Status.BLOCKED):
                job.status = Status.PENDING
                job.next_run_at = now
                job.last_error = None
                self.store.save(job)
                count += 1
        return count
