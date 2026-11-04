"""Prometheus metrics. Each instance owns its registry so tests can create many."""
from typing import Dict

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest
from prometheus_client import CONTENT_TYPE_LATEST

from app.core import Status


class PrometheusMetrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self._executed = Counter(
            "orchestrator_jobs_executed",
            "Job executions by result",
            ["result"],
            registry=self.registry,
        )
        self._retried = Counter(
            "orchestrator_jobs_retried", "Jobs scheduled for a retry", registry=self.registry
        )
        self._recovered = Counter(
            "orchestrator_jobs_recovered",
            "Stranded RUNNING jobs recovered automatically",
            registry=self.registry,
        )
        self._blocked = Counter(
            "orchestrator_jobs_blocked",
            "Jobs blocked by a failed dependency",
            registry=self.registry,
        )
        self._duration = Histogram(
            "orchestrator_job_duration_seconds",
            "Duration of a single job execution",
            registry=self.registry,
        )
        self._recover_time = Histogram(
            "orchestrator_job_time_to_recover_seconds",
            "Time from a job's first failure to its eventual success",
            buckets=(1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, float("inf")),
            registry=self.registry,
        )
        self._status = Gauge(
            "orchestrator_jobs_by_status",
            "Current number of jobs per status",
            ["status"],
            registry=self.registry,
        )
        for status in Status:
            self._status.labels(status=status.value).set(0)
            self._executed.labels(result="success")
            self._executed.labels(result="failure")

    def job_finished(self, result: str, duration_seconds: float) -> None:
        self._executed.labels(result=result).inc()
        self._duration.observe(duration_seconds)

    def job_retried(self) -> None:
        self._retried.inc()

    def job_recovered(self) -> None:
        self._recovered.inc()

    def job_blocked(self) -> None:
        self._blocked.inc()

    def time_to_recover(self, seconds: float) -> None:
        self._recover_time.observe(seconds)

    def set_status_counts(self, counts: Dict[str, int]) -> None:
        for status, value in counts.items():
            self._status.labels(status=status).set(value)

    def render(self) -> bytes:
        return generate_latest(self.registry)

    content_type = CONTENT_TYPE_LATEST
