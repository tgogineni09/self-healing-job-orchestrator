"""SQLAlchemy-backed JobStore (SQLite by default, PostgreSQL in docker-compose)."""
from datetime import datetime
from typing import Iterable, List, Optional

from sqlalchemy import JSON, DateTime, Integer, String, Text, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
from sqlalchemy.pool import StaticPool

from app.core import Job, Status


class Base(DeclarativeBase):
    pass


class JobRow(Base):
    __tablename__ = "jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(200))
    task: Mapped[str] = mapped_column(String(100))
    params: Mapped[dict] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(20), index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, default=3)
    depends_on_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
    next_run_at: Mapped[datetime] = mapped_column(DateTime)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    first_failed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    recovered_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime)


def _to_row(job: Job, row: Optional[JobRow] = None) -> JobRow:
    row = row or JobRow()
    row.name = job.name
    row.task = job.task
    row.params = job.params
    row.status = job.status.value
    row.attempts = job.attempts
    row.max_retries = job.max_retries
    row.depends_on_id = job.depends_on_id
    row.next_run_at = job.next_run_at
    row.started_at = job.started_at
    row.finished_at = job.finished_at
    row.first_failed_at = job.first_failed_at
    row.last_error = job.last_error
    row.recovered_count = job.recovered_count
    row.created_at = job.created_at
    return row


def _to_job(row: JobRow) -> Job:
    return Job(
        id=row.id,
        name=row.name,
        task=row.task,
        params=row.params or {},
        status=Status(row.status),
        attempts=row.attempts,
        max_retries=row.max_retries,
        depends_on_id=row.depends_on_id,
        next_run_at=row.next_run_at,
        started_at=row.started_at,
        finished_at=row.finished_at,
        first_failed_at=row.first_failed_at,
        last_error=row.last_error,
        recovered_count=row.recovered_count,
        created_at=row.created_at,
    )


class SqlJobStore:
    def __init__(self, database_url: str = "sqlite:///./orchestrator.db") -> None:
        kwargs = {}
        if database_url.startswith("sqlite"):
            kwargs["connect_args"] = {"check_same_thread": False}
            if ":memory:" in database_url:
                kwargs["poolclass"] = StaticPool  # one shared in-memory database
        self.engine = create_engine(database_url, **kwargs)
        Base.metadata.create_all(self.engine)
        self._session = sessionmaker(bind=self.engine, expire_on_commit=False)

    def add(self, job: Job) -> Job:
        with self._session() as session:
            row = _to_row(job)
            session.add(row)
            session.commit()
            return _to_job(row)

    def get(self, job_id: int) -> Optional[Job]:
        with self._session() as session:
            row = session.get(JobRow, job_id)
            return _to_job(row) if row else None

    def save(self, job: Job) -> None:
        with self._session() as session:
            row = session.get(JobRow, job.id)
            if row is None:
                raise KeyError(f"job {job.id} not found")
            _to_row(job, row)
            session.commit()

    def list(self, statuses: Optional[Iterable[Status]] = None) -> List[Job]:
        with self._session() as session:
            query = select(JobRow).order_by(JobRow.id)
            if statuses:
                query = query.where(JobRow.status.in_([s.value for s in statuses]))
            return [_to_job(row) for row in session.scalars(query)]
