"""Simulate many jobs against the orchestrator core and report how failures were handled.

Uses the in-memory store and a fake clock, so it needs no database or server and finishes instantly.
Mix (seeded, so the result is reproducible): 70% healthy, 25% flaky (fail once or twice, then succeed),
5% always failing (retries exhausted). Usage:  python scripts/simulate.py [num_jobs]
"""
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.core import InMemoryStore, Job, Orchestrator, Status  # noqa: E402
from app.tasks import TASKS  # noqa: E402


def main(n: int = 1000, seed: int = 1) -> None:
    random.seed(seed)
    store = InMemoryStore()
    orch = Orchestrator(store, TASKS, backoff_base_seconds=2)
    start = datetime(2026, 1, 1)
    for i in range(n):
        r = random.random()
        if r < 0.70:
            job = Job(name=f"healthy-{i}", task="echo", next_run_at=start)
        elif r < 0.95:
            job = Job(name=f"flaky-{i}", task="flaky", params={"fail_times": random.choice([1, 2])}, next_run_at=start)
        else:
            job = Job(name=f"bad-{i}", task="always_fail", max_retries=2, next_run_at=start)
        store.add(job)

    now = start
    for _ in range(40):  # 40 ticks of 2 simulated seconds = 80 simulated seconds
        orch.tick(now)
        now += timedelta(seconds=2)

    jobs = store.list()
    flaky = [j for j in jobs if j.name.startswith("flaky")]
    bad = [j for j in jobs if j.name.startswith("bad")]
    healed = sum(j.status == Status.SUCCEEDED for j in flaky)
    contained = sum(j.status == Status.FAILED for j in bad)
    unsettled = sum(j.status in (Status.PENDING, Status.RUNNING, Status.RETRYING) for j in jobs)
    print(f"Simulated {n} jobs (seed {seed})")
    print(f"Final status counts: {orch.status_counts()}")
    print(f"Transient failures healed automatically: {healed} of {len(flaky)}")
    print(f"Permanent failures contained as FAILED:  {contained} of {len(bad)}")
    print(f"Jobs left unsettled (pending/running/retrying): {unsettled}")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 1000)
