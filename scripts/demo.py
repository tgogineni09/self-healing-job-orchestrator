"""Create a mix of jobs so the dashboard has something to show.

Usage:  python scripts/demo.py [base_url]
Needs only the standard library.
"""
import json
import sys
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"


def post(path: str, body: dict) -> dict:
    req = urllib.request.Request(
        BASE + path,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return json.load(resp)


def main() -> None:
    # 1. Healthy jobs
    for i in range(5):
        post("/jobs", {"name": f"healthy-{i}", "task": "echo", "params": {"message": f"job {i}"}})

    # 2. Transient failures that heal themselves through retries with backoff
    for i in range(3):
        post("/jobs", {"name": f"flaky-{i}", "task": "flaky", "params": {"fail_times": 2}})

    # 3. A pipeline: extract -> transform -> load
    extract = post("/jobs", {"name": "extract", "task": "sleep", "params": {"seconds": 0.5}})
    transform = post("/jobs", {"name": "transform", "task": "echo", "depends_on_id": extract["id"]})
    post("/jobs", {"name": "load", "task": "echo", "depends_on_id": transform["id"]})

    # 4. A permanent failure that blocks its dependants
    bad = post("/jobs", {"name": "bad-parent", "task": "always_fail", "max_retries": 1})
    post("/jobs", {"name": "blocked-child", "task": "echo", "depends_on_id": bad["id"]})

    print(f"Created demo jobs on {BASE}. Open Grafana at http://localhost:3000")


if __name__ == "__main__":
    main()
