"""Built-in demo tasks.

Tasks are plain functions registered by name. Jobs only reference a task by
name and pass JSON params, so no arbitrary code or shell commands are run.
"""
import time
from typing import Callable, Dict


class TaskError(Exception):
    """Raised by a task to signal a (possibly transient) failure."""


def echo(params: dict, attempt: int) -> str:
    return str(params.get("message", "ok"))


def sleep(params: dict, attempt: int) -> None:
    time.sleep(min(float(params.get("seconds", 0.1)), 5.0))


def flaky(params: dict, attempt: int) -> str:
    """Fails for the first `fail_times` attempts, then succeeds (a transient fault)."""
    fail_times = int(params.get("fail_times", 2))
    if attempt <= fail_times:
        raise TaskError(f"transient failure on attempt {attempt}")
    return "recovered"


def always_fail(params: dict, attempt: int) -> None:
    raise TaskError(params.get("reason", "permanent failure"))


TASKS: Dict[str, Callable[[dict, int], object]] = {
    "echo": echo,
    "sleep": sleep,
    "flaky": flaky,
    "always_fail": always_fail,
}
