"""Application-owned daily setup worker."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from concurrent.futures import Future, ThreadPoolExecutor
from threading import Event
from threading import RLock
from typing import Any
from uuid import uuid4

from ftx_paper.contracts.setup import SetupRunRequest


@dataclass(frozen=True, slots=True)
class DailySetupTask:
    strategy_id: str
    task_id: str
    algorithm_version: str
    materialize: Callable[[SetupRunRequest, Callable[[], bool]], Mapping[str, int]]


class SetupTaskRegistry:
    def __init__(self, tasks: Sequence[DailySetupTask] = ()) -> None:
        self._tasks = {(task.strategy_id, task.task_id): task for task in tasks}

    def resolve(self, strategy_id: str, task_id: str) -> DailySetupTask:
        try:
            return self._tasks[(strategy_id, task_id)]
        except KeyError as exc:
            raise ValueError(f"unknown setup task: {strategy_id}/{task_id}") from exc


class SetupWorker:
    """Serialize setup jobs inside the API process."""

    def __init__(self, registry: SetupTaskRegistry, *, max_pending_jobs: int = 8) -> None:
        if max_pending_jobs < 1:
            raise ValueError("max_pending_jobs must be positive")
        self.registry = registry
        self.max_pending_jobs = max_pending_jobs
        self._jobs: dict[str, dict[str, Any]] = {}
        self._cancel_events: dict[str, Event] = {}
        self._futures: dict[str, Future[None]] = {}
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="setup")
        self._lock = RLock()

    def submit(self, request: Mapping[str, Any] | SetupRunRequest) -> str:
        parsed = request if isinstance(request, SetupRunRequest) else SetupRunRequest.from_mapping(request)
        task = self.registry.resolve(parsed.strategy_id, parsed.task_id)
        job_id = f"setup-{uuid4().hex}"
        job: dict[str, Any] = {
            "schema_version": 1, "job_id": job_id, "status": "queued",
            "strategy_id": parsed.strategy_id, "task_id": parsed.task_id,
            "request": {
                "strategy_id": parsed.strategy_id, "task_id": parsed.task_id,
                "mode": parsed.mode, "through_session": parsed.through_session,
                "from_session": parsed.from_session, "rebuild": parsed.rebuild,
                "output_root": parsed.output_root, "requested_by": parsed.requested_by,
            },
        }
        with self._lock:
            pending = sum(
                value.get("status") in {"queued", "running"}
                for value in self._jobs.values()
            )
            if pending >= self.max_pending_jobs:
                raise ValueError("setup worker queue is full")
            self._jobs[job_id] = job
            cancel_event = Event()
            self._cancel_events[job_id] = cancel_event
            self._futures[job_id] = self._executor.submit(
                self._run, job_id, task, parsed, cancel_event,
            )
        return job_id

    def shutdown(self, *, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait, cancel_futures=not wait)

    def _run(
        self,
        job_id: str,
        task: DailySetupTask,
        request: SetupRunRequest,
        cancel_event: Event,
    ) -> None:
        with self._lock:
            job = self._jobs[job_id]
            if cancel_event.is_set():
                job["status"] = "cancelled"
                return
            job["status"] = "running"
        try:
            counts = dict(task.materialize(request, cancel_event.is_set))
            failures = int(counts.get("failed", 0))
            unavailable = int(counts.get("unavailable", 0))
            if cancel_event.is_set():
                status = "cancelled"
            else:
                status = "failed" if failures and not counts.get("materialized") else "partial" if failures or unavailable else "completed"
            with self._lock:
                self._jobs[job_id].update({"status": status, "tasks": {request.task_id: counts}, "through_session": request.through_session})
        except Exception as exc:
            with self._lock:
                self._jobs[job_id].update({"status": "failed", "error": str(exc), "tasks": {request.task_id: {}}})

    def read(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            value = self._jobs.get(job_id)
            return dict(value) if value is not None else None

    def cancel(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.get("status") in {"completed", "failed", "cancelled"}:
                return False
            self._cancel_events[job_id].set()
            if job.get("status") == "queued":
                job["status"] = "cancelled"
            return True
