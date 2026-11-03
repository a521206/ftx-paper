"""Contracts for API-owned daily setup materialization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Mapping


SetupMode = Literal["catch_up", "incremental", "rebuild"]
SetupJobStatus = Literal["queued", "running", "completed", "partial", "failed", "cancelled"]


@dataclass(frozen=True, slots=True)
class SetupRunRequest:
    strategy_id: str
    task_id: str
    mode: SetupMode
    through_session: str | None = None
    from_session: str | None = None
    rebuild: bool = False
    output_root: str | None = None
    requested_by: str = "operator"

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "SetupRunRequest":
        mode = str(value.get("mode", ""))
        if mode not in {"catch_up", "incremental", "rebuild"}:
            raise ValueError("mode must be catch_up, incremental, or rebuild")
        strategy_id = str(value.get("strategy_id", "")).strip()
        task_id = str(value.get("task_id", "")).strip()
        if not strategy_id or not task_id:
            raise ValueError("strategy_id and task_id are required")
        if mode == "rebuild" and not value.get("output_root"):
            raise ValueError("rebuild requires an isolated output_root")
        return cls(
            strategy_id=strategy_id,
            task_id=task_id,
            mode=mode,  # type: ignore[arg-type]
            through_session=str(value["through_session"]) if value.get("through_session") else None,
            from_session=str(value["from_session"]) if value.get("from_session") else None,
            rebuild=bool(value.get("rebuild", False)),
            output_root=str(value["output_root"]) if value.get("output_root") else None,
            requested_by=str(value.get("requested_by", "operator")),
        )


@dataclass(frozen=True, slots=True)
class SetupArtifactIdentity:
    strategy_id: str
    task_id: str
    algorithm_version: str
    config_fingerprint: str
    instrument: str


@dataclass(frozen=True, slots=True)
class SetupJobResult:
    job_id: str
    status: SetupJobStatus
    strategy_id: str
    tasks: Mapping[str, Mapping[str, int]]
    through_session: str | None = None
