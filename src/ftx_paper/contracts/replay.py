"""Canonical request, result, and parity contracts for isolated replay."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from collections.abc import Mapping, Sequence
from typing import Any, Literal, cast


ExecutionScope = Literal["replay"]
LedgerScope = Literal["replay"]
REPLAY_SCHEMA_VERSION = 1
REPLAY_MODES = frozenset({"replay"})
REPLAY_VEHICLES = frozenset({"futures", "synthetic"})
REPLAY_CAPTURE_MODES = frozenset({"none", "summary", "diagnostic", "full"})
_RESULT_FIELDS = frozenset({
    "run_id", "schema_version", "execution_scope", "ledger_scope", "summary", "events",
    "shadow_trades", "shadow_ledger", "authoritative_trades", "authoritative_ledger",
    "parity_artifact", "replay_records",
})
LEGACY_REPLAY_FIELDS = frozenset({
    "trades", "ledger", "diagnostic_trades", "replay_summary", "canonical_summary",
    "execution_mode", "allow_executable_replay", "include_shadow_layers", "sessions", "partitions",
})


def _timestamp(value: Any, field: str) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{field} must be timezone-aware")
        return value.isoformat()
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} is required")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return parsed.isoformat().replace("+00:00", "Z")


def _mapping(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be an object")
    return dict(value)


@dataclass(frozen=True, slots=True)
class ReplayScope:
    run_id: str
    execution_scope: ExecutionScope
    ledger_scope: LedgerScope = "replay"

    def __post_init__(self) -> None:
        if (not self.run_id or self.execution_scope != "replay"
                or self.ledger_scope != "replay"):
            raise ValueError("invalid replay scope")

    def to_dict(self) -> dict[str, str]:
        return {"run_id": self.run_id, "execution_scope": self.execution_scope,
                "ledger_scope": self.ledger_scope}


@dataclass(frozen=True, slots=True)
class ReplayRequest:
    session_date: str
    strategy_id: str
    vehicles: tuple[str, ...]
    mode: Literal["replay"] = "replay"
    include_parity_layers: bool = False
    capture_mode: Literal["none", "summary", "diagnostic", "full"] = "summary"

    def __post_init__(self) -> None:
        if len(self.session_date) != 10 or not self.strategy_id:
            raise ValueError("session_date and strategy_id are required")
        if not self.vehicles or any(vehicle not in REPLAY_VEHICLES for vehicle in self.vehicles):
            raise ValueError("vehicles must contain futures and/or synthetic")
        if "futures" not in self.vehicles:
            raise ValueError("vehicles must include futures")
        if self.mode not in REPLAY_MODES:
            raise ValueError("mode must be 'replay'")
        if self.capture_mode not in REPLAY_CAPTURE_MODES:
            raise ValueError("capture_mode must be none, summary, diagnostic, or full")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ReplayRequest":
        allowed = {"session_date", "strategy_id", "vehicles", "mode", "include_parity_layers", "capture_mode"}
        unexpected = set(value) - allowed
        if unexpected:
            raise ValueError("unsupported replay request fields: " + ", ".join(sorted(unexpected)))
        vehicles = value.get("vehicles")
        if not isinstance(vehicles, (list, tuple)):
            raise ValueError("vehicles must be an array")
        mode = value.get("mode", "replay")
        if not isinstance(mode, str):
            raise ValueError("mode must be 'replay'")
        include = value.get("include_parity_layers", False)
        if not isinstance(include, bool):
            raise ValueError("include_parity_layers must be a boolean")
        capture_mode = value.get("capture_mode", "summary")
        if not isinstance(capture_mode, str) or capture_mode.lower() not in REPLAY_CAPTURE_MODES:
            raise ValueError("capture_mode must be none, summary, diagnostic, or full")
        session_date = value.get("session_date")
        strategy_id = value.get("strategy_id")
        if not isinstance(session_date, str) or not isinstance(strategy_id, str):
            raise ValueError("session_date and strategy_id must be strings")
        if any(not isinstance(item, str) for item in vehicles):
            raise ValueError("vehicles must contain strings")
        return cls(session_date, strategy_id,
                   tuple(item.lower() for item in vehicles),
                   cast(Literal["replay"], mode.lower()), include,
                   cast(Literal["none", "summary", "diagnostic", "full"], capture_mode.lower()))

    def to_dict(self) -> dict[str, Any]:
        return {"session_date": self.session_date, "strategy_id": self.strategy_id,
                 "vehicles": list(self.vehicles), "mode": self.mode,
                 "include_parity_layers": self.include_parity_layers,
                 "capture_mode": self.capture_mode}


@dataclass(frozen=True, slots=True)
class ReplaySummary:
    values: Mapping[str, Any]

    REQUIRED_FIELDS = frozenset({
        "setup_candidates", "location_interactions", "entries_filled",
        "gross_reward_points", "achieved_risk_reward",
    })

    def __post_init__(self) -> None:
        missing = self.REQUIRED_FIELDS - set(self.values)
        if missing:
            raise ValueError("summary is missing required fields: " + ", ".join(sorted(missing)))

    def to_dict(self) -> dict[str, Any]:
        return dict(self.values)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ReplaySummary":
        return cls(_mapping(value, "summary"))


@dataclass(frozen=True, slots=True)
class ReplayTrade:
    values: Mapping[str, Any]

    def __post_init__(self) -> None:
        missing = {"status", "date", "vehicle", "quantity"} - set(self.values)
        if missing:
            raise ValueError("trade is missing required fields: " + ", ".join(sorted(missing)))

    def to_dict(self) -> dict[str, Any]:
        return dict(self.values)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ReplayTrade":
        return cls(_mapping(value, "trade"))


@dataclass(frozen=True, slots=True)
class ReplayLedgerEntry:
    values: Mapping[str, Any]

    def __post_init__(self) -> None:
        if "event_type" not in self.values:
            raise ValueError("ledger entry requires event_type")
        if not any(key in self.values for key in ("timestamp", "occurred_at", "fill_timestamp")):
            raise ValueError("ledger entry requires a timestamp")

    def to_dict(self) -> dict[str, Any]:
        return dict(self.values)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ReplayLedgerEntry":
        return cls(_mapping(value, "ledger entry"))


@dataclass(frozen=True, slots=True)
class ReplayEvent:
    schema_version: int
    run_id: str
    execution_scope: ExecutionScope
    ledger_scope: LedgerScope
    sequence: int
    occurred_at: str
    identity: str
    values: Mapping[str, Any]

    def __post_init__(self) -> None:
        if (self.schema_version != REPLAY_SCHEMA_VERSION or not self.run_id or not self.identity
                or self.sequence < 0 or self.execution_scope != "replay"
                or self.ledger_scope != "replay"):
            raise ValueError("invalid replay event envelope")
        _timestamp(self.occurred_at, "occurred_at")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": self.schema_version, "run_id": self.run_id,
                "execution_scope": self.execution_scope, "ledger_scope": self.ledger_scope,
                "sequence": self.sequence, "occurred_at": self.occurred_at,
                "identity": self.identity, "values": dict(self.values)}


@dataclass(frozen=True, slots=True)
class ReplayResult:
    run_id: str
    schema_version: int
    execution_scope: ExecutionScope
    ledger_scope: LedgerScope
    summary: ReplaySummary
    events: tuple[ReplayEvent, ...]
    shadow_trades: tuple[ReplayTrade, ...]
    shadow_ledger: tuple[ReplayLedgerEntry, ...]
    authoritative_trades: tuple[ReplayTrade, ...]
    authoritative_ledger: tuple[ReplayLedgerEntry, ...]
    parity_artifact: Mapping[str, Any] | None
    replay_records: tuple[Mapping[str, Any], ...] = ()

    def __post_init__(self) -> None:
        if (self.schema_version != REPLAY_SCHEMA_VERSION or not self.run_id
                or self.execution_scope != "replay"
                or self.ledger_scope != "replay"):
            raise ValueError("invalid replay result envelope")

    def to_dict(self) -> dict[str, Any]:
        payload = {"run_id": self.run_id, "schema_version": self.schema_version,
                 "execution_scope": self.execution_scope, "ledger_scope": self.ledger_scope,
                 "summary": self.summary.to_dict(), "events": [item.to_dict() for item in self.events],
                 "shadow_trades": [item.to_dict() for item in self.shadow_trades],
                 "shadow_ledger": [item.to_dict() for item in self.shadow_ledger],
                 "authoritative_trades": [item.to_dict() for item in self.authoritative_trades],
                 "authoritative_ledger": [item.to_dict() for item in self.authoritative_ledger],
                "parity_artifact": dict(self.parity_artifact) if self.parity_artifact is not None else None}
        if self.replay_records:
            payload["replay_records"] = [dict(item) for item in self.replay_records]
        return payload

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ReplayResult":
        unexpected = set(value) - _RESULT_FIELDS
        if unexpected:
            raise ValueError("unsupported replay result fields: " + ", ".join(sorted(unexpected)))
        missing = (_RESULT_FIELDS - {"replay_records"}) - set(value)
        if missing:
            raise ValueError("missing replay result fields: " + ", ".join(sorted(missing)))
        def records(name: str, factory: Any) -> tuple[Any, ...]:
            raw = value[name]
            if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
                raise ValueError(f"{name} must be an array")
            return tuple(factory.from_mapping(item) for item in raw)
        raw_events = value["events"]
        if not isinstance(raw_events, Sequence) or isinstance(raw_events, (str, bytes)):
            raise ValueError("events must be an array")
        events = tuple(ReplayEvent(**dict(item)) for item in raw_events)
        replay_records = value.get("replay_records", ())
        if not isinstance(replay_records, Sequence) or isinstance(replay_records, (str, bytes)):
            raise ValueError("replay_records must be an array")
        if any(not isinstance(item, Mapping) for item in replay_records):
            raise ValueError("replay_records must contain objects")
        return cls(
            str(value["run_id"]), int(value["schema_version"]), value["execution_scope"],
            value["ledger_scope"], ReplaySummary.from_mapping(value["summary"]), events,
            records("shadow_trades", ReplayTrade), records("shadow_ledger", ReplayLedgerEntry),
            records("authoritative_trades", ReplayTrade), records("authoritative_ledger", ReplayLedgerEntry),
            (None if value["parity_artifact"] is None else _mapping(value["parity_artifact"], "parity_artifact")),
            tuple(dict(item) for item in replay_records),
        )


__all__ = ["ExecutionScope", "LedgerScope", "REPLAY_SCHEMA_VERSION", "ReplayEvent", "ReplayLedgerEntry",
           "ReplayRequest", "ReplayResult", "ReplayScope", "ReplaySummary", "ReplayTrade",
           "LEGACY_REPLAY_FIELDS"]
