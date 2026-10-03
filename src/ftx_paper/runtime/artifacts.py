"""Authoritative Paper artifact projection from persisted runtime events.

This module is deliberately a projection only: it copies fields present in
Paper events and uses ``None`` for fields that Paper does not publish.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from typing import Any


ARTIFACT_SCHEMA_VERSION = 2


def _event_date(event: Mapping[str, Any]) -> str:
    payload = event.get("payload", event)
    return str(payload.get("session_date") or payload.get("date") or event.get("created_at", ""))[:10]


def _in_scope(event: Mapping[str, Any], *, date_from: str, date_to: str,
              sessions: tuple[str, ...], vehicles: tuple[str, ...],
              vehicle_bearing: bool = False) -> bool:
    payload = event.get("payload", event)
    date = _event_date(event)
    if date_from and date_to and not date_from <= date <= date_to:
        return False
    if sessions and payload.get("session") is not None and str(payload["session"]) not in sessions:
        return False
    if vehicle_bearing and vehicles:
        value = payload.get("vehicle", payload.get("instrument"))
        if value is not None and str(value) not in vehicles:
            return False
    return True


def build_authoritative_payload(
    events: Iterable[Mapping[str, Any]], *, date_from: str, date_to: str,
    sessions: Iterable[str] = (), vehicles: Iterable[str] = (),
    configuration: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a deterministic, typed Paper payload from existing events."""
    session_values = tuple(str(item) for item in sessions)
    vehicle_values = tuple(str(item) for item in vehicles)
    scoped = [event for event in events if _in_scope(
        event, date_from=date_from, date_to=date_to, sessions=session_values,
        vehicles=vehicle_values,
    )]
    def event_order(event: Mapping[str, Any]) -> tuple[Any, ...]:
        payload = event.get("payload", event)
        raw_sequence = payload.get("sequence")
        sequence = int(raw_sequence) if str(raw_sequence).lstrip("-").isdigit() else 2**31
        return (
            _event_date(event), sequence,
            str(payload.get("decision_at", event.get("created_at", ""))),
            str(payload.get("decision_id", "")),
            str(event.get("event_type", "")).upper(),
            json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")),
        )

    ordered = sorted(scoped, key=event_order)
    candidates: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    settlements: list[dict[str, Any]] = []
    ledgers: dict[str, Any] = {}
    seen: set[str] = set()
    for event in ordered:
        payload = dict(event.get("payload", event))
        event_type = str(event.get("event_type", "")).upper()
        candidate_id = payload.get("candidate_id") or payload.get("decision_id")
        if candidate_id and event_type in {"CANDIDATEDECISION", "CANDIDATE", "DECISION"}:
            candidate_id = str(candidate_id)
            if candidate_id not in seen:
                seen.add(candidate_id)
                candidates.append({
                    "candidate_id": candidate_id,
                    "date": _event_date(event),
                    "time": str(payload.get("decision_at", ""))[11:16],
                    "session": payload.get("session"),
                    "vehicle": None,
                    "ordered_sequence": payload.get("sequence"),
                    "cell": payload.get("cell"),
                    "direction": payload.get("direction"),
                    "eligibility": (
                        "eligible" if payload.get("outcome") in {"candidate", "accepted", "filled"}
                        else "rejected" if payload.get("outcome") in {"rejected", "policy rejection", "sizing rejection"}
                        else "unavailable"
                    ),
                    "rejection_reason": payload.get("reason"),
                    "score": payload.get("score"),
                    "score_factors": payload.get("score_factors", {}),
                    "sizing_diagnostics": payload.get("sizing_pipeline"),
                })
        if candidate_id and event_type in {"ACCEPTEDDECISION", "REJECTEDDECISION", "SIZING_REJECTED"}:
            decisions.append({"candidate_id": str(candidate_id), **payload})
        if event_type in {"FILLED", "EXECUTIONFILLED", "EXITED", "EXECUTIONEXITED"}:
            trades.append(payload)
        if "settlement" in event_type or "settlement_status" in payload:
            settlements.append(payload)
        if "ledger" in event_type or "ledger_snapshot" in payload:
            key = _event_date(event)
            ledgers[key] = payload.get("ledger_snapshot", payload.get("ledger", payload))
    config = dict(configuration or {})
    manifest = {
        "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
        "date_range": {"from": date_from, "to": date_to},
        "sessions": list(session_values), "vehicles": list(vehicle_values),
        "source_config_fingerprint": hashlib.sha256(
            json.dumps(config, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest(),
    }
    return {"manifest": manifest, "candidate_trace": candidates,
            "decision_trace": decisions, "trades": trades,
            "ledger": ledgers, "synthetic_settlements": settlements}


__all__ = ["ARTIFACT_SCHEMA_VERSION", "build_authoritative_payload"]
