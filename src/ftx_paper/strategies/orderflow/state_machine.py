"""Paper replay clock boundary for canonical evidence publications."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
from typing import Any, Callable, Iterable, Sequence

from .causal_events import CausalEventRegistry
from .timestamps import coerce_timestamp, nifty_label, parse_session_timestamp, session_minute


EvidenceAtClock = Callable[[str], dict[str, Any]]
InteractionFactsAtClock = Callable[[str], Sequence[dict[str, Any]]]
@dataclass(frozen=True, slots=True)
class ReplayState:
    """Immutable state exposed after one bounded replay prefix."""

    sequence: int
    latest_5m: dict[str, Any] | None
    evidence: dict[str, Any]
    ordinary_context_ref: dict[str, str] | None = None
    context_publications: tuple[dict[str, Any], ...] = ()
    interaction_response: tuple[dict[str, Any], ...] = ()


class ReplayMachine:
    """Advance replay clocks without deriving strategy or context policy."""

    def __init__(
        self,
        rows: list[dict[str, Any]],
        *,
        evidence_events: Iterable[tuple[datetime | str, dict[str, Any], dict[str, Any] | None]],
        evidence_at_clock: EvidenceAtClock,
        session_date: str,
        interaction_response: Sequence[dict[str, Any]] = (),
        interaction_response_at_clock: InteractionFactsAtClock | None = None,
    ) -> None:
        if not rows:
            raise ValueError("replay session has no rows")
        self._rows = rows
        self._events = tuple(evidence_events)
        self._evidence_at_clock = evidence_at_clock
        self._session_date = session_date
        unique: dict[tuple[str, str], dict[str, Any]] = {}
        for item in interaction_response:
            key = (str(item.get("kind", "")), str(item.get("identity", "")))
            unique.setdefault(key, dict(item))
        self._interaction_response = tuple(unique.values())
        self._interaction_response_at_clock = interaction_response_at_clock
        self._neutral_keys_by_causal_id: dict[tuple[str, str], str] = {}
        self._neutral_event_ordinals: dict[tuple[str, str, str, str, str], int] = {}
        self._causal_events = CausalEventRegistry()
        self._event_count = 0
        self._state = ReplayState(
            sequence=-1,
            latest_5m=None,
            evidence={
                "atr": None,
                "locations": [],
                "vwap_measurements": [],
                "prior_value": None,
                "contexts": [],
                "unavailable": ["evidence_not_yet_available"],
                "context_stream_id": None,
                "location_stream_id": None,
            },
        )

    def run(self) -> tuple[list[dict[str, Any]], ReplayState]:
        records: list[dict[str, Any]] = []
        event_index = 0
        current_5m = self._state.latest_5m
        interaction_response: list[dict[str, Any]] = []
        emitted_interaction_keys: set[tuple[str, str]] = set()
        ordinary_context_ref = self._state.ordinary_context_ref
        for index, row in enumerate(self._rows):
            clock_instant = parse_session_timestamp(self._session_date, row["time"])
            clock = nifty_label(clock_instant)
            while (
                event_index < len(self._events)
                and coerce_timestamp(self._events[event_index][0]) <= clock_instant
            ):
                _, _, current_5m = self._events[event_index]
                event_index += 1
            evidence = dict(self._evidence_at_clock(clock))
            supplied = (
                self._interaction_response_at_clock(clock)
                if self._interaction_response_at_clock is not None else ()
            )
            newly_available = [
                item for item in self._interaction_response
                if coerce_timestamp(str(item.get(
                    "_publication_available_at", item.get("available_at", clock),
                ))) <= clock_instant
                and (str(item.get("kind", "")), str(item.get("identity", "")))
                not in emitted_interaction_keys
            ]
            for item in supplied:
                key = (str(item.get("kind", "")), str(item.get("identity", "")))
                if key not in emitted_interaction_keys:
                    newly_available.append(dict(item))
            for item in newly_available:
                parent = str(item.get("causal_parent_id", ""))
                values = item.get("values")
                observed_at = str(item.get("causal_observed_at", item.get("observed_at", clock)))
                available_at = str(item.get("causal_available_at", item.get("available_at", clock)))
                if parent.startswith("aggregate:"):
                    source_fingerprint = values.get("source_fingerprint") if isinstance(values, dict) else None
                    self._causal_events.publish_source_observation(
                        source_id=parent,
                        session_date=self._session_date,
                        observed_at=observed_at,
                        available_at=available_at,
                        source_fingerprint=(
                            str(source_fingerprint)
                            if source_fingerprint is not None else None
                        ),
                    )
                emitted_interaction_keys.add(
                    (str(item.get("kind", "")), str(item.get("identity", "")))
                )
            newly_available = [self._normalize_event(item) for item in newly_available]
            interaction_response.extend(newly_available)
            contexts = [item for item in evidence.get("contexts", ()) if isinstance(item, dict)]
            if contexts:
                candidate = contexts[-1]
                if isinstance(candidate.get("context_id"), str) and isinstance(candidate.get("version"), str):
                    ordinary_context_ref = {
                        "context_id": candidate["context_id"],
                        "version": candidate["version"],
                    }
            current_events = tuple(newly_available)
            records.append(self._record(index, row, evidence, current_5m, ordinary_context_ref, current_events))
            self._state = ReplayState(index, current_5m, evidence, ordinary_context_ref, (), tuple(interaction_response))
        return records, self._state

    def _normalize_event(self, event: dict[str, Any]) -> dict[str, Any]:
        """Publish canonical neutral identity and causal-parent references."""
        event_data = dict(event)
        event_data.pop("_publication_available_at", None)
        values = event_data.get("values")
        if isinstance(values, dict):
            for key in (
                "interaction_id", "lifecycle_id", "candidate_id", "setup_id",
                "opportunity_id", "intent_id", "reservation_id", "order_id",
                "position_id", "fill_id", "causal_parent_id", "parent_id",
            ):
                if key not in event_data and values.get(key) is not None:
                    event_data[key] = values[key]
            for key in ("level", "reason", "reason_code"):
                if key not in event_data and values.get(key) is not None:
                    event_data[key] = values[key]
        session_date = str(event_data.get("session_date") or self._session_date)
        event_data.setdefault("session_date", session_date)
        observed_at = event_data.get("observed_at", event_data.get("time"))
        available_at = event_data.get("available_at", observed_at)
        event_data.setdefault("observed_at", observed_at)
        event_data.setdefault("available_at", available_at)
        event_data.setdefault("causal_observed_at", observed_at)
        event_data.setdefault("causal_available_at", available_at)
        event_data.setdefault("event_id", f"replay:{self._event_count}")
        event_data.setdefault("sequence", self._event_count)
        event_data.setdefault("policy_version", "orderflow-v1")
        event_data.setdefault("provenance", "causal_replay")
        self._event_count += 1

        causal_ids = tuple(
            (name, str(event_data[name]))
            for name in (
                "interaction_id", "lifecycle_id", "candidate_id", "setup_id",
                "opportunity_id", "intent_id", "reservation_id", "order_id",
                "position_id", "fill_id", "event_id",
            )
            if event_data.get(name) is not None
            and not (name == "event_id" and str(event_data[name]).startswith("replay:"))
        )
        event_basis = {
            "kind": str(event_data.get("kind", "")),
            "session_date": session_date,
            "observed_at": observed_at,
            "available_at": available_at,
            "causal_ids": causal_ids,
            "level": event_data.get("level"),
            "reason": event_data.get("reason") or event_data.get("reason_code"),
        }
        ordinal_key = (
            event_basis["kind"], str(observed_at), str(available_at),
            str(event_data.get("causal_parent_id") or event_data.get("parent_id") or ""),
            str(event_data.get("lifecycle_id") or event_data.get("interaction_id") or ""),
        )
        ordinal = self._neutral_event_ordinals.get(ordinal_key, 0)
        self._neutral_event_ordinals[ordinal_key] = ordinal + 1
        event_basis["same_clock_ordinal"] = ordinal
        if not str(event_data["event_id"]).startswith("replay:"):
            event_key = "niftyzoning:event:" + hashlib.sha256(
                json.dumps({
                    "event_id": event_data["event_id"],
                    "basis": {key: value for key, value in event_basis.items() if key != "same_clock_ordinal"},
                    "lifecycle_sequence": event_data.get("lifecycle_sequence"),
                }, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
            ).hexdigest()
        else:
            event_key = "niftyzoning:derived:" + hashlib.sha256(
                json.dumps(event_basis, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
            ).hexdigest()
        if event_key in self._neutral_keys_by_causal_id.values():
            event_key = f"{event_key}:occurrence-{ordinal}"
        event_data.setdefault("neutral_event_key", event_key)
        event_data["identity"] = event_data["neutral_event_key"]
        parent_ref = next(
            ((name, event_data.get(name)) for name in (
                "causal_parent_id", "parent_id", "lifecycle_id", "setup_id",
                "opportunity_id", "position_id", "intent_id", "interaction_id",
            ) if event_data.get(name) is not None),
            None,
        )
        if (
            event_data.get("causal_parent_id") is None
            and event_data.get("interaction_id") is not None
            and event_data.get("kind") in {
            "interaction_created", "location_interaction_observed", "interaction_ended",
            "response_confirmed", "response_invalidated", "response_expired",
            "role_transition", "hypothesis_arbitrated",
            }
        ):
            parent_ref = ("interaction_id", str(event_data["interaction_id"]))
        parent_key = (
            self._causal_events.parent_key(str(parent_ref[1]))
            if parent_ref and str(parent_ref[1]).startswith("aggregate:")
            else self._neutral_keys_by_causal_id.get((parent_ref[0], str(parent_ref[1])))
            if parent_ref is not None else None
        )
        event_data.setdefault("causal_parent_key", parent_key)
        if isinstance(values, dict):
            values.setdefault("causal_parent_key", parent_key)
        for name, value in causal_ids:
            self._neutral_keys_by_causal_id.setdefault((name, value), event_key)
        return event_data

    def _record(
        self,
        index: int,
        row: dict[str, Any],
        evidence: dict[str, Any],
        latest_5m: dict[str, Any] | None,
        ordinary_context_ref: dict[str, str] | None,
        interaction_response: Sequence[dict[str, Any]],
    ) -> dict[str, Any]:
        timestamp = session_minute(self._session_date, row["time"])
        next_time = (
            session_minute(self._session_date, self._rows[index + 1]["time"])
            if index + 1 < len(self._rows) else timestamp
        )
        return {
            "sequence": index,
            "timestamp": timestamp,
            "available_at": next_time,
            "primary_state_before": "UNAVAILABLE",
            "primary_state_after": "UNAVAILABLE",
            "market_state_before": "UNAVAILABLE",
            "market_state_after": "UNAVAILABLE",
            "predicates": [],
            "events": [dict(item) for item in interaction_response],
            "hypotheses": [],
            "structural_windows": [],
            "bar": row,
            "latest_5m": latest_5m,
            "permission": None,
            "setup": None,
            "intent": None,
            "position": None,
            "location_observations": [],
            "setup_opportunities": [],
            "ordinary_context_ref": ordinary_context_ref,
            "capture_scope": "orderflow_state_machine",
        }


__all__ = ["ReplayMachine", "ReplayState"]
