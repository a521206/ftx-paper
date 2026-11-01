"""Causal orderflow replay preparation shared by runtime and replay tooling."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import math
from pathlib import Path
from typing import Any, cast

from ftx_paper.runtime import RuntimeStore
from ftx_paper.runtime.futures_source import FuturesSessionSourceResolver
from ftx_paper.strategies.orderflow import observe_market
from ftx_paper.strategies.orderflow.state_machine import ReplayMachine
from ftx_paper.strategies.orderflow.evidence import (
    PaperFeatureAdapter,
    SerializedEvidenceAdapter,
    evidence_payload,
    produce_evidence,
)
from ftx_paper.strategies.orderflow.context_location_replay import replay_context_location_artifact
from ftx_paper.strategies.orderflow.interaction_replay import replay_interaction_response
from ftx_paper.strategies.orderflow.location_geometry import LocationBand
from ftx_paper.strategies.orderflow.orderflow_common import Aggregate
from ftx_paper.strategies.orderflow.state_machine_types import LocationRecord, LocationRegistry
from ftx_paper.strategies.orderflow.evidence_publication import EvidencePublication
from ftx_paper.strategies.orderflow.canonical.orderflow.value_publication import (
    prior_profile_is_complete,
    publish_value_references,
)
from ftx_paper.strategies.orderflow.value_publication import (
    PriorSessionValueReference,
    ValuePublication,
)
from ftx_paper.strategies.orderflow.timestamps import coerce_timestamp, nifty_label, session_minute

@dataclass(frozen=True, slots=True)
class _AvailableAggregate:
    available_at: datetime
    timeframe: str
    index: int
    value: dict[str, Any]


def _aggregate(value: dict[str, Any], timeframe: str) -> Aggregate:
    return Aggregate(
        coerce_timestamp(str(value["time"])), coerce_timestamp(str(value["end_time"])), coerce_timestamp(str(value["available_at"])),
        timeframe, int(value["source_bar_count"]), float(value["open"]),
        float(value["high"]), float(value["low"]), float(value["close"]),
        int(value["volume"]), str(value["symbol"]), str(value.get("expiry", "")),
        str(value.get("contract_type", "CONTINUOUS")), str(value["session_date"]),
        str(value["source_fingerprint"]),
    )


def _location_registry(evidence: dict[str, Any], session_date: str) -> LocationRegistry:
    stream_id = str(evidence.get("location_stream_id") or "orderflow:replay")
    registry = LocationRegistry(stream_id)
    for item in evidence.get("locations", ()):
        if not isinstance(item, dict):
            continue
        band = item.get("band", {})
        source_aggregate_id = item.get("source_aggregate_id")
        if source_aggregate_id is not None:
            source_aggregate_id = session_minute(session_date, str(source_aggregate_id))
        registry.register(LocationRecord(
             str(item["location_id"]), str(item["alias"]), float(item["price"]),
             str(item["source_session"]), coerce_timestamp(str(item["created_at"])),
             coerce_timestamp(str(item["available_at"])), stream_id,
            LocationBand(float(band["low"]), float(band["high"])),
            generation=int(item.get("generation", 1)), frozen_atr=item.get("frozen_atr"),
            parent_extreme_generation=item.get("parent_extreme_generation"),
             source_aggregate_id=source_aggregate_id,
            source_observation_id=item.get("source_observation_id"),
            timeframe=str(item.get("timeframe", "")), source_method=item.get("source_method"),
        ))
    return registry


def _normalize_rows(source: Any, session_date: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in source.bars:
        required = ("minute", "open", "high", "low", "close", "volume", "open_interest")
        if any(field not in row or row[field] is None for field in required):
            raise ValueError(f"source row is incomplete for {session_date}")
        try:
            minute = coerce_timestamp(str(row["minute"]), field="source minute")
        except ValueError as exc:
            raise ValueError(f"source row has an invalid minute for {session_date}") from exc
        numeric = [row[field] for field in ("open", "high", "low", "close", "volume", "open_interest")]
        if any(isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)) for value in numeric):
            raise ValueError(f"source row has invalid numeric values for {session_date}")
        rows.append({
            "time": nifty_label(minute),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": int(row["volume"]),
            "oi": int(row["open_interest"]),
            "symbol": "NIFTY",
            "expiry": "",
            "contract_type": "CONTINUOUS",
            "session_date": session_date,
        })
    return rows


def _load_session_rows(
    session_date: str, runtime_dir: Path,
) -> tuple[list[dict[str, Any]], dict[str, tuple[dict[str, Any], ...]]]:
    store = RuntimeStore.open_read_only(runtime_dir)
    resolver = FuturesSessionSourceResolver(store)
    dates = sorted(
        str(value) for value in store.read_dates()
        if date.fromisoformat(str(value)).isoformat() <= session_date
    )
    rows_by_date: dict[str, tuple[dict[str, Any], ...]] = {}
    for value in dates:
        source = resolver.resolve(value)
        if source is not None:
            rows_by_date[value] = tuple(_normalize_rows(source, value))
    rows = rows_by_date.get(session_date)
    if not rows:
        raise ValueError(f"continuous futures source is unavailable for {session_date}")
    return list(rows), rows_by_date


def _aggregate_events(observation: Any) -> tuple[_AvailableAggregate, ...]:
    events: list[_AvailableAggregate] = []
    for timeframe in ("1m", "5m"):
        for index, aggregate in enumerate(observation.aggregates[timeframe]):
            if aggregate.available_at < aggregate.end_time:
                raise ValueError("aggregate is available before completion")
            expected_count = 1 if timeframe == "1m" else 5
            if aggregate.source_bar_count != expected_count:
                raise ValueError(f"incomplete {timeframe} aggregate at {aggregate.time}")
            events.append(_AvailableAggregate(
                aggregate.available_at,
                timeframe,
                index,
                aggregate.to_dict(),
            ))
    return tuple(sorted(events, key=lambda item: (item.available_at, item.timeframe, item.index)))


def _observation_input_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Project replay input timestamps into the canonical source shape."""
    return [
        {
            **row,
            "time": session_minute(str(row["session_date"]), row["time"]),
        }
        for row in rows
    ]


def _observation_artifact(
    rows: list[dict[str, Any]],
) -> tuple[dict[str, Any], tuple[_AvailableAggregate, ...]]:
    observation = observe_market(_observation_input_rows(rows))
    return (
        {
            "schema_version": 1,
            "artifact_type": "canonical_p2_market_observation",
            "source_fingerprint": observation.source_fingerprint,
            "aggregates": {
                timeframe: [item.to_dict() for item in aggregates]
                for timeframe, aggregates in observation.aggregates.items()
            },
        },
        _aggregate_events(observation),
    )


def _prior_values(
    rows_by_date: dict[str, tuple[dict[str, Any], ...]],
) -> dict[str, ValuePublication]:
    publications: dict[str, ValuePublication] = {}
    dates = sorted(rows_by_date)
    for session_date in dates:
        prior_dates = [value for value in dates if value < session_date]
        if not prior_dates:
            continue
        prior_date = prior_dates[-1]
        prior_rows = list(rows_by_date[prior_date])
        rows = list(rows_by_date[session_date])
        references = publish_value_references(
            prior_rows,
            rows,
            session_date,
            prior_profile_complete=prior_profile_is_complete(prior_rows),
        )
        reference = next(
            (item for item in references if item.scope == "prior_session"),
            None,
        )
        if reference is None:
            continue
        available_at = reference.available_at
        observed_at = available_at
        value_reference = PriorSessionValueReference(
            reference.source_sessions[0],
            reference.val,
            reference.poc,
            reference.vah,
            coerce_timestamp(observed_at),
            coerce_timestamp(available_at),
            (reference.reference_id, reference.fingerprint),
        )
        publications[session_date] = ValuePublication(
            value_reference,
            value_reference.observed_at,
            value_reference.available_at,
            value_reference.lineage,
        )
    return publications


def _as_of_evidence(evidence: dict[str, Any], clock: str) -> dict[str, Any]:
    """Expose only publications available at the canonical decision clock."""
    result = dict(evidence)
    for field in ("locations", "vwap_measurements", "contexts"):
        result[field] = [
            item for item in evidence[field]
            if item.get("available_at") is None or coerce_timestamp(str(item["available_at"])) <= coerce_timestamp(clock)
        ]
    atr = evidence.get("atr")
    if isinstance(atr, dict) and atr.get("available_at") is not None and coerce_timestamp(str(atr["available_at"])) > coerce_timestamp(clock):
        result["atr"] = {**atr, "value": None, "unavailable_reason": "atr_not_yet_available"}
        result["unavailable"] = [*result["unavailable"], "atr_not_yet_available"]
    prior = evidence.get("prior_value")
    if isinstance(prior, dict) and prior.get("available_at") is not None and coerce_timestamp(str(prior["available_at"])) > coerce_timestamp(clock):
        result["prior_value"] = {
            **prior, "reference": None,
            "unavailable_reason": "prior_value_not_yet_available",
        }
        result["unavailable"] = [*result["unavailable"], "prior_value_not_yet_available"]
    return result


class OrderflowReplayEngine:
    """Replay raw session rows through the bounded canonical evidence layer."""

    def replay(
        self,
        rows: list[dict[str, Any]],
        rows_by_date: dict[str, tuple[dict[str, Any], ...]],
        *,
        session_date: str,
        include_interactions: bool = False,
        fill_permission_margin: float | None = 0.05,
    ) -> dict[str, Any]:
        if not rows:
            raise ValueError("replay session has no rows")
        if any(str(row.get("session_date")) != session_date for row in rows):
            raise ValueError("replay rows contain a session mismatch")
        artifact, events = _observation_artifact(rows)
        adapter = PaperFeatureAdapter(rows_by_date, prior_values=_prior_values(rows_by_date))
        evidence_events: list[
            tuple[datetime, dict[str, Any], dict[str, Any] | None, dict[str, Any]]
        ] = []
        latest_5m: dict[str, Any] | None = None
        prefix_aggregates: dict[str, list[dict[str, Any]]] = {"1m": [], "5m": []}
        for position, event in enumerate(events, start=1):
            if event.timeframe == "5m":
                latest_5m = event.value
            prefix_aggregates[event.timeframe].append(event.value)
            prefix = {
                **artifact,
                "aggregates": {
                    timeframe: list(values)
                    for timeframe, values in prefix_aggregates.items()
                },
            }
            evidence_events.append((
                event.available_at,
                evidence_payload(
                    produce_evidence(prefix, session_date=session_date, adapter=adapter),
                )["evidence"],
                latest_5m,
                {**event.value, "timeframe": event.timeframe},
            ))

        event_index = 0
        current_evidence: dict[str, Any] = {
            "atr": None,
            "locations": [],
            "vwap_measurements": [],
            "prior_value": None,
            "contexts": [],
            "unavailable": ["evidence_not_yet_available"],
            "context_stream_id": None,
            "location_stream_id": None,
        }

        def evidence_at_clock(clock: str) -> dict[str, Any]:
            nonlocal current_evidence, event_index
            while event_index < len(evidence_events) and evidence_events[event_index][0] <= coerce_timestamp(clock):
                current_evidence = evidence_events[event_index][1]
                event_index += 1
            return _as_of_evidence(current_evidence, clock)

        final_clock = session_minute(session_date, rows[-1]["time"])
        final_evidence = _as_of_evidence(
            evidence_events[-1][1] if evidence_events else current_evidence,
            final_clock,
        )
        phase3_artifact = artifact
        phase4_evidence = cast(EvidencePublication, produce_evidence(
            phase3_artifact,
            session_date=session_date,
            adapter=SerializedEvidenceAdapter({"evidence": final_evidence}),
        )["publication"])
        phase4_replay = replay_context_location_artifact(
            phase3_artifact,
            session_date=session_date,
            evidence=phase4_evidence,
            replay_clock=final_clock,
        )
        interaction_replay = None
        interaction_response: tuple[dict[str, Any], ...] = ()
        if include_interactions:
            aggregates = {
                timeframe: tuple(_aggregate(item, timeframe) for item in artifact["aggregates"][timeframe])
                for timeframe in ("1m", "5m")
            }
            interaction_replay = replay_interaction_response(
                aggregates, phase4_replay, _location_registry(final_evidence, session_date),
                atr=phase4_evidence.atr.value,
                fill_permission_margin=fill_permission_margin,
                prior_value=phase4_evidence.prior_value,
                raw_rows=rows,
                rows_by_date=rows_by_date,
            )
            interaction_response = interaction_replay.facts
        machine = ReplayMachine(
            rows,
            evidence_events=evidence_events,
            evidence_at_clock=evidence_at_clock,
            session_date=session_date,
        )
        # ReplayMachine is retained only as the bounded evidence projection.
        # Interaction and execution chronology are owned by the transition
        # owner inside replay_interaction_response above.
        replay_records, final_state = machine.run()

        final_evidence = _as_of_evidence(final_state.evidence, final_clock)
        locations = final_evidence["locations"]
        interaction_phase_facts = (
            interaction_replay.parity_phase_facts()
            if interaction_replay is not None else {}
        )
        source_identity = {
            "symbol": rows[0]["symbol"],
            "expiry": rows[0]["expiry"],
            "contract_type": rows[0]["contract_type"],
            "source_fingerprint": artifact["source_fingerprint"],
        }

        return {
            "artifact_type": "canonical_phase4_5_replay" if include_interactions else "canonical_phase3_5_replay",
            "schema_version": 1,
            "causal": True,
            "capture_mode": "phase4_5" if include_interactions else "phase3_5",
            "replay_mode": "evidence",
            "date": session_date,
            "session_date": session_date,
            "source_identity": source_identity,
            "observation_metadata": {
                "aggregate_counts": {
                    timeframe: len(artifact["aggregates"][timeframe])
                    for timeframe in ("1m", "5m")
                },
                "source_fingerprint": artifact["source_fingerprint"],
            },
            "phase3_artifact": phase3_artifact,
            "replay_records": replay_records,
            "registered_locations": locations,
            "prior_value": final_evidence["prior_value"],
            "context_stream_id": final_evidence["context_stream_id"],
            "parity_phase_facts": {
                "context_publication": list(final_state.context_publications),
                **phase4_replay.parity_phase_facts(),
                 # The transition owner contributes interaction and execution
                 # streams; ReplayMachine contributes evidence chronology only.
                **({"opportunity_lifecycle": interaction_phase_facts["opportunity_lifecycle"]}
                   if interaction_phase_facts else {}),
                 **({"interaction_response": list(interaction_response)} if include_interactions else {}),
                **({name: list(values) for name, values in interaction_replay.execution_phase_facts.items()}
                   if interaction_replay is not None else {}),
            },
            "evidence": final_evidence,
        }

    def replay_session(
        self,
        session_date: str,
        *,
        runtime_dir: Path,
        include_interactions: bool = False,
        fill_permission_margin: float | None = 0.05,
    ) -> dict[str, Any]:
        if date.fromisoformat(session_date).isoformat() != session_date:
            raise ValueError("date must use ISO YYYY-MM-DD format")
        rows, rows_by_date = _load_session_rows(session_date, runtime_dir)
        return self.replay(
            rows,
            rows_by_date,
            session_date=session_date,
            include_interactions=include_interactions,
            fill_permission_margin=fill_permission_margin,
        )
