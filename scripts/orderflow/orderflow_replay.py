"""Temporary Phase 3.5 evidence harness, not the production replay engine.

This runner is intentionally disposable: it creates bounded evidence artifacts
needed while the canonical orderflow layers are being ported and verified. It
must be retired once Paper live/replay is wired to the shared canonical
transition boundary. It must not become a second strategy state machine or be
called from the live callback path.

The runner mirrors the canonical ``ReplayMachine.from_session_inputs``
preparation boundary without entering the later interaction, opportunity, or
execution layers.  It loads the selected futures session from the Paper store,
builds canonical observations, and emits one canonical-shaped replay record per
source row with the evidence available at that row's decision clock.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime
import json
import math
from pathlib import Path
from typing import Any

from ftx_paper.config import PaperConfig
from ftx_paper.runtime import RuntimeStore
from ftx_paper.runtime.futures_source import FuturesSessionSourceResolver
from ftx_paper.strategies.orderflow import observe_market
from ftx_paper.strategies.orderflow.evidence import (
    PaperFeatureAdapter,
    evidence_payload,
    produce_evidence,
)

PRICE_DECIMALS = 1


@dataclass(frozen=True, slots=True)
class _AvailableAggregate:
    available_at: str
    timeframe: str
    index: int
    value: dict[str, Any]


def _normalize_rows(source: Any, session_date: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in source.bars:
        required = ("minute", "open", "high", "low", "close", "volume", "open_interest")
        if any(field not in row or row[field] is None for field in required):
            raise ValueError(f"source row is incomplete for {session_date}")
        try:
            minute = datetime.fromisoformat(str(row["minute"]))
        except ValueError as exc:
            raise ValueError(f"source row has an invalid minute for {session_date}") from exc
        numeric = [row[field] for field in ("open", "high", "low", "close", "volume", "open_interest")]
        if any(isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)) for value in numeric):
            raise ValueError(f"source row has invalid numeric values for {session_date}")
        rows.append({
            "time": minute.strftime("%H:%M"),
            "open": round(float(row["open"]), PRICE_DECIMALS),
            "high": round(float(row["high"]), PRICE_DECIMALS),
            "low": round(float(row["low"]), PRICE_DECIMALS),
            "close": round(float(row["close"]), PRICE_DECIMALS),
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


def _observation_artifact(
    rows: list[dict[str, Any]],
) -> tuple[dict[str, Any], tuple[_AvailableAggregate, ...]]:
    observation = observe_market(rows)
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


def _prefix_evidence(
    artifact: dict[str, Any],
    events: tuple[_AvailableAggregate, ...],
    position: int,
    *,
    session_date: str,
    adapter: PaperFeatureAdapter,
) -> dict[str, Any]:
    prefix: dict[str, Any] = {**artifact, "aggregates": {"1m": [], "5m": []}}
    for event in events[:position]:
        prefix["aggregates"][event.timeframe].append(event.value)
    return evidence_payload(
        produce_evidence(prefix, session_date=session_date, adapter=adapter),
    )["evidence"]


def _as_of_evidence(evidence: dict[str, Any], clock: str) -> dict[str, Any]:
    """Expose only publications available at the canonical decision clock."""
    result = dict(evidence)
    for field in ("locations", "vwap_measurements", "contexts"):
        result[field] = [
            item for item in evidence[field]
            if item.get("available_at") is None or str(item["available_at"]) <= clock
        ]
    atr = evidence.get("atr")
    if isinstance(atr, dict) and atr.get("available_at") is not None and str(atr["available_at"]) > clock:
        result["atr"] = {**atr, "value": None, "unavailable_reason": "atr_not_yet_available"}
        result["unavailable"] = [*result["unavailable"], "atr_not_yet_available"]
    prior = evidence.get("prior_value")
    if isinstance(prior, dict) and prior.get("available_at") is not None and str(prior["available_at"]) > clock:
        result["prior_value"] = {
            **prior, "reference": None,
            "unavailable_reason": "prior_value_not_yet_available",
        }
        result["unavailable"] = [*result["unavailable"], "prior_value_not_yet_available"]
    return result


def _replay_record(
    index: int,
    row: dict[str, Any],
    rows: list[dict[str, Any]],
    evidence: dict[str, Any],
    latest_5m: dict[str, Any] | None,
) -> dict[str, Any]:
    session_date = str(row["session_date"])
    timestamp = f"{session_date}T{str(row['time'])[:5]}"
    next_time = (
        f"{session_date}T{str(rows[index + 1]['time'])[:5]}"
        if index + 1 < len(rows) else timestamp
    )
    contexts = [item for item in evidence.get("contexts", ()) if isinstance(item, dict)]
    latest_context = max(
        contexts,
        key=lambda item: (str(item.get("available_at", "")), str(item.get("version", ""))),
        default=None,
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
        "events": [],
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
        "ordinary_context_ref": (
            {"context_id": latest_context["context_id"], "version": latest_context["version"]}
            if latest_context is not None
            and latest_context.get("context_id") is not None
            and latest_context.get("version") is not None
            else None
        ),
        "capture_scope": "phase3_5_evidence_only",
    }


class OrderflowReplayEngine:
    """Replay raw session rows through the bounded canonical evidence layer."""

    def replay(
        self,
        rows: list[dict[str, Any]],
        rows_by_date: dict[str, tuple[dict[str, Any], ...]],
        *,
        session_date: str,
    ) -> dict[str, Any]:
        if not rows:
            raise ValueError("replay session has no rows")
        if any(str(row.get("session_date")) != session_date for row in rows):
            raise ValueError("replay rows contain a session mismatch")
        artifact, events = _observation_artifact(rows)
        adapter = PaperFeatureAdapter(rows_by_date)
        evidence_events: list[tuple[str, dict[str, Any], dict[str, Any] | None]] = []
        latest_5m: dict[str, Any] | None = None
        for position, event in enumerate(events, start=1):
            if event.timeframe == "5m":
                latest_5m = event.value
            evidence_events.append((
                event.available_at,
                _prefix_evidence(artifact, events, position, session_date=session_date, adapter=adapter),
                latest_5m,
            ))

        replay_records: list[dict[str, Any]] = []
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
        current_5m: dict[str, Any] | None = None
        event_index = 0
        for index, row in enumerate(rows):
            clock = f"{session_date}T{str(row['time'])[:5]}"
            while event_index < len(evidence_events) and evidence_events[event_index][0] <= clock:
                _, current_evidence, current_5m = evidence_events[event_index]
                event_index += 1
            replay_records.append(_replay_record(
                index, row, rows, _as_of_evidence(current_evidence, clock), current_5m,
            ))

        final_clock = f"{session_date}T{str(rows[-1]['time'])[:5]}"
        final_evidence = _as_of_evidence(current_evidence, final_clock)
        locations = final_evidence["locations"]
        source_identity = {
            "symbol": rows[0]["symbol"],
            "expiry": rows[0]["expiry"],
            "contract_type": rows[0]["contract_type"],
            "source_fingerprint": artifact["source_fingerprint"],
        }

        return {
            "artifact_type": "canonical_phase3_5_replay",
            "schema_version": 1,
            "causal": True,
            "capture_mode": "phase3_5",
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
            "replay_records": replay_records,
            "registered_locations": locations,
            "prior_value": final_evidence["prior_value"],
            "context_stream_id": final_evidence["context_stream_id"],
            "parity_phase_facts": {
                "context_publication": final_evidence["contexts"],
            },
            "evidence": final_evidence,
        }

    def replay_session(self, session_date: str, *, runtime_dir: Path) -> dict[str, Any]:
        if date.fromisoformat(session_date).isoformat() != session_date:
            raise ValueError("date must use ISO YYYY-MM-DD format")
        rows, rows_by_date = _load_session_rows(session_date, runtime_dir)
        return self.replay(rows, rows_by_date, session_date=session_date)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="ISO trading date")
    parser.add_argument("--runtime-dir", type=Path, help="Paper runtime directory")
    parser.add_argument("--output", type=Path, help="Write replay JSON instead of stdout")
    args = parser.parse_args(argv)
    try:
        runtime_dir = args.runtime_dir or PaperConfig.from_env().runtime_dir
        result = OrderflowReplayEngine().replay_session(args.date, runtime_dir=runtime_dir)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        result = {"status": "INCOMPLETE", "reason": str(exc)}
    rendered = json.dumps(result, indent=2, sort_keys=True)
    if args.output is not None and result.get("status") != "INCOMPLETE":
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)
    return 0 if result.get("status") != "INCOMPLETE" else 1


if __name__ == "__main__":
    raise SystemExit(main())
