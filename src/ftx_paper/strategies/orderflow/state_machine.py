"""Paper replay state-machine boundary.

This is the host-neutral orchestration seam for the staged canonical port.  It
does not define orderflow policy or lifecycle rules.  The current phase emits
evidence-only records; later canonical layers can replace the transition body
without moving replay clocking back into a script.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable

from .contextual import build_context_scores
from .hierarchy import MarketContext
from .orderflow_common import Aggregate
from .state_machine_types import MarketContextStore


EvidenceAtClock = Callable[[str], dict[str, Any]]


def _market_state(scores: tuple[float, float] | None) -> str:
    if scores is None:
        return "UNAVAILABLE"
    if scores[0] > scores[1]:
        return "LONG"
    if scores[1] > scores[0]:
        return "SHORT"
    return "BALANCE"


@dataclass(frozen=True, slots=True)
class ReplayState:
    """Immutable state exposed after one bounded replay prefix."""

    sequence: int
    latest_5m: dict[str, Any] | None
    evidence: dict[str, Any]
    ordinary_context_ref: dict[str, str] | None = None
    context_publications: tuple[dict[str, Any], ...] = ()


class ReplayMachine:
    """Advance Paper replay input through the shared transition boundary.

    The machine deliberately contains no strategy decisions.  It owns only
    chronological input delivery and the per-row capture shape required by the
    temporary evidence parity phase.
    """

    def __init__(
        self,
        rows: list[dict[str, Any]],
        *,
        evidence_events: Iterable[tuple[str, dict[str, Any], dict[str, Any] | None]],
        evidence_at_clock: EvidenceAtClock,
        session_date: str,
    ) -> None:
        if not rows:
            raise ValueError("replay session has no rows")
        self._rows = rows
        self._events = tuple(evidence_events)
        self._evidence_at_clock = evidence_at_clock
        self._session_date = session_date
        self._contexts = MarketContextStore()
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
            ordinary_context_ref=None,
            context_publications=(),
        )

    def run(self) -> tuple[list[dict[str, Any]], ReplayState]:
        records: list[dict[str, Any]] = []
        event_index = 0
        current_5m = self._state.latest_5m
        ordinary_context_ref = self._state.ordinary_context_ref
        context_publications = list(self._state.context_publications)
        completed_5m: list[Aggregate] = []
        for index, row in enumerate(self._rows):
            clock = f"{self._session_date}T{str(row['time'])[:5]}"
            while event_index < len(self._events) and self._events[event_index][0] <= clock:
                available_at, event_evidence, current_5m = self._events[event_index]
                if current_5m is not None and current_5m.get("timeframe") == "5m":
                    publication = self._publish_context(
                        current_5m, event_evidence, available_at, completed_5m,
                    )
                    completed_5m.append(self._aggregate(current_5m))
                    if publication is not None:
                        ordinary_context_ref = {
                            "context_id": publication["values"]["context_id"],
                            "version": publication["values"]["version"],
                        }
                        context_publications.append(publication)
                event_index += 1
            evidence = dict(self._evidence_at_clock(clock))
            evidence["contexts"] = [
                {"context_id": item["values"]["context_id"], "version": item["values"]["version"]}
                for item in context_publications
                if item["available_at"] <= clock
            ]
            contexts = [item for item in evidence.get("contexts", ()) if isinstance(item, dict)]
            if contexts:
                candidate = contexts[-1]
                if isinstance(candidate.get("context_id"), str) and isinstance(candidate.get("version"), str):
                    ordinary_context_ref = {
                        "context_id": candidate["context_id"],
                        "version": candidate["version"],
                    }
            records.append(self._record(index, row, evidence, current_5m, ordinary_context_ref))
            self._state = ReplayState(index, current_5m, evidence, ordinary_context_ref, tuple(context_publications))
        return records, self._state

    def _publish_context(
        self, aggregate_value: dict[str, Any], evidence: dict[str, Any], available_at: str,
        completed_5m: list[Aggregate],
    ) -> dict[str, Any] | None:
        aggregate = self._aggregate(aggregate_value)
        if aggregate.available_at < aggregate.end_time or aggregate.source_bar_count != 5:
            return None
        atr = evidence.get("atr")
        atr_value = float(atr["value"]) if isinstance(atr, dict) and atr.get("value") is not None else None
        prior = [item for item in self._contexts.versions.get(
            f"5m-context-{aggregate.symbol}-{aggregate.session_date}-1m_5m", (),
        )]
        context_id = f"5m-context-{aggregate.symbol}-{aggregate.session_date}-1m_5m"
        version = f"v{len(prior) + 1}"
        end_clock = aggregate.end_time[-5:]
        raw_rows = [
            row for row in self._rows
            if str(row.get("time", ""))[:5] <= end_clock
            and all(field in row for field in ("open", "high", "low", "close", "volume"))
        ]
        aggregates = {"5m": [*completed_5m, aggregate], "1m": [self._row_aggregate(row) for row in raw_rows]}
        scores = build_context_scores(aggregate, raw_rows=raw_rows, aggregates=aggregates, atr=atr_value)
        context = MarketContext(
            context_id, version, aggregate.end_time, aggregate.available_at, None,
            _market_state(scores), long_score=scores[0] if scores else None,
            short_score=scores[1] if scores else None,
            evidence=({"aggregate_id": aggregate.time},), source_timeframe="5m",
            definition_version="1m_5m",
            source_provenance=({"source_aggregate_id": aggregate.time, "available_at": available_at},),
        )
        reference = self._contexts.publish(context)
        self._contexts.ordinary_context = reference
        return {
            "kind": "context_published",
            "occurred_at": aggregate.end_time,
            "observed_at": aggregate.end_time,
            "available_at": aggregate.available_at,
            "sequence": len(prior),
            "identity": f"context:{reference.context_id}:{reference.version}",
            "causal_parent_id": f"aggregate:5m:{aggregate.time}",
            "values": {
                "context_id": reference.context_id,
                "version": reference.version,
                "location": context.location,
                "market_state": context.market_state,
                "long_score": context.long_score,
                "short_score": context.short_score,
                "parent_context_id": context.parent_context_id,
                "evidence": list(context.evidence),
                "source_timeframe": context.source_timeframe,
                "definition_version": context.definition_version,
                "source_provenance": list(context.source_provenance),
            },
            "unavailable": [] if scores is not None else ["missing_atr"],
        }

    @staticmethod
    def _aggregate(value: dict[str, Any]) -> Aggregate:
        return Aggregate(
            str(value["time"]), str(value["end_time"]), str(value["available_at"]),
            str(value["timeframe"]), int(value["source_bar_count"]), float(value["open"]),
            float(value["high"]), float(value["low"]), float(value["close"]),
            int(value["volume"]), str(value["symbol"]), str(value["expiry"]),
            str(value["contract_type"]), str(value["session_date"]),
            str(value["source_fingerprint"]),
        )

    @staticmethod
    def _row_aggregate(row: dict[str, Any]) -> Aggregate:
        time = str(row["time"])
        hour, minute = (int(part) for part in time[-5:].split(":")[:2])
        minute += 1
        if minute == 60:
            hour, minute = hour + 1, 0
        end_time = f"{time[:10]}{hour:02d}:{minute:02d}" if "T" in time else f"{hour:02d}:{minute:02d}"
        return Aggregate(
            time, end_time, end_time, "1m", 1, float(row["open"]), float(row["high"]),
            float(row["low"]), float(row["close"]), int(row["volume"]), str(row.get("symbol", "NIFTY")),
            str(row.get("expiry", "")), str(row.get("contract_type", "FUT")),
            str(row.get("session_date", "")), str(row.get("source_fingerprint", "source")),
        )

    def _record(
        self,
        index: int,
        row: dict[str, Any],
        evidence: dict[str, Any],
        latest_5m: dict[str, Any] | None,
        ordinary_context_ref: dict[str, str] | None,
    ) -> dict[str, Any]:
        timestamp = f"{self._session_date}T{str(row['time'])[:5]}"
        next_time = (
            f"{self._session_date}T{str(self._rows[index + 1]['time'])[:5]}"
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
            "ordinary_context_ref": ordinary_context_ref,
            "capture_scope": "phase3_5_evidence_only",
        }


__all__ = ["ReplayMachine", "ReplayState"]
