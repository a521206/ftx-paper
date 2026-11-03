"""Canonical causal market observations for Layer 1."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Any, Mapping

from .orderflow_common import Aggregate, ObservationIssue, ObservationIssueKind, build_aggregate_result
from .timestamps import parse_nifty_label, parse_session_timestamp, session_minute
from .state_machine_source import source_fingerprint

MARKET_CLOSE_TIME = "15:30"


def is_observation_minute(session_date: str, value: Any) -> bool:
    """Return whether a source minute falls before the exclusive market close."""
    return session_minute(session_date, value)[-5:] < MARKET_CLOSE_TIME


def rows_available_for_observation(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row for row in rows
        if is_observation_minute(str(row["session_date"]), row["time"])
    ]


def _row_timestamp(row: Mapping[str, Any]) -> datetime:
    return parse_session_timestamp(str(row["session_date"]), row["time"])


@dataclass(frozen=True, slots=True)
class ObservationGap:
    index: int
    previous_time: str
    next_time: str


@dataclass(frozen=True, slots=True)
class MarketObservation:
    aggregates: Mapping[str, tuple[Aggregate, ...]]
    source_gaps: tuple[ObservationGap, ...]
    session_boundaries: Mapping[int, tuple[str, str]]
    source_fingerprint: str
    issues: tuple[ObservationIssue, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "aggregates", MappingProxyType({key: tuple(value) for key, value in self.aggregates.items()}))
        object.__setattr__(self, "source_gaps", tuple(self.source_gaps))
        object.__setattr__(self, "session_boundaries", MappingProxyType(dict(self.session_boundaries)))
        object.__setattr__(self, "issues", tuple(self.issues))


def observe_market(rows: list[dict[str, Any]]) -> MarketObservation:
    observed_rows = rows_available_for_observation(rows)
    previous: datetime | None = None
    for row in observed_rows:
        timestamp = _row_timestamp(row)
        if previous is not None and timestamp <= previous:
            raise ValueError("market observation rows must be strictly ordered")
        previous = timestamp
    results = {timeframe: build_aggregate_result(observed_rows, timeframe) for timeframe in (1, 5)}
    issues = [issue for result in results.values() for issue in result.issues]
    gaps: list[ObservationGap] = []
    boundaries: dict[int, tuple[str, str]] = {}
    for index, (prior, current) in enumerate(zip(observed_rows, observed_rows[1:], strict=False), 1):
        prior_date, current_date = str(prior["session_date"]), str(current["session_date"])
        if current_date != prior_date:
            boundaries[index] = (prior_date, current_date)
            continue
        delta = _row_timestamp(current) - _row_timestamp(prior)
        if delta.total_seconds() > 60:
            gaps.append(ObservationGap(index, str(prior["time"]), str(current["time"])))
            covered = any(issue.kind == ObservationIssueKind.MISSING_SOURCE_BAR and issue.session_date == current_date
                and issue.group_start and issue.group_end and parse_nifty_label(f"{current_date}T{issue.group_start}") <= _row_timestamp(prior) <= parse_nifty_label(f"{current_date}T{issue.group_end}") for issue in issues)
            if not covered:
                issues.append(ObservationIssue(
                    ObservationIssueKind.MISSING_SOURCE_BAR,
                    current_date,
                    None,
                    session_minute(prior_date, prior["time"])[-5:],
                    session_minute(current_date, current["time"])[-5:],
                    "internal source-bar gap",
                ))
    return MarketObservation({f"{timeframe}m": result.aggregates for timeframe, result in results.items()}, tuple(gaps), boundaries, source_fingerprint(observed_rows), tuple(issues))
