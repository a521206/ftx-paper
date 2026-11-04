"""Causal microstructure evidence used by orderflow trigger evaluation."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Mapping, Sequence

from .orderflow_common import build_aggregate_result
from .state_machine_types import DISPLACEMENT_RULES
from .templates import Direction


def _volume_multiple(
    row: Mapping[str, Any],
    prior_rows: Sequence[Mapping[str, Any]],
    prior_session_rows: Sequence[Sequence[Mapping[str, Any]]],
) -> tuple[float, str, int]:
    same_minute = [
        float(bar["volume"])
        for session in prior_session_rows
        for bar in session
        if str(bar.get("time", ""))[:5] == str(row.get("time", ""))[:5]
    ]
    if len(same_minute) >= DISPLACEMENT_RULES.minimum_prior_sessions:
        baseline = sum(same_minute) / len(same_minute)
        return (float(row["volume"]) / baseline if baseline else 0.0,
                "prior_session_same_minute", len(same_minute))
    recent = [float(bar["volume"]) for bar in prior_rows]
    baseline = sum(recent) / len(recent) if recent else 0.0
    return (float(row["volume"]) / baseline if baseline else 0.0,
            "rolling_session_fallback", len(same_minute))


def acceptance_trigger_evidence(
    row: Mapping[str, Any],
    prior_rows: Sequence[Mapping[str, Any]],
    prior_session_rows: Sequence[Sequence[Mapping[str, Any]]],
    direction: Direction,
    *,
    include_diagnostics: bool = True,
) -> dict[str, Any]:
    """Require directional 1m displacement before an acceptance entry."""
    multiple, baseline, session_count = _volume_multiple(
        row, prior_rows, prior_session_rows,
    )
    prior_ranges = [
        float(bar["high"]) - float(bar["low"]) for bar in prior_rows
    ]
    average_range = sum(prior_ranges) / len(prior_ranges) if prior_ranges else 0.0
    bar_range = float(row["high"]) - float(row["low"])
    body = float(row["close"]) - float(row["open"])
    qualified = (
        bool(prior_ranges)
        and average_range > 0
        and bar_range > 0
        and body != 0
        and (body > 0) == (direction is Direction.LONG)
        and bar_range >= average_range * DISPLACEMENT_RULES.min_range_multiple
        and abs(body) / bar_range >= DISPLACEMENT_RULES.min_body_fraction
    )
    evidence: dict[str, Any] = {
        "qualified": qualified,
        "volume_multiple": multiple,
        "volume_baseline": baseline,
        "prior_session_count": session_count,
        "prior_range_average": average_range,
        "detector": asdict(DISPLACEMENT_RULES),
    }
    if include_diagnostics:
        evidence.update({
            "trigger_bar": [bar.to_dict() for bar in build_aggregate_result([row], 1).aggregates],
            "baseline_bars": [bar.to_dict() for bar in build_aggregate_result(list(prior_rows), 1).aggregates],
            "prior_session_volume_bars": [
                [dict(bar) for bar in session] for session in prior_session_rows
            ],
        })
    return evidence


def failed_continuation(
    prior: list[Mapping[str, Any]],
    completed1: Sequence[Any],
    completed5: Sequence[Any],
    *,
    direction: str,
    entry_price: float,
    failure_boundary: float,
    entry_time: str | None,
    synthetic: Sequence[Any] | None = None,
) -> dict[str, Any] | None:
    """Detect canonical post-entry continuation failure from causal evidence."""
    if direction not in {"long", "short"} or entry_time is None or len(prior) < 2:
        return None
    risk = abs(entry_price - failure_boundary)
    if risk <= 0:
        return None
    post_entry = [item for item in prior if str(item["time"]) >= entry_time]
    if not post_entry:
        return None
    favorable = (
        max(float(item["high"]) for item in post_entry) - entry_price
        if direction == "long"
        else entry_price - min(float(item["low"]) for item in post_entry)
    )
    if favorable < risk:
        return None

    if synthetic is not None:
        if len(synthetic) < 2:
            return None
        previous, latest = synthetic[-2], synthetic[-1]
        boundary = previous.high if direction == "short" else previous.low
        broke = (
            latest.close > boundary
            if direction == "short"
            else latest.close < boundary
        )
        if not broke:
            return None
        return {
            "direction": direction,
            "synthetic_boundary": boundary,
            "synthetic_break": broke,
            "synthetic_close": latest.close,
            "favorable_excursion": favorable,
            "risk": risk,
            "evidence_time": str(latest.last_source_end),
            "evidence_available_at": str(latest.available_at),
        }

    if len(completed1) < 2 or len(completed5) < 2:
        return None
    previous1, latest1 = completed1[-2], completed1[-1]
    previous5, latest5 = completed5[-2], completed5[-1]
    if len(completed1) >= 3:
        previous1 = completed1[-3]
    if len(completed5) >= 3:
        previous5 = completed5[-3]
    one_boundary = previous1.high if direction == "short" else previous1.low
    five_boundary = previous5.high if direction == "short" else previous5.low
    one_break = (
        latest1.close > one_boundary
        if direction == "short"
        else latest1.close < one_boundary
    )
    five_break = (
        latest5.close > five_boundary
        if direction == "short"
        else latest5.close < five_boundary
    )
    if not (one_break and five_break):
        return None
    return {
        "direction": direction,
        "one_minute_boundary": one_boundary,
        "one_minute_break": one_break,
        "one_minute_close": latest1.close,
        "five_minute_boundary": five_boundary,
        "five_minute_break": five_break,
        "five_minute_close": latest5.close,
        "favorable_excursion": favorable,
        "risk": risk,
        "evidence_time": str(latest5.time),
        "evidence_available_at": str(latest5.available_at),
    }


__all__ = ["acceptance_trigger_evidence", "failed_continuation"]
