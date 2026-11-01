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


__all__ = ["acceptance_trigger_evidence"]
