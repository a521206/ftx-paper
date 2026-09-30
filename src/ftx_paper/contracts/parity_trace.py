"""Independent, deterministic trace contract for canonical/Paper parity tooling.

This module contains no strategy logic and imports no canonical implementation.
Adapters can project either engine's output into the same ordered stage shape.
"""
from __future__ import annotations

from copy import deepcopy
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal


ParityStatus = Literal["MATCHED", "MISMATCHED", "INCOMPLETE", "UNAVAILABLE"]


def observed_bar_count_to_index(sequence: int) -> int:
    """Project Paper's one-based observed-bar count to canonical zero-based k.

    This is for trace projection only. The original Paper sequence remains the
    value passed to its risk and cooldown gates.
    """
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
        raise ValueError("Paper sequence must be a positive observed-bar count")
    return sequence - 1

# Stable coarse-to-fine order. Adapters may emit these stages incrementally;
# reports must never reorder them to hide an earlier divergence.
STAGE_ORDER = (
    "input_context", "features_locations_transitions", "candidates_decisions",
    "risk_sizing", "orders_fills_exits_settlement", "portfolio_ledger",
)


@dataclass(frozen=True, slots=True)
class TraceRecord:
    """One ordered observation at a named stage."""

    stage: str
    ordinal: int
    identity: Mapping[str, Any]
    values: Mapping[str, Any]
    unavailable_fields: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class StageTrace:
    """Ordered records plus explicit stage coverage and missing capabilities."""

    stage: str
    records: tuple[TraceRecord, ...]
    complete: bool = True
    unavailable_fields: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if any(record.stage != self.stage for record in self.records):
            raise ValueError("record stage must match containing stage")
        if tuple(record.ordinal for record in self.records) != tuple(range(len(self.records))):
            raise ValueError("record ordinals must be contiguous and ordered from zero")


@dataclass(frozen=True, slots=True)
class StageComparison:
    stage: str
    status: ParityStatus
    canonical_count: int
    paper_count: int
    first_difference: Mapping[str, Any] | None = None
    unavailable_fields: tuple[str, ...] = ()


def compare_stage(
    canonical: StageTrace | None,
    paper: StageTrace | None,
    *,
    stage: str,
    ignored_value_fields: Iterable[str] = (),
) -> StageComparison:
    """Compare ordered records; only explicitly ignored representation fields are normalized away.

    Missing stage output is INCOMPLETE. Published but unavailable fields are
    UNAVAILABLE unless an independently visible value already mismatches.
    Record order and identity are always meaningful.
    """
    if canonical is None or paper is None:
        missing = tuple(field for field, value in (("canonical_stage", canonical),
                                                    ("paper_stage", paper)) if value is None)
        return StageComparison(stage, "INCOMPLETE", len(canonical.records) if canonical else 0,
                               len(paper.records) if paper else 0,
                               unavailable_fields=missing)
    if canonical.stage != stage or paper.stage != stage:
        raise ValueError("requested stage must match both trace stages")

    ignored = set(ignored_value_fields)
    unavailable = set(canonical.unavailable_fields) | set(paper.unavailable_fields)
    for row in (*canonical.records, *paper.records):
        unavailable.update(row.unavailable_fields)
    common = min(len(canonical.records), len(paper.records))
    for index in range(common):
        left, right = canonical.records[index], paper.records[index]
        if left.identity != right.identity:
            return StageComparison(stage, "MISMATCHED", len(canonical.records), len(paper.records),
                                   {"ordinal": index, "field": "identity", "canonical": dict(left.identity),
                                    "paper": dict(right.identity)}, tuple(sorted(unavailable)))
        record_unavailable = set(left.unavailable_fields) | set(right.unavailable_fields)
        keys = (set(left.values) | set(right.values)) - ignored - record_unavailable
        for key in sorted(keys):
            if key not in left.values or key not in right.values:
                return StageComparison(stage, "MISMATCHED", len(canonical.records), len(paper.records),
                                       {"ordinal": index, "field": key,
                                        "canonical": left.values.get(key), "paper": right.values.get(key),
                                        "reason": "field absent on one side"}, tuple(sorted(unavailable)))
            if left.values[key] != right.values[key]:
                return StageComparison(stage, "MISMATCHED", len(canonical.records), len(paper.records),
                                       {"ordinal": index, "field": key, "canonical": left.values[key],
                                        "paper": right.values[key]}, tuple(sorted(unavailable)))
    if len(canonical.records) != len(paper.records):
        return StageComparison(stage, "MISMATCHED", len(canonical.records), len(paper.records),
                               {"ordinal": common, "field": "record_count", "canonical": len(canonical.records),
                                "paper": len(paper.records)}, tuple(sorted(unavailable)))
    if not canonical.complete or not paper.complete:
        return StageComparison(stage, "INCOMPLETE", len(canonical.records), len(paper.records),
                               unavailable_fields=tuple(sorted(unavailable)))
    if unavailable:
        return StageComparison(stage, "UNAVAILABLE", len(canonical.records), len(paper.records),
                               unavailable_fields=tuple(sorted(unavailable)))
    return StageComparison(stage, "MATCHED", len(canonical.records), len(paper.records))


@dataclass(frozen=True, slots=True)
class HarnessResult:
    input_fingerprint: str
    canonical: tuple[StageTrace, ...]
    paper: tuple[StageTrace, ...]
    comparisons: tuple[StageComparison, ...]


def run_harness(
    input_bars: Sequence[Any],
    input_context: Mapping[str, Any],
    *,
    input_fingerprint: str,
    canonical_runner: Callable[[tuple[Any, ...], Mapping[str, Any]], Iterable[StageTrace]],
    paper_runner: Callable[[tuple[Any, ...], Mapping[str, Any]], Iterable[StageTrace]],
    stages: Sequence[str],
) -> HarnessResult:
    """Run injected adapters on the same immutable-by-convention inputs.

    Adapters are called independently and receive identical bar/context objects;
    the harness never imports or dispatches to either strategy implementation.
    """
    # Independent deep copies prevent one adapter from changing the inputs the
    # other receives while keeping each side's input content identical.
    bars = tuple(input_bars)
    context = dict(input_context)
    canonical_bars, canonical_context = deepcopy(bars), deepcopy(context)
    canonical_input = deepcopy((canonical_bars, canonical_context))
    canonical = tuple(canonical_runner(canonical_bars, canonical_context))
    if (canonical_bars, canonical_context) != canonical_input:
        raise ValueError("canonical runner mutated parity inputs")

    paper_bars, paper_context = deepcopy(bars), deepcopy(context)
    paper_input = deepcopy((paper_bars, paper_context))
    paper = tuple(paper_runner(paper_bars, paper_context))
    if (paper_bars, paper_context) != paper_input:
        raise ValueError("paper runner mutated parity inputs")
    c_by_stage = {trace.stage: trace for trace in canonical}
    p_by_stage = {trace.stage: trace for trace in paper}
    if len(c_by_stage) != len(canonical) or len(p_by_stage) != len(paper):
        raise ValueError("runner returned duplicate stage traces")
    comparisons = tuple(compare_stage(c_by_stage.get(stage), p_by_stage.get(stage), stage=stage)
                        for stage in stages)
    return HarnessResult(input_fingerprint, canonical, paper, comparisons)


__all__ = ["HarnessResult", "ParityStatus", "STAGE_ORDER", "StageComparison", "StageTrace", "TraceRecord",
           "compare_stage", "observed_bar_count_to_index", "run_harness"]
