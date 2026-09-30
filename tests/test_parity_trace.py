import pytest

from ftx_paper.contracts.parity_trace import (
    StageTrace, TraceRecord, compare_stage, observed_bar_count_to_index, run_harness,
)


def test_paper_observed_sequence_projects_to_canonical_zero_based_bar_index():
    assert observed_bar_count_to_index(3) == 2
    assert observed_bar_count_to_index(1) == 0
    with pytest.raises(ValueError):
        observed_bar_count_to_index(0)


def trace(stage="input", values=None, *, unavailable=(), complete=True):
    record = TraceRecord(stage, 0, {"date": "2026-09-03", "minute": "09:15"},
                         values or {"close": 100.0}, tuple(unavailable))
    return StageTrace(stage, (record,), complete, tuple(unavailable))


def test_comparator_preserves_record_order_and_reports_first_field():
    left = StageTrace("features", (
        TraceRecord("features", 0, {"minute": "09:15"}, {"vwap": 100.0}),
        TraceRecord("features", 1, {"minute": "09:16"}, {"vwap": 101.0}),
    ))
    right = StageTrace("features", (
        TraceRecord("features", 0, {"minute": "09:15"}, {"vwap": 100.0}),
        TraceRecord("features", 1, {"minute": "09:16"}, {"vwap": 101.5}),
    ))
    result = compare_stage(left, right, stage="features")
    assert result.status == "MISMATCHED"
    assert result.first_difference == {"ordinal": 1, "field": "vwap", "canonical": 101.0, "paper": 101.5}


def test_comparator_distinguishes_incomplete_unavailable_and_matched():
    assert compare_stage(None, trace(), stage="input").status == "INCOMPLETE"
    assert compare_stage(trace(unavailable=("vix",)), trace(), stage="input").status == "UNAVAILABLE"
    assert compare_stage(trace(), trace(), stage="input").status == "MATCHED"
    assert compare_stage(trace(values={"vix": -1}, unavailable=("vix",)),
                         trace(values={"vix": 14.0}, unavailable=("vix",)),
                         stage="input").status == "UNAVAILABLE"


def test_comparator_only_ignores_explicit_representation_fields():
    left = trace(values={"close": 100.0, "implementation_id": "canonical-1"})
    right = trace(values={"close": 100.0, "implementation_id": "paper-1"})
    assert compare_stage(left, right, stage="input").status == "MISMATCHED"
    assert compare_stage(left, right, stage="input", ignored_value_fields=("implementation_id",)).status == "MATCHED"


def test_harness_passes_same_bars_and_context_to_independent_runners():
    seen = []
    source = [{"close": 100.0}]

    def runner(bars, context):
        seen.append((bars, context))
        return (trace(),)

    result = run_harness(source, {"session": "morning"}, input_fingerprint="fixture-sha",
                         canonical_runner=runner, paper_runner=runner, stages=("input", "features"))
    assert seen[0] == seen[1] and seen[0] is not seen[1]
    assert source == [{"close": 100.0}]
    assert result.input_fingerprint == "fixture-sha"
    assert [item.status for item in result.comparisons] == ["MATCHED", "INCOMPLETE"]


def test_harness_rejects_runner_input_mutation():
    def mutating_runner(bars, context):
        bars[0]["close"] = -1
        return (trace(),)

    with pytest.raises(ValueError, match="canonical runner mutated parity inputs"):
        run_harness([{"close": 100.0}], {"session": "morning"}, input_fingerprint="fixture-sha",
                    canonical_runner=mutating_runner, paper_runner=lambda *_: (trace(),),
                    stages=("input",))
