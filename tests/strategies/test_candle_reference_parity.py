from datetime import datetime
from types import SimpleNamespace

from scripts.orderflow.compare_canonical_replay import (
    _first_execution_trace_difference,
    _normalize_phase_record_for_compare,
    _phase6_structural_difference,
)
from ftx_paper.strategies.orderflow.orderflow_common import candle_reference


def _candle(start: str, end: str) -> SimpleNamespace:
    return SimpleNamespace(
        session_date="2026-09-29",
        timeframe="5m",
        time=datetime.fromisoformat(start),
        end_time=datetime.fromisoformat(end),
    )


def test_candle_reference_uses_utc_z_timestamps() -> None:
    reference = candle_reference(_candle("2026-09-29T03:45:00+00:00", "2026-09-29T03:50:00+00:00"))
    assert reference == {
        "session_date": "2026-09-29",
        "timeframe": "5m",
        "start_at": "2026-09-29T03:45:00.000000Z",
        "end_at": "2026-09-29T03:50:00.000000Z",
    }


def test_canonical_ist_and_paper_candle_representations_compare_equal() -> None:
    canonical = {"values": {"context_candle_reference": {
        "session_date": "2026-09-29", "timeframe": "5m",
        "start_at": "2026-09-29T09:15", "end_at": "2026-09-29T09:20",
    }, "context_candle_id": "2026-09-29T09:15"}}
    paper = {"values": {"context_candle_reference": candle_reference(
        _candle("2026-09-29T03:45:00+00:00", "2026-09-29T03:50:00+00:00")
    ), "context_candle_id": "paper-local-id"}}
    left = _normalize_phase_record_for_compare(canonical, "2026-09-29")
    right = _normalize_phase_record_for_compare(paper, "2026-09-29")
    assert left["values"]["context_candle_reference"] == right["values"]["context_candle_reference"]


def test_different_candles_do_not_compare_equal() -> None:
    reference = candle_reference(_candle("2026-09-29T03:45:00+00:00", "2026-09-29T03:50:00+00:00"))
    other = candle_reference(_candle("2026-09-29T03:50:00+00:00", "2026-09-29T03:55:00+00:00"))
    assert _phase6_structural_difference(
        [{"values": {"context_candle_reference": reference}}],
        [{"values": {"context_candle_reference": other}}],
    ) is not None


def test_candle_ids_are_not_generic_causal_timestamps() -> None:
    record = {"values": {
        "context_candle_id": "2026-09-29T09:15",
        "context_candle_reference": candle_reference(
            _candle("2026-09-29T03:45:00+00:00", "2026-09-29T03:50:00+00:00")
        ),
    }}
    normalized = _normalize_phase_record_for_compare(record, "2026-09-29")
    assert normalized["values"]["context_candle_id"] == "2026-09-29T09:15"


def test_execution_trace_difference_aligns_by_causal_clock() -> None:
    canonical = [{"observed_at": "2026-09-29T03:50:00Z", "available_at": "2026-09-29T03:51:00Z"}]
    paper = [
        {"observed_at": "2026-09-29T03:49:00Z", "available_at": "2026-09-29T03:50:00Z"},
        {"observed_at": "2026-09-29T03:50:00Z", "available_at": "2026-09-29T03:51:00Z"},
    ]
    difference = _first_execution_trace_difference(canonical, paper)
    assert difference is not None
    assert difference["clock"]["observed_at"] == "2026-09-29T03:49:00Z"
    assert difference["canonical"] is None
