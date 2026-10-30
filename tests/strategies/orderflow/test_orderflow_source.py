from datetime import datetime, timedelta, timezone
from dataclasses import replace

import pytest

from ftx_paper.contracts import Instrument, MarketBar
from ftx_paper.strategies.orderflow import (
    normalize_session_date,
    prepare_replay_input,
)


def _bar(symbol: str, minute: int, *, expiry: str | None = "2026-01-29") -> MarketBar:
    return MarketBar(
        Instrument(symbol, "NSE", "FUTURES", expiry=expiry),
        datetime(2026, 1, 1, 3, 45, tzinfo=timezone.utc) + timedelta(minutes=minute),
        100.0, 101.0, 99.0, 100.0, 10.0,
    )


def test_session_date_normalization_uses_ist_for_timestamps() -> None:
    assert normalize_session_date(datetime(2026, 1, 1, 19, 0, tzinfo=timezone.utc)) == "2026-01-02"
    assert normalize_session_date("2026-01-01") == "2026-01-01"
    with pytest.raises(ValueError, match="timezone"):
        normalize_session_date(datetime(2026, 1, 1))


def test_prepare_requires_resolved_contract_selection() -> None:
    selected = [_bar("NIFTY26JAN", index) for index in range(2)]
    rejected = [_bar("NIFTY26FEB", index, expiry="2026-02-26") for index in range(2)]

    with pytest.raises(ValueError, match="source must be resolved"):
        prepare_replay_input([*selected, *rejected], "2026-01-01")


def test_prepare_allows_explicit_source_contract_selection() -> None:
    replay = prepare_replay_input(
        [_bar("NIFTY26JAN", 0), _bar("NIFTY26FEB", 0, expiry="2026-02-26")],
        "2026-01-01",
        source_contract="2026-02-26",
    )

    assert replay.metadata.source_contract == "2026-02-26"
    assert replay.rows[0].instrument.symbol == "NIFTY26FEB"


def test_prepare_rejects_duplicate_normalized_session_minutes() -> None:
    first = _bar("NIFTY26JAN", 0)
    second = MarketBar(
        first.instrument, first.timestamp.replace(second=30),
        first.open, first.high, first.low, first.close, first.volume,
    )
    with pytest.raises(ValueError, match="unique session minutes"):
        prepare_replay_input([first, second], "2026-01-01")


def test_prepare_rejects_out_of_order_source_rows() -> None:
    with pytest.raises(ValueError, match="chronological"):
        prepare_replay_input([_bar("NIFTY26JAN", 1), _bar("NIFTY26JAN", 0)], "2026-01-01")


def test_prepare_ignores_irrelevant_instrument_ordering() -> None:
    valid = _bar("NIFTY26JAN", 0)
    irrelevant = MarketBar(
        Instrument("NIFTY", "NSE", "SPOT"),
        datetime(2026, 1, 1, 3, 46, tzinfo=timezone.utc),
        100.0, 101.0, 99.0, 100.0, 10.0,
    )
    replay = prepare_replay_input(
        [valid, irrelevant, irrelevant], "2026-01-01", replayed_session=True,
    )
    assert replay.replayed


def test_prepare_rejects_rows_without_an_eligible_futures_source() -> None:
    row = MarketBar(
        Instrument("NIFTY", "NSE", "SPOT"),
        datetime(2026, 1, 1, 3, 45, tzinfo=timezone.utc),
        100.0, 101.0, 99.0, 100.0, 10.0,
    )
    with pytest.raises(ValueError, match="eligible futures"):
        prepare_replay_input([row], "2026-01-01")


def test_prepare_rejects_ambiguous_expiry_selection() -> None:
    rows = [_bar("NIFTY26JAN", 0), _bar("BANK26JAN", 0)]
    with pytest.raises(ValueError, match="multiple futures underlyings"):
        prepare_replay_input(rows, "2026-01-01", source_contract="2026-01-29")


def test_prepare_keeps_requested_date_separate_when_no_rows_replay() -> None:
    replay = prepare_replay_input([], "2026-01-01")

    assert replay.requested_date == "2026-01-01"
    assert replay.session_date == "2026-01-01"
    assert not replay.replayed
    assert replay.metadata.row_count == 0
    assert replay.metadata.timestamp_convention.startswith("1m label")
    assert replay.metadata.rollover_policy_version == "dated-nearest-expiry-v1"


def test_prepare_records_discarded_session_dates_without_mixing_state() -> None:
    other = replace(_bar("NIFTY26JAN", 0), timestamp=datetime(2026, 1, 2, 3, 45, tzinfo=timezone.utc))
    replay = prepare_replay_input([_bar("NIFTY26JAN", 0), other], "2026-01-01")

    assert replay.metadata.discarded_session_dates == ("2026-01-02",)


def test_out_of_session_rows_cannot_change_selected_source_validation_or_fingerprint() -> None:
    valid = _bar("NIFTY26JAN", 0)
    out_of_session = replace(valid, timestamp=datetime(2026, 1, 1, 3, 44, tzinfo=timezone.utc))

    baseline = prepare_replay_input([valid], "2026-01-01")
    with_extra = prepare_replay_input([valid, out_of_session, out_of_session], "2026-01-01")

    assert with_extra.rows == baseline.rows
    assert with_extra.metadata.source_fingerprint == baseline.metadata.source_fingerprint


def test_continuous_input_preserves_resolver_identity() -> None:
    replay = prepare_replay_input(
        [_bar("NIFTY26JAN", 0)], "2026-01-01", selection_rule="continuous_identity",
        source_contract="2026-01-29",
    )

    assert replay.rows[0].instrument.symbol == "NIFTY26JAN"
    assert replay.rows[0].instrument.expiry == "2026-01-29"


def test_resolver_provenance_keeps_storage_candidates_separate() -> None:
    replay = prepare_replay_input(
        [_bar("NIFTY", 0, expiry=None)], "2026-01-01",
        selection_rule="continuous_identity", replayed_session=True,
        source_provenance={
            "storage_identity": {"symbol": "NIFTY26JAN", "expiry": "2026-01-29"},
            "eligible_contracts": [{"symbol": "NIFTY26JAN", "expiry": "2026-01-29"}],
            "rejected_contracts": [{
                "symbol": "NIFTY26FEB", "expiry": "2026-02-26",
                "reason": "rollover_not_selected",
            }],
        },
    )

    assert replay.metadata.selected_candidates == ("NIFTY|NSE|FUTURES|||",)
    assert replay.metadata.storage_selected_candidates == ("NIFTY26JAN|2026-01-29",)
    assert replay.metadata.storage_rejected_candidates == (
        "NIFTY26FEB|2026-02-26|rollover_not_selected",
    )
