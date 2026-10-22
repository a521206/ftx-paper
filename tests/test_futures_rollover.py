from datetime import date, datetime, timezone

from ftx_paper.broker.zerodha.adapter import _nearest_live_futures
from ftx_paper.contracts import (
    FuturesContract,
    SELECTION_POLICY_VERSION,
    parse_expiry,
    select_front_futures_contract,
)
from ftx_paper.runtime.futures_source import FuturesSessionSourceResolver
from ftx_paper.runtime import RuntimeStore
from ftx_paper.contracts import Instrument, MarketBar


def test_front_contract_stays_selected_through_its_expiry_session():
    session = date(2026, 9, 29)
    contracts = [
        FuturesContract("NIFTY26SEPFUT", date(2026, 9, 29)),
        FuturesContract("NIFTY26OCTFUT", date(2026, 10, 27)),
    ]

    selection = select_front_futures_contract(contracts, session_date=session)

    assert selection is not None
    assert selection.symbol == "NIFTY26SEPFUT"
    assert selection.decision() == {
        "rule": "keep the nearest contract through its expiry session",
        "front_expiry": "2026-09-29",
        "days_to_front_expiry": 0,
        "selected_expiry": "2026-09-29",
    }
    assert selection.policy_version == SELECTION_POLICY_VERSION


def test_front_contract_rolls_on_the_session_after_expiry():
    contracts = [
        FuturesContract("NIFTY26SEPFUT", date(2026, 9, 29)),
        FuturesContract("NIFTY26OCTFUT", date(2026, 10, 27)),
    ]

    selection = select_front_futures_contract(contracts, session_date=date(2026, 9, 30))

    assert selection is not None
    assert selection.symbol == "NIFTY26OCTFUT"


def test_front_contract_selection_is_order_independent_and_breaks_ties_by_symbol():
    contracts = [
        FuturesContract("NIFTY26OCTFUT", date(2026, 10, 27)),
        FuturesContract("NIFTYA26OCTFUT", date(2026, 10, 27)),
        FuturesContract("NIFTY26SEPFUT", date(2026, 9, 29)),
    ]

    forward = select_front_futures_contract(contracts, session_date=date(2026, 9, 1))
    backward = select_front_futures_contract(list(reversed(contracts)), session_date=date(2026, 9, 1))

    assert forward is not None and backward is not None
    assert forward.symbol == backward.symbol == "NIFTY26SEPFUT"


def test_front_contract_returns_none_without_an_unexpired_contract():
    contracts = [FuturesContract("NIFTY26SEPFUT", date(2026, 9, 29))]

    assert select_front_futures_contract(contracts, session_date=date(2026, 9, 30)) is None


def test_parse_expiry_accepts_datetime_date_and_iso_text_only():
    assert parse_expiry(datetime(2026, 9, 29, 15, 30, tzinfo=timezone.utc)) == date(2026, 9, 29)
    assert parse_expiry(date(2026, 9, 29)) == date(2026, 9, 29)
    assert parse_expiry("2026-09-29") == date(2026, 9, 29)
    assert parse_expiry("2026-09-29T15:30:00+05:30") == date(2026, 9, 29)
    assert parse_expiry("not-a-date") is None
    assert parse_expiry(None) is None
    assert parse_expiry("") is None


def _bars_for(symbol: str, expiry: str, day: date) -> MarketBar:
    return MarketBar(
        Instrument(symbol, "NFO", "FUT", expiry),
        datetime(day.year, day.month, day.day, 4, 0, tzinfo=timezone.utc),
        100, 110, 90, 105,
    )


def test_live_and_replay_selection_agree_across_rollover(tmp_path):
    store = RuntimeStore(tmp_path)
    session_days = (date(2026, 9, 28), date(2026, 9, 29), date(2026, 9, 30))
    store.append_market_bars(tuple(
        bar
        for day in session_days
        for bar in (
            _bars_for("NIFTY26SEPFUT", "2026-09-29", day),
            _bars_for("NIFTY26OCTFUT", "2026-10-27", day),
        )
    ), source="fixture")
    broker_rows = [
        {"tradingsymbol": "NIFTY26SEPFUT", "name": "NIFTY", "instrument_type": "FUT",
         "expiry": "2026-09-29", "instrument_token": 1},
        {"tradingsymbol": "NIFTY26OCTFUT", "name": "NIFTY", "instrument_type": "FUT",
         "expiry": "2026-10-27", "instrument_token": 2},
    ]

    for day in session_days:
        live = _nearest_live_futures(broker_rows, underlying="NIFTY", as_of=day)
        replay = FuturesSessionSourceResolver(store).resolve(day.isoformat())
        assert live is not None and replay is not None
        assert live["tradingsymbol"] == replay.symbol
        assert parse_expiry(live["expiry"]) == date.fromisoformat(replay.expiry)
