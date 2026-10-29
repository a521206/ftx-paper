"""Reload complete historical session bars from XTS into the runtime store.

The command is dry-run by default.  Pass ``--apply`` only after reviewing the
instrument resolution and fetched bar counts.  Existing rows are replaced by
the runtime store's normal market-bar upsert key; unrelated symbols and dates
are not touched.

Examples::

    .venv\\Scripts\\python.exe scripts\\reload_xts_market_data.py
    .venv\\Scripts\\python.exe scripts\\reload_xts_market_data.py --date 2026-10-08 --apply
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, time, timedelta
from typing import Any

from ftx_paper.broker.xts.adapter import XTSInstrument, instrument_expiry_iso, resolve_instruments
from ftx_paper.broker.xts.auth import XTSAuth
from ftx_paper.config import IST, PaperConfig
from ftx_paper.contracts import Instrument, MarketBar, MarketRole
from ftx_paper.infrastructure.sqlite.runtime_store import SqliteRuntimeStore


SESSION_START = time(9, 15)
SESSION_END = time(15, 30)  # exclusive
EXPECTED_MINUTES = 375
ANCHOR_SYMBOLS = frozenset(("NIFTY 50", "INDIA VIX"))


def _session_minutes(session_date: date) -> tuple[datetime, ...]:
    start = datetime.combine(session_date, SESSION_START, tzinfo=IST)
    return tuple(start + timedelta(minutes=index) for index in range(EXPECTED_MINUTES))


def _target_dates(store: SqliteRuntimeStore, requested: list[str]) -> tuple[date, ...]:
    values = requested or [
        item for item in store.read_market_dates()
        if item.startswith("2026-10-") and item < "2026-10-29"
    ]
    result: list[date] = []
    for value in values:
        parsed = date.fromisoformat(value)
        if parsed >= datetime.now(IST).date():
            raise ValueError(f"refusing current/future session: {value}")
        if parsed.isoformat() not in store.read_market_dates():
            raise ValueError(f"session is not present in runtime market_dates: {value}")
        result.append(parsed)
    return tuple(sorted(set(result)))


def _specifications() -> tuple[dict[str, object], ...]:
    return (
        {"exchange": "NSECM", "tradingsymbol": "NIFTY 50", "role": "spot"},
        {"exchange": "NSECM", "tradingsymbol": "INDIA VIX", "role": "vix"},
        {"exchange": "NFO", "underlying": "NIFTY", "role": "futures"},
    )


def _stored_futures_instrument(
    store: SqliteRuntimeStore, session_date: date,
) -> XTSInstrument:
    """Resolve the selected historical contract from the XTS token cache."""
    candidates = {
        (str(row.get("symbol")), str(row.get("expiry", ""))[:10])
        for row in store.read_market_bars(session_date.isoformat())
        if str(row.get("instrument_type", "")).upper() in {"FUT", "FUTURES"}
        and str(row.get("expiry", ""))[:10] >= session_date.isoformat()
    }
    if not candidates:
        raise ValueError(f"no stored futures contract for {session_date.isoformat()}")
    symbol, expiry = min(candidates, key=lambda item: item[1])
    cached = next(
        (
            row for row in store.read_runtime_contracts(
                exchange="NFO", underlying="NIFTY", role="futures", provider="xts",
            )
            if str(row.get("tradingsymbol")) == symbol
            and str(row.get("expiry", ""))[:10] == expiry
        ),
        None,
    )
    if cached is None:
        raise ValueError(f"no XTS token cached for {symbol} ({expiry})")
    return XTSInstrument(
        instrument_token=int(cached["instrument_token"]), segment_id=2,
        exchange="NFO", symbol=symbol, tradingsymbol=symbol,
        instrument_type="FUT", role=MarketRole.FUTURES, expiry=expiry,
        strike=None, name="NIFTY", lot_size=1, tick_size=0.05,
    )


def _bars_for_instrument(
    client: Any, item: XTSInstrument, session_date: date,
) -> tuple[MarketBar, ...]:
    rows = client.ohlc(
        item["segment_id"], item["instrument_token"],
        datetime.combine(session_date, SESSION_START, tzinfo=IST),
        datetime.combine(session_date, SESSION_END, tzinfo=IST),
        compression=60,
    )
    expected = _session_minutes(session_date)
    by_timestamp: dict[datetime, MarketBar] = {}
    instrument_type = str(item.get("instrument_type", "INDEX")).upper()
    if str(item["symbol"]).strip().upper() in {"NIFTY", "NIFTY 50", "INDIA VIX"}:
        instrument_type = "INDEX"
    expiry = instrument_expiry_iso(item.get("expiry")) if instrument_type in {"FUT", "CE", "PE"} else None
    for row in rows:
        raw_time = row.get("time") or row.get("timestamp")
        if raw_time is None:
            continue
        timestamp = datetime.fromisoformat(str(raw_time))
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=IST)
        timestamp = timestamp.astimezone(IST).replace(second=0, microsecond=0)
        if timestamp not in expected:
            continue
        values = tuple(float(row[name]) for name in ("open", "high", "low", "close"))
        by_timestamp[timestamp] = MarketBar(
            instrument=Instrument(
                symbol=str(item["symbol"]), exchange=str(item["exchange"]),
                instrument_type=instrument_type, expiry=expiry,
            ),
            timestamp=timestamp, open=values[0], high=values[1],
            low=values[2], close=values[3], volume=float(row.get("volume", 0.0)),
            open_interest=float(row.get("oi", 0.0)) if instrument_type in {"FUT", "FUTURES"} else None,
        )
    return tuple(by_timestamp[key] for key in expected if key in by_timestamp)


def _validate_counts(counts: Counter[str], expected_symbols: tuple[str, ...]) -> str:
    """Classify a session without treating sparse futures bars as corruption."""
    normalized = {symbol.upper() for symbol in expected_symbols}
    anchors = [counts[symbol] for symbol in normalized & ANCHOR_SYMBOLS]
    if not anchors or all(count == 0 for count in anchors):
        return "holiday_or_unavailable"
    if any(count == 0 for count in anchors) or len(set(anchors)) != 1:
        raise RuntimeError("XTS returned inconsistent spot/index data")

    anchor_count = anchors[0]
    if anchor_count > EXPECTED_MINUTES:
        raise RuntimeError("XTS returned more than one session of spot/index data")
    futures = [
        counts[symbol] for symbol in normalized - ANCHOR_SYMBOLS
        if counts[symbol] > 0
    ]
    if any(counts[symbol] == 0 for symbol in normalized - ANCHOR_SYMBOLS):
        raise RuntimeError("XTS returned missing required futures data")
    if any(count > anchor_count for count in futures):
        raise RuntimeError("XTS returned inconsistent futures data")
    return "complete" if anchor_count == EXPECTED_MINUTES else "partial"


def reload_dates(
    store: SqliteRuntimeStore, client: Any, dates: tuple[date, ...], *, apply: bool,
) -> int:
    total = 0
    for session_date in dates:
        instruments = resolve_instruments(
            client, _specifications(), contract_store=store, as_of=session_date,
        )
        historical_futures = _stored_futures_instrument(store, session_date)
        instruments = [
            historical_futures
            if str(item.get("instrument_type", "")).upper() in {"FUT", "FUTURES"}
            else item
            for item in instruments
        ]
        fetched = tuple(
            bar for item in instruments
            for bar in _bars_for_instrument(client, item, session_date)
        )
        counts = Counter(bar.instrument.symbol.upper() for bar in fetched)
        expected_symbols = tuple(str(item["symbol"]).upper() for item in instruments)
        status = _validate_counts(counts, expected_symbols)
        print(
            f"{session_date.isoformat()}: "
            f"{[(item['symbol'], item['instrument_token']) for item in instruments]} "
            f"counts={counts} status={status}"
        )
        if apply:
            store.append_market_bars(fetched, source="xts_reload")
        total += len(fetched)
    return total


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", action="append", default=[], help="Session date, repeatable")
    parser.add_argument("--apply", action="store_true", help="Write fetched bars; default is dry-run")
    args = parser.parse_args(argv)
    config = PaperConfig.from_env()
    store = SqliteRuntimeStore.open_read_only(config.runtime_dir) if not args.apply else SqliteRuntimeStore(config.runtime_dir)
    dates = _target_dates(store, args.date)
    auth = XTSAuth.from_config_path(config.xts_config, token_db=config.auth_db)
    client = auth.authenticated_client()
    total = reload_dates(store, client, dates, apply=args.apply)
    print(f"{'Reloaded' if args.apply else 'Validated'} {total} bars across {len(dates)} sessions")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
