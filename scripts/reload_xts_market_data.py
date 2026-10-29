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
from ftx_paper.contracts import Instrument, MarketBar
from ftx_paper.infrastructure.sqlite.runtime_store import SqliteRuntimeStore


SESSION_START = time(9, 15)
SESSION_END = time(15, 30)  # exclusive
EXPECTED_MINUTES = 375


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


def reload_dates(
    store: SqliteRuntimeStore, client: Any, dates: tuple[date, ...], *, apply: bool,
) -> int:
    total = 0
    for session_date in dates:
        instruments = resolve_instruments(client, _specifications(), as_of=session_date)
        fetched = tuple(
            bar for item in instruments
            for bar in _bars_for_instrument(client, item, session_date)
        )
        counts = Counter(bar.instrument.symbol.upper() for bar in fetched)
        expected_symbols = tuple(str(item["symbol"]).upper() for item in instruments)
        complete = all(counts[symbol] == EXPECTED_MINUTES for symbol in expected_symbols)
        print(
            f"{session_date.isoformat()}: "
            f"{[(item['symbol'], item['instrument_token']) for item in instruments]} "
            f"counts={counts} complete={complete}"
        )
        if not complete:
            raise RuntimeError(f"XTS returned incomplete required data for {session_date.isoformat()}")
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
