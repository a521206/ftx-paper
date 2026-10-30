"""Export the Paper DB-backed orderflow market observation."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from ftx_paper.config import SESSION_DATA_END_MINUTES, SESSION_OPEN_MINUTES, PaperConfig
from ftx_paper.runtime import RuntimeStore
from ftx_paper.runtime.futures_source import FuturesSessionSourceResolver
from ftx_paper.strategies.orderflow import MarketObservation, observe_market


PRICE_DECIMALS = 1


def _minute(value: Any) -> str:
    text = str(value)
    if "T" in text:
        text = text.split("T", 1)[1]
    return text[:5]


def _minute_of_day(value: str) -> int:
    hour, minute = (int(item) for item in value.split(":", 1))
    return hour * 60 + minute


def _normalize_row(row: dict[str, Any], session_date: str) -> dict[str, Any]:
    return {
        "time": _minute(row["minute"]),
        "open": round(float(row["open"]), PRICE_DECIMALS),
        "high": round(float(row["high"]), PRICE_DECIMALS),
        "low": round(float(row["low"]), PRICE_DECIMALS),
        "close": round(float(row["close"]), PRICE_DECIMALS),
        "volume": int(row["volume"] or 0),
        "oi": int(row.get("open_interest") or 0),
        "symbol": "NIFTY",
        "expiry": "",
        "contract_type": "CONTINUOUS",
        "session_date": session_date,
    }


def _load_rows_for_date(session_date: str, runtime_dir: Path) -> list[dict[str, Any]]:
    store = RuntimeStore.open_read_only(runtime_dir)
    raw_rows = store.read_market_bars(session_date)
    source = FuturesSessionSourceResolver(store).resolve(session_date)
    if source is None:
        raise ValueError(f"continuous futures source is unavailable or unverified for {session_date}")

    identity = source.identity
    selected = [
        row for row in raw_rows
        if str(row.get("symbol")) == identity.symbol
        and str(row.get("expiry", ""))[:10] == identity.expiry
        and str(row.get("instrument_type", "")).upper() in {"FUT", "FUTURES"}
        and SESSION_OPEN_MINUTES <= _minute_of_day(_minute(row["minute"])) <= SESSION_DATA_END_MINUTES
    ]
    selected.sort(key=lambda row: str(row["minute"]))
    if not selected:
        raise ValueError(f"no verified futures rows found for {session_date}")
    return [_normalize_row(row, session_date) for row in selected]


def _payload(observation: MarketObservation) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "artifact_type": "canonical_p2_market_observation",
        "source_fingerprint": observation.source_fingerprint,
        "aggregates": {
            timeframe: [aggregate.to_dict() for aggregate in aggregates]
            for timeframe, aggregates in observation.aggregates.items()
        },
        "source_gaps": [
            {"index": gap.index, "previous_time": gap.previous_time, "next_time": gap.next_time}
            for gap in observation.source_gaps
        ],
        "issues": [
            {
                "kind": issue.kind.value,
                "session_date": issue.session_date,
                "timeframe": issue.timeframe,
                "group_start": issue.group_start,
                "group_end": issue.group_end,
                "details": issue.details,
            }
            for issue in observation.issues
        ],
        "session_boundaries": {
            str(index): list(boundary)
            for index, boundary in observation.session_boundaries.items()
        },
    }


def export_market_observation(session_date: str, output_path: Path, runtime_dir: Path) -> dict[str, Any]:
    payload = _payload(observe_market(_load_rows_for_date(session_date, runtime_dir)))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{output_path.name}.", suffix=".tmp", dir=output_path.parent,
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(payload, stream, indent=2, sort_keys=True)
            stream.write("\n")
        os.replace(temporary_name, output_path)
    except BaseException:
        Path(temporary_name).unlink(missing_ok=True)
        raise
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="ISO trading date")
    parser.add_argument("--runtime-dir", type=Path, default=None)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    runtime_dir = args.runtime_dir or PaperConfig.from_env().runtime_dir
    export_market_observation(args.date, args.output, runtime_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
