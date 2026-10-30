"""Canonical reproducibility metadata for orderflow source rows."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def _integer_or_zero(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def canonical_source_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def minute(value: Any) -> str:
        text = str(value)
        return text.split("T", 1)[1][:5] if "T" in text else text[:5]

    return [{
        "time": minute(row.get("time") or row.get("minute")),
        "open": float(row["open"]), "high": float(row["high"]), "low": float(row["low"]),
        "close": float(row["close"]), "volume": _integer_or_zero(row.get("volume")),
        "oi": _integer_or_zero(row.get("oi", row.get("open_interest"))), "symbol": "NIFTY",
        "expiry": "", "contract_type": "CONTINUOUS", "session_date": str(row.get("session_date") or ""),
    } for row in rows]


def source_fingerprint(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(canonical_source_rows(rows), sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


def source_metadata(rows: list[dict[str, Any]]) -> dict[str, Any]:
    times = [str(row["time"])[:5] for row in rows]
    expected = [f"{9 + (15 + index) // 60:02d}:{(15 + index) % 60:02d}" for index in range(len(times))]
    fingerprint = source_fingerprint(rows)
    return {
        "symbol": sorted({str(row.get("symbol", "")) for row in rows}),
        "contract_type": sorted({str(row.get("contract_type", "")) for row in rows}),
        "expiry": sorted({str(row.get("expiry", "")) for row in rows}),
        "session_date": sorted({str(row.get("session_date", "")) for row in rows}),
        "source_bar_count": len(rows), "source_fingerprint": fingerprint, "fingerprint": fingerprint,
        "selected_symbol": "NIFTY", "selected_expiry": "", "instrument_type": "FUT_CONTINUOUS",
        "rollover_policy_version": "continuous-source-v1",
        "timestamp_convention": "1m label; completed at next minute; Asia/Kolkata",
        "ordered_unique_minutes": times == sorted(times) and len(times) == len(set(times)),
        "missing_minute_count": len(set(expected) - set(times)),
    }
