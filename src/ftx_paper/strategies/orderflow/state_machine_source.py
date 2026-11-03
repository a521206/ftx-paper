"""Canonical reproducibility metadata for orderflow source rows."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

from .timestamps import session_minute


def _integer_or_zero(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def canonical_source_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        session_date = row.get("session_date")
        if not isinstance(session_date, str) or not session_date:
            raise ValueError("source row session_date must be a non-empty string")
        value = row.get("time") or row.get("minute")
        if not isinstance(value, (datetime, str)):
            raise ValueError("source row time must be a timestamp")
        normalized.append({
            "time": session_minute(session_date, value)[-5:],
            "open": float(row["open"]), "high": float(row["high"]), "low": float(row["low"]),
            "close": float(row["close"]), "volume": _integer_or_zero(row.get("volume")),
            "oi": _integer_or_zero(row.get("oi", row.get("open_interest"))), "symbol": "NIFTY",
            "expiry": "", "session_date": session_date,
        })
    return normalized


def _fingerprint(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(rows, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


def source_fingerprint(rows: list[dict[str, Any]]) -> str:
    return _fingerprint(canonical_source_rows(rows))


def source_metadata(rows: list[dict[str, Any]]) -> dict[str, Any]:
    canonical = canonical_source_rows(rows)
    times = [str(row["time"]) for row in canonical]
    expected = [f"{9 + (15 + index) // 60:02d}:{(15 + index) % 60:02d}" for index in range(len(times))]
    fingerprint = _fingerprint(canonical)
    return {
        "symbol": sorted({str(row.get("symbol", "")) for row in rows}),
        "expiry": sorted({str(row.get("expiry", "")) for row in rows}),
        "session_date": sorted({str(row.get("session_date", "")) for row in rows}),
        "source_bar_count": len(rows), "source_fingerprint": fingerprint, "fingerprint": fingerprint,
        "selected_symbol": "NIFTY", "selected_expiry": "", "instrument_type": "FUT_CONTINUOUS",
        "rollover_policy_version": "continuous-source-v1",
        "timestamp_convention": "1m label; completed at next minute; Asia/Kolkata",
        "ordered_unique_minutes": times == sorted(times) and len(times) == len(set(times)),
        "missing_minute_count": len(set(expected) - set(times)),
    }
