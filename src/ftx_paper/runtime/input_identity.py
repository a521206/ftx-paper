"""Strategy-neutral normalized input identity helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from hashlib import sha256
import json
from zoneinfo import ZoneInfo
from typing import Any

from ftx_paper.strategies.orderflow.timestamps import parse_utc

NORMALIZED_BAR_FINGERPRINT_RECIPE = "ordered-canonical-bars-v1"
_SESSION_ZONE = ZoneInfo("Asia/Kolkata")


def normalized_bar_fingerprint(rows: Sequence[Mapping[str, Any]]) -> str:
    normalized = []
    for row in rows:
        minute = str(row.get("minute") or row.get("time") or "")
        try:
            timestamp = parse_utc(minute, field="parity bar timestamp")
        except ValueError as exc:
            raise ValueError("parity bar timestamps must be ISO timestamps") from exc
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("parity bar timestamps must be timezone-aware")
        if any(row.get(name) is None for name in ("open", "high", "low", "close")):
            raise ValueError("parity bars require complete OHLCV values")
        try:
            values = {
                "open": float(row["open"]), "high": float(row["high"]),
                "low": float(row["low"]), "close": float(row["close"]),
                "volume": int(row["volume"]) if row.get("volume") is not None else 0,
                "oi": int(row.get("oi", row.get("open_interest")) or 0),
            }
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("parity bar OHLCV values must be numeric") from exc
        session_timestamp = timestamp.astimezone(_SESSION_ZONE)
        normalized.append({
            "time": session_timestamp.strftime("%H:%M"), **values,
            "symbol": "NIFTY", "expiry": "",
            "session_date": str(row.get("session_date") or session_timestamp.date().isoformat()),
        })
    return sha256(json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


__all__ = ["NORMALIZED_BAR_FINGERPRINT_RECIPE", "normalized_bar_fingerprint"]
