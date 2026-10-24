"""Pure encoding/decoding helpers for the Shoonya (Finvasia) market-data protocol."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any


def subscription_messages(instruments: Sequence[tuple[str, str | int]]) -> str:
    """Build the subscription message for Shoonya touchline feed.

    instruments is a sequence of (exchange, token) pairs, e.g. [("NSE", 26000), ("NFO", 35000)].
    Shoonya format: {"t": "t", "k": "NSE|26000#NFO|35000"}
    """
    keys = "#".join(f"{exchange}|{token}" for exchange, token in instruments)
    return json.dumps({"t": "t", "k": keys})


def unsubscription_messages(instruments: Sequence[tuple[str, str | int]]) -> str:
    """Build the unsubscription message for Shoonya touchline feed."""
    keys = "#".join(f"{exchange}|{token}" for exchange, token in instruments)
    return json.dumps({"t": "u", "k": keys})


def connection_message(uid: str, token: str) -> str:
    """Build the WebSocket initial connection/auth handshake payload."""
    return json.dumps({
        "t": "c",
        "uid": uid,
        "actid": uid,
        "susertoken": token,
        "source": "API",
    })


def heartbeat_message() -> str:
    """Build the client heartbeat ping payload."""
    return json.dumps({"t": "h"})


def decode_message(data: str | bytes) -> list[dict[str, Any]]:
    """Decode incoming Shoonya text/JSON frame into normalized quote dictionaries."""
    if isinstance(data, bytes):
        text = data.decode("utf-8", errors="replace")
    else:
        text = data
    text = text.strip()
    if not text:
        return []

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []

    items = parsed if isinstance(parsed, list) else [parsed]
    payloads: list[dict[str, Any]] = []

    for item in items:
        if not isinstance(item, Mapping):
            continue
        msg_type = str(item.get("t", "")).lower()
        # "tk" = touchline snapshot, "tf" = touchline feed update, "dk" = depth snapshot, "df" = depth feed update
        if msg_type in {"tk", "tf", "dk", "df"}:
            token_raw = item.get("tk")
            if token_raw is None:
                continue
            try:
                token = int(token_raw)
            except (ValueError, TypeError):
                continue

            last_price_raw = item.get("lp")
            if last_price_raw is None:
                continue
            try:
                last_price = float(last_price_raw)
            except (ValueError, TypeError):
                continue

            # Feed time "ft" is epoch seconds in IST/UTC or string epoch
            raw_ft = item.get("ft")
            if raw_ft is not None:
                try:
                    timestamp_ms = int(float(raw_ft) * 1000)
                except (ValueError, TypeError):
                    timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            else:
                timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

            payload: dict[str, Any] = {
                "instrument_token": token,
                "exchange": str(item.get("e", "")),
                "last_price": last_price,
                "last_traded_price": last_price,
                "timestamp": timestamp_ms,
            }

            if "v" in item:
                try:
                    payload["volume_traded"] = float(item["v"])
                except (ValueError, TypeError):
                    pass
            if "oi" in item:
                try:
                    payload["oi"] = float(item["oi"])
                except (ValueError, TypeError):
                    pass
            if "o" in item:
                try:
                    payload["open_price"] = float(item["o"])
                except (ValueError, TypeError):
                    pass
            if "h" in item:
                try:
                    payload["high_price"] = float(item["h"])
                except (ValueError, TypeError):
                    pass
            if "l" in item:
                try:
                    payload["low_price"] = float(item["l"])
                except (ValueError, TypeError):
                    pass
            if "c" in item:
                try:
                    payload["close_price"] = float(item["c"])
                except (ValueError, TypeError):
                    pass

            payloads.append(payload)

    return payloads
