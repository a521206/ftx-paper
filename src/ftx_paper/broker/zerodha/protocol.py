"""Pure encoding/decoding helpers for the Zerodha market-data protocol."""

from __future__ import annotations

import json
import struct
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any


def subscription_messages(tokens: list[int]) -> tuple[str, str]:
    """Build subscribe and full-quote mode commands for one connection."""
    return (
        json.dumps({"a": "subscribe", "v": tokens}),
        json.dumps({"a": "mode", "v": ["full", tokens]}),
    )


def decode_binary_frame(data: bytes) -> tuple[dict[str, Any], ...]:
    """Decode one Kite binary frame into normalized callback payloads."""
    if len(data) < 2:
        return ()
    count = struct.unpack(">H", data[:2])[0]
    offset = 2
    payloads: list[dict[str, Any]] = []
    for _ in range(count):
        if offset + 2 > len(data):
            break
        length = struct.unpack(">H", data[offset : offset + 2])[0]
        offset += 2
        if offset + length > len(data):
            break
        packet = data[offset : offset + length]
        offset += length
        if len(packet) < 8:
            continue
        token, price_paise = struct.unpack(">Ii", packet[:8])
        payload: dict[str, Any] = {
            "instrument_token": token,
            "last_price": price_paise / 100.0,
            "last_traded_price": price_paise / 100.0,
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        }
        if len(packet) >= 44:
            fields = struct.unpack(">11i", packet[:44])
            payload.update({
                "last_traded_quantity": fields[2],
                "average_traded_price": fields[3] / 100.0,
                "volume_traded": fields[4],
                "total_buy_quantity": fields[5],
                "total_sell_quantity": fields[6],
                "open_price": fields[7] / 100.0,
                "high_price": fields[8] / 100.0,
                "low_price": fields[9] / 100.0,
                "close_price": fields[10] / 100.0,
            })
        if len(packet) >= 64:
            _, oi, _, _, exchange_timestamp = struct.unpack(">iiiii", packet[44:64])
            payload["oi"] = oi
            payload["open_interest"] = oi
            payload["exchange_timestamp"] = exchange_timestamp
        payloads.append(payload)
    return tuple(payloads)


def decode_text_error(message: str) -> str | None:
    """Return a broker error reason from a text frame, if present."""
    try:
        payload = json.loads(message)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, Mapping):
        return None
    if payload.get("type") != "error" and payload.get("status") != "error":
        return None
    return str(payload.get("message") or payload.get("code") or payload)
