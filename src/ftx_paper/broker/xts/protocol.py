"""Pure encoding/decoding helpers for Symphony / IIFL XTS market-data protocol."""

from __future__ import annotations

import json
import struct
import zlib
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

SEGMENT_MAP: dict[str, int] = {
    "NSECM": 1,
    "NSE": 1,
    "NSE_CM": 1,
    "1": 1,
    "NSEFO": 2,
    "NFO": 2,
    "NSE_FO": 2,
    "2": 2,
    "NSECD": 3,
    "CDS": 3,
    "3": 3,
    "BSECM": 11,
    "BSE": 11,
    "BSE_CM": 11,
    "11": 11,
    "BSEFO": 12,
    "BFO": 12,
    "12": 12,
    "MCXFO": 51,
    "MCX": 51,
    "51": 51,
}

REVERSE_SEGMENT_MAP: dict[int, str] = {
    1: "NSE",
    2: "NFO",
    3: "CDS",
    11: "BSE",
    12: "BFO",
    51: "MCX",
}


def normalize_segment(exchange: str | int) -> int:
    """Normalize exchange name or segment id to integer XTS exchangeSegment."""
    if isinstance(exchange, int):
        return exchange
    cleaned = str(exchange).strip().upper()
    if cleaned.isdigit():
        return int(cleaned)
    if cleaned in SEGMENT_MAP:
        return SEGMENT_MAP[cleaned]
    raise ValueError(f"Unknown XTS exchange segment: {exchange}")


def exchange_code_for_segment(segment: int) -> str:
    """Return standard short exchange code (NSE, NFO, etc.) for segment ID."""
    return REVERSE_SEGMENT_MAP.get(segment, f"EXCH_{segment}")


def build_subscription_payload(
    instruments: Sequence[tuple[str | int, int]],
    message_code: int = 1502,
) -> dict[str, Any]:
    """Build REST subscription payload for Symphony XTS."""
    items = []
    for exchange, token in instruments:
        seg = normalize_segment(exchange)
        items.append({
            "exchangeSegment": seg,
            "exchangeInstrumentID": int(token),
        })
    return {
        "instruments": items,
        "xtsMessageCode": message_code,
    }


def parse_xts_timestamp(val: Any) -> int:
    """Parse XTS timestamp (epoch seconds, milliseconds, or datetime string) to ms."""
    if val is None:
        return int(datetime.now(UTC).timestamp() * 1000)
    if isinstance(val, (int, float)):
        # If greater than 10^11, assume already in milliseconds
        return int(val) if val > 100_000_000_000 else int(val * 1000)
    raw = str(val).strip()
    if not raw:
        return int(datetime.now(UTC).timestamp() * 1000)
    if raw.isdigit():
        num = int(raw)
        return num if num > 100_000_000_000 else num * 1000
    try:
        fnum = float(raw)
        return int(fnum) if fnum > 100_000_000_000 else int(fnum * 1000)
    except ValueError:
        pass

    for fmt in ("%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%b %d %Y %H%M%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(raw[:19], fmt).replace(tzinfo=IST)
            return int(dt.timestamp() * 1000)
        except ValueError:
            pass

    return int(datetime.now(UTC).timestamp() * 1000)


def extract_socket_io_data(text: str) -> tuple[str, Any] | None:
    """Extract event name and payload from Socket.IO Engine.IO frame.
    
    Standard frames:
    42["1502-json-full", "{...}"]
    """
    text = text.strip()
    if not text.startswith("42"):
        return None
    content = text[2:]
    try:
        parsed = json.loads(content)
        if isinstance(parsed, list) and len(parsed) >= 2:
            event_name = str(parsed[0])
            data = parsed[1]
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError:
                    pass
            return event_name, data
        elif isinstance(parsed, list) and len(parsed) == 1:
            return "message", parsed[0]
    except Exception:
        pass
    return None


def decode_touchline_data(data: Any) -> list[dict[str, Any]]:
    """Decode XTS Touchline (1502 / 1501) payload into normalized market quote dicts."""
    if not isinstance(data, Mapping):
        return []

    # Could be wrapped in result or quotesList
    if "result" in data and isinstance(data["result"], Mapping):
        data = data["result"]

    seg = data.get("ExchangeSegment")
    token = data.get("ExchangeInstrumentID")
    if token is None:
        return []

    try:
        inst_token = int(token)
    except (ValueError, TypeError):
        return []

    seg_int = int(seg) if seg is not None and str(seg).isdigit() else 1
    exchange = exchange_code_for_segment(seg_int)

    touchline = data.get("Touchline")
    if not isinstance(touchline, Mapping):
        touchline = data

    ltp_raw = touchline.get("LastTradedPrice") or touchline.get("LTP") or touchline.get("last_price")
    if ltp_raw is None:
        return []

    try:
        last_price = float(ltp_raw)
    except (ValueError, TypeError):
        return []

    ts_raw = touchline.get("LastUpdateTime") or touchline.get("ExchangeTimestamp") or data.get("ExchangeTimestamp")
    timestamp_ms = parse_xts_timestamp(ts_raw)

    payload: dict[str, Any] = {
        "instrument_token": inst_token,
        "exchange": exchange,
        "last_price": last_price,
        "last_traded_price": last_price,
        "timestamp": timestamp_ms,
    }

    vol_raw = touchline.get("TotalTradedQuantity") or touchline.get("Volume") or touchline.get("LastTradedQunatity") or touchline.get("LastTradedQuantity")
    if vol_raw is not None:
        try:
            payload["volume_traded"] = float(vol_raw)
        except (ValueError, TypeError):
            pass

    oi_raw = touchline.get("OpenInterest") or touchline.get("OI")
    if oi_raw is not None:
        try:
            payload["oi"] = float(oi_raw)
        except (ValueError, TypeError):
            pass

    for key, target in [("Open", "open_price"), ("High", "high_price"), ("Low", "low_price"), ("Close", "close_price")]:
        v = touchline.get(key)
        if v is not None:
            try:
                payload[target] = float(v)
            except (ValueError, TypeError):
                pass

    return [payload]


def decode_binary_packets(data: bytes) -> list[dict[str, Any]]:
    """Decode raw binary frames (xts-binary-packet) streamed from IIFL/Symphony XTS."""
    results: list[dict[str, Any]] = []
    if not data:
        return results

    idx = 0
    # Engine.IO message prefix byte (0x04) if present
    if data[0] == 4:
        idx = 1

    while idx + 17 <= len(data):
        is_compressed = bool(data[idx])
        msg_code, seg, inst_id, book, mkt, decomp_len, comp_len = struct.unpack_from('<HHihhHH', data, idx + 1)
        if comp_len <= 0 or idx + 17 + comp_len > len(data):
            break

        chunk = data[idx + 17 : idx + 17 + comp_len]
        try:
            decomp = zlib.decompress(chunk, -15) if is_compressed else chunk
        except Exception:
            idx += 17 + comp_len
            continue

        if msg_code in (1502, 1501) and len(decomp) >= 416:
            try:
                ltp = float(struct.unpack_from('<d', decomp, 320)[0])
                vol = float(struct.unpack_from('<i', decomp, 352)[0])
                atp = float(struct.unpack_from('<d', decomp, 360)[0])
                lut = int(struct.unpack_from('<I', decomp, 368)[0])
                open_p = float(struct.unpack_from('<d', decomp, 384)[0])
                high_p = float(struct.unpack_from('<d', decomp, 392)[0])
                low_p = float(struct.unpack_from('<d', decomp, 400)[0])
                close_p = float(struct.unpack_from('<d', decomp, 408)[0])

                exchange = exchange_code_for_segment(seg)
                if lut > 1_700_000_000:
                    timestamp_ms = int(lut * 1000)
                else:
                    timestamp_ms = int(datetime.now(UTC).timestamp() * 1000)

                results.append({
                    "instrument_token": inst_id,
                    "exchange": exchange,
                    "last_price": ltp,
                    "last_traded_price": ltp,
                    "timestamp": timestamp_ms,
                    "volume_traded": vol,
                    "open_price": open_p,
                    "high_price": high_p,
                    "low_price": low_p,
                    "close_price": close_p,
                    "average_price": atp,
                })
            except Exception:
                pass

        idx += 17 + comp_len

    return results
