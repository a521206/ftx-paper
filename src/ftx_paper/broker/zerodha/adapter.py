from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypedDict
from datetime import datetime, timedelta
import threading
import time
from zoneinfo import ZoneInfo

from ftx_paper.broker.protocol import Fill
from ftx_paper.contracts import (
    Instrument, MarketBar, OrderAck, OrderIntent, normalize_exchange_timestamp,
)

KITE_EXCHANGE_MAP = {"NSE_INDEX": "NSE", "BSE_INDEX": "BSE"}
IST = ZoneInfo("Asia/Kolkata")
HISTORICAL_REQUEST_INTERVAL_SECONDS = 0.5
HISTORICAL_RATE_LIMIT_RETRIES = 3
HISTORICAL_RATE_LIMIT_BACKOFF_SECONDS = 1.0
_historical_rate_limit_lock = threading.Lock()
_next_historical_request_at = 0.0


class ZerodhaInstrument(TypedDict):
    instrument_token: int
    exchange: str
    symbol: str
    tradingsymbol: str
    instrument_type: str
    role: str | None


@dataclass
class RuntimeRoles:
    futures: ZerodhaInstrument
    vix: ZerodhaInstrument
    options: dict[str, object] | None = None


def resolve_instruments(client: Any, specifications: list[dict[str, object]]) -> list[ZerodhaInstrument]:
    """Resolve configured exchange/tradingsymbol pairs to broker tokens."""
    if not specifications:
        raise ValueError("No Zerodha instruments configured")
    resolved = []
    contracts_by_exchange: dict[str, list[dict[str, Any]]] = {}
    for spec in specifications:
        exchange = str(spec.get("exchange", "")).strip()
        symbol = str(spec.get("tradingsymbol", "")).strip()
        if not exchange or not symbol:
            raise ValueError("Each Zerodha instrument needs exchange and tradingsymbol")
        kite_exchange = KITE_EXCHANGE_MAP.get(exchange, exchange)
        if kite_exchange not in contracts_by_exchange:
            contracts_by_exchange[kite_exchange] = client.instruments(kite_exchange)
        rows = contracts_by_exchange[kite_exchange]
        match = next((row for row in rows if row.get("tradingsymbol") == symbol), None)
        if match is None:
            raise ValueError(f"Zerodha instrument not found: {exchange}:{symbol}")
        resolved.append({**dict(match), "exchange": exchange, "symbol": symbol, "role": spec.get("role")})
    return resolved


def _reserve_historical_request_slot() -> None:
    """Throttle historical requests across all startup workers in this process."""
    global _next_historical_request_at
    with _historical_rate_limit_lock:
        now = time.monotonic()
        wait_seconds = max(0.0, _next_historical_request_at - now)
        _next_historical_request_at = max(now, _next_historical_request_at) + HISTORICAL_REQUEST_INTERVAL_SECONDS
    if wait_seconds > 0:
        time.sleep(wait_seconds)


def _is_rate_limit_error(exc: Exception) -> bool:
    if "too many requests" in str(exc).lower():
        return True
    response = getattr(exc, "response", None)
    return getattr(response, "status_code", None) == 429 or getattr(exc, "code", None) == 429


def _retry_after_seconds(exc: Exception) -> float | None:
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None) or getattr(exc, "headers", None)
    if not headers:
        return None
    value = headers.get("Retry-After") or headers.get("retry-after")
    try:
        return max(0.0, float(value)) if value is not None else None
    except (TypeError, ValueError):
        return None


def classify_runtime_roles(instruments: list[ZerodhaInstrument]) -> RuntimeRoles:
    futures: ZerodhaInstrument | None = None
    vix: ZerodhaInstrument | None = None
    options_list: list[ZerodhaInstrument] = []
    for item in instruments:
        symbol = str(item.get("symbol", "")).upper()
        exchange = str(item.get("exchange", ""))
        role = str(item.get("role", "")).lower()
        if role == "vix" or symbol in {"INDIA VIX", "INDIAVIX"}:
            vix = item
        elif role == "futures" or (exchange == "NFO" and symbol.endswith("FUT")):
            futures = item
        elif exchange == "NFO" and symbol.endswith(("CE", "PE")):
            options_list.append(item)
    if futures is None or vix is None:
        missing = [name for name, value in (("futures", futures), ("vix", vix)) if value is None]
        raise ValueError(f"Missing required FTX Zerodha instrument roles: {', '.join(missing)}")
    return RuntimeRoles(futures=futures, vix=vix, options={"contracts": options_list} if options_list else None)


def load_startup_backfill(client: Any, instruments: list[ZerodhaInstrument], *, days: int = 2) -> tuple[MarketBar, ...]:
    """Fetch bounded one-minute warmup bars and normalize them at the broker edge."""
    end = datetime.now(IST).date()
    start = end - timedelta(days=max(1, days + 3))
    bars: list[MarketBar] = []
    for item in instruments:
        rows = None
        for attempt in range(HISTORICAL_RATE_LIMIT_RETRIES + 1):
            _reserve_historical_request_slot()
            try:
                rows = client.historical_data(int(item["instrument_token"]), start, end, "minute")
                break
            except Exception as exc:
                if not _is_rate_limit_error(exc) or attempt >= HISTORICAL_RATE_LIMIT_RETRIES:
                    if _is_rate_limit_error(exc):
                        raise RuntimeError(
                            "Zerodha historical API rate limit exceeded after bounded retries"
                        ) from exc
                    raise
                delay = _retry_after_seconds(exc)
                if delay is None:
                    delay = HISTORICAL_RATE_LIMIT_BACKOFF_SECONDS * (2**attempt)
                time.sleep(delay)
        for row in rows or ():
            timestamp = row.get("date")
            parsed = normalize_exchange_timestamp(timestamp)
            bars.append(MarketBar(
                instrument=Instrument(str(item["symbol"]), str(item["exchange"]), str(item.get("instrument_type", "INDEX"))),
                timestamp=parsed,
                open=float(row["open"]), high=float(row["high"]), low=float(row["low"]), close=float(row["close"]),
                volume=float(row["volume"]) if row.get("volume") is not None else None,
                open_interest=float(row["oi"]) if row.get("oi") is not None else None,
            ))
    return tuple(sorted(bars, key=lambda bar: bar.timestamp))


class ZerodhaBroker:
    """Disabled order boundary; ftx-paper is a simulated trading runtime."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def submit(self, order: OrderIntent) -> OrderAck:
        raise RuntimeError("Real Zerodha order submission is disabled in ftx-paper")

    def close(self) -> None:
        return None

    def poll_fill(self, order: OrderIntent, broker_order_id: str) -> Fill | None:
        """Return a fill only after Kite reports a completed execution."""
        try:
            history = self._client.order_history(order_id=broker_order_id)
        except Exception as exc:
            raise RuntimeError("Zerodha order history lookup failed") from exc
        completed = next((item for item in reversed(history or ()) if str(item.get("status", "")).upper() == "COMPLETE"), None)
        if completed is None:
            return None
        quantity = int(completed.get("filled_quantity", order.quantity))
        price = float(completed.get("average_price", 0))
        timestamp = str(completed.get("exchange_timestamp") or completed.get("order_timestamp") or "")
        if quantity <= 0 or price <= 0 or not timestamp:
            raise RuntimeError("Zerodha returned an incomplete fill record")
        return Fill(order.client_order_id, order.instrument, quantity, price, timestamp)
