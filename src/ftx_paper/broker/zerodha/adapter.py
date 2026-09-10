from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypedDict
from datetime import date, datetime, timedelta

from ftx_paper.broker.protocol import Fill
from ftx_paper.contracts import Instrument, MarketBar, OrderAck, OrderIntent

KITE_EXCHANGE_MAP = {"NSE_INDEX": "NSE", "BSE_INDEX": "BSE"}


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
    for spec in specifications:
        exchange = str(spec.get("exchange", "")).strip()
        symbol = str(spec.get("tradingsymbol", "")).strip()
        if not exchange or not symbol:
            raise ValueError("Each Zerodha instrument needs exchange and tradingsymbol")
        rows = client.instruments(KITE_EXCHANGE_MAP.get(exchange, exchange))
        match = next((row for row in rows if row.get("tradingsymbol") == symbol), None)
        if match is None:
            raise ValueError(f"Zerodha instrument not found: {exchange}:{symbol}")
        resolved.append({**dict(match), "exchange": exchange, "symbol": symbol, "role": spec.get("role")})
    return resolved


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
    end = date.today()
    start = end - timedelta(days=max(1, days + 3))
    bars: list[MarketBar] = []
    for item in instruments:
        rows = client.historical_data(int(item["instrument_token"]), start, end, "minute")
        for row in rows:
            timestamp = row.get("date")
            parsed = timestamp if isinstance(timestamp, datetime) else datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
            bars.append(MarketBar(
                instrument=Instrument(str(item["symbol"]), str(item["exchange"]), str(item.get("instrument_type", "INDEX"))),
                timestamp=parsed,
                open=float(row["open"]), high=float(row["high"]), low=float(row["low"]), close=float(row["close"]),
                volume=float(row["volume"]) if row.get("volume") is not None else None,
                open_interest=float(row["oi"]) if row.get("oi") is not None else None,
            ))
    return tuple(sorted(bars, key=lambda bar: bar.timestamp))


class ZerodhaBroker:
    """Kite boundary. All optional Kite imports and response mapping stay here."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def submit(self, order: OrderIntent) -> OrderAck:
        try:
            from kiteconnect import KiteConnect
        except ImportError as exc:
            raise RuntimeError("Install ftx-paper[zerodha] to use Zerodha") from exc
        transaction = getattr(KiteConnect, f"TRANSACTION_TYPE_{order.side.value}")
        variety = getattr(KiteConnect, "VARIETY_REGULAR")
        order_id = self._client.place_order(
            variety=variety,
            exchange=order.instrument.exchange,
            tradingsymbol=order.instrument.symbol,
            transaction_type=transaction,
            quantity=order.quantity,
            order_type=order.order_type.value,
            product="MIS",
        )
        return OrderAck(order.client_order_id, str(order_id), "ACCEPTED")

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
