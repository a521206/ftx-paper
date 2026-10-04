from __future__ import annotations

from dataclasses import dataclass
from typing import Any, NotRequired, TypedDict
from datetime import date, datetime, timedelta
import logging
import re
import threading
import time
from zoneinfo import ZoneInfo

from requests import exceptions as requests_exceptions

from ftx_paper.broker.protocol import Fill
from ftx_paper.contracts import (
    Instrument, MarketBar, MarketRole, OptionRole, OptionType, OrderAck, OrderIntent, OrderSide, Role,
    normalize_exchange_timestamp, parse_role, role_to_key,
)

KITE_EXCHANGE_MAP = {"NSE_INDEX": "NSE", "BSE_INDEX": "BSE"}
IST = ZoneInfo("Asia/Kolkata")
HISTORICAL_REQUEST_INTERVAL_SECONDS = 0.5
HISTORICAL_RATE_LIMIT_RETRIES = 3
HISTORICAL_RATE_LIMIT_BACKOFF_SECONDS = 1.0
HISTORICAL_TRANSPORT_RETRIES = 3
HISTORICAL_TRANSPORT_BACKOFF_SECONDS = 1.0
HISTORICAL_MAX_RETRY_AFTER_SECONDS = 60.0
_historical_rate_limit_lock = threading.Lock()
_next_historical_request_at = 0.0
logger = logging.getLogger(__name__)


def _is_index_symbol(symbol: object) -> bool:
    return str(symbol).upper() in {"NIFTY", "NIFTY 50", "INDIA VIX", "INDIAVIX"}


class ZerodhaInstrument(TypedDict):
    instrument_token: int
    exchange: str
    symbol: str
    tradingsymbol: str
    instrument_type: str
    role: Role | None
    expiry: NotRequired[date | str | None]
    strike: NotRequired[float | int | None]
    name: NotRequired[str | None]
    lot_size: NotRequired[int | None]
    tick_size: NotRequired[float | None]


def _instrument_expiry(value: object) -> date:
    """Normalize Zerodha's date-like expiry values without leaking datetime."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def instrument_expiry_iso(value: object) -> str | None:
    """Return an ISO date string for a broker expiry value, or None when absent."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    return _instrument_expiry(value).isoformat()


def _normalized_instrument_name(value: object) -> str:
    return str(value or "").upper().replace(" ", "")


def _futures_underlying(symbol: str, row: dict[str, Any] | None) -> str | None:
    metadata_name = _normalized_instrument_name(row.get("name")) if row else ""
    if metadata_name:
        return metadata_name
    match = re.match(r"^([A-Z][A-Z0-9]*?)(?=\d{2})", symbol.upper())
    return match.group(1) if match else None


def _nearest_live_futures(
    rows: list[dict[str, Any]], *, underlying: str, as_of: date,
) -> dict[str, Any] | None:
    candidates = []
    for row in rows:
        if (_normalized_instrument_name(row.get("name")) != underlying
                or str(row.get("instrument_type", "")).upper() != "FUT"):
            continue
        expiry = row.get("expiry")
        if expiry is None:
            continue
        parsed_expiry = _instrument_expiry(expiry)
        if parsed_expiry >= as_of:
            candidates.append((parsed_expiry, row))
    return min(candidates, key=lambda item: item[0])[1] if candidates else None


def _is_live_futures_row(row: dict[str, Any], *, underlying: str, as_of: date) -> bool:
    expiry = row.get("expiry")
    return (
        _normalized_instrument_name(row.get("name")) == underlying
        and str(row.get("instrument_type", "")).upper() == "FUT"
        and expiry is not None
        and _instrument_expiry(expiry) >= as_of
    )


@dataclass
class RuntimeRoles:
    futures: ZerodhaInstrument
    vix: ZerodhaInstrument
    options: dict[str, object] | None = None


def resolve_instruments(
    client: Any,
    specifications: list[dict[str, object]],
    *,
    contract_store: Any | None = None,
) -> list[ZerodhaInstrument]:
    """Resolve configured exchange/tradingsymbol pairs to broker tokens.

    Futures are selected from the broker instrument master and persisted through
    ``contract_store``. The configuration identifies the underlying, not a
    date-coded contract symbol.
    """
    if not specifications:
        raise ValueError("No Zerodha instruments configured")
    resolved = []
    contracts_by_exchange: dict[str, list[dict[str, Any]]] = {}
    for spec in specifications:
        exchange = str(spec.get("exchange", "")).strip()
        symbol = str(spec.get("tradingsymbol", "")).strip()
        raw_role = spec.get("role")
        if raw_role is not None and not isinstance(raw_role, (str, MarketRole, OptionRole)):
            raise ValueError("Zerodha instrument role must be a string or role")
        role = parse_role(raw_role) if raw_role is not None else None
        kite_exchange = KITE_EXCHANGE_MAP.get(exchange, exchange)
        is_nfo_futures = role is MarketRole.FUTURES and kite_exchange == "NFO"
        if not exchange or (not symbol and not is_nfo_futures):
            raise ValueError("Each Zerodha instrument needs exchange and tradingsymbol, except futures")
        if kite_exchange not in contracts_by_exchange:
            contracts_by_exchange[kite_exchange] = client.instruments(kite_exchange)
        rows = contracts_by_exchange[kite_exchange]
        underlying = _normalized_instrument_name(spec.get("underlying"))
        match = next((row for row in rows if row.get("tradingsymbol") == symbol), None) if symbol else None
        today = datetime.now(IST).date() if is_nfo_futures else None
        if is_nfo_futures:
            if not underlying:
                underlying = _futures_underlying(symbol, match)
            if underlying is None or today is None:
                raise ValueError("futures instruments require an underlying")
            stored = (
                contract_store.read_runtime_contract(
                    exchange=exchange, underlying=underlying, role=role_to_key(role) if role is not None else "futures",
                ) if contract_store is not None and underlying else None
            )
            stored_symbol = str(stored.get("tradingsymbol", "")) if stored else ""
            stored_match = next((row for row in rows if row.get("tradingsymbol") == stored_symbol), None)
            if stored_match is not None and _is_live_futures_row(
                stored_match, underlying=underlying, as_of=today,
            ):
                match = stored_match
            if match is None or not _is_live_futures_row(match, underlying=underlying, as_of=today):
                match = _nearest_live_futures(rows, underlying=underlying, as_of=today) if underlying else None
            if match is not None:
                if contract_store is not None:
                    contract_store.record_runtime_contract({
                        "exchange": exchange, "underlying": underlying,
                         "role": role_to_key(role) if role is not None else "futures", "tradingsymbol": match["tradingsymbol"],
                        "instrument_token": match["instrument_token"],
                        "expiry": _instrument_expiry(match["expiry"]).isoformat(),
                    })
                if symbol and symbol != match["tradingsymbol"]:
                    logger.warning("Zerodha futures contract changed from %s to %s expiring %s", symbol, match["tradingsymbol"], match.get("expiry"))
                symbol = str(match["tradingsymbol"])
        if match is None:
            raise ValueError(f"Zerodha instrument not found: {exchange}:{symbol or underlying}")
        resolved.append({**dict(match), "exchange": exchange, "symbol": str(match["tradingsymbol"]), "role": role})
    return resolved


def discover_option_surface_contracts(
    client: Any,
    *,
    underlying: str = "NIFTY",
    expiry: str | None = None,
    as_of: date | None = None,
) -> list[ZerodhaInstrument]:
    """Discover every NIFTY CE/PE contract for the nearest live expiry."""
    target = underlying.upper().replace(" ", "")
    today = as_of or datetime.now(IST).date()
    contracts = []
    for row in client.instruments("NFO"):
        row_expiry = row.get("expiry")
        instrument_type = str(row.get("instrument_type", "")).upper()
        if (str(row.get("name", "")).upper().replace(" ", "") != target
                or instrument_type not in {"CE", "PE"}
                or row_expiry is None or row.get("strike") is None):
            continue
        parsed_expiry = _instrument_expiry(row_expiry)
        if parsed_expiry >= today:
            contracts.append((parsed_expiry, row))
    if not contracts:
        return []
    selected_expiry = expiry
    if selected_expiry in {None, "", "nearest"}:
        selected_expiry = min(item[0] for item in contracts).isoformat()
    discovered: list[ZerodhaInstrument] = []
    seen: set[tuple[str, str]] = set()
    for parsed_expiry, row in contracts:
        symbol = str(row["tradingsymbol"])
        key = ("NFO", symbol)
        if parsed_expiry.isoformat() != selected_expiry or key in seen:
            continue
        seen.add(key)
        discovered.append(ZerodhaInstrument(
            instrument_token=int(row["instrument_token"]), exchange="NFO", symbol=symbol,
            tradingsymbol=symbol, instrument_type=str(row["instrument_type"]), role=None,
            expiry=parsed_expiry, strike=float(row["strike"]), name=str(row.get("name", "")),
        ))
    return discovered


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
        return min(HISTORICAL_MAX_RETRY_AFTER_SECONDS, max(0.0, float(value))) if value is not None else None
    except (TypeError, ValueError):
        return None


def _historical_retry_delay(exc: Exception, attempt: int) -> float | None:
    """Return a bounded retry delay for recoverable historical-data failures."""
    if _is_rate_limit_error(exc):
        retry_after = _retry_after_seconds(exc)
        return retry_after if retry_after is not None else HISTORICAL_RATE_LIMIT_BACKOFF_SECONDS * (2**attempt)
    if isinstance(exc, (requests_exceptions.ConnectionError, requests_exceptions.Timeout)):
        return HISTORICAL_TRANSPORT_BACKOFF_SECONDS * (2**attempt)
    return None


def classify_runtime_roles(instruments: list[ZerodhaInstrument]) -> RuntimeRoles:
    futures: ZerodhaInstrument | None = None
    vix: ZerodhaInstrument | None = None
    options_list: list[ZerodhaInstrument] = []
    for item in instruments:
        symbol = str(item.get("symbol", "")).upper()
        exchange = str(item.get("exchange", ""))
        raw_role = item.get("role")
        role = parse_role(raw_role) if isinstance(raw_role, (str, MarketRole, OptionRole)) else None
        if role is MarketRole.VIX or symbol in {"INDIA VIX", "INDIAVIX"}:
            vix = item
        elif role is MarketRole.FUTURES or (exchange == "NFO" and symbol.endswith("FUT")):
            futures = item
        elif exchange == "NFO" and symbol.endswith(("CE", "PE")):
            options_list.append(item)
    if futures is None or vix is None:
        missing = [name for name, value in (("futures", futures), ("vix", vix)) if value is None]
        raise ValueError(f"Missing required FTX Zerodha instrument roles: {', '.join(missing)}")
    return RuntimeRoles(futures=futures, vix=vix, options={"contracts": options_list} if options_list else None)


def load_startup_backfill(
    client: Any,
    instruments: list[ZerodhaInstrument],
    *,
    days: int = 2,
    contract_store: Any | None = None,
) -> tuple[MarketBar, ...]:
    """Fetch bounded one-minute warmup bars and normalize them at the broker edge."""
    end = datetime.now(IST).date()
    start = end - timedelta(days=max(1, days + 3))
    backfill_instruments = list(instruments)
    if any(str(item.get("instrument_type", "")).upper() == "FUT" for item in instruments):
        instrument_loader = getattr(client, "instruments", None)
        if callable(instrument_loader):
            known_tokens = {int(item["instrument_token"]) for item in backfill_instruments}
            underlyings = {
                _normalized_instrument_name(item.get("name"))
                or (_futures_underlying(str(item["symbol"]), dict(item)) or "")
                for item in instruments
                if str(item.get("instrument_type", "")).upper() == "FUT"
            }
            underlying = next(iter(underlyings), "NIFTY")
            candidates: list[tuple[date, dict[str, Any]]] = []
            for row in instrument_loader("NFO") or ():
                if (_normalized_instrument_name(row.get("name")) not in underlyings
                        or str(row.get("instrument_type", "")).upper() != "FUT"
                        or row.get("expiry") is None):
                    continue
                expiry = _instrument_expiry(row["expiry"])
                if expiry < end or int(row["instrument_token"]) in known_tokens:
                    continue
                candidates.append((expiry, row))
            for _, row in sorted(candidates, key=lambda item: item[0])[:1]:
                backfill_instruments.append({
                    **row,
                    "exchange": "NFO",
                    "symbol": str(row["tradingsymbol"]),
                    "role": MarketRole.FUTURES,
                })
                if contract_store is not None:
                    contract_store.record_runtime_contract({
                        "exchange": "NFO", "underlying": underlying,
                        "role": "futures", "tradingsymbol": str(row["tradingsymbol"]),
                        "instrument_token": int(row["instrument_token"]),
                        "expiry": expiry.isoformat(),
                    })
                known_tokens.add(int(row["instrument_token"]))
            if contract_store is not None:
                for contract in contract_store.read_runtime_contracts(
                    exchange="NFO", underlying=underlying, role="futures",
                ):
                    try:
                        expiry = date.fromisoformat(str(contract["expiry"])[:10])
                    except (TypeError, ValueError):
                        logger.warning("Skipping cached futures contract with invalid expiry: %s", contract)
                        continue
                    if expiry < start or int(contract["instrument_token"]) in known_tokens:
                        continue
                    backfill_instruments.append({
                        "instrument_token": int(contract["instrument_token"]),
                        "exchange": "NFO",
                        "symbol": str(contract["tradingsymbol"]),
                        "tradingsymbol": str(contract["tradingsymbol"]),
                        "instrument_type": "FUT",
                        "role": MarketRole.FUTURES,
                        "expiry": expiry,
                        "name": underlying,
                    })
                    known_tokens.add(int(contract["instrument_token"]))
    bars: list[MarketBar] = []
    for item in backfill_instruments:
        instrument_type = str(item.get("instrument_type", "INDEX"))
        if _is_index_symbol(item["symbol"]):
            instrument_type = "INDEX"
        rows = None
        max_retries = max(HISTORICAL_RATE_LIMIT_RETRIES, HISTORICAL_TRANSPORT_RETRIES)
        for attempt in range(max_retries + 1):
            _reserve_historical_request_slot()
            try:
                rows = client.historical_data(
                    int(item["instrument_token"]), start, end, "minute",
                    oi=instrument_type in {"FUT", "CE", "PE"},
                )
                break
            except Exception as exc:
                retry_delay = _historical_retry_delay(exc, attempt)
                retry_limit = (
                    HISTORICAL_RATE_LIMIT_RETRIES
                    if _is_rate_limit_error(exc)
                    else HISTORICAL_TRANSPORT_RETRIES
                )
                if retry_delay is None or attempt >= retry_limit:
                    if _is_rate_limit_error(exc):
                        raise RuntimeError(
                            "Zerodha historical API rate limit exceeded after bounded retries"
                        ) from exc
                    raise
                logger.warning(
                    "Transient Zerodha historical-data failure for %s; retrying in %.1fs (%d/%d): %s",
                    item["symbol"], retry_delay, attempt + 1, retry_limit, exc,
                )
                time.sleep(retry_delay)
        for row in rows or ():
            timestamp = row.get("date")
            parsed = normalize_exchange_timestamp(timestamp)
            expiry = instrument_expiry_iso(item.get("expiry")) if instrument_type in {"FUT", "CE", "PE"} else None
            raw_strike = item.get("strike")
            strike = float(raw_strike) if raw_strike is not None and instrument_type in {"CE", "PE"} else None
            bars.append(MarketBar(
                instrument=Instrument(
                    str(item["symbol"]), str(item["exchange"]), instrument_type,
                    expiry=expiry,
                    strike=strike,
                    option_type=OptionType(instrument_type) if instrument_type in {"CE", "PE"} else None,
                ),
                timestamp=parsed,
                open=float(row["open"]), high=float(row["high"]), low=float(row["low"]), close=float(row["close"]),
                volume=float(row["volume"]) if row.get("volume") is not None else None,
                open_interest=(float(row["oi"]) if row.get("oi") is not None else 0.0)
                if instrument_type == "FUT" else (float(row["oi"]) if row.get("oi") is not None else None),
            ))
    return tuple(sorted(bars, key=lambda bar: bar.timestamp))


class ZerodhaBroker:
    """Kite execution adapter for futures and two-leg synthetic orders."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def _place(self, instrument: Instrument, side: OrderSide, quantity: int) -> str:
        return str(self._client.place_order(
            variety="regular", exchange=instrument.exchange,
            tradingsymbol=instrument.symbol, transaction_type=side.value,
            quantity=quantity, product="NRML", order_type="MARKET",
        ))

    def submit(self, order: OrderIntent) -> OrderAck:
        if order.vehicle == "synthetic":
            if order.synthetic_legs is None:
                raise ValueError("synthetic order is missing its CE/PE legs")
            call, put = order.synthetic_legs
            put_side = OrderSide.SELL if order.side is OrderSide.BUY else OrderSide.BUY
            first_id = self._place(call, order.side, order.quantity)
            try:
                second_id = self._place(put, put_side, order.quantity)
            except Exception:
                cancel = getattr(self._client, "cancel_order", None)
                if not callable(cancel):
                    raise RuntimeError("cannot compensate first synthetic leg")
                cancel(variety="regular", order_id=first_id)
                history = self._client.order_history(order_id=first_id)
                if any(str(item.get("status", "")).upper() == "COMPLETE"
                       for item in (history or ())):
                    raise RuntimeError("first synthetic leg filled before second-leg failure")
                raise
            ids = (first_id, second_id)
            broker_id = ",".join(ids)
            return OrderAck(order.client_order_id, broker_id, "OPEN")
        return OrderAck(order.client_order_id, self._place(order.instrument, order.side, order.quantity), "OPEN")

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
        return Fill(order.client_order_id, order.instrument, quantity, price, timestamp, order.vehicle)

    def poll_fills(self, order: OrderIntent, broker_order_id: str) -> tuple[Fill, ...]:
        if order.vehicle != "synthetic":
            fill = self.poll_fill(order, broker_order_id)
            return (fill,) if fill is not None else ()
        if order.synthetic_legs is None:
            return ()
        ids = tuple(broker_order_id.split(","))
        fills: list[Fill] = []
        sides = (order.side, OrderSide.SELL if order.side is OrderSide.BUY else OrderSide.BUY)
        for leg, leg_id, side in zip(order.synthetic_legs, ids, sides):
            history = self._client.order_history(order_id=leg_id)
            completed = next((item for item in reversed(history or ()) if str(item.get("status", "")).upper() == "COMPLETE"), None)
            if completed is None:
                continue
            quantity = int(completed.get("filled_quantity", 0))
            price = float(completed.get("average_price", 0))
            timestamp = str(completed.get("exchange_timestamp") or completed.get("order_timestamp") or "")
            if quantity <= 0 or price <= 0 or not timestamp:
                raise RuntimeError("Zerodha returned an incomplete synthetic leg fill")
            fills.append(Fill(order.client_order_id, leg, quantity, price, timestamp, order.vehicle))
        return tuple(fills)

    def abort_partial(self, order: OrderIntent, broker_order_id: str,
                      fills: tuple[Fill, ...]) -> bool:
        """Cancel open legs and verify that every filled leg was flattened."""
        ids = tuple(broker_order_id.split(","))
        completed_symbols = {fill.instrument.symbol for fill in fills}
        legs = order.synthetic_legs or (order.instrument,)
        sides = (order.side, OrderSide.SELL if order.side is OrderSide.BUY else OrderSide.BUY)
        for leg, leg_id, side in zip(legs, ids, sides):
            history = self._client.order_history(order_id=leg_id)
            complete = next((item for item in reversed(history or ())
                             if str(item.get("status", "")).upper() == "COMPLETE"), None)
            if complete is None:
                cancel = getattr(self._client, "cancel_order", None)
                if not callable(cancel):
                    return False
                cancel(variety="regular", order_id=leg_id)
                continue
            if leg.symbol in completed_symbols:
                quantity = int(complete.get("filled_quantity", 0))
                reverse = OrderSide.SELL if side is OrderSide.BUY else OrderSide.BUY
                reverse_id = self._place(leg, reverse, quantity)
                reverse_history = self._client.order_history(order_id=reverse_id)
                if not any(str(item.get("status", "")).upper() == "COMPLETE"
                           for item in (reverse_history or ())):
                    raise RuntimeError(f"failed to verify flatten order for {leg.symbol}")
        return True
