"""Shoonya adapter normalizing broker instruments, option surface, and historical warmup."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import logging
import re
from typing import Any, NotRequired, TypedDict
from zoneinfo import ZoneInfo

from ftx_paper.broker.protocol import Fill
from ftx_paper.contracts import (
    Instrument,
    MarketBar,
    MarketRole,
    OptionRole,
    OptionType,
    OrderAck,
    OrderIntent,
    Role,
    normalize_exchange_timestamp,
    parse_role,
    role_to_key,
)

IST = ZoneInfo("Asia/Kolkata")
logger = logging.getLogger(__name__)


def _is_index_symbol(symbol: object) -> bool:
    return str(symbol).upper() in {"NIFTY", "NIFTY 50", "INDIA VIX"}


class ShoonyaInstrument(TypedDict):
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
    """Normalize date-like expiry values from strings, dates, or datetimes."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = str(value).strip()
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            pass
    return date.fromisoformat(raw[:10])


def instrument_expiry_iso(value: object) -> str | None:
    """Return an ISO date string for a broker expiry value, or None when absent."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    return _instrument_expiry(value).isoformat()


def _normalized_instrument_name(value: object) -> str:
    return str(value or "").upper().replace(" ", "")


def _futures_underlying(symbol: str, row: dict[str, Any] | None) -> str | None:
    metadata_name = _normalized_instrument_name(row.get("name")) if row else ""
    if metadata_name and metadata_name != symbol.upper():
        return metadata_name
    match = re.match(r"^([A-Z][A-Z0-9]*?)(?=\d{2})", symbol.upper())
    return match.group(1) if match else None


def _nearest_live_futures(
    rows: list[dict[str, Any]], *, underlying: str, as_of: date
) -> dict[str, Any] | None:
    candidates = []
    for row in rows:
        row_name = _normalized_instrument_name(row.get("name"))
        inst_type = str(row.get("instrument_type", "")).upper()
        sym = str(row.get("tradingsymbol", "")).upper()
        if (row_name != underlying and not sym.startswith(underlying)) or inst_type != "FUT":
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
    row_name = _normalized_instrument_name(row.get("name"))
    sym = str(row.get("tradingsymbol", "")).upper()
    return (
        (row_name == underlying or sym.startswith(underlying))
        and str(row.get("instrument_type", "")).upper() == "FUT"
        and expiry is not None
        and _instrument_expiry(expiry) >= as_of
    )


@dataclass
class RuntimeRoles:
    futures: ShoonyaInstrument
    vix: ShoonyaInstrument
    options: dict[str, object] | None = None


def resolve_instruments(
    client: Any,
    specifications: list[dict[str, object]],
    *,
    contract_store: Any | None = None,
) -> list[ShoonyaInstrument]:
    """Resolve configured exchange/tradingsymbol pairs to Shoonya instrument tokens."""
    if not specifications:
        raise ValueError("No Shoonya instruments configured")
    resolved: list[ShoonyaInstrument] = []
    contracts_by_exchange: dict[str, list[dict[str, Any]]] = {}

    for spec in specifications:
        exchange = str(spec.get("exchange", "")).strip().upper()
        symbol = str(spec.get("tradingsymbol", "")).strip()
        raw_role = spec.get("role")
        if raw_role is not None and not isinstance(raw_role, (str, MarketRole, OptionRole)):
            raise ValueError("Shoonya instrument role must be a string or role")
        role = parse_role(raw_role) if raw_role is not None else None

        is_nfo_futures = role is MarketRole.FUTURES and exchange == "NFO"
        if not exchange or (not symbol and not is_nfo_futures):
            raise ValueError("Each Shoonya instrument needs exchange and tradingsymbol, except futures")

        if exchange not in contracts_by_exchange:
            contracts_by_exchange[exchange] = client.instruments(exchange)
        rows = contracts_by_exchange[exchange]

        underlying = _normalized_instrument_name(spec.get("underlying"))
        match = (
            next(
                (
                    row
                    for row in rows
                    if str(row.get("tradingsymbol", "")).upper() == symbol.upper()
                ),
                None,
            )
            if symbol
            else None
        )
        today = datetime.now(IST).date() if is_nfo_futures else None

        if is_nfo_futures:
            if not underlying:
                underlying = _futures_underlying(symbol, match)
            if underlying is None or today is None:
                raise ValueError("futures instruments require an underlying")

            stored = (
                contract_store.read_runtime_contract(
                    exchange=exchange,
                    underlying=underlying,
                    role=role_to_key(role) if role is not None else "futures",
                )
                if contract_store is not None and underlying
                else None
            )
            stored_symbol = str(stored.get("tradingsymbol", "")) if stored else ""
            stored_match = (
                next(
                    (
                        row
                        for row in rows
                        if str(row.get("tradingsymbol", "")).upper() == stored_symbol.upper()
                    ),
                    None,
                )
                if stored_symbol
                else None
            )

            if stored_match is not None and _is_live_futures_row(
                stored_match, underlying=underlying, as_of=today
            ):
                match = stored_match
            if match is None or not _is_live_futures_row(match, underlying=underlying, as_of=today):
                match = _nearest_live_futures(rows, underlying=underlying, as_of=today) if underlying else None

            if match is not None:
                if contract_store is not None:
                    contract_store.record_runtime_contract({
                        "exchange": exchange,
                        "underlying": underlying,
                        "role": role_to_key(role) if role is not None else "futures",
                        "tradingsymbol": match["tradingsymbol"],
                        "instrument_token": match["instrument_token"],
                        "expiry": _instrument_expiry(match["expiry"]).isoformat(),
                    })
                if symbol and symbol != match["tradingsymbol"]:
                    logger.warning(
                        "Shoonya futures contract changed from %s to %s expiring %s",
                        symbol,
                        match["tradingsymbol"],
                        match.get("expiry"),
                    )
                symbol = str(match["tradingsymbol"])

        if match is None:
            raise ValueError(f"Shoonya instrument not found: {exchange}:{symbol or underlying}")

        resolved.append(
            ShoonyaInstrument(
                instrument_token=int(match["instrument_token"]),
                exchange=exchange,
                symbol=str(match["tradingsymbol"]),
                tradingsymbol=str(match["tradingsymbol"]),
                instrument_type=str(match.get("instrument_type", "INDEX")),
                role=role,
                expiry=match.get("expiry"),
                strike=float(match["strike"]) if match.get("strike") is not None else None,
                name=str(match.get("name", "")),
            )
        )


    return resolved


def discover_option_surface_contracts(
    client: Any,
    *,
    underlying: str = "NIFTY",
    expiry: str | None = None,
    as_of: date | None = None,
) -> list[ShoonyaInstrument]:
    """Discover every NIFTY CE/PE contract for the nearest live expiry."""
    target = underlying.upper().replace(" ", "")
    today = as_of or datetime.now(IST).date()
    contracts = []

    for row in client.instruments("NFO"):
        row_expiry = row.get("expiry")
        instrument_type = str(row.get("instrument_type", "")).upper()
        row_name = str(row.get("name", "")).upper().replace(" ", "")
        tsym = str(row.get("tradingsymbol", "")).upper()

        if (row_name != target and not tsym.startswith(target)) or instrument_type not in {"CE", "PE"}:
            continue
        if row_expiry is None or row.get("strike") is None:
            continue

        parsed_expiry = _instrument_expiry(row_expiry)
        if parsed_expiry >= today:
            contracts.append((parsed_expiry, row))

    if not contracts:
        return []

    selected_expiry = expiry
    if selected_expiry in {None, "", "nearest"}:
        selected_expiry = min(item[0] for item in contracts).isoformat()

    discovered: list[ShoonyaInstrument] = []
    seen: set[tuple[str, str]] = set()

    for parsed_expiry, row in contracts:
        symbol = str(row["tradingsymbol"])
        key = ("NFO", symbol)
        if parsed_expiry.isoformat() != selected_expiry or key in seen:
            continue
        seen.add(key)
        discovered.append(
            ShoonyaInstrument(
                instrument_token=int(row["instrument_token"]),
                exchange="NFO",
                symbol=symbol,
                tradingsymbol=symbol,
                instrument_type=str(row["instrument_type"]),
                role=None,
                expiry=parsed_expiry,
                strike=float(row["strike"]),
                name=str(row.get("name", "")),
            )
        )

    return discovered


def classify_runtime_roles(
    instruments: Sequence[ShoonyaInstrument] | Sequence[dict[str, Any]],
) -> RuntimeRoles:
    futures: ShoonyaInstrument | None = None
    vix: ShoonyaInstrument | None = None
    options_list: list[ShoonyaInstrument] = []

    for item in instruments:
        symbol = str(item.get("symbol", "")).upper()
        exchange = str(item.get("exchange", "")).upper()
        raw_role = item.get("role")
        role = parse_role(raw_role) if isinstance(raw_role, (str, MarketRole, OptionRole)) else None
        strike_raw = item.get("strike")
        strike_val = float(strike_raw) if strike_raw is not None else None
        typed_item: ShoonyaInstrument = ShoonyaInstrument(
            instrument_token=int(item["instrument_token"]),
            exchange=exchange,
            symbol=symbol,
            tradingsymbol=str(item.get("tradingsymbol", symbol)),
            instrument_type=str(item.get("instrument_type", "INDEX")),
            role=role,
            expiry=item.get("expiry"),
            strike=strike_val,
            name=str(item.get("name", "")),
        )

        if role is MarketRole.VIX or "VIX" in symbol:
            vix = typed_item
        elif role is MarketRole.FUTURES or (exchange == "NFO" and symbol.endswith("FUT")):
            futures = typed_item
        elif exchange == "NFO" and symbol.endswith(("CE", "PE")):
            options_list.append(typed_item)

    if futures is None or vix is None:
        missing = [name for name, value in (("futures", futures), ("vix", vix)) if value is None]
        raise ValueError(f"Missing required FTX Shoonya instrument roles: {', '.join(missing)}")

    return RuntimeRoles(
        futures=futures,
        vix=vix,
        options={"contracts": options_list} if options_list else None,
    )


def load_startup_backfill(
    client: Any,
    instruments: Sequence[ShoonyaInstrument] | Sequence[dict[str, Any]],
    *,
    days: int = 2,
    contract_store: Any | None = None,
) -> tuple[MarketBar, ...]:
    """Fetch bounded one-minute warmup bars and normalize them at the broker edge."""
    end = datetime.now(IST).date()
    start = end - timedelta(days=max(1, days + 3))
    backfill_instruments = list(instruments)

    bars: list[MarketBar] = []
    for item in backfill_instruments:
        instrument_type = str(item.get("instrument_type", "INDEX"))
        if _is_index_symbol(item["symbol"]):
            instrument_type = "INDEX"

        exchange = str(item.get("exchange", "NSE"))
        token = item["instrument_token"]

        try:
            rows = client.historical_data(
                exchange,
                token,
                start,
                end,
                interval="1",
            )
        except Exception as exc:
            logger.warning("Shoonya historical data failed for %s (%s): %s", item["symbol"], token, exc)
            rows = []

        for row in rows or ():
            # Shoonya TPSeries row has: time ('14-10-2024 09:15:00'), ssboe (epoch sec), into, inth, intl, intc, intv, intoi
            raw_time = row.get("ssboe") or row.get("time") or row.get("date")
            if raw_time is None:
                continue

            if isinstance(raw_time, (int, float)) or (isinstance(raw_time, str) and raw_time.isdigit()):
                parsed = normalize_exchange_timestamp(int(raw_time))
            else:
                # String timestamp
                raw_str = str(raw_time).strip()
                try:
                    dt = datetime.strptime(raw_str, "%d-%m-%Y %H:%M:%S").replace(tzinfo=IST)
                    parsed = normalize_exchange_timestamp(dt)
                except ValueError:
                    try:
                        parsed = normalize_exchange_timestamp(raw_str)
                    except ValueError:
                        continue

            expiry = instrument_expiry_iso(item.get("expiry")) if instrument_type in {"FUT", "CE", "PE"} else None
            raw_strike = item.get("strike")
            strike = float(raw_strike) if raw_strike is not None and instrument_type in {"CE", "PE"} else None

            o = float(row.get("into") or row.get("open") or 0.0)
            h = float(row.get("inth") or row.get("high") or 0.0)
            low_val = float(row.get("intl") or row.get("low") or 0.0)
            c = float(row.get("intc") or row.get("close") or 0.0)
            v = float(row.get("intv") or row.get("v") or row.get("volume") or 0.0)
            oi_val = row.get("intoi") or row.get("oi")
            oi = float(oi_val) if oi_val is not None else (0.0 if instrument_type == "FUT" else None)

            bars.append(
                MarketBar(
                    instrument=Instrument(
                        str(item["symbol"]),
                        str(item["exchange"]),
                        instrument_type,
                        expiry=expiry,
                        strike=strike,
                        option_type=OptionType(instrument_type) if instrument_type in {"CE", "PE"} else None,
                    ),
                    timestamp=parsed,
                    open=o,
                    high=h,
                    low=low_val,
                    close=c,
                    volume=v if v > 0 else None,
                    open_interest=oi,
                )
            )

    return tuple(sorted(bars, key=lambda bar: bar.timestamp))


class ShoonyaBroker:
    """Shoonya execution adapter placeholder; paper mode routes all orders to PaperBroker."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def submit(self, order: OrderIntent) -> OrderAck:
        raise NotImplementedError("Shoonya live execution is disabled. Use PaperBroker for simulated trading.")

    def close(self) -> None:
        return None

    def poll_fill(self, order: OrderIntent, broker_order_id: str) -> Fill | None:
        return None
