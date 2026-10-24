"""Symphony / IIFL XTS adapter normalizing broker instruments, option surface, and historical warmup."""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
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
)

from .protocol import exchange_code_for_segment, normalize_segment

IST = ZoneInfo("Asia/Kolkata")
logger = logging.getLogger(__name__)


def _is_index_symbol(symbol: object) -> bool:
    return str(symbol).upper() in {"NIFTY", "NIFTY 50", "INDIA VIX"}


class XTSInstrument(TypedDict):
    instrument_token: int
    segment_id: int
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
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw[:11].strip(), fmt).date()
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


def _nearest_live_futures(
    rows: list[dict[str, Any]], *, underlying: str, as_of: date
) -> dict[str, Any] | None:
    candidates = []
    for row in rows:
        row_name = _normalized_instrument_name(row.get("name"))
        inst_type = str(row.get("instrument_type", "")).upper()
        sym = str(row.get("tradingsymbol", "") or row.get("symbol", "")).upper()
        if row_name:
            if row_name != underlying:
                continue
        if inst_type != "FUT":
            continue
        # Exclude calendar spread contracts which have two month codes (e.g. NIFTY26OCT26NOVFUT)
        if not re.match(rf"^{re.escape(underlying)}\d{{2}}[A-Z]{{3}}FUT$", sym):
            continue
        expiry = row.get("expiry")
        if expiry is None:
            continue
        parsed_expiry = _instrument_expiry(expiry)
        if parsed_expiry >= as_of:
            candidates.append((parsed_expiry, row))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return candidates[0][1]


@dataclass(frozen=True, slots=True)
class ClassifiedRoles:
    spot: XTSInstrument
    futures: XTSInstrument
    vix: XTSInstrument
    options: dict[str, Sequence[XTSInstrument]] | None = None


def resolve_instruments(
    client: Any,
    specifications: Sequence[dict[str, Any]],
    *,
    contract_store: Any | None = None,
    as_of: date | None = None,
) -> list[XTSInstrument]:
    """Resolve configured exchange/tradingsymbol pairs to XTS instrument tokens."""
    if not specifications:
        raise ValueError("No XTS instruments configured")
    resolved: list[XTSInstrument] = []
    as_of_date = as_of or datetime.now(IST).date()

    for item in specifications:
        exchange_str = str(item.get("exchange", "NSECM"))
        seg_id = normalize_segment(exchange_str)
        role = item.get("role")
        if role is not None and not isinstance(role, (str, Role)):
            raise ValueError("XTS instrument role must be a string or role")

        symbol = item.get("tradingsymbol")
        underlying = item.get("underlying")
        if not symbol and not underlying:
            raise ValueError("Each XTS instrument needs exchange and tradingsymbol, except futures")

        # Load candidate records
        master_exchange = exchange_code_for_segment(seg_id)
        rows = client.instruments(master_exchange)
        found: dict[str, Any] | None = None

        if role == "futures" or (isinstance(role, Role) and role is MarketRole.FUTURES) or underlying:
            und = str(underlying or symbol or "NIFTY").upper().replace(" ", "")
            found = _nearest_live_futures(rows, underlying=und, as_of=as_of_date)
            if not found:
                # Try search API if master search did not yield candidates
                try:
                    search_results = client.search(und)
                    found = _nearest_live_futures(search_results, underlying=und, as_of=as_of_date)
                except Exception:
                    pass
        else:
            sym_clean = str(symbol).strip().upper().replace(" ", "")
            for r in rows:
                r_tsym = str(r.get("tradingsymbol") or r.get("symbol") or "").strip().upper().replace(" ", "")
                r_name = str(r.get("name") or "").strip().upper().replace(" ", "")
                if sym_clean in (r_tsym, r_name):
                    found = r
                    break

            if not found:
                # Fallback: search API
                try:
                    search_results = client.search(str(symbol))
                    for r in search_results:
                        r_tsym = str(r.get("DisplayName") or r.get("TradingSymbol") or "").strip().upper().replace(" ", "")
                        if sym_clean == r_tsym:
                            seg = int(r.get("ExchangeSegment", seg_id))
                            found = {
                                "instrument_token": int(r["ExchangeInstrumentID"]),
                                "segment_id": seg,
                                "exchange": exchange_code_for_segment(seg),
                                "symbol": r.get("DisplayName") or str(symbol),
                                "tradingsymbol": r.get("DisplayName") or str(symbol),
                                "name": r.get("Name") or str(symbol),
                                "instrument_type": "INDEX" if seg in (1, 11) else "EQUITY",
                                "expiry": None,
                                "strike": None,
                                "lot_size": 1,
                                "tick_size": 0.05,
                            }
                            break
                except Exception:
                    pass

        if not found:
            # Special well-known standard defaults for NSE indices if lookup is empty in mock/fallback
            sym_clean = str(symbol or underlying).strip().upper().replace(" ", "")
            if sym_clean in ("NIFTY", "NIFTY50") and seg_id == 1:
                found = {
                    "instrument_token": 26000,
                    "segment_id": 1,
                    "exchange": "NSE",
                    "symbol": "Nifty 50",
                    "tradingsymbol": "Nifty 50",
                    "name": "NIFTY",
                    "instrument_type": "INDEX",
                    "expiry": None,
                    "strike": None,
                    "lot_size": 1,
                    "tick_size": 0.05,
                }
            elif sym_clean in ("INDIAVIX", "VIX") and seg_id == 1:
                found = {
                    "instrument_token": 26001,
                    "segment_id": 1,
                    "exchange": "NSE",
                    "symbol": "India VIX",
                    "tradingsymbol": "India VIX",
                    "name": "INDIA VIX",
                    "instrument_type": "INDEX",
                    "expiry": None,
                    "strike": None,
                    "lot_size": 1,
                    "tick_size": 0.05,
                }
            else:
                raise ValueError(f"XTS instrument not found: {exchange_str}:{symbol or underlying}")

        resolved.append(
            XTSInstrument(
                instrument_token=int(found["instrument_token"]),
                segment_id=int(found.get("segment_id", seg_id)),
                exchange=str(found.get("exchange", master_exchange)),
                symbol=str(found.get("symbol") or found.get("tradingsymbol")),
                tradingsymbol=str(found.get("tradingsymbol") or found.get("symbol")),
                instrument_type=str(found.get("instrument_type", "INDEX")),
                role=parse_role(role) if role is not None else None,
                expiry=found.get("expiry"),
                strike=found.get("strike"),
                name=found.get("name"),
                lot_size=int(found.get("lot_size", 1)),
                tick_size=float(found.get("tick_size", 0.05)),
            )
        )

    return resolved


def discover_option_surface_contracts(
    client: Any,
    *,
    underlying: str = "NIFTY",
    as_of: date | None = None,
    max_strikes_per_side: int = 15,
) -> list[XTSInstrument]:
    """Discover active nearest-expiry call and put option contracts for the underlying."""
    as_of_date = as_of or datetime.now(IST).date()
    rows = client.instruments("NFO")
    underlying_norm = _normalized_instrument_name(underlying)

    candidates: list[dict[str, Any]] = []
    expiries: set[date] = set()

    for r in rows:
        r_name = _normalized_instrument_name(r.get("name"))
        sym = str(r.get("tradingsymbol") or r.get("symbol") or "").upper()
        if r_name != underlying_norm and not sym.startswith(underlying_norm):
            continue
        inst_type = str(r.get("instrument_type", "")).upper()
        if inst_type not in {"CE", "PE"}:
            continue
        expiry = r.get("expiry")
        if expiry is None:
            continue
        exp_date = _instrument_expiry(expiry)
        if exp_date >= as_of_date:
            candidates.append(r)
            expiries.add(exp_date)

    if not expiries:
        return []

    nearest_expiry = min(expiries)
    target_candidates = [
        r for r in candidates if _instrument_expiry(r["expiry"]) == nearest_expiry
    ]

    discovered: list[XTSInstrument] = []
    seen: set[tuple[str, int]] = set()

    for r in target_candidates:
        token = int(r["instrument_token"])
        sym = str(r.get("symbol") or r.get("tradingsymbol"))
        key = (sym, token)
        if key in seen:
            continue
        seen.add(key)

        discovered.append(
            XTSInstrument(
                instrument_token=token,
                segment_id=int(r.get("segment_id", 2)),
                exchange=str(r.get("exchange", "NFO")),
                symbol=sym,
                tradingsymbol=str(r.get("tradingsymbol") or sym),
                instrument_type=str(r.get("instrument_type", "OPT")),
                role=OptionRole(sym),
                expiry=r.get("expiry"),
                strike=r.get("strike"),
                name=r.get("name"),
                lot_size=int(r.get("lot_size", 65)),
                tick_size=float(r.get("tick_size", 0.05)),
            )
        )

    return discovered


def classify_runtime_roles(
    instruments: Sequence[XTSInstrument] | Sequence[dict[str, Any]],
) -> ClassifiedRoles:
    spot: XTSInstrument | None = None
    futures: XTSInstrument | None = None
    vix: XTSInstrument | None = None
    options_list: list[XTSInstrument] = []

    for item in instruments:
        lot_raw = item.get("lot_size")
        tick_raw = item.get("tick_size")
        typed_item: XTSInstrument = XTSInstrument(
            instrument_token=int(item["instrument_token"]),
            segment_id=int(item.get("segment_id", 1)),
            exchange=str(item["exchange"]),
            symbol=str(item["symbol"]),
            tradingsymbol=str(item.get("tradingsymbol") or item["symbol"]),
            instrument_type=str(item.get("instrument_type", "")),
            role=item.get("role"),
            expiry=item.get("expiry"),
            strike=item.get("strike"),
            name=item.get("name"),
            lot_size=int(lot_raw) if lot_raw is not None else 1,
            tick_size=float(tick_raw) if tick_raw is not None else 0.05,
        )
        role = typed_item.get("role")
        if role is MarketRole.SPOT or (isinstance(role, str) and role.lower() == "spot"):
            spot = typed_item
        elif role is MarketRole.FUTURES or (isinstance(role, str) and role.lower() == "futures"):
            futures = typed_item
        elif role is MarketRole.VIX or (isinstance(role, str) and role.lower() == "vix"):
            vix = typed_item
        elif isinstance(role, OptionRole) or str(typed_item.get("instrument_type", "")).upper() in {"CE", "PE"}:
            options_list.append(typed_item)

    missing = []
    if spot is None:
        missing.append("spot")
    if futures is None:
        missing.append("futures")
    if vix is None:
        missing.append("vix")
    if missing:
        raise ValueError(f"Missing required FTX XTS instrument roles: {', '.join(missing)}")

    assert spot is not None and futures is not None and vix is not None
    return ClassifiedRoles(
        spot=spot,
        futures=futures,
        vix=vix,
        options={"contracts": options_list} if options_list else None,
    )


def load_startup_backfill(
    client: Any,
    instruments: Sequence[XTSInstrument] | Sequence[dict[str, Any]],
    *,
    days: int = 2,
    contract_store: Any | None = None,
) -> tuple[MarketBar, ...]:
    """Fetch bounded 1-minute historical warmup bars from XTS."""
    end = datetime.now(IST).date()
    start = end - timedelta(days=max(1, days + 3))
    backfill_instruments = list(instruments)

    bars: list[MarketBar] = []
    for item in backfill_instruments:
        instrument_type = str(item.get("instrument_type", "INDEX"))
        if _is_index_symbol(item["symbol"]):
            instrument_type = "INDEX"

        seg = item.get("segment_id", 1)
        token = item["instrument_token"]

        try:
            rows = client.ohlc(
                seg,
                token,
                start,
                end,
                compression=60,
            )
        except Exception as exc:
            logger.warning("XTS OHLC historical warmup failed for %s (%s): %s", item["symbol"], token, exc)
            rows = []

        for row in rows or ():
            raw_time = row.get("time") or row.get("timestamp")
            if raw_time is None:
                continue

            if isinstance(raw_time, (int, float)) or (isinstance(raw_time, str) and raw_time.isdigit()):
                parsed = normalize_exchange_timestamp(int(raw_time))
            else:
                raw_str = str(raw_time).strip()
                try:
                    dt = datetime.strptime(raw_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
                    parsed = normalize_exchange_timestamp(dt)
                except ValueError:
                    try:
                        parsed = normalize_exchange_timestamp(raw_str)
                    except ValueError:
                        continue

            expiry = instrument_expiry_iso(item.get("expiry")) if instrument_type in {"FUT", "CE", "PE"} else None
            raw_strike = item.get("strike")
            strike = float(raw_strike) if raw_strike is not None and instrument_type in {"CE", "PE"} else None

            o = float(row.get("open", 0.0))
            h = float(row.get("high", 0.0))
            low_val = float(row.get("low", 0.0))
            c = float(row.get("close", 0.0))
            v = float(row.get("volume", 0.0))
            oi = float(row.get("oi", 0.0)) if row.get("oi") is not None else (0.0 if instrument_type == "FUT" else None)

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

    return tuple(bars)


class XTSBroker:
    """XTS execution adapter placeholder; paper mode routes all orders to PaperBroker."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def submit(self, order: OrderIntent) -> OrderAck:
        raise NotImplementedError("XTS live execution is disabled. Use PaperBroker for simulated trading.")

    def close(self) -> None:
        pass

    def poll_fill(self, order: OrderIntent, broker_order_id: str) -> Fill | None:
        return None
