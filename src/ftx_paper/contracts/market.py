from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
import math
from typing import Mapping
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")


def normalize_exchange_timestamp(value: datetime | str) -> datetime:
    """Normalize an exchange timestamp to timezone-aware UTC."""
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("exchange timestamp must include timezone information")
    return parsed.astimezone(timezone.utc)


def market_minute_key(timestamp: datetime) -> tuple[str, str]:
    """Return the canonical IST trading date and minute for a UTC bar."""
    normalized = normalize_exchange_timestamp(timestamp).astimezone(IST)
    return normalized.date().isoformat(), normalized.strftime("%H:%M")


class OptionType(StrEnum):
    CALL = "CE"
    PUT = "PE"


class MarketRole(StrEnum):
    FUTURES = "futures"
    VIX = "vix"
    SPOT = "spot"


@dataclass(frozen=True, slots=True)
class OptionRole:
    """Stable role identity for one dynamically configured option contract."""

    symbol: str

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValueError("option role symbol must be non-empty")


Role = MarketRole | OptionRole


def role_to_key(role: Role) -> str:
    """Return the stable wire key used in runtime payloads and diagnostics."""
    if isinstance(role, MarketRole):
        return role.value
    if isinstance(role, OptionRole):
        return f"option:{role.symbol}"
    raise TypeError(f"unsupported role type: {type(role).__name__}")


def parse_role(value: Role | str) -> Role:
    """Parse an external role value at the configuration/broker boundary."""
    if isinstance(value, (MarketRole, OptionRole)):
        return value
    raw = value.strip()
    normalized = raw.lower()
    try:
        return MarketRole(normalized)
    except ValueError:
        if normalized.startswith("option:") and raw[7:].strip():
            return OptionRole(raw[7:].strip())
        raise ValueError(f"invalid market role: {value!r}") from None


@dataclass(frozen=True, slots=True)
class Instrument:
    symbol: str
    exchange: str
    instrument_type: str
    expiry: str | None = None
    strike: float | None = None
    option_type: OptionType | None = None


@dataclass(frozen=True, slots=True)
class MarketBar:
    instrument: Instrument
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None
    open_interest: float | None = None


@dataclass(frozen=True, slots=True)
class SyntheticPremiumPair:
    """Validated entry quote for the two legs of a synthetic future."""

    ce: float
    pe: float

    def __post_init__(self) -> None:
        if not all(math.isfinite(value) and value > 0 for value in (self.ce, self.pe)):
            raise ValueError("synthetic CE and PE premiums must be finite and positive")


@dataclass(frozen=True, slots=True)
class SyntheticFutureQuote:
    """A same-strike CE/PE pair and its synthetic-future mark."""

    strike: float
    ce: MarketBar
    pe: MarketBar

    @property
    def price(self) -> float:
        # Long synthetic future = long CE + short PE + strike.
        return self.strike + self.ce.close - self.pe.close


def synthetic_future_quote(
    futures: MarketBar,
    option_bars: Mapping[Role, MarketBar],
    *,
    symbols: tuple[str, str] | None = None,
) -> SyntheticFutureQuote | None:
    """Select the closest same-strike CE/PE pair available for a futures bar."""
    candidates = [bar for bar in option_bars.values()
                  if bar.instrument.instrument_type.upper() in {"CE", "PE"}
                  and bar.instrument.strike is not None
                  and bar.close > 0]
    if symbols is not None:
        candidates = [bar for bar in candidates if bar.instrument.symbol in symbols]
    grouped: dict[tuple[str | None, float], dict[str, MarketBar]] = {}
    for bar in candidates:
        key = (bar.instrument.expiry, float(bar.instrument.strike))
        grouped.setdefault(key, {})[bar.instrument.instrument_type.upper()] = bar
    pairs = [(expiry_strike, legs) for expiry_strike, legs in grouped.items()
             if "CE" in legs and "PE" in legs]
    if not pairs:
        return None
    _, legs = min(pairs, key=lambda item: abs(item[0][1] - futures.close))
    strike = float(legs["CE"].instrument.strike)
    return SyntheticFutureQuote(strike, legs["CE"], legs["PE"])
