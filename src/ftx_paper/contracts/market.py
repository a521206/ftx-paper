from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
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
