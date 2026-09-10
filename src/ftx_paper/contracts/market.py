from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


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
