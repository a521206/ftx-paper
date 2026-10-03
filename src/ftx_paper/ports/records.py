"""Typed persistence records exchanged across application ports.

These records deliberately do not model SQLite rows or API schemas.
"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Mapping

from ftx_paper.contracts import MarketBar


@dataclass(frozen=True, slots=True)
class StoredEvent:
    id: int
    event_type: str
    payload: dict[str, object]
    created_at: datetime


@dataclass(frozen=True, slots=True)
class StoredMarketBar:
    symbol: str
    exchange: str
    minute: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float | None
    open_interest: float | None
    instrument_type: str | None
    expiry: date | None
    strike: float | None
    option_type: str | None
    source: str
    ingested_at: datetime


@dataclass(frozen=True, slots=True)
class ReplayRun:
    run_id: str
    status: str
    request: dict[str, object]
    result: dict[str, object] | None
    error: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class RuntimeStatus:
    payload: dict[str, object]
    updated_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class RuntimeContract:
    exchange: str
    underlying: str
    role: str
    tradingsymbol: str
    instrument_token: int
    expiry: date
    selected_at: datetime


@dataclass(frozen=True, slots=True)
class ProcessLease:
    service: str
    pid: int
    started_at: datetime
    instance_id: str


@dataclass(frozen=True, slots=True)
class AuthToken:
    provider: str
    token_date: date
    access_token: str
