"""Standalone ICT bar records."""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class SourceBar:
    """One normalized source bar with explicit causal availability."""

    instrument: str
    timestamp: datetime
    available_at: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    source_id: str
    session_date: str
    provenance: str = ""


@dataclass(frozen=True, slots=True)
class CompletedBar:
    """A complete session-aligned aggregate and its source provenance."""

    instrument: str
    timeframe_minutes: int
    timestamp: datetime
    end_timestamp: datetime
    available_at: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    source_ids: tuple[str, ...]
    source_provenance: tuple[str, ...]
    source_fingerprint: str
    session_date: str

    @property
    def timeframe(self) -> str:
        return f"{self.timeframe_minutes}m"
