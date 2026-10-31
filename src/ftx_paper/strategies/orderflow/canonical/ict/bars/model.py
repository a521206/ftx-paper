"""Standalone ICT bar records."""

from dataclasses import dataclass
from datetime import datetime

from ....timestamps import require_utc


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

    def __post_init__(self) -> None:
        object.__setattr__(self, "timestamp", require_utc(self.timestamp, field="source bar timestamp"))
        object.__setattr__(self, "available_at", require_utc(self.available_at, field="source bar available_at"))
        if self.available_at < self.timestamp:
            raise ValueError("source bar availability cannot precede timestamp")


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

    def __post_init__(self) -> None:
        for name in ("timestamp", "end_timestamp", "available_at"):
            object.__setattr__(self, name, require_utc(getattr(self, name), field=f"completed bar {name}"))
        if self.end_timestamp < self.timestamp or self.available_at < self.end_timestamp:
            raise ValueError("completed bar timestamps are not causally ordered")

    @property
    def timeframe(self) -> str:
        return f"{self.timeframe_minutes}m"
