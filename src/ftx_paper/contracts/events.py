from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any


@dataclass(frozen=True, slots=True)
class RuntimeEvent:
    event_type: str
    timestamp: datetime
    payload: dict[str, Any]
    event_id: str | None = None
    source: str | None = None
    correlation_id: str | None = None
    strategy_id: str | None = None
    account_id: str | None = None
    schema_version: int = 1
    sequence: int | None = None
    session_date: str | None = None
    observed_at: datetime | None = None
    available_at: datetime | None = None
    decision_at: datetime | None = None
    strategy_version: str | None = None
    config_hash: str | None = None
    provenance: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise ValueError("unsupported runtime event schema version")
        timestamps = (self.observed_at, self.available_at, self.decision_at)
        for value in timestamps:
            if value is not None and (value.tzinfo is None or value.utcoffset() is None):
                raise ValueError("runtime event timestamps must be timezone-aware")
        present = [value for value in timestamps if value is not None]
        if present != sorted(present):
            raise ValueError("runtime event timestamps must be causally ordered")

    def ordering_key(self) -> tuple[str, int, str, str, str, str]:
        """Return the deterministic cross-store ordering key for this event."""
        causal = self.decision_at or self.available_at or self.observed_at or self.timestamp
        payload = json.dumps(self.payload, sort_keys=True, separators=(",", ":"), default=str)
        return (
            self.session_date or "", self.sequence if self.sequence is not None else 0,
            causal.isoformat(), self.event_id or "", self.event_type, payload,
        )
