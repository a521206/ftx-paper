from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
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
