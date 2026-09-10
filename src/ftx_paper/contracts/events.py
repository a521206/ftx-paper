from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class RuntimeEvent:
    event_type: str
    timestamp: datetime
    payload: dict[str, Any]
