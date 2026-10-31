from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from .hierarchy import Permission, RegimePermission
from .timestamps import coerce_timestamp


class OpeningGapState(StrEnum):
    CANDIDATE = "GAP_CANDIDATE"
    ACCEPTED = "GAP_ACCEPTED"
    RECLAIMED = "GAP_RECLAIMED"
    RETURN_TO_VALUE = "RETURN_TO_VALUE"
    EXPIRED = "EXPIRED"


@dataclass(frozen=True, slots=True)
class OpeningGapPolicy:
    acceptance_bars: int = 1
    expiry_bars: int = 5

    def classify(self, *, opening: dict[str, Any], prior_low: float | None, prior_high: float | None,
                  prior_value_low: float | None, prior_value_high: float | None, available_at: datetime | str,
                 value_reference_id: str | None = None, value_scope: str | None = None) -> dict[str, object]:
        price = float(opening["open"])
        available_at = coerce_timestamp(available_at)
        extra = {"value_reference_id": value_reference_id, "value_scope": value_scope} if value_reference_id is not None else {}
        if prior_low is not None and price < prior_low:
            below_value = prior_value_low is not None and price < prior_value_low
            return {"direction": "down", "reference": "PD_LOW", "classification": "GAP_DOWN_INITIATIVE" if below_value else "GAP_DOWN_CANDIDATE", "reference_level": prior_low, "gap_points": prior_low - price, "opening": opening, "state": "GAP_DOWN_CANDIDATE", "accepted_below_reference": False, "available_at": available_at, **extra}
        if prior_high is not None and price > prior_high:
            above_value = prior_value_high is not None and price > prior_value_high
            return {"direction": "up", "reference": "PD_HIGH", "classification": "GAP_UP_INITIATIVE" if above_value else "GAP_UP_CANDIDATE", "reference_level": prior_high, "gap_points": price - prior_high, "opening": opening, "state": "GAP_UP_CANDIDATE", "accepted_above_reference": False, "available_at": available_at, **extra}
        return {"direction": "internal", "classification": "RETURN_TO_VALUE_CONTEXT" if prior_value_low is not None and prior_value_high is not None and prior_value_low <= price <= prior_value_high else "BALANCE_CONTEXT", "opening": opening, "available_at": available_at, **extra}

    def transition(self, *, direction: str, completed_bars: int, close: float, reference_level: float,
                   prior_value_low: float | None, prior_value_high: float | None) -> str | None:
        if direction == "down" and close >= reference_level:
            return "GAP_DOWN_RECLAIMED"
        if direction == "up" and close <= reference_level:
            return "GAP_UP_RECLAIMED"
        if prior_value_low is not None and prior_value_high is not None and prior_value_low <= close <= prior_value_high:
            return OpeningGapState.RETURN_TO_VALUE.value
        if completed_bars == self.acceptance_bars:
            return "GAP_DOWN_ACCEPTED" if direction == "down" else "GAP_UP_ACCEPTED"
        if completed_bars >= self.expiry_bars:
            return OpeningGapState.EXPIRED.value
        return None

    def regime_for(self, *, state: str | None, direction: str | None, version: str, context: str,
                   restrictions: tuple[str, ...] = ()) -> RegimePermission:
        long = Permission.ALLOWED
        short = Permission.ALLOWED
        if state in {"GAP_DOWN_ACCEPTED", "GAP_DOWN_INITIATIVE"} and direction == "down":
            long, short = Permission.BLOCKED, Permission.ALLOWED
        elif state in {"GAP_UP_ACCEPTED", "GAP_UP_INITIATIVE"} and direction == "up":
            long, short = Permission.ALLOWED, Permission.BLOCKED
        return RegimePermission(version, context, long, short, restrictions, state, direction)
