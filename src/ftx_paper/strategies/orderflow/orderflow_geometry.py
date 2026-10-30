"""Canonical-derived geometry owned exclusively by the Paper orderflow strategy."""

from __future__ import annotations

from dataclasses import dataclass


BAND_ATR_FRACTION = 0.06


@dataclass(frozen=True, slots=True)
class LocationBand:
    """Immutable price band owned by one causal location generation."""

    low: float
    high: float

    def __post_init__(self) -> None:
        if self.low > self.high:
            raise ValueError("location band low must not exceed high")

    def intersects(self, *, high: float, low: float) -> bool:
        return low <= self.high and high >= self.low

    @property
    def center(self) -> float:
        return (self.low + self.high) / 2.0


__all__ = ["BAND_ATR_FRACTION", "LocationBand"]
