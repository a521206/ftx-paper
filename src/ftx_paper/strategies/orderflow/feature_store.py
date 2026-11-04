"""Shared source-row store used by canonical feature publication."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import NotRequired, Required, TypedDict, cast


class CanonicalInputRow(TypedDict):
    """Normalized one-minute source row consumed by orderflow features."""

    session_date: Required[str]
    time: Required[str]
    open: Required[float]
    high: Required[float]
    low: Required[float]
    close: Required[float]
    volume: NotRequired[int]
    oi: NotRequired[int]
    symbol: NotRequired[str]
    expiry: NotRequired[str]


class PriorSessionLevels(TypedDict):
    """Optional prior-day levels published to the active session."""

    low: NotRequired[float]
    high: NotRequired[float]


class FeatureStore:
    """Own the explicit historical and completed-session feature input rows.

    The store is deliberately in-memory: persistence of market bars remains the
    runtime store's responsibility, while replay and live use the same feature
    calculation API and causal row scope.
    """

    def __init__(
        self,
        rows_by_date: Mapping[str, Sequence[CanonicalInputRow]] | None = None,
    ) -> None:
        self._rows_by_date: dict[str, list[CanonicalInputRow]] = {}
        if rows_by_date:
            self.seed(rows_by_date)

    @property
    def rows_by_date(self) -> dict[str, tuple[CanonicalInputRow, ...]]:
        return {
            date: tuple(cast(CanonicalInputRow, dict(row)) for row in rows)
            for date, rows in self._rows_by_date.items()
        }

    def seed(self, rows_by_date: Mapping[str, Sequence[CanonicalInputRow]]) -> None:
        self._rows_by_date = {
            str(date): [cast(CanonicalInputRow, dict(row)) for row in rows]
            for date, rows in rows_by_date.items()
        }

    def append(self, rows: Sequence[CanonicalInputRow]) -> None:
        for row in rows:
            date = str(row["session_date"])
            bucket = self._rows_by_date.setdefault(date, [])
            identity = (str(row["time"]), str(row.get("symbol", "")))
            bucket[:] = [
                existing for existing in bucket
                if (str(existing["time"]), str(existing.get("symbol", ""))) != identity
            ]
            bucket.append(cast(CanonicalInputRow, dict(row)))
            bucket.sort(key=lambda item: str(item["time"]))


__all__ = ["CanonicalInputRow", "FeatureStore", "PriorSessionLevels"]
