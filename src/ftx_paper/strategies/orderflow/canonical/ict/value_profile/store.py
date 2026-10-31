"""Persistence port for completed session daily profiles."""

from __future__ import annotations

from typing import Protocol

from ftx_paper.strategies.orderflow.canonical.ict.value_profile.records import DailyProfileRecord


class DailyProfileStore(Protocol):
    def load(
        self,
        *,
        instrument: str,
        timeframe_minutes: int,
        config_fingerprint: str,
        session_date: str,
    ) -> DailyProfileRecord | None: ...

    def save(self, record: DailyProfileRecord) -> None: ...
