"""Paper-owned feature provider for the Phase 3.5 publication boundary."""

from __future__ import annotations
# pyright: reportArgumentType=false

import hashlib
from collections.abc import Mapping, Sequence

from ..features import daily_atr
from ..market_observation import observe_market
from ..state_machine_types import SessionLevels
from ..value_publication import ValuePublication
from ..evidence_publication import AtrPublication
from .producer import EvidenceAdapter


class PaperFeatureAdapter(EvidenceAdapter):
    """Calculate Phase 3.5 inputs from Paper-owned market rows and features."""

    def __init__(
        self,
        rows_by_date: Mapping[str, Sequence[Mapping[str, object]]],
        *,
        prior_values: Mapping[str, ValuePublication] | None = None,
    ) -> None:
        self._rows = {date: tuple(rows) for date, rows in rows_by_date.items()}
        self._prior_values = dict(prior_values or {})
        self._observations = {
            date: observe_market([dict(row) for row in rows])
            for date, rows in self._rows.items()
        }
        self._atr_cache: dict[tuple[str, str], AtrPublication] = {}
        self._levels_cache: dict[str, SessionLevels | None] = {}
        self._prior_value_cache: dict[str, ValuePublication] = {}
        first = next((row for rows in self._rows.values() for row in rows), None)
        if first is None:
            self.location_stream_id = "orderflow:locations:1m"
        else:
            self.location_stream_id = (
                f"{first.get('symbol', 'NIFTY')}:{first.get('expiry', '')}:"
                f"{first.get('contract_type', 'CONTINUOUS')}:1m"
            )

    def atr(self, session_date: str, symbol: str) -> AtrPublication:
        key = (session_date, symbol)
        if key not in self._atr_cache:
            value = daily_atr(session_date, self._rows)
            if value is None:
                result = AtrPublication.unavailable("insufficient_daily_atr_history")
            else:
                prior_dates = sorted(date for date in self._rows if date < session_date)
                observed_at = f"{prior_dates[-1]}T15:30" if prior_dates else None
                if observed_at is None:
                    result = AtrPublication.unavailable("insufficient_daily_atr_history")
                else:
                    lineage = f"daily_atr:{symbol}:{session_date}:{value:.12g}"
                    result = AtrPublication(
                        value, observed_at, f"{session_date}T09:15",
                        hashlib.sha256(lineage.encode()).hexdigest(), (lineage,),
                    )
            self._atr_cache[key] = result
        return self._atr_cache[key]

    def session_levels(self, session_date: str) -> SessionLevels | None:
        if session_date not in self._levels_cache:
            rows = self._rows.get(session_date, ())
            if not rows:
                result = None
            else:
                prior_dates = sorted(date for date in self._rows if date < session_date)
                prior_rows = self._rows[prior_dates[-1]] if prior_dates else ()
                first = next(
                    (item for item in self._observations[session_date].aggregates["5m"] if item.available_at),
                    None,
                )
                if first is None:
                    raise ValueError(f"no completed 5m context available for {session_date}")
                result = SessionLevels(
                    opening_range_low=first.low,
                    opening_range_high=first.high,
                    prior_day_low=min((float(str(row["low"])) for row in prior_rows), default=None),
                    prior_day_high=max((float(str(row["high"])) for row in prior_rows), default=None),
                    opening_range_available_at=first.available_at,
                    prior_day_available_at=f"{session_date}T09:15",
                )
            self._levels_cache[session_date] = result
        return self._levels_cache[session_date]

    def prior_session(self, session_date: str) -> str | None:
        prior_dates = sorted(date for date in self._rows if date < session_date)
        return prior_dates[-1] if prior_dates else None

    def prior_value(self, session_date: str) -> ValuePublication:
        if session_date not in self._prior_value_cache:
            self._prior_value_cache[session_date] = self._prior_values.get(
                session_date,
                ValuePublication(None, None, None, (), "missing_prior_session_value_evidence"),
            )
        return self._prior_value_cache[session_date]

__all__ = ["PaperFeatureAdapter"]
