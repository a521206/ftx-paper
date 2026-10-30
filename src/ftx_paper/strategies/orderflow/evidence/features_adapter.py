"""Paper-owned feature provider for the Phase 3.5 publication boundary."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence

from ..contextual import publish_market_contexts
from ..features import daily_atr, derive_session_levels
from ..hierarchy import MarketContext
from ..market_observation import observe_market
from ..prior_session_value import PriorSessionValueStore, build_prior_session_profile
from ..state_machine_types import MarketContextStore, SessionLevels
from ..value_publication import ValuePublication, publish_prior_session_value
from ..evidence_publication import AtrPublication
from .producer import EvidenceAdapter


class PaperFeatureAdapter(EvidenceAdapter):
    """Calculate Phase 3.5 inputs from Paper-owned market rows and features."""

    context_stream_id = "orderflow:context:5m"
    def __init__(self, rows_by_date: Mapping[str, Sequence[Mapping[str, object]]]) -> None:
        self._rows = {date: tuple(rows) for date, rows in rows_by_date.items()}
        self._observations = {
            date: observe_market([dict(row) for row in rows])
            for date, rows in self._rows.items()
        }
        self._atr_cache: dict[tuple[str, str], AtrPublication] = {}
        self._levels_cache: dict[str, SessionLevels | None] = {}
        self._prior_value_cache: dict[str, ValuePublication] = {}
        self._contexts_cache: dict[str, tuple[MarketContext, ...]] = {}
        first = next((row for rows in self._rows.values() for row in rows), None)
        if first is None:
            self.location_stream_id = "orderflow:locations:1m"
        else:
            self.location_stream_id = (
                f"{first.get('symbol', 'NIFTY')}:{first.get('expiry', '')}:"
                f"{first.get('contract_type', 'CONTINUOUS')}:1m"
            )
        profiles = tuple(
            profile
            for date, rows in self._rows.items()
            if (profile := build_prior_session_profile(rows, session_date=date)) is not None
        )
        self._values = PriorSessionValueStore(profiles)

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
                result = derive_session_levels(
                    self._observations[session_date].aggregates["5m"], prior_rows, session_date,
                )
            self._levels_cache[session_date] = result
        return self._levels_cache[session_date]

    def prior_session(self, session_date: str) -> str | None:
        prior_dates = sorted(date for date in self._rows if date < session_date)
        return prior_dates[-1] if prior_dates else None

    def prior_value(self, session_date: str) -> ValuePublication:
        if session_date not in self._prior_value_cache:
            self._prior_value_cache[session_date] = publish_prior_session_value(
                self._values, session_date=session_date,
            )
        return self._prior_value_cache[session_date]

    def contexts(self, session_date: str) -> Sequence[MarketContext]:
        if session_date not in self._contexts_cache:
            rows = self._rows.get(session_date, ())
            if not rows:
                result = ()
            else:
                observation = self._observations[session_date]
                store = MarketContextStore()
                publish_market_contexts(
                    observation.aggregates["5m"],
                    stream_id=self.context_stream_id,
                    store=store,
                    raw_rows=rows,
                    aggregates=observation.aggregates,
                    atr=self.atr(session_date, str(rows[0].get("symbol", "NIFTY"))).value,
                )
                result = tuple(store.versions.get(self.context_stream_id, ()))
            self._contexts_cache[session_date] = result
        return self._contexts_cache[session_date]


__all__ = ["PaperFeatureAdapter"]
