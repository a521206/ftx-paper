"""Paper-owned feature provider for the Phase 3.5 publication boundary."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from ..market_observation import MarketObservation, observe_market
from ..state_machine_types import SessionLevels
from ..value_publication import ValuePublication
from ..evidence_publication import AtrPublication
from ..feature_store import CanonicalInputRow, PriorSessionLevels
from ..timestamps import parse_nifty_label
from .producer import EvidenceAdapter


class PaperFeatureAdapter(EvidenceAdapter):
    """Calculate Phase 3.5 inputs from Paper-owned market rows and features."""

    def __init__(
        self,
        rows_by_date: Mapping[str, Sequence[CanonicalInputRow]],
        *,
        prior_values: Mapping[str, ValuePublication] | None = None,
        atr_values: Mapping[str, AtrPublication] | None = None,
        prior_session_levels: Mapping[str, PriorSessionLevels] | None = None,
    ) -> None:
        self._rows = {str(date): tuple(dict(row) for row in rows) for date, rows in rows_by_date.items()}
        self._prior_values = dict(prior_values or {})
        self._atr_values = None if atr_values is None else dict(atr_values)
        self._prior_session_levels = None if prior_session_levels is None else dict(prior_session_levels)
        self._observations: dict[str, MarketObservation] = {}
        self._atr_cache: dict[tuple[str, str], AtrPublication] = {}
        self._levels_cache: dict[str, SessionLevels | None] = {}
        self._prior_value_cache: dict[str, ValuePublication] = {}
        first = next((row for rows in self._rows.values() for row in rows), None)
        if first is None:
            self.location_stream_id = "orderflow:locations:1m"
        else:
            self.location_stream_id = (
                f"{first.get('symbol', 'NIFTY')}:{first.get('expiry', '')}:"
                "1m"
            )

    def atr(self, session_date: str, symbol: str) -> AtrPublication:
        key = (session_date, symbol)
        if self._atr_values is not None and session_date in self._atr_values:
            return self._atr_values[session_date]
        if key not in self._atr_cache:
            reason = "derived_artifact_unavailable" if self._atr_values is not None else "insufficient_daily_atr_history"
            self._atr_cache[key] = AtrPublication.unavailable(reason)
        return self._atr_cache[key]

    def session_levels(self, session_date: str) -> SessionLevels | None:
        if session_date not in self._levels_cache:
            rows = self._rows.get(session_date, ())
            if not rows:
                result = None
            else:
                observation = self._observations.get(session_date)
                if observation is None:
                    observation = observe_market([
                        dict(row) for row in rows
                    ])
                    self._observations[session_date] = observation
                first = next(
                    (item for item in observation.aggregates["5m"] if item.available_at),
                    None,
                )
                if first is None:
                    result = None
                else:
                    prior_levels = (
                        self._prior_session_levels.get(session_date, {})
                        if self._prior_session_levels is not None else {}
                    )
                    prior_low = prior_levels.get("low")
                    prior_high = prior_levels.get("high")
                    result = SessionLevels(
                        opening_range_low=first.low,
                        opening_range_high=first.high,
                        prior_day_low=(
                            float(prior_low) if prior_low is not None else None
                        ),
                        prior_day_high=(
                            float(prior_high) if prior_high is not None else None
                        ),
                        opening_range_available_at=first.available_at,
                        prior_day_available_at=parse_nifty_label(f"{session_date}T09:15"),
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
