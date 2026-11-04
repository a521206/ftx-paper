"""Canonical evidence and formation providers for the Paper host boundary.

Providers own ingress and publication timing only.  They do not decide policy,
repair gaps, or derive facts from terminal transition state.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, cast

from ftx_paper.contracts import MarketRole

from .coordinator import OrderflowClockInput
from .evidence.features_adapter import PaperFeatureAdapter
from .evidence.producer import unavailable_evidence_reasons
from .orderflow_common import Aggregate, build_aggregate_result
from .evidence_publication import AtrPublication
from .location_observation import causal_children, location_observation
from .timestamps import coerce_timestamp, nifty_label, parse_nifty_label, session_minute
from .vwap_location import CausalVwapAccumulator, VwapMeasurement
from .opportunity_coordinator import LifecycleFormation
from .feature_store import FeatureStore
from .state_machine_types import LocationRegistry, SessionLevels
from .value_publication import ValuePublication


@dataclass(frozen=True, slots=True)
class CanonicalInputEnvelope:
    """One published orderflow clock input and its causal lineage."""

    input: OrderflowClockInput
    source_ids: tuple[str, ...]
    unavailable: tuple[str, ...] = ()
    input_source: str = "stream_envelope"


class OrderflowInputStream:
    """Incrementally publish causal clock inputs from one ordered bar stream.

    Historical replay and live ingress both enter through ``append_continuous_bars``;
    supplied canonical inputs can bypass construction without inference. When
    only ``rows_by_date`` is supplied, the latest date is streamed; other dates
    are reference history only.
    """

    def __init__(
        self,
        rows: Sequence[Mapping[str, Any]] = (),
        *,
        rows_by_date: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
        feature_store: FeatureStore | None = None,
        prior_values: Mapping[str, Any] | None = None,
        atr_values: Mapping[str, Any] | None = None,
        prior_session_levels: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        dates = dict(rows_by_date or {})
        if rows and not dates:
            dates[str(rows[0]["session_date"])] = rows
        self._feature_store = feature_store or FeatureStore(dates)
        self._prior_values = dict(prior_values or {})
        self._atr_values = None if atr_values is None else dict(atr_values)
        self._prior_session_levels = None if prior_session_levels is None else dict(prior_session_levels)
        selected_rows = tuple(dict(row) for row in rows)
        if not selected_rows and dates:
            target = max(str(key) for key in dates)
            if target not in dates:
                raise ValueError(f"target orderflow session is absent: {target}")
            selected_rows = tuple(self._feature_store.rows_by_date[target])
        self._by_identity: dict[str, CanonicalInputEnvelope] = {}
        self._session_date: str | None = None
        self._prior_session: str | None = None
        self._last_row_timestamp: datetime | None = None
        self._five_minute_rows: list[dict[str, Any]] = []
        self._five_minute_start: int | None = None
        self._aggregates: dict[str, list[Aggregate]] = {"1m": [], "5m": []}
        self._vwap_accumulators: dict[str, CausalVwapAccumulator] = {}
        self._vwap_measurements: dict[str, list[VwapMeasurement]] = {"1m": [], "5m": []}
        self._published_vwap_sources: set[str] = set()
        self._location_registry: LocationRegistry | None = None
        self._session_levels: SessionLevels | None = None
        self._published_session_levels = False
        self._session_high: float | None = None
        self._session_low: float | None = None
        self._adapter = PaperFeatureAdapter(
            self._feature_store.rows_by_date,
            prior_values=self._prior_values,
            atr_values=self._atr_values,
            prior_session_levels=self._prior_session_levels,
        )
        for row in selected_rows:
            self._append_row(row)

    def publish_setup(
        self,
        session_date: str,
        *,
        atr: AtrPublication | None = None,
        prior_value: ValuePublication | None = None,
        prior_session_levels: Mapping[str, Any] | None = None,
    ) -> None:
        """Update explicit setup publications; they take effect at their availability."""
        if atr is not None:
            if self._atr_values is None:
                self._atr_values = {}
            self._atr_values[session_date] = atr
        if prior_value is not None:
            self._prior_values[session_date] = prior_value
        if prior_session_levels is not None:
            if self._prior_session_levels is None:
                self._prior_session_levels = {}
            self._prior_session_levels[session_date] = dict(prior_session_levels)
        self._adapter = PaperFeatureAdapter(
            self._feature_store.rows_by_date,
            prior_values=self._prior_values,
            atr_values=self._atr_values,
            prior_session_levels=self._prior_session_levels,
        )

    def _build_session_levels(self, session_date: str) -> SessionLevels | None:
        """Derive one-bar opening range and prior-day extremes (canonical derive_session_levels).

        The opening range is taken from the session's first completed 5m aggregate.
        Returns None when the opening range is not yet derivable from loaded rows.
        """
        rows = self._feature_store.rows_by_date.get(session_date, ())
        session_rows = [
            row for row in rows
            if "09:15" <= str(row["time"])[-5:] <= "15:30"
        ]
        if len(session_rows) < 5:
            return None
        opening = build_aggregate_result(session_rows[:5], 5).aggregates
        if not opening:
            return None
        first = opening[0]
        prior_levels = (self._prior_session_levels or {}).get(session_date, {})
        return SessionLevels(
            opening_range_low=first.low,
            opening_range_high=first.high,
            prior_day_low=(
                float(prior_levels["low"]) if "low" in prior_levels else None
            ),
            prior_day_high=(
                float(prior_levels["high"]) if "high" in prior_levels else None
            ),
            opening_range_available_at=first.available_at,
            prior_day_available_at=parse_nifty_label(f"{session_date}T09:15"),
        )

    def _append_row(self, row: Mapping[str, Any]) -> CanonicalInputEnvelope | None:
        source = dict(row)
        session_date = str(source["session_date"])
        timestamp = coerce_timestamp(session_minute(session_date, source["time"]))
        if self._last_row_timestamp is not None and timestamp <= self._last_row_timestamp:
            raise ValueError("canonical input rows must be strictly ordered")
        self._last_row_timestamp = timestamp
        if session_date != self._session_date:
            if self._session_date is not None:
                if session_date < self._session_date:
                    raise ValueError("canonical input sessions must be strictly ordered")
                self._prior_session = self._session_date
            else:
                prior_dates = [date for date in self._feature_store.rows_by_date if date < session_date]
                self._prior_session = max(prior_dates, default=None)
            self._session_date = session_date
            self._five_minute_rows = []
            self._five_minute_start = None
            self._aggregates = {"1m": [], "5m": []}
            self._vwap_accumulators = {}
            self._vwap_measurements = {"1m": [], "5m": []}
            self._published_vwap_sources = set()
            self._location_registry = LocationRegistry(
                f"{source.get('symbol', 'NIFTY')}:{source.get('expiry', '')}:1m"
            )
            self._session_levels = self._build_session_levels(session_date)
            self._published_session_levels = False
            self._session_high = None
            self._session_low = None
        if not "09:15" <= session_minute(session_date, source["time"])[-5:] <= "15:30":
            return None
        one_minute_values = build_aggregate_result([source], 1).aggregates
        if not one_minute_values:
            return None
        one = one_minute_values[0]
        self._aggregates["1m"].append(one)

        local_time = session_minute(session_date, source["time"])[-5:]
        hour, minute = (int(part) for part in local_time.split(":"))
        offset = hour * 60 + minute - (9 * 60 + 15)
        five_minute_start = (9 * 60 + 15) + (offset // 5) * 5
        five_minute_gap: dict[str, Any] | None = None
        if five_minute_start != self._five_minute_start:
            if self._five_minute_rows and len(self._five_minute_rows) < 5:
                assert self._five_minute_start is not None
                expected = tuple(
                    f"{(self._five_minute_start + index) // 60:02d}:"
                    f"{(self._five_minute_start + index) % 60:02d}"
                    for index in range(5)
                )
                observed = tuple(str(item["time"])[-5:] for item in self._five_minute_rows)
                five_minute_gap = {
                    "bucket_start": expected[0],
                    "expected_minutes": expected,
                    "observed_minutes": observed,
                    "missing_minutes": tuple(item for item in expected if item not in observed),
                    "reason": "incomplete_or_misaligned_1m_references",
                }
            self._five_minute_start = five_minute_start
            self._five_minute_rows = []
        self._five_minute_rows.append(source)
        five_minute_values = (
            build_aggregate_result(self._five_minute_rows, 5).aggregates
            if len(self._five_minute_rows) == 5 else ()
        )
        new_five = five_minute_values[0] if five_minute_values else None
        if new_five is not None:
            self._aggregates["5m"].append(new_five)

        atr_publication = self._adapter.atr(session_date, str(source.get("symbol", "NIFTY")))
        atr = (
            atr_publication
            if atr_publication.value is None
            or (
                atr_publication.available_at is not None
                and atr_publication.available_at <= one.available_at
            )
            else AtrPublication.unavailable("atr_not_yet_available")
        )
        prior_publication = self._adapter.prior_value(session_date)
        prior = (
            prior_publication
            if prior_publication.available_at is None
            or prior_publication.available_at <= one.available_at
            else ValuePublication(None, None, None, (), "prior_value_not_yet_available")
        )
        if self._location_registry is None:
            raise RuntimeError("location registry was not initialized")
        registry = self._location_registry

        if new_five is not None:
            if self._session_levels is None:
                prior_levels = (self._prior_session_levels or {}).get(session_date, {})
                self._session_levels = SessionLevels(
                    opening_range_low=new_five.low,
                    opening_range_high=new_five.high,
                    prior_day_low=(
                        float(prior_levels["low"]) if "low" in prior_levels else None
                    ),
                    prior_day_high=(
                        float(prior_levels["high"]) if "high" in prior_levels else None
                    ),
                    opening_range_available_at=new_five.available_at,
                    prior_day_available_at=parse_nifty_label(f"{session_date}T09:15"),
                )
            self._session_high = (
                new_five.high if self._session_high is None
                else max(self._session_high, new_five.high)
            )
            self._session_low = (
                new_five.low if self._session_low is None
                else min(self._session_low, new_five.low)
            )

        atr_value = atr.value
        atr_available_at = (
            (atr.available_at or one.available_at) if atr_value is not None else None
        )
        if atr_value is not None and atr_available_at is not None:
            if self._session_levels is not None and not self._published_session_levels:
                if atr_available_at > self._session_levels.opening_range_available_at:
                    self._session_levels = replace(
                        self._session_levels,
                        opening_range_available_at=atr_available_at,
                    )
                registry.publish_session_levels(
                    self._session_levels,
                    source_session=session_date,
                    prior_session=self._prior_session,
                    atr=atr_value,
                )
                self._published_session_levels = True
            if self._aggregates["5m"]:
                assert self._session_high is not None and self._session_low is not None
                latest = replace(
                    self._aggregates["5m"][-1],
                    available_at=max(self._aggregates["5m"][-1].available_at, atr_available_at),
                )
                registry.publish_session_extreme(
                    "SESSION_HIGH", self._session_high, latest, atr=atr_value,
                )
                registry.publish_session_extreme(
                    "SESSION_LOW", self._session_low, latest, atr=atr_value,
                )

        for aggregate in (one, new_five):
            if aggregate is None:
                continue
            timeframe = aggregate.timeframe
            accumulator = self._vwap_accumulators.setdefault(
                timeframe, CausalVwapAccumulator(session_date=session_date),
            )
            measurement = accumulator.append(aggregate)
            self._vwap_measurements[timeframe].append(measurement)
        if atr_value is not None and atr_available_at is not None and self._aggregates["5m"]:
            for timeframe in ("1m", "5m"):
                for measurement in self._vwap_measurements[timeframe]:
                    if (
                        measurement.available_at > one.available_at
                        or measurement.source_observation_id in self._published_vwap_sources
                    ):
                        continue
                    registry.publish_vwap(
                        value=measurement.value,
                        available_at=max(measurement.available_at, atr_available_at),
                        source_session=session_date,
                        source_aggregate_id=measurement.source_aggregate_id,
                        source_observation_id=measurement.source_observation_id,
                        timeframe=measurement.timeframe,
                        atr=atr_value,
                    )
                    self._published_vwap_sources.add(measurement.source_observation_id)

        latest5 = self._aggregates["5m"][-1] if self._aggregates["5m"] else None
        vwap_measurements = tuple(
            measurement
            for timeframe in ("1m", "5m")
            for measurement in self._vwap_measurements[timeframe]
            if measurement.available_at <= one.available_at
        )

        observations_1m = []
        observations_5m = []
        for record in registry.available_as_of(one.available_at):
            if record.alias == "VWAP" and (
                latest5 is None or atr.value is None
            ):
                continue
            if not record.timeframe or record.timeframe == "1m":
                observations_1m.append(location_observation(one, record))
            if latest5 is not None and (not record.timeframe or record.timeframe == "5m"):
                children = causal_children(latest5, self._aggregates["1m"])
                if len(children) != 5:
                    continue
                child_observations = tuple(
                    location_observation(
                        child,
                        record,
                        available_at=max(child.available_at, record.available_at),
                    )
                    for child in children
                )
                observations_1m.extend(child_observations)
                observations_5m.append(location_observation(
                    latest5,
                    record,
                    child_observation_ids=tuple(item.observation_id for item in child_observations),
                ))
        observations_1m = list({
            observation.observation_id: observation
            for observation in observations_1m
        }.values())

        unavailable = list(unavailable_evidence_reasons(
            atr, session_levels_available=self._session_levels is not None,
        ))
        levels = (
            self._session_levels
            if self._session_levels is not None
            and self._session_levels.opening_range_available_at <= one.available_at
            else None
        )
        source_id = f"aggregate:1m:{nifty_label(one.time)}"
        envelope = CanonicalInputEnvelope(
            OrderflowClockInput(
                one_minute=one,
                five_minute=latest5,
                atr=atr,
                session_levels=levels,
                prior_session=self._prior_session,
                prior_value=prior,
                vwap_measurements=vwap_measurements,
                observations_1m=tuple(observations_1m),
                observations_5m=tuple(observations_5m),
                input_diagnostic={
                    "eligible_location_records": len(tuple(registry.available_as_of(one.available_at))),
                    **({"five_minute_gap": five_minute_gap} if five_minute_gap else {}),
                },
            ),
            (source_id, *atr.lineage, *prior.lineage),
            tuple(unavailable),
        )
        self._by_identity[source_id] = envelope
        return envelope

    def __call__(self, bundle: object) -> OrderflowClockInput:
        envelope = self.from_bundle(bundle)
        return replace(
            envelope.input,
            input_diagnostic={
                **envelope.input.input_diagnostic,
                "input_source": envelope.input_source,
                "observations_1m": len(envelope.input.observations_1m),
                "observations_5m": len(envelope.input.observations_5m),
            },
        )

    @property
    def envelopes(self) -> tuple[CanonicalInputEnvelope, ...]:
        """Published bounded prefix in canonical availability order."""
        return tuple(self._by_identity.values())

    @classmethod
    def from_rows(
        cls,
        rows: Sequence[Mapping[str, Any]],
        *,
        rows_by_date: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
        prior_values: Mapping[str, Any] | None = None,
        atr_values: Mapping[str, Any] | None = None,
        prior_session_levels: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> "OrderflowInputStream":
        return cls(
            rows, rows_by_date=rows_by_date, prior_values=prior_values,
            atr_values=atr_values, prior_session_levels=prior_session_levels,
        )

    @classmethod
    def from_continuous_bars(
        cls,
        bars: Sequence[Any],
        *,
        feature_history_bars: Sequence[Any] = (),
        prior_values: Mapping[str, Any] | None = None,
        atr_values: Mapping[str, Any] | None = None,
        prior_session_levels: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> "OrderflowInputStream":
        """Create a stream from selected continuous futures bars chronologically."""
        rows = cls._rows_from_continuous_bars(bars)
        rows_by_date: dict[str, list[dict[str, Any]]] = {}
        for row in (*cls._rows_from_continuous_bars(feature_history_bars), *rows):
            rows_by_date.setdefault(str(row["session_date"]), []).append(row)
        for session_rows in rows_by_date.values():
            session_rows.sort(key=lambda row: str(row["time"]))
        target = max(rows_by_date, default=None)
        return cls(
            rows_by_date.get(target, ()) if target is not None else (),
            rows_by_date=rows_by_date,
            prior_values=prior_values,
            atr_values=atr_values,
            prior_session_levels=prior_session_levels,
        )

    def append_continuous_bars(self, bars: Sequence[Any]) -> None:
        rows = self._rows_from_continuous_bars(bars)
        for row in rows:
            self._append_row(row)
            self._feature_store.append((row,))

    @staticmethod
    def _rows_from_continuous_bars(bars: Sequence[Any]) -> list[dict[str, Any]]:
        rows = []
        for bar in bars:
            if getattr(bar.instrument, "instrument_type", "") not in {"FUT", "FUTURES"}:
                continue
            if bar.instrument.symbol != "NIFTY" or bar.instrument.expiry is not None:
                raise ValueError("orderflow input requires a selected continuous futures bar")
            rows.append({
                "time": nifty_label(bar.timestamp), "open": bar.open, "high": bar.high,
                "low": bar.low, "close": bar.close, "volume": int(bar.volume or 0),
                "oi": int(bar.open_interest or 0), "symbol": bar.instrument.symbol,
                "expiry": bar.instrument.expiry or "",
                "session_date": nifty_label(bar.timestamp)[:10],
            })
        return rows

    def from_bundle(self, bundle: object) -> CanonicalInputEnvelope:
        if isinstance(bundle, Mapping):
            supporting = bundle.get("supporting_inputs")
            if isinstance(supporting, Mapping):
                supplied = supporting.get("orderflow_clock_input")
                if isinstance(supplied, OrderflowClockInput):
                    return CanonicalInputEnvelope(supplied, (), (), "bundle_orderflow_clock_input")
                envelope = supporting.get("canonical_input")
                if isinstance(envelope, CanonicalInputEnvelope):
                    return replace(envelope, input_source="bundle_canonical_input")
            supplied = bundle.get("orderflow_clock_input")
            if isinstance(supplied, OrderflowClockInput):
                return CanonicalInputEnvelope(supplied, (), (), "bundle_orderflow_clock_input")
            envelope = bundle.get("canonical_input")
            if isinstance(envelope, CanonicalInputEnvelope):
                return replace(envelope, input_source="bundle_canonical_input")
        if hasattr(bundle, "values"):
            return self.from_evidence(bundle)
        supporting = getattr(bundle, "supporting_inputs", None)
        if isinstance(supporting, Mapping):
            supplied = supporting.get("orderflow_clock_input")
            if isinstance(supplied, OrderflowClockInput):
                return CanonicalInputEnvelope(supplied, (), (), "bundle_orderflow_clock_input")
            envelope = supporting.get("canonical_input")
            if isinstance(envelope, CanonicalInputEnvelope):
                return replace(envelope, input_source="bundle_canonical_input")
        bars = getattr(bundle, "bars", {})
        bar = bars.get(MarketRole.FUTURES) if isinstance(bars, Mapping) else None
        timestamp = getattr(bar, "timestamp", None)
        if bar is not None and timestamp is not None:
            timestamp = coerce_timestamp(timestamp)
            identity = f"aggregate:1m:{nifty_label(timestamp)}"
            source = self._append_bundle_ingress(bar, identity)
            if identity not in self._by_identity:
                raise ValueError(f"completed futures bar was not published: {identity}")
            return replace(self._by_identity[identity], input_source=source)
        raise ValueError("bundle has no canonical orderflow clock input")

    def _append_bundle_ingress(self, bar: object, identity: str) -> str:
        """Observe a bundle bar that no feature-append callback published.

        Live hosts append completed bars before observation; bounded replay has
        no such callback, so its bundle bar enters the same stream here.
        """
        if identity in self._by_identity:
            return "stream_envelope"
        self.append_continuous_bars((bar,))
        return "stream_appended_bar"

    def from_evidence(self, evidence: object) -> CanonicalInputEnvelope:
        """Consume an explicit completed-bar evidence envelope from live ingress."""
        values = getattr(evidence, "values", None)
        if not isinstance(values, Mapping) and isinstance(evidence, Mapping):
            values = evidence.get("values")
        if isinstance(values, Mapping):
            supplied = values.get("canonical_input", values.get("orderflow_clock_input"))
            if isinstance(supplied, CanonicalInputEnvelope):
                return replace(supplied, input_source="evidence_canonical_input")
            if isinstance(supplied, OrderflowClockInput):
                return CanonicalInputEnvelope(supplied, (), (), "evidence_orderflow_clock_input")
        raise ValueError("evidence has no explicit canonical orderflow input")


class CanonicalFormationProvider:
    """Expose canonical owner formations once, in causal order."""

    def __init__(
        self,
        owner: object | None = None,
        *,
        formations: Iterable[LifecycleFormation] = (),
        source: Callable[[OrderflowClockInput], Iterable[LifecycleFormation]] | None = None,
    ) -> None:
        if source is None and owner is not None:
            candidate = getattr(owner, "formations_for", None)
            if callable(candidate):
                owner_formations = cast(
                    Callable[[object], Iterable[LifecycleFormation]], candidate,
                )

                def owner_source(clock: OrderflowClockInput) -> Iterable[LifecycleFormation]:
                    return owner_formations(clock.one_minute.available_at)

                source = owner_source
        self._source = source
        self._formations = tuple(formations)
        self._seen: set[str] = set()

    def __call__(self, clock: OrderflowClockInput) -> tuple[LifecycleFormation, ...]:
        supplied = self._source(clock) if self._source is not None else self._formations
        result: list[LifecycleFormation] = []
        for item in supplied:
            identity = item.identity or item.lifecycle_id
            if identity in self._seen:
                continue
            if coerce_timestamp(item.available_at) > clock.one_minute.available_at:
                continue
            self._seen.add(identity)
            result.append(item)
        return tuple(sorted(result, key=lambda item: (coerce_timestamp(item.available_at), item.identity)))


__all__ = ["CanonicalFormationProvider", "CanonicalInputEnvelope", "OrderflowInputStream"]
