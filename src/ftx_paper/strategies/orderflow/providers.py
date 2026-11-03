"""Canonical evidence and formation providers for the Paper host boundary.

Providers own ingress and publication timing only.  They do not decide policy,
repair gaps, or derive facts from terminal transition state.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any, cast

from ftx_paper.contracts import MarketRole

from .coordinator import OrderflowClockInput
from .evidence.features_adapter import PaperFeatureAdapter
from .evidence.producer import produce_evidence
from .orderflow_common import Aggregate
from .evidence_publication import EvidencePublication
from .location_observation import causal_children, location_observation
from .timestamps import coerce_timestamp, nifty_label
from .vwap_location import causal_vwap_prefix
from .opportunity_coordinator import LifecycleFormation
from .feature_store import FeatureStore
from .state_machine_types import LocationRegistry


def _aggregate(value: Mapping[str, Any], timeframe: str) -> Aggregate:
    return Aggregate(
        coerce_timestamp(str(value["time"])),
        coerce_timestamp(str(value["end_time"])),
        coerce_timestamp(str(value["available_at"])),
        timeframe,
        int(value["source_bar_count"]),
        float(value["open"]), float(value["high"]), float(value["low"]),
        float(value["close"]), int(value["volume"]), str(value["symbol"]),
        str(value.get("expiry", "")),
        str(value["session_date"]), str(value["source_fingerprint"]),
    )


@dataclass(frozen=True, slots=True)
class CanonicalInputEnvelope:
    """One provider publication and its causal lineage."""

    input: OrderflowClockInput
    source_ids: tuple[str, ...]
    unavailable: tuple[str, ...] = ()
    input_source: str = "provider_envelope"


class CanonicalInputProvider:
    """Produce the same causal clock inputs for bounded replay and live ingress.

    ``rows`` is a bounded, already selected source scope.  Live callers can use
    ``from_bundle`` with a bundle containing an explicit ``orderflow_clock_input``
    or a ``CanonicalInputEnvelope``; no missing publication is inferred.
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
        self._rows = tuple(dict(row) for row in rows)
        if not self._rows and dates:
            target = max(str(key) for key in dates)
            self._rows = tuple(self._feature_store.rows_by_date[target])
        self._by_identity: dict[str, CanonicalInputEnvelope] = {}
        self._build()

    def _build(self) -> None:
        self._by_identity = {}
        if not self._rows:
            return
        from .feature_input import build_observation_artifact

        session_date = str(self._rows[0]["session_date"])
        if any(str(row.get("session_date")) != session_date for row in self._rows):
            raise ValueError("canonical input rows must belong to one session")
        artifact, events = build_observation_artifact([dict(row) for row in self._rows])
        parsed = {
            timeframe: tuple(_aggregate(item, timeframe) for item in artifact["aggregates"][timeframe])
            for timeframe in ("1m", "5m")
        }
        rows_by_date = self._feature_store.rows_by_date
        adapter = PaperFeatureAdapter(
            rows_by_date, prior_values=self._prior_values, atr_values=self._atr_values,
            prior_session_levels=self._prior_session_levels,
        )
        vwap = {
            timeframe: causal_vwap_prefix(values, session_date=session_date)
            for timeframe, values in parsed.items()
        }
        parsed_values = {
            timeframe: tuple(item.value for item in events if item.timeframe == timeframe)
            for timeframe in ("1m", "5m")
        }
        parsed_available_at = {
            timeframe: tuple(coerce_timestamp(str(item["available_at"])) for item in values)
            for timeframe, values in parsed_values.items()
        }
        available: dict[str, list[dict[str, Any]]] = {"1m": [], "5m": []}
        skipped_vwap_sources: set[str] = set()
        for event in events:
            available[event.timeframe].append(event.value)
            if event.timeframe != "1m":
                continue
            available_at = event.available_at
            available = {
                timeframe: [item for item, item_available_at in zip(
                    values, parsed_available_at[timeframe], strict=True,
                ) if item_available_at <= available_at]
                for timeframe, values in parsed_values.items()
            }
            prefix = {
                **artifact,
                "aggregates": {key: list(value) for key, value in available.items()},
            }
            evidence = produce_evidence(
                prefix,
                session_date=session_date,
                adapter=adapter,
                parsed_aggregates={
                    key: tuple(item for item, item_available_at in zip(
                        parsed[key], parsed_available_at[key], strict=True,
                    ) if item_available_at <= available_at)
                    for key in parsed
                },
                precomputed_vwap_measurements=vwap,
            )["publication"]
            if not isinstance(evidence, EvidencePublication):
                raise TypeError("canonical evidence producer returned an invalid publication")
            if event.timeframe != "1m":
                continue
            one = _aggregate(event.value, "1m")
            latest5 = max(
                (item for item, item_available_at in zip(
                    parsed["5m"], parsed_available_at["5m"], strict=True,
                ) if item_available_at <= available_at),
                key=lambda item: item.available_at,
                default=None,
            )
            atr = evidence.atr
            levels = adapter.session_levels(session_date)
            prior = adapter.prior_value(session_date)
            location_registry = LocationRegistry(
                evidence.location_stream_id or f"orderflow:{session_date}"
            )
            for record in evidence.locations:
                location_registry.register(record)
            vwap_measurements = tuple(
                measurement
                for timeframe in ("1m", "5m")
                for measurement in vwap[timeframe]
                if measurement.available_at <= available_at
                and measurement.source_observation_id not in skipped_vwap_sources
            )
            if latest5 is not None and atr.value is None:
                skipped_vwap_sources.update(
                    measurement.source_observation_id
                    for measurement in vwap_measurements
                )
                vwap_measurements = ()
            source_id = f"aggregate:1m:{nifty_label(one.time)}"
            unavailable = tuple(evidence.unavailable)
            observations_1m = []
            observations_5m = []
            aggregates_by_timeframe = {
                "1m": tuple(item for item, item_available_at in zip(
                    parsed["1m"], parsed_available_at["1m"], strict=True,
                ) if item_available_at <= available_at),
                "5m": tuple(item for item, item_available_at in zip(
                    parsed["5m"], parsed_available_at["5m"], strict=True,
                ) if item_available_at <= available_at),
            }
            aggregate_by_id = {
                timeframe: {nifty_label(item.time): item for item in values}
                for timeframe, values in aggregates_by_timeframe.items()
            }
            for record in location_registry.available_as_of(one.available_at):
                if record.alias == "VWAP" and (
                    latest5 is None
                    or atr.value is None
                    or record.source_observation_id in skipped_vwap_sources
                ):
                    continue
                if not record.timeframe or record.timeframe == "1m":
                    one_observation = location_observation(
                        one, record,
                    )
                    observations_1m.append(one_observation)
                if latest5 is not None and (
                    not record.timeframe or record.timeframe == "5m"
                ):
                    source = aggregate_by_id["5m"].get(nifty_label(latest5.time))
                    if source is None or source.time != latest5.time:
                        continue
                    children = causal_children(
                        source, aggregates_by_timeframe["1m"],
                    )
                    if len(children) != 5:
                        continue
                    child_observations = tuple(
                        location_observation(
                            item, record,
                            available_at=max(
                                coerce_timestamp(item.available_at),
                                coerce_timestamp(record.available_at),
                            ),
                        )
                        for item in children
                    )
                    observations_1m.extend(child_observations)
                    observations_5m.append(location_observation(
                        source, record,
                        child_observation_ids=tuple(item.observation_id for item in child_observations),
                    ))
            observations_1m = list({
                observation.observation_id: observation
                for observation in observations_1m
            }.values())
            self._by_identity[source_id] = CanonicalInputEnvelope(
                OrderflowClockInput(
                    one_minute=one,
                    five_minute=latest5,
                    atr=atr,
                    session_levels=levels,
                    prior_session=adapter.prior_session(session_date),
                    prior_value=prior,
                    vwap_measurements=vwap_measurements,
                    observations_1m=tuple(observations_1m),
                    observations_5m=tuple(observations_5m),
                    input_diagnostic={
                        "eligible_location_records": len(
                            tuple(location_registry.available_as_of(one.available_at))
                        ),
                    },
                ),
                (source_id, *atr.lineage, *prior.lineage),
                unavailable,
            )

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
    ) -> "CanonicalInputProvider":
        return cls(
            rows, rows_by_date=rows_by_date, prior_values=prior_values,
            atr_values=atr_values, prior_session_levels=prior_session_levels,
        )

    @classmethod
    def from_market_bars(
        cls,
        bars: Sequence[Any],
        *,
        feature_history_bars: Sequence[Any] = (),
        prior_values: Mapping[str, Any] | None = None,
        atr_values: Mapping[str, Any] | None = None,
        prior_session_levels: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> "CanonicalInputProvider":
        """Adapt Paper completed futures bars into the canonical source shape."""
        rows = cls._rows_from_market_bars(bars)
        rows_by_date: dict[str, list[dict[str, Any]]] = {}
        for row in (*cls._rows_from_market_bars(feature_history_bars), *rows):
            rows_by_date.setdefault(str(row["session_date"]), []).append(row)
        target = max(rows_by_date, default=None)
        return cls(
            rows_by_date.get(target, ()) if target is not None else (),
            rows_by_date=rows_by_date,
            prior_values=prior_values,
            atr_values=atr_values,
            prior_session_levels=prior_session_levels,
        )

    def append_market_bars(self, bars: Sequence[Any]) -> None:
        rows = self._rows_from_market_bars(bars)
        if not rows:
            return
        self._feature_store.append(rows)
        session_date = str(rows[-1]["session_date"])
        self._rows = tuple(self._feature_store.rows_by_date[session_date])
        self._build()

    @staticmethod
    def _rows_from_market_bars(bars: Sequence[Any]) -> list[dict[str, Any]]:
        return [
            {
                "time": nifty_label(bar.timestamp), "open": bar.open, "high": bar.high,
                "low": bar.low, "close": bar.close, "volume": int(bar.volume or 0),
                "oi": int(bar.open_interest or 0), "symbol": bar.instrument.symbol,
                "expiry": bar.instrument.expiry or "",
                "session_date": nifty_label(bar.timestamp)[:10],
            }
            for bar in bars
            if getattr(bar.instrument, "instrument_type", "") in {"FUT", "FUTURES"}
        ]

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
            if identity in self._by_identity:
                return replace(self._by_identity[identity], input_source="provider_envelope")
            self.append_market_bars((bar,))
            if identity in self._by_identity:
                return replace(self._by_identity[identity], input_source="provider_appended_bar")
            raise ValueError(f"completed futures bar was not published: {identity}")
        raise ValueError("bundle has no canonical orderflow clock input")

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


__all__ = ["CanonicalFormationProvider", "CanonicalInputEnvelope", "CanonicalInputProvider"]
