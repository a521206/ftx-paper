"""Shared incremental orderflow coordination for live and replay.

The coordinator owns chronological detector state.  Evidence is supplied by the
host as publications; this module deliberately does not manufacture session
levels, ATR, VWAP, locations, or child observations.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import hashlib
from typing import Any

from .evidence_publication import AtrPublication
from .interaction_detector import InteractionDetector, InteractionTransition
from .location_observation import LocationObservation
from .orderflow_common import Aggregate
from .timestamps import coerce_timestamp, nifty_label
from .state_machine_types import LocationRegistry, SessionLevels
from .value_publication import PriorSessionValueReference, ValuePublication
from .vwap_location import VwapMeasurement
from .snapshot_codec import restore_aggregate


@dataclass(frozen=True, slots=True)
class OrderflowInput:
    """One completed decision-clock input and point-in-time publications."""

    one_minute: Aggregate
    five_minute: Aggregate | None = None
    atr: AtrPublication | None = None
    session_levels: SessionLevels | None = None
    prior_session: str | None = None
    prior_value: ValuePublication | None = None
    vwap_measurements: tuple[VwapMeasurement, ...] = ()
    observations_1m: tuple[LocationObservation, ...] = ()
    observations_5m: tuple[LocationObservation, ...] = ()
    input_diagnostic: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class OrderflowAdvance:
    """Facts emitted by one idempotent coordinator transition."""

    events: tuple[dict[str, Any], ...] = ()
    transitions: tuple[InteractionTransition, ...] = ()


@dataclass(slots=True)
class OrderflowCoordinator:
    """Canonical interaction owner shared by incremental live and replay runs."""

    location_registry: LocationRegistry
    detector: InteractionDetector = field(init=False)
    _one_minute: list[Aggregate] = field(default_factory=list, init=False)
    _five_minute: list[Aggregate] = field(default_factory=list, init=False)
    _seen: set[str] = field(default_factory=set, init=False)
    _levels_published: set[str] = field(default_factory=set, init=False)
    _vwap_published: set[tuple[str, str]] = field(default_factory=set, init=False)
    latest_atr: AtrPublication | None = field(default=None, init=False)
    latest_prior_value: ValuePublication | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        self.detector = InteractionDetector(self.location_registry)

    @staticmethod
    def input_identity(item: OrderflowInput) -> str:
        return f"aggregate:{item.one_minute.timeframe}:{nifty_label(item.one_minute.time)}"

    def has_seen(self, item: OrderflowInput) -> bool:
        return self.input_identity(item) in self._seen

    def bind_registry(self, registry: LocationRegistry) -> None:
        """Rebind the coordinator and detector to one restored registry."""
        self.location_registry = registry
        self.detector.registry = registry

    def snapshot_state(self) -> dict[str, Any]:
        """Return coordinator-owned chronological state for recovery."""
        return {
            "one_minute": [item.to_dict() for item in self._one_minute],
            "five_minute": [item.to_dict() for item in self._five_minute],
            "seen": sorted(self._seen),
            "levels_published": sorted(self._levels_published),
            "vwap_published": [list(item) for item in sorted(self._vwap_published)],
            "latest_atr": self.latest_atr,
            "latest_prior_value": self.latest_prior_value,
            "detector": self.detector.snapshot_state(),
        }

    def restore_state(self, raw: Mapping[str, Any]) -> None:
        """Restore coordinator-owned state without exposing mutable internals."""
        self._one_minute = [
            aggregate
            for item in raw.get("one_minute", ())
            if (aggregate := restore_aggregate(item)) is not None
        ]
        self._five_minute = [
            aggregate
            for item in raw.get("five_minute", ())
            if (aggregate := restore_aggregate(item)) is not None
        ]
        self._seen = {str(item) for item in raw.get("seen", ())}
        self._levels_published = {str(item) for item in raw.get("levels_published", ())}
        self._vwap_published = {
            (str(item[0]), str(item[1]))
            for item in raw.get("vwap_published", ())
            if isinstance(item, list) and len(item) == 2
        }
        latest_atr = raw.get("latest_atr")
        self.latest_atr = self._restore_atr(latest_atr)
        latest_value = raw.get("latest_prior_value")
        self.latest_prior_value = self._restore_value(latest_value)
        self.detector.restore_state(
            raw.get("detector", {}),
            source_candles={"1m": self._one_minute, "5m": self._five_minute},
        )

    @staticmethod
    def _restore_atr(raw: Any) -> AtrPublication | None:
        if not isinstance(raw, Mapping):
            return None
        return AtrPublication(
            value=float(raw["value"]) if raw.get("value") is not None else None,
            observed_at=(coerce_timestamp(raw["observed_at"]) if raw.get("observed_at") else None),
            available_at=(coerce_timestamp(raw["available_at"]) if raw.get("available_at") else None),
            source_observation_id=raw.get("source_observation_id"),
            lineage=tuple(raw.get("lineage", ())),
            unavailable_reason=raw.get("unavailable_reason"),
        )

    @staticmethod
    def _restore_value(raw: Any) -> ValuePublication | None:
        if not isinstance(raw, Mapping):
            return None
        reference = raw.get("reference")
        restored_reference = None
        if isinstance(reference, Mapping):
            restored_reference = PriorSessionValueReference(
                session_date=str(reference["session_date"]),
                value_area_low=float(reference["value_area_low"]),
                point_of_control=float(reference["point_of_control"]),
                value_area_high=float(reference["value_area_high"]),
                observed_at=coerce_timestamp(reference["observed_at"]),
                available_at=coerce_timestamp(reference["available_at"]),
                lineage=tuple(reference.get("lineage", ())),
            )
        return ValuePublication(
            reference=restored_reference,
            observed_at=(coerce_timestamp(raw["observed_at"]) if raw.get("observed_at") else None),
            available_at=(coerce_timestamp(raw["available_at"]) if raw.get("available_at") else None),
            lineage=tuple(raw.get("lineage", ())),
            unavailable_reason=raw.get("unavailable_reason"),
        )

    @property
    def five_minute_aggregates(self) -> tuple[Aggregate, ...]:
        """Completed 5m evidence available to the execution boundary."""
        return tuple(self._five_minute)

    @property
    def one_minute_aggregates(self) -> tuple[Aggregate, ...]:
        """Completed 1m source aggregates in causal order."""
        return tuple(self._one_minute)

    def ingest(self, item: OrderflowInput) -> OrderflowAdvance:
        """Ingest one completed source bar without evaluating interactions."""
        one = item.one_minute
        source_id = self.input_identity(item)
        if source_id in self._seen:
            return OrderflowAdvance()
        self._seen.add(source_id)
        self._one_minute.append(one)
        atr = item.atr.value if item.atr is not None else None
        if item.atr is not None:
            self.latest_atr = item.atr
        if item.prior_value is not None:
            self.latest_prior_value = item.prior_value
        if item.session_levels is not None and atr is not None:
            key = f"{one.session_date}:{item.session_levels}"
            if key not in self._levels_published:
                self.location_registry.publish_session_levels(
                    item.session_levels,
                    source_session=one.session_date,
                    prior_session=item.prior_session,
                    atr=atr,
                )
                self._levels_published.add(key)
        events: list[dict[str, Any]] = []
        if item.five_minute is not None:
            five = item.five_minute
            five_id = f"aggregate:{five.timeframe}:{nifty_label(five.time)}"
            if five_id not in {f"aggregate:5m:{nifty_label(bar.time)}" for bar in self._five_minute}:
                self._five_minute.append(five)
                if atr is not None:
                    self.location_registry.publish_session_extrema(
                        tuple(self._five_minute), session_date=five.session_date, atr=atr,
                    )
        if atr is not None and self._five_minute:
            for measurement in item.vwap_measurements:
                if measurement.available_at > one.available_at:
                    continue
                vwap_identity = (
                    measurement.timeframe,
                    measurement.source_aggregate_id,
                )
                if vwap_identity in self._vwap_published:
                    continue
                self.location_registry.publish_vwap(
                    value=measurement.value,
                    available_at=measurement.available_at,
                    source_session=one.session_date,
                    source_aggregate_id=measurement.source_aggregate_id,
                    source_observation_id=measurement.source_observation_id,
                    timeframe=measurement.timeframe,
                    atr=atr,
                )
                self._vwap_published.add(vwap_identity)
        return OrderflowAdvance(tuple(events), ())

    def observe_interactions(
        self,
        item: OrderflowInput,
        *,
        new_five_minute: bool,
        input_diagnostics: list[dict[str, Any]] | None = None,
    ) -> OrderflowAdvance:
        """Evaluate location interactions after permission/context processing."""
        one = item.one_minute
        atr = item.atr.value if item.atr is not None else None
        events: list[dict[str, Any]] = []
        current_observations_1m = tuple(
            observation
            for observation in item.observations_1m
            if observation.timeframe == "1m"
            and observation.source_aggregate_id == nifty_label(one.time)
        )
        visible_1m = tuple(
            location for location in self.location_registry.available_as_of(one.available_at)
            if not location.timeframe or location.timeframe == "1m"
        )
        if input_diagnostics is not None:
            input_diagnostics.append({
                "kind": "location_observation_input",
                "input_source": "canonical_location_registry",
                "timeframe": "1m",
                "aggregate_id": one.time,
                "available_at": one.available_at,
                "visible_location_records": len(visible_1m),
                "observations_emitted": len(current_observations_1m),
            })
        transitions: tuple[InteractionTransition, ...] = ()
        if new_five_minute:
            assert item.five_minute is not None
            five = item.five_minute
            current_observations_5m = tuple(
                observation
                for observation in item.observations_5m
                if observation.timeframe == "5m"
                and observation.source_aggregate_id == nifty_label(five.time)
            )
            visible_5m = tuple(
                location for location in self.location_registry.available_as_of(five.available_at)
                if not location.timeframe or location.timeframe == "5m"
            )
            if input_diagnostics is not None:
                input_diagnostics.append({
                    "kind": "location_observation_input",
                    "input_source": "canonical_location_registry",
                    "timeframe": "5m",
                    "aggregate_id": five.time,
                    "available_at": five.available_at,
                    "visible_location_records": len(visible_5m),
                    "observations_emitted": len(current_observations_5m),
                    "location_observation_gaps": max(
                        0,
                        len(visible_5m) - len({
                            (observation.location_id, observation.reference_generation)
                            for observation in current_observations_5m
                        }),
                    ),
                })
            valid_5m_keys = {
                (observation.location_id, observation.reference_generation)
                for observation in current_observations_5m
                if observation.timeframe == "5m"
            }
            current_5m_child_ids = {
                child_id
                for observation in current_observations_5m
                for child_id in observation.child_observation_ids
            }
            current_5m_observations_1m = tuple(
                observation for observation in item.observations_1m
                if observation.observation_id in current_5m_child_ids
            ) + current_observations_1m
            for location in self.location_registry.available_as_of(five.available_at):
                if location.timeframe and location.timeframe != "5m":
                    continue
                key = (location.location_id, location.generation)
                if key in valid_5m_keys:
                    continue
                events.append({
                    "kind": "location_observation_gap",
                    "identity": f"location_observation_gap:{nifty_label(five.time)}:{location.location_id or location.alias}:{location.generation}",
                    "causal_parent_id": f"aggregate:5m:{nifty_label(five.time)}",
                    "location": location.alias,
                    "location_id": location.location_id,
                    "reference_generation": location.generation,
                    "timeframe": "5m",
                    "source_aggregate_id": nifty_label(five.time),
                    "observed_at": five.end_time,
                    "available_at": five.available_at,
                    "reason": "incomplete_or_misaligned_1m_references",
                })
            transitions = tuple(self.detector.observe_1m(
                current=one,
                available=list(self._one_minute),
                atr=atr,
                observations=current_observations_1m,
            ))
            transitions += tuple(self.detector.observe(
                current=five,
                available5=list(self._five_minute),
                available1=list(self._one_minute),
                atr=atr,
                observations5=current_observations_5m,
                observations1=current_5m_observations_1m,
            ))
        else:
            transitions = tuple(self.detector.observe_1m(
                current=one,
                available=list(self._one_minute),
                atr=atr,
                observations=current_observations_1m,
            ))
        for transition in transitions:
            events.extend(self._transition_events(transition, one))
        return OrderflowAdvance(tuple(events), tuple(transitions))

    @staticmethod
    def _transition_events(transition: InteractionTransition, clock: Aggregate) -> tuple[dict[str, Any], ...]:
        snapshot = transition.interaction.snapshot()
        interaction_id = transition.interaction.interaction_id
        events: list[dict[str, Any]] = []
        observation = transition.observation
        provenance = observation
        if (
            observation is not None
            and transition.interaction.status.value == "ACTIVE"
        ):
            events.append({
                "kind": "location_interaction_observed",
                "reason": "location_interaction_observed",
                "decision_stage": "opportunity",
                "outcome": "observed",
                "decision_id": hashlib.sha256(
                    f"opportunity|{interaction_id}|{observation.observation_id}".encode()
                ).hexdigest()[:24],
                "interaction_id": interaction_id,
                "interaction_key": (
                    f"{observation.location.upper()}:{observation.timeframe}:"
                    f"{observation.reference_generation}"
                ),
                "location": observation.location,
                "timeframe": observation.timeframe,
                "reference_generation": observation.reference_generation,
                "source_observation_ids": [observation.observation_id],
                "observed_at": observation.observed_at,
                "available_at": observation.available_at,
                "feature_values": {
                    "open": observation.open,
                    "high": observation.high,
                    "low": observation.low,
                    "close": observation.close,
                },
            })
        if transition.created:
            events.append({
                "kind": "interaction_created",
                "interaction_id": interaction_id,
                "location": str(transition.interaction.location),
                "timeframe": transition.interaction.timeframe,
                "reference_generation": transition.interaction.reference_generation,
                "reference_level": transition.interaction.reference_level,
                "band_low": transition.interaction.band_low,
                "band_high": transition.interaction.band_high,
                "source_aggregate_id": (
                    provenance.reference_source_aggregate_id if provenance else None
                ),
                "source_observation_id": provenance.source_observation_id if provenance else None,
                "stage": "OBSERVING_LOCATION",
                "evidence": [transition.interaction.creation_candle.to_dict()],
                "observed_at": clock.end_time,
                "available_at": clock.available_at,
            })
        if transition.departed:
            provenance = transition.interaction.reference_provenance
            events.append({
                "kind": "interaction_ended",
                "interaction_id": interaction_id,
                "location": str(transition.interaction.location),
                "timeframe": transition.interaction.timeframe,
                "reference_generation": transition.interaction.reference_generation,
                "reason": snapshot.get("close_reason"),
                "source_observation_id": provenance.get("source_observation_id") if provenance else None,
                "observed_at": clock.end_time,
                "available_at": clock.available_at,
            })
        return tuple(events)


__all__ = ["OrderflowAdvance", "OrderflowInput", "OrderflowCoordinator"]
