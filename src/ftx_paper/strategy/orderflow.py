"""Diagnostic orderflow strategy for causal location observations."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import hashlib
import json
from typing import Any, cast

from ftx_paper.domain.decision_context import (
    CapitalSnapshot,
    DecisionContext,
    ExposureSnapshot,
    MarginSnapshot,
)
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.contracts import Instrument, MarketBar, MarketRole, OrderIntent, OrderRole, OrderSide
from ftx_paper.config import SESSION_EXIT_DEADLINE_TIME
from ftx_paper.market.bundles import DecisionBundle

from .coordinator import SharedDecisionCoordinator
from .orderflow_contract import FeatureValue, OrderflowObservation, feature_values_to_dict, stable_orderflow_id
from .orderflow_context import (
    ContextHistory, MarketContext, ContextVersionRef, OpeningGapState, SessionLevels,
    advance_opening_gap, classify_context_relation, classify_opening_gap,
)
from .orderflow_opportunity import (
    ExecutionGroupRegistry, OpportunityStatus, SetupOpportunity,
)
from .opportunity_timeout import advance_opportunity_timeout
from .orderflow_admission import evaluate_admission
from .orderflow_capture import CaptureMode
from .orderflow_interaction import EvidenceLineage, HypothesisState, InteractionCloseReason, InteractionDetector, InteractionStatus
from .orderflow_location import (
    LocationBand, LocationObservation, LocationRecord, LocationRegistry,
    is_diagnostic_location,
)
from .orderflow_response import evaluate_policy_responses
from .orderflow_scores import score_aggregate
from .orderflow_vwap import (
    MarketAggregate, VwapLocationProvider, calculate_session_vwap, calculate_vwap_stop,
    evaluate_vwap_diagnostic_response,
)
from .orderflow_trigger import observe_trigger
from .policy_contract import ExecutionMode, FeatureRequirement, PolicyDecision
from .protocol import StrategyMetadata
from .snapshot_envelope import unwrap_snapshot, wrap_snapshot
from .exits import ExitAction, ExitStateMachine, PositionState


def _instrument_snapshot(instrument: Instrument) -> dict[str, object]:
    return {
        "symbol": instrument.symbol,
        "exchange": instrument.exchange,
        "instrument_type": instrument.instrument_type,
        "expiry": instrument.expiry,
        "strike": instrument.strike,
        "option_type": instrument.option_type.value if instrument.option_type is not None else None,
    }


def _position_snapshot(position: PositionState) -> dict[str, object]:
    return {
        "instrument": _instrument_snapshot(position.instrument),
        "entry_price": position.entry_price,
        "stop_price": position.stop_price,
        "quantity": position.quantity,
        "side": position.side.value,
        "stop_policy": position.stop_policy,
        "cell": position.cell,
        "exit_mode": position.exit_mode,
        "entry_fill_time": position.entry_fill_time.isoformat() if position.entry_fill_time else None,
        "exit_reference_price": position.exit_reference_price,
        "target_price": position.target_price,
        "vehicle": position.vehicle,
        "synthetic_legs": [_instrument_snapshot(item) for item in position.synthetic_legs]
        if position.synthetic_legs else None,
        "entry_bar": position.entry_bar,
        "entry_order_id": position.entry_order_id,
        "initial_risk_points": position.initial_risk_points,
        "partial_exit_enabled": position.partial_exit_enabled,
        "stop_source": position.stop_source,
        "post_3r_activated": position.post_3r_activated,
    }


def _bar_snapshot(bar: MarketBar) -> dict[str, object]:
    return {
        "instrument": _instrument_snapshot(bar.instrument),
        "timestamp": bar.timestamp.isoformat(),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        "open_interest": bar.open_interest,
    }


def _bar_from_snapshot(raw: Mapping[str, object]) -> MarketBar:
    instrument = raw.get("instrument")
    if not isinstance(instrument, Mapping):
        raise ValueError("orderflow completed bar instrument is invalid")
    timestamp = raw.get("timestamp")
    if not isinstance(timestamp, str):
        raise ValueError("orderflow completed bar timestamp is invalid")
    values = tuple(raw.get(name) for name in ("open", "high", "low", "close"))
    if any(not isinstance(value, (int, float)) or isinstance(value, bool) for value in values):
        raise ValueError("orderflow completed bar prices are invalid")
    numeric_values = cast(tuple[int | float, int | float, int | float, int | float], values)
    open_price, high_price, low_price, close_price = (float(value) for value in numeric_values)
    volume_value = raw.get("volume")
    volume = (
        float(volume_value)
        if isinstance(volume_value, (int, float)) and not isinstance(volume_value, bool)
        else None
    )
    open_interest_value = raw.get("open_interest")
    open_interest = (
        float(open_interest_value)
        if isinstance(open_interest_value, (int, float)) and not isinstance(open_interest_value, bool)
        else None
    )
    return MarketBar(
        _instrument_from_snapshot(instrument), datetime.fromisoformat(timestamp),
        open_price, high_price, low_price, close_price,
        volume, open_interest,
    )


def _exit_action_snapshot(action: ExitAction) -> dict[str, object]:
    return {
        "reason": action.reason,
        "price": action.price,
        "cell": action.cell,
        "bars_held": action.bars_held,
        "mae_bp": action.mae_bp,
        "mfe_bp": action.mfe_bp,
        "intent": {
            "client_order_id": action.intent.client_order_id,
            "instrument": _instrument_snapshot(action.intent.instrument),
            "side": action.intent.side.value,
            "quantity": action.intent.quantity,
            "role": action.intent.role.value,
            "reason": action.intent.reason,
            "entry_order_id": action.intent.entry_order_id,
            "vehicle": action.intent.vehicle,
            "exit_mode": action.intent.exit_mode,
            "synthetic_legs": [_instrument_snapshot(item) for item in action.intent.synthetic_legs]
            if action.intent.synthetic_legs else None,
            "entry_bar": action.intent.entry_bar,
        },
    }


def _instrument_from_snapshot(raw: Mapping[str, object]) -> Instrument:
    from ftx_paper.contracts import OptionType

    option_type = raw.get("option_type")
    strike_value = raw.get("strike")
    strike = (
        float(strike_value)
        if isinstance(strike_value, (int, float)) and not isinstance(strike_value, bool)
        else None
    )
    return Instrument(
        str(raw["symbol"]), str(raw["exchange"]), str(raw["instrument_type"]),
        raw.get("expiry") if isinstance(raw.get("expiry"), str) else None,
        strike,
        OptionType(option_type) if isinstance(option_type, str) else None,
    )


def _position_from_snapshot(raw: Mapping[str, object]) -> PositionState:
    instrument = raw.get("instrument")
    if not isinstance(instrument, Mapping):
        raise ValueError("orderflow position instrument is invalid")
    legs_raw = raw.get("synthetic_legs")
    legs = None
    if isinstance(legs_raw, (list, tuple)):
        if len(legs_raw) != 2 or any(not isinstance(item, Mapping) for item in legs_raw):
            raise ValueError("orderflow synthetic legs are invalid")
        legs = (_instrument_from_snapshot(legs_raw[0]), _instrument_from_snapshot(legs_raw[1]))
    entry_fill_time = raw.get("entry_fill_time")
    if entry_fill_time is not None and not isinstance(entry_fill_time, str):
        raise ValueError("orderflow entry fill time is invalid")
    entry_price_value = raw.get("entry_price")
    stop_price_value = raw.get("stop_price")
    if (
        not isinstance(entry_price_value, (int, float))
        or isinstance(entry_price_value, bool)
        or not isinstance(stop_price_value, (int, float))
        or isinstance(stop_price_value, bool)
    ):
        raise ValueError("orderflow position prices are invalid")
    entry_price = float(entry_price_value)
    stop_price = float(stop_price_value)
    quantity_value = raw.get("quantity")
    if not isinstance(quantity_value, int) or isinstance(quantity_value, bool):
        raise ValueError("orderflow position quantity is invalid")
    exit_reference_value = raw.get("exit_reference_price")
    exit_reference_price = (
        float(exit_reference_value)
        if isinstance(exit_reference_value, (int, float)) and not isinstance(exit_reference_value, bool)
        else None
    )
    target_value = raw.get("target_price")
    target_price = (
        float(target_value)
        if isinstance(target_value, (int, float)) and not isinstance(target_value, bool)
        else None
    )
    initial_risk_value = raw.get("initial_risk_points")
    initial_risk_points = (
        float(initial_risk_value)
        if isinstance(initial_risk_value, (int, float)) and not isinstance(initial_risk_value, bool)
        else None
    )
    return PositionState(
        _instrument_from_snapshot(instrument), entry_price, stop_price,
        quantity_value, OrderSide(str(raw["side"])),
        stop_policy=raw.get("stop_policy") if isinstance(raw.get("stop_policy"), str) else None,
        cell=raw.get("cell") if isinstance(raw.get("cell"), str) else None,
        exit_mode=str(raw.get("exit_mode", "signal")),
        entry_fill_time=datetime.fromisoformat(entry_fill_time) if entry_fill_time else None,
        exit_reference_price=exit_reference_price,
        target_price=target_price,
        vehicle=str(raw.get("vehicle", "futures")), synthetic_legs=legs,
        entry_bar=int(raw["entry_bar"]) if isinstance(raw.get("entry_bar"), int) else None,
        entry_order_id=raw.get("entry_order_id") if isinstance(raw.get("entry_order_id"), str) else None,
        initial_risk_points=initial_risk_points,
        partial_exit_enabled=bool(raw.get("partial_exit_enabled", False)),
        stop_source=str(raw.get("stop_source", "initial")),
        post_3r_activated=bool(raw.get("post_3r_activated", False)),
    )


def _exit_action_from_snapshot(raw: Mapping[str, object]) -> ExitAction:
    intent_raw = raw.get("intent")
    if not isinstance(intent_raw, Mapping):
        raise ValueError("orderflow exit intent is invalid")
    instrument_raw = intent_raw.get("instrument")
    if not isinstance(instrument_raw, Mapping):
        raise ValueError("orderflow exit instrument is invalid")
    intent = OrderIntent(
        str(intent_raw["client_order_id"]), _instrument_from_snapshot(instrument_raw),
        OrderSide(str(intent_raw["side"])), int(intent_raw["quantity"]),
        role=OrderRole(str(intent_raw["role"])), reason=str(intent_raw.get("reason", "")),
        entry_order_id=intent_raw.get("entry_order_id") if isinstance(intent_raw.get("entry_order_id"), str) else None,
        vehicle=str(intent_raw.get("vehicle", "futures")), exit_mode=intent_raw.get("exit_mode") if isinstance(intent_raw.get("exit_mode"), str) else None,
        entry_bar=int(intent_raw["entry_bar"]) if isinstance(intent_raw.get("entry_bar"), int) else None,
    )
    return ExitAction(str(raw["reason"]), float(raw["price"]), intent,
                      raw.get("cell") if isinstance(raw.get("cell"), str) else None,
                      int(raw.get("bars_held", 0)), float(raw.get("mae_bp", 0.0)),
                      float(raw.get("mfe_bp", 0.0)))


ORDERFLOW_NAME = "ftx-paper-orderflow"
ORDERFLOW_VERSION = "0.1.0"


@dataclass(frozen=True, slots=True)
class OrderflowConfig:
    name: str = ORDERFLOW_NAME
    version: str = ORDERFLOW_VERSION
    enabled_timeframes: tuple[str, ...] = ("1m", "5m")
    session_windows: tuple[tuple[str, str], ...] = (("09:15", "15:30"),)
    response_timeframe: str = "5m"
    session_reset: str = "trading_date"
    response_type: str = "confirmed_rejection"
    response_window_candles: int = 3
    confirmation_rule: str = "completed_response_candle"
    invalidation_rule: str = "close_outside_frozen_band"
    trigger_type: str = "breakout"
    admission_policy_version: str = "diagnostic_v1"
    stop_policy: str = "shared_adaptive_stop"
    exit_policy: str = "shared_exit_state_machine"
    feature_set: tuple[str, ...] = ("close", "volume")
    enabled_vehicles: tuple[str, ...] = ("futures",)
    fill_permission_margin: float | None = 0.05
    execution_mode: ExecutionMode = ExecutionMode.DIAGNOSTIC
    timezone: str = "Asia/Kolkata"
    market_session: tuple[str, str] = ("09:15", "15:30")
    vehicle: str = "futures"
    replay_mode: str = "causal"
    capture_mode: CaptureMode = CaptureMode.FULL
    vwap_enabled: bool = True

    def __post_init__(self) -> None:
        if self.response_timeframe not in self.enabled_timeframes:
            raise ValueError("response timeframe must be enabled")
        if not self.feature_set or len(self.feature_set) != len(set(self.feature_set)):
            raise ValueError("orderflow feature_set must be non-empty and unique")
        if "futures" not in self.enabled_vehicles:
            raise ValueError("orderflow requires futures as the enabled vehicle")
        if self.response_window_candles <= 0:
            raise ValueError("response window must be positive")
        if self.timezone != "Asia/Kolkata":
            raise ValueError("orderflow currently requires the Asia/Kolkata timezone")
        if len(self.market_session) != 2 or self.market_session[0] >= self.market_session[1]:
            raise ValueError("market_session must be an increasing start/end pair")
        if self.vehicle != "futures":
            raise ValueError("orderflow currently supports futures only")
        if self.replay_mode != "causal":
            raise ValueError("orderflow currently requires causal replay mode")

    @property
    def config_hash(self) -> str:
        payload = asdict(self)
        payload["execution_mode"] = self.execution_mode.value
        payload["capture_mode"] = self.capture_mode.value
        return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _number(observation: OrderflowObservation, name: str) -> float | None:
    value = observation.features.get(name)
    return _feature_number(value)


def _feature_number(value: FeatureValue) -> float | None:
    """Convert an available numeric feature without masking unavailable values."""
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _volume_average(observations: list[OrderflowObservation]) -> float | None:
    values = [_feature_number(item.features.get("volume")) for item in observations]
    numeric_values = [value for value in values if value is not None]
    if len(numeric_values) != len(values):
        return None
    return sum(numeric_values) / len(numeric_values) if numeric_values else None


def _score_rejection(
    *, direction: str, long_score: float | None, short_score: float | None,
    margin: float | None,
) -> str | None:
    """Apply the canonical permission margin to a complete score pair."""
    if margin is None:
        return None
    if long_score is None or short_score is None:
        return "setup_scores_unavailable"
    own = long_score if direction == "long" else short_score
    opposing = short_score if direction == "long" else long_score
    return "fill_permission_mismatch" if own - opposing < margin else None


def opening_gap_context_available_at(observation: OrderflowObservation) -> datetime:
    """Opening-gap classification only consumes the first bar's open."""
    return observation.field_available_at("open")


def _decision_id(*parts: object) -> str:
    return hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()[:24]


class OrderflowPolicy:
    def __init__(self, config: OrderflowConfig = OrderflowConfig()) -> None:
        self.config = config
        self._session_id: str | None = None
        self._processed_observation_ids: set[str] = set()
        self._processed_location_observation_ids: set[str] = set()
        self._location_detectors = {timeframe: InteractionDetector(timeframe) for timeframe in ("1m", "5m")}
        self._location_detector = self._location_detectors["5m"]
        self._context_history = ContextHistory()
        self._location_registry = LocationRegistry()
        self._context_version = 0
        self._session_high: float | None = None
        self._session_low: float | None = None
        self._admission_suspended = False
        self._gap_end: datetime | None = None
        self._opening_gap_state = OpeningGapState("UNAVAILABLE")
        self._opening_gap_initialized = False
        self._opening_gap_observation_id: str | None = None
        self._opening_gap_last_evaluated_id: str | None = None
        self._opening_seed: dict[str, object] | None = None
        self._opportunities: dict[str, SetupOpportunity] = {}
        self._execution_groups = ExecutionGroupRegistry()
        self._vwap_provider = VwapLocationProvider()
        self._evaluation_observations: tuple[OrderflowObservation, ...] = ()
        self._evaluation_observation_ids: frozenset[str] = frozenset()
        self._evaluation_observation_indices: dict[str, int] = {}

    @property
    def execution_mode(self) -> ExecutionMode:
        return self.config.execution_mode

    @property
    def required_features(self) -> tuple[FeatureRequirement, ...]:
        return tuple(
            FeatureRequirement(name, timeframe=self.config.response_timeframe)
            for name in self.config.feature_set
        )

    def _confirmation_scores(
        self, observation: OrderflowObservation,
    ) -> tuple[float, float] | None:
        """Recompute canonical scores from the confirmation evidence boundary."""
        atr = next(
            (item.frozen_atr for item in observation.location_observations
             if item.frozen_atr is not None and item.frozen_atr > 0),
            None,
        )
        if atr is None:
            atr = next(
                (
                    location.frozen_atr
                    for item in self._evaluation_observations
                    if item.observed_at <= observation.observed_at
                    for location in item.location_observations
                    if location.frozen_atr is not None and location.frozen_atr > 0
                ),
                None,
            )
        if atr is None:
            return None
        all_observations = self._evaluation_observations
        children_by_id = {
            item.observation_id: item for item in all_observations
        }
        child_ids = observation.context.child_observation_ids if observation.context else ()
        children = tuple(
            children_by_id[item_id] for item_id in child_ids if item_id in children_by_id
        )
        if observation.timeframe == "5m" and len(children) != len(child_ids):
            return None
        child_scores: list[tuple[float, float]] = []
        for child in children:
            prior = next(
                (item for item in reversed(all_observations)
                 if item.timeframe == child.timeframe and item.observed_at < child.observed_at),
                None,
            )
            prior_items = [
                item for item in all_observations
                if item.timeframe == child.timeframe and item.observed_at < child.observed_at
            ][-20:]
            try:
                score = score_aggregate(
                    child.features,
                    prior.features if prior is not None else None,
                    atr=atr,
                    volume_average=_volume_average(prior_items),
                )
            except ValueError:
                return None
            if score is None:
                return None
            child_scores.append(score)
        prior_parent = next(
            (item for item in reversed(all_observations)
             if item.timeframe == observation.timeframe
             and item.observed_at < observation.observed_at),
            None,
        )
        prior_parents = [
            item for item in all_observations
            if item.timeframe == observation.timeframe
            and item.observed_at < observation.observed_at
        ][-20:]
        try:
            return score_aggregate(
                observation.features,
                prior_parent.features if prior_parent is not None else None,
                atr=atr,
                child_scores=tuple(child_scores),
                volume_average=_volume_average(prior_parents),
            )
        except ValueError:
            return None

    def evaluate(self, bundle: DecisionBundle, context: DecisionContext) -> tuple[PolicyDecision, ...]:
        orderflow = bundle.orderflow_context
        base: dict[str, Any] = {
            "strategy_id": self.config.name,
            "strategy_name": self.config.name,
            "strategy_version": self.config.version,
            "config_hash": self.config.config_hash,
            "bundle_id": bundle.bundle_id,
            "decision_at": context.as_of.isoformat(),
            "available_at": context.as_of.isoformat(),
            "availability_cutoff": orderflow.cutoff.isoformat() if orderflow else None,
            "decision_stage": "trigger",
            "cell": "orderflow",
            "subject": {"vehicle": "futures", "location": "orderflow"},
            "policy": {},
            "evidence": {"metrics": {}, "state": {}, "provenance": {}},
        }
        if orderflow is None or orderflow.session_id != bundle.trading_date:
            return (PolicyDecision({**base, "outcome": "unavailable", "reason": "orderflow_context_unavailable",
                                     "decision_id": _decision_id(
                                         "unavailable", bundle.trading_date, "context", bundle.bundle_id,
                                     )}),)
        session_transitions: list[PolicyDecision] = []
        if self._session_id != orderflow.session_id:
            boundary = context.as_of
            session_transitions = [
                PolicyDecision({
                    **base,
                    "outcome": "rejected",
                    "reason": InteractionCloseReason.SESSION_END.value,
                    "decision_id": _decision_id("lifecycle", item.interaction_id, "session_end"),
                    "interaction_id": item.interaction_id,
                    "location": item.location_alias,
                    "reference_generation": item.reference_generation,
                    "interaction_state": InteractionStatus.ENDED.value,
                    "decision_stage": "lifecycle",
                })
                for detector in self._location_detectors.values()
                for item in detector.close_all(InteractionCloseReason.SESSION_END, boundary)
            ]
            # Registry generations are advanced once at the authoritative
            # session boundary; carried references remain immutable evidence.
            self._location_registry.remove_diagnostic_locations()
            self._location_registry.next_generation(at=boundary, carry=self._session_id is not None)
            self._processed_observation_ids.clear()
            self._processed_location_observation_ids.clear()
            self._location_detectors = {timeframe: InteractionDetector(timeframe) for timeframe in ("1m", "5m")}
            self._location_detector = self._location_detectors["5m"]
            self._context_history = ContextHistory()
            if self._session_id is None:
                self._location_registry = LocationRegistry()
            self._context_version = 0
            self._session_high = None
            self._session_low = None
            self._admission_suspended = False
            self._gap_end = None
            self._opening_gap_state = OpeningGapState("UNAVAILABLE")
            self._opening_gap_initialized = False
            self._opening_gap_observation_id = None
            self._opening_gap_last_evaluated_id = None
            self._opening_seed = None
            self._opportunities.clear()
            self._session_id = orderflow.session_id

        incoming_observations = tuple(sorted(
            (*orderflow.observations_for("1m"), *orderflow.observations_for("5m")),
            key=lambda item: (item.available_at, item.timeframe, item.observed_at, item.observation_id),
        ))
        incoming_ids = frozenset(
            observation_id
            for item in incoming_observations
            for observation_id in (
                item.observation_id,
                *(location.observation_id for location in item.location_observations),
            )
        )
        if incoming_ids != self._evaluation_observation_ids:
            self._evaluation_observations = incoming_observations
            self._evaluation_observation_ids = incoming_ids
            self._evaluation_observation_indices = {
                item.observation_id: index
                for index, item in enumerate(incoming_observations)
            }
        observations = self._evaluation_observations
        vwap_locations: tuple[LocationRecord, ...] = ()
        has_causal_5m = any(
            item.timeframe == "5m"
            and item.available_at <= min(context.as_of, orderflow.cutoff)
            for item in observations
        )
        if self.config.vwap_enabled and has_causal_5m:
            vwap_measurements = self._vwap_provider.publish(
                cast(tuple[MarketAggregate, ...], tuple(
                    item for item in observations
                    if item.context is not None
                    and item.context.replay_provenance.get("producer") == "ftx-paper"
                )),
                cutoff=min(context.as_of, orderflow.cutoff),
            )
            atr = next(
                (
                    location.frozen_atr
                    for item in observations
                    for location in item.location_observations
                    if location.location_alias.upper() != "VWAP"
                    and location.frozen_atr is not None
                    and location.frozen_atr > 0
                ),
                None,
            )
            vwap_locations = self._location_registry.publish_measurements(
                vwap_measurements, atr=atr,
                band_fraction=self._vwap_provider.config.band_atr_fraction,
            )
        locations_by_source: dict[str, list[LocationRecord]] = {}
        for location in vwap_locations:
            locations_by_source.setdefault(location.source_aggregate_id, []).append(location)
        if locations_by_source:
            def numeric(item: OrderflowObservation, name: str) -> float:
                value = _number(item, name)
                if value is None:
                    raise ValueError(f"VWAP location requires numeric {name!r} evidence")
                return value

            enriched = list(observations)
            for source_id, locations in locations_by_source.items():
                index = self._evaluation_observation_indices.get(source_id)
                if index is None:
                    continue
                item = enriched[index]
                enriched[index] = replace(
                    item,
                    location_observations=item.location_observations + tuple(
                        LocationObservation(
                            location.location_id, location.alias, location.generation, location.price,
                            location.band.lower, location.band.upper,
                            numeric(item, "open"), numeric(item, "high"),
                            numeric(item, "low"), numeric(item, "close"),
                            location.band.contains(numeric(item, "high"))
                            and location.band.contains(numeric(item, "low")),
                            item.observation_id, item.observed_at, item.available_at,
                            stable_orderflow_id("location_observation", item.observation_id, location.location_id),
                            location.location_type,
                            location.source_observation_id,
                            timeframe=item.timeframe,
                            child_observation_ids=(
                                item.context.child_observation_ids
                                if item.timeframe == "5m" and item.context is not None else ()
                            ),
                            method="vwap_provider",
                            reference_source_aggregate_id=location.source_aggregate_id,
                            reference_source_timeframe=location.timeframe or item.timeframe,
                        )
                        for location in locations
                    ),
                )
            observations = tuple(enriched)
            self._evaluation_observations = observations
        if not observations:
            return (*session_transitions, PolicyDecision({**base, "outcome": "unavailable", "reason": "required_features_unavailable",
                                     "decision_id": _decision_id(
                                         "unavailable", orderflow.session_id, "features", bundle.bundle_id,
                                     )}))

        results: list[PolicyDecision] = list(session_transitions)
        futures = bundle.bars.get(MarketRole.FUTURES)
        expected_instrument = futures.instrument.symbol if futures is not None else None
        causal_cutoff = min(context.as_of, orderflow.cutoff)

        def is_causally_available(item: OrderflowObservation) -> bool:
            return (
                (expected_instrument is None or item.instrument == expected_instrument)
                and item.available_at <= causal_cutoff
            )

        # Publish the opening-gap fact from the first causally available 1m
        # observation.  Waiting for the first 5m parent changes the strategy's
        # permission clock and drops the producer fact entirely.
        if not self._opening_gap_initialized:
            first_1m = next(
                (
                    item for item in observations
                    if item.timeframe == "1m" and is_causally_available(item)
                ),
                None,
            )
            if first_1m is not None:
                opening = _number(first_1m, "open")
                previous_close = _number(first_1m, "previous_close")
                if opening is not None and previous_close is not None:
                    prior_high = _number(first_1m, "prior_value_high")
                    prior_low = _number(first_1m, "prior_value_low")
                    reference = (
                        prior_low if prior_low is not None and opening < prior_low
                        else prior_high if prior_high is not None and opening > prior_high
                        else previous_close
                    )
                    reference_id = (
                        "PD_LOW" if prior_low is not None and opening < prior_low
                        else "PD_HIGH" if prior_high is not None and opening > prior_high
                        else None
                    )
                    classified_gap = classify_opening_gap(
                        opening_price=opening,
                        prior_close=reference,
                        observed_at=first_1m.observed_at,
                        reference_id=reference_id,
                    )
                    self._opening_gap_state = OpeningGapState(
                        "GAP_CANDIDATE" if classified_gap.status in {"UP", "DOWN"}
                        else classified_gap.status,
                        classified_gap.gap_points, classified_gap.reference_price,
                        classified_gap.observed_at, (first_1m.observation_id,),
                        reference_id=classified_gap.reference_id,
                    )
                    self._opening_gap_observation_id = first_1m.observation_id
                    context_available_at = opening_gap_context_available_at(first_1m)
                    results.append(PolicyDecision({
                         **base,
                         "observed_at": first_1m.observed_at.isoformat(),
                         "decision_at": context_available_at.isoformat(),
                         "available_at": context_available_at.isoformat(),
                        "outcome": "recorded",
                        "decision_stage": "interaction",
                        "reason": "opening_gap_context",
                        "decision_id": _decision_id("opening_gap_context", first_1m.observation_id),
                        "direction": "down" if (self._opening_gap_state.gap_points or 0) < 0 else "up" if (self._opening_gap_state.gap_points or 0) > 0 else "none",
                        "gap_points": self._opening_gap_state.gap_points,
                        "reference_level": self._opening_gap_state.reference_price,
                        "source_observation_ids": [first_1m.observation_id],
                    }))
                    self._opening_gap_initialized = True

        next_opening_gap_1m = min(
            (
                item for item in observations
                if item.timeframe == "1m"
                and is_causally_available(item)
                and item.observation_id != self._opening_gap_observation_id
                and item.observation_id != self._opening_gap_last_evaluated_id
            ),
            key=lambda item: (item.available_at, item.observed_at, item.observation_id),
            default=None,
        )
        first_causal_5m = next(
            (
                item for item in observations
                if item.timeframe == "5m"
                and is_causally_available(item)
                and (
                    item.context is None
                    or item.context.session_id == orderflow.session_id
                )
            ),
            None,
        )
        ordered_observations = sorted(
            observations,
            key=lambda item: (
                0 if item.timeframe == "5m" else 1,
                item.available_at,
                item.observed_at,
                item.observation_id,
            ),
        )
        for observation in ordered_observations:
            already_processed = observation.observation_id in self._processed_observation_ids
            if (
                already_processed
                and not any(
                    location.observation_id not in self._processed_location_observation_ids
                    for location in observation.location_observations
                )
            ):
                continue
            if observation.timeframe not in self._location_detectors:
                continue
            if expected_instrument is not None and observation.instrument != expected_instrument:
                continue
            if observation.observed_at > causal_cutoff or observation.available_at > causal_cutoff:
                continue
            if (
                observation.timeframe == "1m"
                and self._opening_gap_initialized
                and next_opening_gap_1m is not None
                and observation.observation_id == next_opening_gap_1m.observation_id
            ):
                self._opening_gap_last_evaluated_id = observation.observation_id
                advanced_gap = advance_opening_gap(
                    self._opening_gap_state,
                    close=_number(observation, "close"),
                    available_at=observation.available_at,
                    evidence_id=observation.observation_id,
                    complete=(observation.context is not None and observation.context.completeness == "complete"),
                )
                previous_gap = self._opening_gap_state
                self._opening_gap_state = advanced_gap
                if advanced_gap.status != previous_gap.status:
                    transition_at = observation.observed_at.isoformat()
                    results.append(PolicyDecision({
                        **base,
                        "observed_at": observation.observed_at.isoformat(),
                        "decision_at": transition_at,
                        "available_at": transition_at,
                        "outcome": "recorded",
                        "decision_stage": "interaction",
                        "reason": "opening_gap_transition",
                        "decision_id": _decision_id("opening_gap_transition", observation.observation_id),
                        "from_state": previous_gap.status,
                        "state": advanced_gap.status,
                        "reference": advanced_gap.reference_id,
                        "reference_level": advanced_gap.reference_price,
                        "source_observation_ids": [observation.observation_id],
                    }))
                    results.append(PolicyDecision({
                        **base,
                        "decision_at": transition_at,
                        "available_at": transition_at,
                        "outcome": "recorded",
                        "decision_stage": "interaction",
                        "reason": "opening_gap_permission",
                        "decision_id": _decision_id("opening_gap_permission", observation.observation_id),
                        "direction": "down" if (advanced_gap.gap_points or 0) < 0 else "up",
                        "state": advanced_gap.status,
                        "reference": advanced_gap.reference_id,
                        "source_observation_ids": [observation.observation_id],
                    }))
            if observation.context is None or observation.context.session_id != orderflow.session_id:
                continue
            if (
                observation.timeframe == "1m"
                and self._opening_seed is not None
                and self._opening_seed.get("attached_observation_id") is None
                and observation.observation_id != self._opening_gap_observation_id
            ):
                seed = self._opening_seed
                formation_available_at = seed.get("formation_available_at")
                if isinstance(formation_available_at, str):
                    formation_available = datetime.fromisoformat(
                        formation_available_at.replace("Z", "+00:00")
                    )
                    if observation.available_at <= formation_available:
                        continue
                raw_boundary = seed.get("reference_level")
                if not isinstance(raw_boundary, (int, float)) or isinstance(raw_boundary, bool):
                    continue
                boundary = float(raw_boundary)
                close = _number(observation, "close")
                high = _number(observation, "high")
                low = _number(observation, "low")
                if close is not None and high is not None and low is not None:
                    direction = str(seed["direction"])
                    crossed = (
                        close >= boundary if direction == "long" else close <= boundary
                    )
                    touched = low <= boundary <= high
                    opening_policy = str(seed.get("opening_policy", ""))
                    if not touched and not crossed:
                        continue
                    if opening_policy == "opening_breakout" and not crossed:
                        continue
                    # Canonical retest seeds first record a rejection/touch;
                    # they only become a retest after later recovery evidence.
                    opening_kind = (
                        "opening_breakout"
                        if opening_policy == "opening_breakout"
                        else "opening_rejection"
                    )
                    self._opening_seed = {
                        **seed,
                        "attached_observation_id": observation.observation_id,
                        "opening_interaction_kind": opening_kind,
                    }
                    results.append(PolicyDecision({
                        **base,
                        "decision_stage": "interaction",
                        "outcome": "recorded",
                        "reason": "opening_interaction_attached",
                        "decision_id": _decision_id(
                            "opening_interaction", str(seed["seed_id"]), observation.observation_id,
                        ),
                        "candidate_provenance": "opening_hypothesis",
                        "interaction_id": str(seed["seed_id"]),
                        "opening_interaction_kind": opening_kind,
                        "location": seed["location"],
                        "direction": direction,
                        "reference_level": boundary,
                            "reference_generation": seed["reference_generation"],
                            "source_observation_ids": [observation.observation_id],
                            "available_at": observation.available_at.isoformat(),
                    }))
            gap_status = observation.context.replay_provenance.get("gap_status")
            if observation.timeframe == "5m" and not already_processed:
                self._context_version += 1
                high = observation.features.get("high")
                low = observation.features.get("low")
                previous_high = self._session_high
                previous_low = self._session_low
                if isinstance(high, (int, float)):
                    self._session_high = max(self._session_high or float(high), float(high))
                if isinstance(low, (int, float)):
                    self._session_low = min(self._session_low or float(low), float(low))
                for extreme, previous, current in (
                    ("high", previous_high, self._session_high),
                    ("low", previous_low, self._session_low),
                ):
                    if (
                        observation.context.replay_provenance.get("producer") != "ftx-paper"
                        or current is None or current == previous
                    ):
                        continue
                    results.append(PolicyDecision({
                        **base,
                        "observed_at": observation.observed_at.isoformat(),
                        # Extreme registration is decided when the completed
                        # parent observation is available, not at its final
                        # minute before the availability boundary.
                        "decision_at": observation.available_at.isoformat(),
                        "available_at": observation.available_at.isoformat(),
                        "decision_stage": "candidate",
                        "outcome": "recorded",
                        "reason": "session_extreme_registered",
                        "decision_id": _decision_id("session_extreme", extreme, observation.observation_id),
                        "extreme": extreme,
                        "value": current,
                        "source_observation_ids": [observation.observation_id],
                    }))
                if observation.available_at > causal_cutoff:
                    continue
                level_values = {
                    item.location_alias.upper(): item.reference
                    for item in observation.location_observations
                    if not is_diagnostic_location(item.location_type)
                }
                opening_gap = classify_opening_gap(
                    opening_price=_number(observation, "open"),
                    prior_close=_number(observation, "previous_close"),
                    observed_at=observation.available_at,
                )
                context_gap = (
                    self._opening_gap_state
                    if self._opening_gap_state.status != "UNAVAILABLE" else opening_gap
                )
                parent_context = MarketContext(
                    context_id=f"context:{orderflow.session_id}",
                    version=self._context_version,
                    available_at=observation.available_at,
                    levels=SessionLevels(
                        orderflow.session_id,
                        level_values.get("OR_HIGH"),
                        level_values.get("OR_LOW"),
                        level_values.get("PD_HIGH"),
                        level_values.get("PD_LOW"),
                        "PD_HIGH" in level_values and "PD_LOW" in level_values,
                        observation.available_at,
                        source_ids=(observation.observation_id,),
                    ),
                    opening_gap=context_gap if context_gap.status != "UNAVAILABLE" else OpeningGapState(
                        "GAP_CANDIDATE" if gap_status == "source_gap" else "UNAVAILABLE",
                        observed_at=observation.available_at,
                        evidence_ids=(observation.observation_id,),
                    ),
                    permission=(
                        "UNAVAILABLE" if gap_status == "source_gap"
                        else "SHORT" if (context_gap.status == "DOWN" or (context_gap.gap_points or 0) < 0)
                        else "LONG" if (context_gap.status == "UP" or (context_gap.gap_points or 0) > 0)
                        else "BOTH"
                    ),
                    long_score=_number(observation, "long_score"),
                    short_score=_number(observation, "short_score"),
                    evidence_ids=(observation.observation_id,),
                    evidence=({
                        "completed_bar": True,
                        "observation_id": observation.observation_id,
                        "long_score": _number(observation, "long_score"),
                        "short_score": _number(observation, "short_score"),
                    },),
                    source_provenance=({
                        "source_observation_id": observation.observation_id,
                        "score_source_observation_id": observation.observation_id,
                    },),
                )
                self._context_history.publish(parent_context)
                parent_reference = ContextVersionRef(parent_context.context_id, parent_context.version)
                for location_observation in observation.location_observations:
                    self._context_version += 1
                    self._context_history.publish(MarketContext(
                        context_id=f"interaction:{location_observation.location_alias}:{orderflow.session_id}",
                        version=self._context_version,
                        available_at=observation.available_at,
                        levels=parent_context.levels,
                        opening_gap=parent_context.opening_gap,
                        permission=parent_context.permission,
                        long_score=parent_context.long_score,
                        short_score=parent_context.short_score,
                        evidence_ids=(observation.observation_id, location_observation.observation_id),
                        value_reference=parent_context.value_reference,
                        location=location_observation.location_alias,
                        parent_context_id=parent_context.context_id,
                        evidence=({"location_observation_id": location_observation.observation_id},),
                        source_provenance=({
                            "source_observation_id": observation.observation_id,
                            "source_aggregate_id": location_observation.source_aggregate_id,
                            "parent_context": asdict(parent_reference),
                        },),
                    ))
                active_interactions = {
                    item.interaction_id: item
                    for detector in self._location_detectors.values()
                    for item in detector.active_interactions
                }
                for opportunity_id, opportunity in tuple(self._opportunities.items()):
                    if opportunity.status is not OpportunityStatus.ACTIVE:
                        continue
                    source = active_interactions.get(opportunity.source_interaction_id or "")
                    if source is None:
                        continue
                    decision_context = self._context_history.as_of_location(
                        source.location_alias, observation.available_at,
                    )
                    if decision_context is not None:
                        reference = ContextVersionRef(
                            decision_context.context_id, decision_context.version,
                        )
                        if opportunity.decision_context != reference:
                            self._opportunities[opportunity_id] = replace(
                                opportunity, decision_context=reference,
                            )
            if gap_status == "source_gap":
                self._admission_suspended = True
                self._gap_end = observation.interval_end or observation.available_at
                for opportunity_id, opportunity in tuple(self._opportunities.items()):
                    if opportunity.status is not OpportunityStatus.ACTIVE:
                        continue
                    invalidated = opportunity.invalidate(
                        at=observation.available_at,
                        reason="source_gap",
                        evidence_ids=(observation.observation_id,),
                    )
                    self._opportunities[opportunity_id] = invalidated
                    results.append(PolicyDecision({
                        **base,
                        "decision_stage": "lifecycle",
                        "outcome": "rejected",
                        "reason": "opportunity_invalidated_by_source_gap",
                        "decision_id": _decision_id(
                            "lifecycle", opportunity.lifecycle_id,
                            observation.observation_id, "source_gap",
                        ),
                        "opportunity_id": opportunity.lifecycle_id,
                        "setup_id": opportunity.setup.setup_id,
                        "source_observation_ids": [observation.observation_id],
                    }))
                for interaction in self._location_detectors[observation.timeframe].observe_gap(observation.available_at):
                    results.append(PolicyDecision({
                        **base,
                        "decision_stage": "lifecycle",
                        "outcome": "rejected",
                        "reason": InteractionCloseReason.DATA_GAP.value,
                        "decision_id": _decision_id(
                            "lifecycle", interaction.interaction_id,
                            observation.observation_id, "gap",
                        ),
                        "location": interaction.location_alias,
                        "interaction_id": interaction.interaction_id,
                        "timeframe": observation.timeframe,
                        "reference_generation": interaction.reference_generation,
                        "interaction_state": InteractionStatus.ENDED.value,
                        "source_observation_ids": [observation.observation_id],
                    }))
            elif (
                self._admission_suspended
                and gap_status == "none"
                and observation.timeframe == "5m"
                and self._gap_end is not None
                and observation.available_at > self._gap_end
            ):
                self._admission_suspended = False
                self._gap_end = None
            location_observations = observation.location_observations
            if observation.timeframe == "5m" and not orderflow.validate_complete_parent(observation):
                if location_observations:
                    raise ValueError("5m location observation parent is incomplete")
                continue
            opening_parent = (
                first_causal_5m is not None
                and observation.observation_id == first_causal_5m.observation_id
            )
            if opening_parent:
                opening_close = _number(observation, "close")
                opening_open = _number(observation, "open")
                if opening_close is not None and opening_open is not None and opening_close != opening_open:
                    direction = "short" if opening_close < opening_open else "long"
                    long_score = _number(observation, "long_score")
                    short_score = _number(observation, "short_score")
                    own_score = short_score if direction == "short" else long_score
                    opposing_score = long_score if direction == "short" else short_score
                    opening_alias = "OR_LOW" if direction == "short" else "OR_HIGH"
                    seed_location = next(
                        (item for item in location_observations
                         if item.location_alias.upper() == opening_alias),
                        None,
                    )
                    if (
                        seed_location is not None
                        and self._opening_seed is None
                        and own_score is not None
                        and opposing_score is not None
                        and own_score > opposing_score
                    ):
                        seed_id = _decision_id(
                            "opening_seed", orderflow.session_id,
                            observation.observation_id, opening_alias, direction,
                        )
                        self._opening_seed = {
                            "seed_id": seed_id,
                            "direction": direction,
                            "location": opening_alias,
                            "reference_level": seed_location.reference,
                            "reference_generation": seed_location.reference_generation,
                            "source_observation_id": observation.observation_id,
                            "attached_observation_id": None,
                            "formation_available_at": observation.available_at.isoformat(),
                            "opening_policy": (
                                "opening_retest"
                                if (
                                    direction == "short" and opening_close <= seed_location.reference
                                ) or (
                                    direction == "long" and opening_close >= seed_location.reference
                                )
                                else "opening_breakout"
                            ),
                        }
                        results.append(PolicyDecision({
                            **base,
                            "decision_stage": "candidate",
                            "outcome": "recorded",
                            "reason": "opening_hypothesis",
                            "decision_id": seed_id,
                            "candidate_provenance": "opening_hypothesis",
                            "direction": direction,
                            "location": opening_alias,
                            "reference_level": seed_location.reference,
                            "reference_generation": seed_location.reference_generation,
                            "source_observation_ids": [observation.observation_id],
                            "long_score": long_score,
                            "short_score": short_score,
                            "available_at": observation.available_at.isoformat(),
                        }))
                if opening_parent:
                    # The first 5m OR candle seeds the opening hypothesis; it
                    # is not an ordinary OR interaction.
                    location_observations = tuple(
                        item for item in location_observations
                        if item.location_alias.upper() not in {"OR_HIGH", "OR_LOW"}
                    )
            self._processed_observation_ids.add(observation.observation_id)
            interaction_context_events: list[PolicyDecision] = []
            interaction_created_events: list[PolicyDecision] = []
            for location_observation in sorted(
                location_observations,
                key=lambda item: (is_diagnostic_location(item.location_type), item.location_alias),
            ):
                if not isinstance(location_observation, LocationObservation):
                    raise ValueError("invalid causal location observation")
                if (
                    location_observation.timeframe
                    and location_observation.timeframe != observation.timeframe
                ):
                    raise ValueError("location observation timeframe does not match parent")
                if observation.timeframe == "5m":
                    parent_context = observation.context
                    if parent_context is None or (
                        location_observation.child_observation_ids
                        != parent_context.child_observation_ids
                    ):
                        raise ValueError("5m location observation lineage does not match parent")
                location_key = location_observation.observation_id
                if location_key in self._processed_location_observation_ids:
                    continue
                self._processed_location_observation_ids.add(location_key)
                location = LocationRecord(
                    location_observation.location_id,
                    location_observation.location_alias,
                    location_observation.reference,
                    LocationBand(location_observation.band_lower, location_observation.band_upper),
                    location_observation.reference_generation,
                    location_observation.available_at,
                    location_observation.reference_source_aggregate_id
                    or location_observation.source_aggregate_id,
                    timeframe=observation.timeframe,
                    source_identity=observation.instrument,
                    location_type=location_observation.location_type,
                    source_observation_id=location_observation.source_observation_id,
                    frozen_atr=location_observation.frozen_atr,
                )
                if not any(item.location_id == location.location_id for item in self._location_registry.records):
                    self._location_registry.publish(location)
                    if observation.context.replay_provenance.get("producer") == "ftx-paper":
                        lifecycle_values = {
                            "location": location.alias,
                            "location_id": location.location_id,
                            "reference_generation": location.generation,
                            "reference_level": location.price,
                            "timeframe": observation.timeframe,
                            "source_aggregate_id": location.source_aggregate_id,
                            "source_observation_ids": [location_key],
                        }
                        lifecycle_at = location.available_at
                        interaction_context_events.append(PolicyDecision({
                                **base,
                                "decision_at": lifecycle_at.isoformat(),
                                "available_at": location.available_at.isoformat(),
                                "decision_stage": "interaction",
                                "outcome": "recorded",
                                "reason": "interaction_context_published",
                                "decision_id": _decision_id("interaction_context", location.location_id, location.generation),
                                **lifecycle_values,
                            }))
                        interaction_created_events.append(PolicyDecision({
                                **base,
                                "decision_at": lifecycle_at.isoformat(),
                                "available_at": location.available_at.isoformat(),
                                "decision_stage": "interaction",
                                "outcome": "recorded",
                                "reason": "interaction_created",
                                "decision_id": _decision_id("interaction_created", location.location_id, location.generation),
                                **lifecycle_values,
                            }))
                detector = self._location_detectors[observation.timeframe]
                closed_before = len(detector.closed_interactions)
                interaction = detector.observe(
                    location,
                    observation,
                    observation_id=location_observation.observation_id,
                )
                for ended in detector.closed_interactions[closed_before:]:
                    results.append(PolicyDecision({
                        **base,
                        "decision_stage": "lifecycle",
                        "outcome": "rejected",
                        "reason": ended.close_reason.value if ended.close_reason else "interaction_ended",
                        "decision_id": _decision_id(
                            "lifecycle", ended.interaction_id, location_key, "ended",
                        ),
                        "location": ended.location_alias,
                        "interaction_id": ended.interaction_id,
                        "reference_generation": ended.reference_generation,
                        "response_state": "ENDED",
                        "source_observation_ids": [location_key],
                    }))
                    if ended.close_reason is None:
                        raise RuntimeError("ended interaction is missing close reason")
                    for opportunity_id, opportunity in tuple(self._opportunities.items()):
                        if (
                            opportunity.status is OpportunityStatus.ACTIVE
                            and opportunity.source_interaction_id == ended.interaction_id
                        ):
                            invalidated = opportunity.invalidate(
                                at=ended.closed_at or observation.available_at,
                                reason=f"source_interaction_{ended.close_reason.value.lower()}",
                                evidence_ids=(location_key,),
                            )
                            self._opportunities[opportunity_id] = invalidated
                            results.append(PolicyDecision({
                                **base,
                                "decision_stage": "opportunity",
                                "outcome": "rejected",
                                "reason": invalidated.transitions[-1].reason,
                                "decision_id": _decision_id(
                                    "opportunity_invalidated", opportunity_id, location_key,
                                ),
                                "opportunity_id": opportunity_id,
                                "source_observation_ids": [location_key],
                            }))
                if interaction is not None and interaction.status is InteractionStatus.ACTIVE:
                    results.append(PolicyDecision({
                        **base,
                        "decision_stage": "opportunity",
                        "outcome": "recorded",
                        "reason": "location_interaction_observed",
                        "decision_id": _decision_id("opportunity", interaction.interaction_id, location_key),
                        "location": interaction.location_alias,
                        "interaction_id": interaction.interaction_id,
                        "timeframe": observation.timeframe,
                        "reference_generation": interaction.reference_generation,
                        "source_observation_ids": [location_key],
                        "observed_at": location_observation.observed_at.isoformat(),
                         "available_at": location_observation.available_at.isoformat(),
                      "feature_values": feature_values_to_dict(observation.features),
                       }))
                    if is_diagnostic_location(interaction.location_type):
                        diagnostic = evaluate_vwap_diagnostic_response(
                            interaction, interaction.contact_candles,
                        )
                        results.append(PolicyDecision({
                            **base,
                            "decision_stage": "interaction",
                            "outcome": "recorded",
                            "reason": "vwap_diagnostic_response",
                            "decision_id": _decision_id(
                                "vwap_diagnostic", interaction.interaction_id, location_key,
                            ),
                             **diagnostic,
                         }))
            results.extend(interaction_context_events)
            results.extend(interaction_created_events)
            # A 5m interaction owns projections from both source timeframes.
            # Advance them before timeout and response evaluation so every
            # consumer sees the same causally available cumulative snapshots.
            if observation.timeframe == "5m":
                projection_sources = {
                    timeframe: tuple(
                        item for item in observations
                        if item.timeframe == timeframe
                        and item.available_at <= observation.available_at
                    )
                    for timeframe in ("1m", "5m")
                }
                detector5 = self._location_detectors["5m"]
                detector5.advance_projections(projection_sources, as_of=observation.available_at)
                for candidate in detector5.active_interactions:
                    snapshots = dict(candidate.synthetic_snapshots).get("5m", ())
                    if not snapshots:
                        continue
                    long_score = _number(observation, "long_score")
                    short_score = _number(observation, "short_score")
                    if long_score is not None or short_score is not None:
                        detector5.record_score(
                            candidate.interaction_id, snapshot=snapshots[-1],
                            context_version=observation.context.generation if observation.context else None,
                            long_score=long_score, short_score=short_score,
                        )
            # Advance opportunity timeouts before any response can form a new
            # same-bar candidate.  This keeps lifecycle ordering explicit.
            previous_opportunities = dict(self._opportunities)
            detector = self._location_detectors[observation.timeframe]
            if observation.timeframe == "5m":
                for key, item in tuple(self._opportunities.items()):
                    source_interaction = next(
                        (
                            candidate
                            for candidate in (*detector.active_interactions, *detector.closed_interactions)
                            if candidate.interaction_id == item.source_interaction_id
                        ),
                        None,
                    )
                    timeout = advance_opportunity_timeout(
                        item, interaction=source_interaction,
                        evidence=(observation,), as_of=observation.available_at,
                    )
                    if timeout.opportunity is not None:
                        self._opportunities[key] = timeout.opportunity
            response_interactions = list(detector.active_interactions)
            response_interactions.extend(
                item for item in detector.closed_interactions
                if item.closed_at == observation.available_at
            )
            for interaction in response_interactions:
                if interaction.timeframe != self.config.response_timeframe:
                    continue
                response_observations = tuple(sorted((
                    item for item in observations
                    if item.timeframe == self.config.response_timeframe
                    # The interaction's completed contact candle is the first
                    # response-window evidence in the canonical replay.
                    and item.observed_at >= interaction.contact_candles[0].observed_at
                ), key=lambda item: (item.observed_at, item.available_at, item.observation_id)))
                if not response_observations:
                    continue
                decision_cutoff = min(context.as_of, orderflow.cutoff)
                available_response_observations = tuple(
                    item for item in response_observations if item.available_at <= decision_cutoff
                )
                if interaction.location_alias in {"PD_LOW", "PD_HIGH"}:
                    role = interaction.pd_role or (
                        "SUPPORT" if interaction.location_alias == "PD_LOW" else "RESISTANCE"
                    )
                    transition_candidates = tuple(
                        candidate for candidate in available_response_observations
                        if interaction.pd_role_transition_available_at is None
                        or candidate.available_at > interaction.pd_role_transition_available_at
                    )
                    for candidate in transition_candidates:
                        high = candidate.features.get("high")
                        low = candidate.features.get("low")
                        if not isinstance(high, (int, float)) or not isinstance(low, (int, float)):
                            continue
                        upper = interaction.band_upper if interaction.band_upper is not None else interaction.reference_level
                        lower = interaction.band_lower if interaction.band_lower is not None else interaction.reference_level
                        transitioned = "SUPPORT" if low > upper else (
                            "RESISTANCE" if high < lower else None
                        )
                        if transitioned is not None and transitioned != role:
                            transition_evidence = (candidate.observation_id,)
                            for opportunity_id, opportunity in tuple(self._opportunities.items()):
                                if (
                                    opportunity.source_interaction_id == interaction.interaction_id
                                    and opportunity.status is OpportunityStatus.ACTIVE
                                    and opportunity.setup.formed_at <= candidate.available_at
                                ):
                                    invalidated = opportunity.invalidate(
                                        at=candidate.available_at,
                                        reason="prior_day_role_transition",
                                        evidence_ids=transition_evidence,
                                    )
                                    self._opportunities[opportunity_id] = invalidated
                                    results.append(PolicyDecision({
                                        **base,
                                        "decision_stage": "opportunity",
                                        "outcome": "rejected",
                                        "reason": "prior_day_role_transition",
                                        "decision_id": _decision_id(
                                            "opportunity_invalidated", opportunity_id,
                                            candidate.observation_id,
                                        ),
                                        "opportunity_id": opportunity_id,
                                        "source_observation_ids": list(transition_evidence),
                                    }))
                            updated = detector.update_prior_day_role(
                                interaction.interaction_id, transitioned,
                                candidate.observation_id, candidate.available_at,
                            )
                            if updated is not None:
                                interaction = updated
                            role = transitioned
                evaluations = evaluate_policy_responses(
                    interaction, available_response_observations,
                    max_candles=self.config.response_window_candles,
                    cutoff=decision_cutoff,
                    initial_armed={
                        item.hypothesis_id: item.armed
                        for item in interaction.hypothesis_states
                    },
                )
                for template, evaluation in evaluations:
                    hypothesis_id = template.template_id
                    future = next((item for item in response_observations if item.available_at > decision_cutoff), None)
                    if future is not None and evaluation.status is None:
                        next_status = "UNAVAILABLE"
                        reason = "response_evidence_unavailable_at_cutoff"
                        evidence_ids = tuple(future.evidence_ids or (future.observation_id,))
                        expected_available_at = future.available_at
                        direction = None
                        confirmation_id = None
                    else:
                        next_status = evaluation.status.value if evaluation.status is not None else "PENDING_RESPONSE"
                        reason = evaluation.reason or next_status.lower()
                        evidence_ids = evaluation.evidence_ids
                        expected_available_at = getattr(evaluation, "expected_available_at", None)
                        raw_direction = evaluation.direction
                        direction = raw_direction.value if raw_direction is not None else None
                        confirmation_id = evaluation.confirmation_observation_id
                    source_observation_ids = tuple(
                        item.observation_id for item in response_observations
                        if set(item.evidence_ids).intersection(evidence_ids)
                    )
                    if not source_observation_ids and confirmation_id is not None:
                        source_observation_ids = (confirmation_id,)
                    if next_status == "PENDING":
                        next_status = "PENDING_RESPONSE"
                    current = next((item for item in interaction.hypothesis_states if item.hypothesis_id == hypothesis_id), None)
                    if current is not None and (
                        tuple(evidence_ids) == current.evidence_ids
                        and next_status == current.status
                        and evaluation.armed == current.armed
                        and direction == current.direction
                        and confirmation_id == current.confirmation_observation_id
                        and expected_available_at == current.expected_available_at
                    ):
                        continue
                    state = HypothesisState(
                        hypothesis_id, template.template_id, next_status,
                        evaluation.armed, direction, tuple(evidence_ids), confirmation_id,
                         evaluation.confirmation_available_at if next_status == "CONFIRMED" else None, expected_available_at,
                        reason if next_status == "INVALIDATED" else None,
                        reason if next_status == "EXPIRED" else None,
                        tuple(
                            EvidenceLineage(
                                item.observation_id,
                                item.source_observation_id or item.observation_id,
                                item.timeframe,
                                item.observed_at,
                                item.available_at,
                                str(item.context.replay_provenance.get("source_fingerprint"))
                                if item.context is not None and item.context.replay_provenance.get("source_fingerprint") is not None
                                else None,
                            )
                            for item in response_observations
                            if item.observation_id in source_observation_ids
                        ),
                    )
                    updated = self._location_detectors[observation.timeframe].update_response(
                        interaction.interaction_id, next_status, hypothesis_id, state=state,
                    )
                    if updated is None:
                        continue
                    results.append(PolicyDecision({
                    **base,
                    "decision_stage": "response",
                     "outcome": "recorded" if next_status == "CONFIRMED" else "unavailable" if next_status == "UNAVAILABLE" else "rejected",
                     "reason": reason,
                    "decision_id": _decision_id(
                        "response", interaction.interaction_id,
                         confirmation_id or (evidence_ids[-1] if evidence_ids else observation.observation_id),
                        next_status,
                    ),
                    "location": interaction.location_alias,
                    "interaction_id": interaction.interaction_id,
                    "reference_generation": interaction.reference_generation,
                    "response_state": next_status,
                    "template_id": template.template_id,
                    "hypothesis_state": asdict(state),
                    "response_evidence_ids": list(evidence_ids),
                    "source_observation_ids": list(source_observation_ids),
                    "expected_available_at": expected_available_at.isoformat() if expected_available_at else None,
                }))
                    if next_status != "CONFIRMED":
                        continue
                    formed_at = evaluation.confirmation_available_at or observation.available_at
                    if direction is None:
                        results.append(PolicyDecision({
                            **base,
                            "decision_stage": "admission",
                            "outcome": "unavailable",
                            "reason": "response_direction_unavailable",
                            "decision_id": _decision_id("unavailable", "direction", interaction.interaction_id),
                            "interaction_id": interaction.interaction_id,
                             "source_observation_ids": list(evidence_ids),
                        }))
                        continue
                    formation_context = self._context_history.as_of_location(
                        interaction.location_alias, formed_at,
                    )
                    confirmation_observation = next(
                        (item for item in response_observations
                         if item.observation_id == confirmation_id),
                        None,
                    )
                    confirmation_close = (
                        _number(confirmation_observation, "close")
                        if confirmation_observation is not None else None
                    )
                    acceptance = (
                        confirmation_close is not None
                        and (
                            confirmation_close > (interaction.band_upper or interaction.reference_level)
                            if direction == "long"
                            else confirmation_close < (interaction.band_lower or interaction.reference_level)
                        )
                    )
                    relation = classify_context_relation(
                        location=interaction.location_alias,
                        direction=direction,
                        evidence={"acceptance": acceptance, "evidence_ids": evidence_ids},
                    )
                    # Permission uses the confirmation aggregate's local scores,
                    # matching the canonical replay rather than the older
                    # formation-context snapshot.
                    decision_scores = (
                        self._confirmation_scores(confirmation_observation)
                        if confirmation_observation is not None else None
                    )
                    long_score = decision_scores[0] if decision_scores is not None else None
                    short_score = decision_scores[1] if decision_scores is not None else None
                    score_rejection = _score_rejection(
                        direction=direction,
                        long_score=long_score,
                        short_score=short_score,
                        margin=self.config.fill_permission_margin,
                    )
                    if score_rejection is not None:
                        results.append(PolicyDecision({
                            **base,
                            "decision_stage": "admission",
                            "outcome": "rejected",
                            "reason": score_rejection,
                            "decision_id": _decision_id(
                                "score_admission", interaction.interaction_id, confirmation_id,
                            ),
                            "interaction_id": interaction.interaction_id,
                            "direction": direction,
                            "long_score": long_score,
                            "short_score": short_score,
                            "score_margin": self.config.fill_permission_margin,
                            "source_observation_ids": list(evidence_ids),
                            "evidence": {
                                "metrics": {
                                    "long_score": long_score,
                                    "short_score": short_score,
                                    "fill_permission_margin": self.config.fill_permission_margin,
                                },
                                "provenance": {
                                    "context_version": formation_context.version if formation_context else None,
                                    "source_observation_ids": list(evidence_ids),
                                },
                            },
                        }))
                        continue
                    opportunity = SetupOpportunity.create(
                        interaction_id=interaction.interaction_id,
                        direction=direction,
                        formed_at=formed_at,
                        expires_at=formed_at + timedelta(
                            minutes=self.config.response_window_candles
                            * (5 if interaction.timeframe == "5m" else 1)
                        ),
                        entry_lower=interaction.band_lower or interaction.reference_level,
                        entry_upper=interaction.band_upper or interaction.reference_level,
                        failure_boundary=(
                            interaction.band_lower if direction == "long" else interaction.band_upper
                        ) or interaction.reference_level,
                        evidence_ids=evidence_ids,
                        evidence_timeframe=interaction.timeframe,
                        regime_version=formation_context.version if formation_context is not None else None,
                        template_id=template.template_id,
                        policy_version=template.version,
                        conflict_group_id=template.execution_conflict_group,
                        context_relation=relation.value,
                         formation_context=(
                             ContextVersionRef(formation_context.context_id, formation_context.version)
                             if formation_context is not None else None
                         ),
                         source_instrument=observation.instrument,
                         source_session_id=(
                             observation.context.session_id if observation.context is not None else None
                         ),
                     )
                    if opportunity.lifecycle_id not in self._opportunities:
                        current_context = formation_context
                        context_relation = relation
                        admission = evaluate_admission(
                            opportunity,
                            context_available=current_context is not None,
                            regime_allowed=(
                                current_context is not None
                                and current_context.permission != "UNAVAILABLE"
                                and (
                                    (
                                        direction == "long"
                                        and current_context.permission in {"LONG", "BOTH"}
                                    )
                                    or (
                                        direction == "short"
                                        and current_context.permission in {"SHORT", "BOTH"}
                                    )
                                )
                                and not self._admission_suspended
                            ),
                            opening_gap_allowed=(
                                current_context is not None
                                and current_context.opening_gap.status != "UNAVAILABLE"
                                and (
                                    current_context.opening_gap.status not in {"UP", "DOWN"}
                                    or (direction == "long" and (current_context.opening_gap.gap_points or 0) >= 0)
                                    or (direction == "short" and (current_context.opening_gap.gap_points or 0) <= 0)
                                )
                                and not self._admission_suspended
                            ),
                            setup_valid=True,
                            direction_matches=direction is not None,
                             confirmations_available=confirmation_id is not None,
                            current_at=formed_at,
                            context_version=(
                                str(current_context.version) if current_context is not None else None
                            ),
                            context_relation=context_relation.value,
                            evidence_ids=evidence_ids,
                            evaluate_portfolio_gates=False,
                        )
                        self._opportunities[opportunity.lifecycle_id] = opportunity
                        results.append(PolicyDecision({
                            **base,
                            "decision_stage": "opportunity",
                            "outcome": "recorded",
                            "reason": "setup_candidate_formed",
                            "decision_id": _decision_id(
                                "candidate", opportunity.lifecycle_id,
                            ),
                            "opportunity_id": opportunity.lifecycle_id,
                            "setup_id": opportunity.setup.setup_id,
                            "interaction_id": interaction.interaction_id,
                            "location": interaction.location_alias,
                            "direction": direction,
                            "template_id": template.template_id,
                            "source_observation_ids": list(evidence_ids),
                            "observed_at": formed_at.isoformat(),
                            "available_at": formed_at.isoformat(),
                            "long_score": long_score,
                            "short_score": short_score,
                            "evidence": {
                                "metrics": {
                                    "long_score": long_score,
                                    "short_score": short_score,
                                    "fill_permission_margin": self.config.fill_permission_margin,
                                },
                                "provenance": {
                                    "context_version": current_context.version if current_context else None,
                                    "source_observation_ids": list(evidence_ids),
                                },
                            },
                        }))
                        trigger_observation = next(
                            (
                                item for item in response_observations
                                if item.observation_id == confirmation_id
                            ),
                            observation,
                        )
                        trigger = observe_trigger(
                            trigger_observation,
                            boundary=opportunity.setup.failure_boundary,
                            # Diagnostic mode reports the same eligibility
                            # projection; execution authorization is separate.
                            execution_mode=ExecutionMode.EXECUTABLE.value,
                            trigger_type=template.trigger_type.value,
                            effective_at=formed_at.isoformat(),
                            expires_at=opportunity.setup.expires_at.isoformat(),
                            setup_version=opportunity.setup.setup_id,
                            permission_version=(
                                str(current_context.version) if current_context is not None else None
                            ),
                            expected_setup_version=opportunity.setup.setup_id,
                            expected_permission_version=(
                                str(current_context.version) if current_context is not None else None
                            ),
                            armed=True,
                        )
                        results.append(PolicyDecision({
                            **base,
                            "decision_stage": "trigger",
                            "outcome": "accepted" if trigger.eligible else "rejected",
                            "reason": trigger.reason,
                            "decision_id": _decision_id(
                                "trigger", opportunity.lifecycle_id,
                                trigger_observation.observation_id,
                            ),
                            "opportunity_id": opportunity.lifecycle_id,
                            "setup_id": opportunity.setup.setup_id,
                            "interaction_id": interaction.interaction_id,
                            "location": interaction.location_alias,
                            "direction": direction,
                            "trigger": asdict(trigger),
                            "source_observation_ids": list(evidence_ids),
                            "observed_at": trigger.observed_at,
                            "available_at": trigger.available_at,
                        }))
                        accepted = (
                            admission.status.value == "PASSED"
                            and (self.execution_mode is not ExecutionMode.EXECUTABLE or trigger.eligible)
                        )
                        order = None
                        if accepted and self.execution_mode is ExecutionMode.EXECUTABLE:
                            instrument = futures.instrument if futures is not None else next(
                                (item.instrument for item in bundle.bars.values()),
                                Instrument(observation.instrument, "NFO", "FUTURES"),
                            )
                            available_lots = (
                                int(context.margin.available // context.margin.required_per_lot)
                                if context.margin.required_per_lot > 0
                                else int(context.margin.available > 0)
                            )
                            quantity = max(0, min(available_lots, 1))
                            if quantity > 0 and instrument is not None:
                                order = OrderIntent(
                                    client_order_id=f"orderflow-entry:{_decision_id(
                                        opportunity.lifecycle_id,
                                    )}",
                                    instrument=instrument,
                                    side=OrderSide.BUY if direction == "long" else OrderSide.SELL,
                                     quantity=quantity,
                                    reason="causally_accepted_orderflow_opportunity",
                                     cell=interaction.location_alias.lower(),
                                     stop_price=opportunity.setup.failure_boundary,
                                     stop_policy=template.stop_policy.value,
                                     exit_mode=self.config.exit_policy,
                                    role=OrderRole.ENTRY,
                                     entry_bar=context.as_of.minute,
                                     strategy_id=self.config.name,
                                     execution_group_id=opportunity.execution_group_id,
                                     lifecycle_id=opportunity.lifecycle_id,
                                     setup_availability=opportunity.setup.formed_at.isoformat(),
                                 )
                        results.append(PolicyDecision({
                            **base,
                            "decision_stage": "admission",
                            "outcome": "accepted" if accepted else "rejected",
                            "reason": "orderflow_admission",
                            "decision_id": admission.decision_id,
                            "opportunity_id": opportunity.lifecycle_id,
                            "setup_id": opportunity.setup.setup_id,
                            "interaction_id": interaction.interaction_id,
                            "location": interaction.location_alias,
                            "direction": direction,
                            "template_id": template.template_id,
                            "admission_status": admission.status.value,
                            "admission_reason": admission.reason,
                            "admission_predicates": [asdict(item) for item in admission.predicates],
                            "formation_context": (
                                asdict(opportunity.formation_context)
                                if opportunity.formation_context is not None else None
                            ),
                            "decision_context": (
                                asdict(opportunity.decision_context)
                                if opportunity.decision_context is not None else None
                            ),
                            "trigger": asdict(trigger),
                            "trigger_eligible": trigger.eligible,
                            "source_observation_ids": list(evidence_ids),
                         }, order=order))
            for opportunity_id, opportunity in self._opportunities.items():
                previous_opportunity = previous_opportunities.get(opportunity_id)
                if previous_opportunity is None or len(opportunity.transitions) <= len(previous_opportunity.transitions):
                    continue
                for transition in opportunity.transitions[len(previous_opportunity.transitions):]:
                    if transition.status is not OpportunityStatus.EXPIRED:
                        continue
                    results.append(PolicyDecision({
                        **base,
                        "decision_stage": "lifecycle",
                        "outcome": "rejected",
                        "reason": "setup_expired",
                        "decision_id": _decision_id(
                            "lifecycle", opportunity.lifecycle_id,
                            transition.at.isoformat(), "setup_expired",
                        ),
                        "opportunity_id": opportunity.lifecycle_id,
                        "setup_id": opportunity.setup.setup_id,
                        "interaction_id": opportunity.response_episode_id.removeprefix("response-"),
                        "observed_at": transition.at.isoformat(),
                        "available_at": transition.at.isoformat(),
                        "source_observation_ids": list(transition.evidence_ids),
                    }))
            if self._admission_suspended:
                results.append(PolicyDecision({
                    **base,
                    "outcome": "unavailable",
                    "reason": "admission_suspended_after_source_gap",
                    "decision_id": _decision_id("unavailable", "gap", observation.observation_id),
                    "source_observation_ids": [observation.observation_id],
                    "expected_available_at": self._gap_end.isoformat() if self._gap_end else None,
                }))
                continue
        if not results and all(observation.observation_id in self._processed_observation_ids for observation in observations):
            return ()
        lifecycle_reasons = {"interaction_context_published", "interaction_created"}
        lifecycle_events = [item for item in results if item.payload.get("reason") in lifecycle_reasons]
        if lifecycle_events:
            lifecycle_events.sort(
                key=lambda item: item.payload.get("reason") != "interaction_context_published",
            )
            first_lifecycle_index = min(
                index for index, item in enumerate(results)
                if item.payload.get("reason") in lifecycle_reasons
            )
            results = [item for item in results if item.payload.get("reason") not in lifecycle_reasons]
            results[first_lifecycle_index:first_lifecycle_index] = lifecycle_events
        return tuple(results)

    def on_execution_event(self, event: ExecutionNotification) -> None:
        # Diagnostic mode cannot own execution state.
        return None

    def snapshot(self) -> Mapping[str, object]:
        return {
            "schema_version": 3,
            "config": asdict(self.config) | {"execution_mode": self.config.execution_mode.value},
            "session_id": self._session_id,
            "processed_observation_ids": sorted(self._processed_observation_ids),
            "processed_location_observation_ids": sorted(self._processed_location_observation_ids),
            "location_detectors": {
                timeframe: detector.snapshot()
                for timeframe, detector in self._location_detectors.items()
            },
            "context_history": self._context_history.to_dict(),
            "location_registry": [item.to_dict() for item in self._location_registry.records],
            "context_version": self._context_version,
            "session_high": self._session_high,
            "session_low": self._session_low,
            "admission_suspended": self._admission_suspended,
            "gap_end": self._gap_end.isoformat() if self._gap_end else None,
            "opening_gap_last_evaluated_id": self._opening_gap_last_evaluated_id,
            "opening_seed": dict(self._opening_seed) if self._opening_seed is not None else None,
            "opportunities": [self._opportunities[key].snapshot() for key in sorted(self._opportunities)],
            "execution_groups": self._execution_groups.snapshot(),
        }

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, object]) -> "OrderflowPolicy":
        schema_version = snapshot.get("schema_version")
        if schema_version not in {2, 3}:
            raise ValueError("unsupported orderflow snapshot schema")
        raw_config = snapshot.get("config")
        if not isinstance(raw_config, Mapping):
            raise ValueError("orderflow snapshot config must be an object")
        values = dict(raw_config)
        allowed_config = set(OrderflowConfig.__dataclass_fields__)
        values.setdefault("timezone", "Asia/Kolkata")
        values.setdefault("market_session", ("09:15", "15:30"))
        values.setdefault("vehicle", "futures")
        values.setdefault("replay_mode", "causal")
        values.setdefault("capture_mode", CaptureMode.FULL)
        if set(values) != allowed_config:
            raise ValueError("orderflow snapshot config fields are invalid")
        values["execution_mode"] = ExecutionMode(values.get("execution_mode", "diagnostic"))
        values["capture_mode"] = CaptureMode(values.get("capture_mode", CaptureMode.FULL))
        try:
            policy = cls(OrderflowConfig(**values))
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid orderflow snapshot config") from exc
        session_id = snapshot.get("session_id")
        if session_id is not None and not isinstance(session_id, str):
            raise ValueError("invalid orderflow snapshot session_id")
        policy._session_id = session_id
        raw_processed = snapshot.get("processed_observation_ids", ())
        if not isinstance(raw_processed, (list, tuple)) or any(not isinstance(item, str) for item in raw_processed):
            raise ValueError("invalid processed orderflow observation IDs")
        policy._processed_observation_ids = set(raw_processed)
        raw_location_processed = snapshot.get("processed_location_observation_ids")
        if not isinstance(raw_location_processed, (list, tuple)) or any(
            not isinstance(item, str) for item in raw_location_processed
        ):
            raise ValueError("invalid processed location observation IDs")
        policy._processed_location_observation_ids = set(raw_location_processed)
        raw_detectors = snapshot.get("location_detectors")
        if schema_version == 2:
            legacy_detector = snapshot.get("location_detector")
            if not isinstance(legacy_detector, Mapping):
                raise ValueError("invalid legacy orderflow location detector snapshot")
            raw_detectors = {
                "1m": InteractionDetector("1m").snapshot(),
                "5m": legacy_detector,
            }
        if not isinstance(raw_detectors, Mapping):
            raise ValueError("invalid orderflow location detector snapshots")
        if set(raw_detectors) != {"1m", "5m"}:
            raise ValueError("orderflow location detector timeframes are invalid")
        if any(not isinstance(value, Mapping) for value in raw_detectors.values()):
            raise ValueError("invalid orderflow location detector snapshot")
        policy._location_detectors = {
            str(timeframe): InteractionDetector.from_snapshot(value)
            for timeframe, value in raw_detectors.items()
        }
        policy._location_detector = policy._location_detectors["5m"]
        admission_suspended = snapshot.get("admission_suspended")
        gap_end = snapshot.get("gap_end")
        if "gap_end" not in snapshot or not isinstance(admission_suspended, bool):
            raise ValueError("invalid orderflow admission suspension state")
        if gap_end is not None and not isinstance(gap_end, str):
            raise ValueError("invalid orderflow gap end")
        policy._admission_suspended = admission_suspended
        policy._gap_end = datetime.fromisoformat(gap_end) if gap_end is not None else None
        last_gap_observation = snapshot.get("opening_gap_last_evaluated_id")
        if last_gap_observation is not None and not isinstance(last_gap_observation, str):
            raise ValueError("invalid opening gap evaluation observation ID")
        policy._opening_gap_last_evaluated_id = last_gap_observation
        raw_opening_seed = snapshot.get("opening_seed")
        if raw_opening_seed is not None and not isinstance(raw_opening_seed, Mapping):
            raise ValueError("invalid opening seed snapshot")
        policy._opening_seed = dict(raw_opening_seed) if isinstance(raw_opening_seed, Mapping) else None
        raw_context_version = snapshot.get("context_version", 0)
        if not isinstance(raw_context_version, int) or raw_context_version < 0:
            raise ValueError("invalid orderflow context version")
        policy._context_version = raw_context_version
        raw_session_high = snapshot.get("session_high")
        raw_session_low = snapshot.get("session_low")
        policy._session_high = float(raw_session_high) if isinstance(raw_session_high, (int, float)) else None
        policy._session_low = float(raw_session_low) if isinstance(raw_session_low, (int, float)) else None
        raw_context_history = snapshot.get("context_history")
        if isinstance(raw_context_history, Mapping):
            policy._context_history = ContextHistory.from_dict(raw_context_history)
        raw_location_registry = snapshot.get("location_registry")
        if raw_location_registry is not None:
            policy._location_registry = LocationRegistry.from_dict(raw_location_registry)
        raw_opportunities = snapshot.get("opportunities")
        if not isinstance(raw_opportunities, (list, tuple)):
            raise ValueError("invalid orderflow opportunities")
        for item in raw_opportunities:
            if not isinstance(item, Mapping):
                raise ValueError("invalid orderflow opportunity")
            opportunity = SetupOpportunity.from_snapshot(item)
            if opportunity.lifecycle_id in policy._opportunities:
                raise ValueError("duplicate orderflow opportunity ID")
            policy._opportunities[opportunity.lifecycle_id] = opportunity
        raw_execution_groups = snapshot.get("execution_groups")
        if not isinstance(raw_execution_groups, Mapping):
            raise ValueError("invalid orderflow execution group snapshot")
        policy._execution_groups = ExecutionGroupRegistry.from_snapshot(raw_execution_groups)
        return policy


class OrderflowStrategy:
    """Strategy facade preserving the public Strategy protocol."""

    def __init__(self, config: OrderflowConfig = OrderflowConfig()) -> None:
        self.policy = OrderflowPolicy(config)
        self.coordinator = SharedDecisionCoordinator(self.policy)
        self._decision_positions: dict[str, tuple[PositionState, ExitStateMachine]] = {}
        self._pending_exits: dict[str, str] = {}
        self._pending_exit_actions: dict[str, tuple[ExitAction, datetime]] = {}
        self._completed_bars: list[MarketBar] = []
        self._stop_update_evidence: list[dict[str, object]] = []

    def bind_account_context(self, portfolio: object, capital_context: object) -> None:
        """Record the runtime-owned account boundary without owning its state."""
        del portfolio, capital_context

    @property
    def metadata(self) -> StrategyMetadata:
        return StrategyMetadata(self.policy.config.name, self.policy.config.version, self.policy.config.config_hash)

    def on_bundle(self, bundle: DecisionBundle):
        cutoff = (
            bundle.orderflow_context.cutoff
            if bundle.orderflow_context is not None
            else self._fallback_cutoff(bundle)
        )
        context = DecisionContext(
            as_of=cutoff,
            account_revision=0,
            capital=CapitalSnapshot(0.0, 0.0, 0.0, 0.0),
            margin=MarginSnapshot(0.0, 0.0, 0.0),
            exposure=ExposureSnapshot(0.0, 0),
        )
        return self.coordinator.evaluate(bundle, context)

    @staticmethod
    def _fallback_cutoff(bundle: DecisionBundle):
        from ftx_paper.config import IST

        raw = bundle.minute if "T" in bundle.minute else f"{bundle.trading_date}T{bundle.minute}"
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=IST)
        return parsed

    def evaluate(self, bundle: DecisionBundle, context: DecisionContext):
        return self.coordinator.evaluate(bundle, context)

    def on_execution_event(self, event: ExecutionNotification) -> None:
        self.coordinator.on_execution_event(event)

    def register_entry(self, order: OrderIntent, *, fill_price: float | None = None,
                       entry_fill_time: datetime | str | None = None,
                       reference_price: float | None = None) -> None:
        """Attach the executable orderflow entry to its causal exit state."""
        if order.stop_price is None or order.client_order_id in self._decision_positions:
            return
        if isinstance(entry_fill_time, str):
            entry_fill_time = datetime.fromisoformat(entry_fill_time)
        position = PositionState(
            order.instrument, fill_price if fill_price is not None else 0.0,
            order.stop_price, order.quantity, order.side, stop_policy=order.stop_policy,
            cell=order.cell,
            exit_mode=order.exit_mode or "signal", entry_fill_time=entry_fill_time,
            exit_reference_price=reference_price, vehicle=order.vehicle,
            synthetic_legs=order.synthetic_legs, entry_bar=order.entry_bar,
            entry_order_id=order.client_order_id,
            initial_risk_points=abs((fill_price if fill_price is not None else 0.0) - order.stop_price),
        )
        self._decision_positions[order.client_order_id] = (
            position, ExitStateMachine(close_time=SESSION_EXIT_DEADLINE_TIME),
        )

    def on_closed_bar(self, bar: MarketBar) -> tuple[ExitAction, ...]:
        actions: list[ExitAction] = []
        self._completed_bars.append(bar)
        delayed_actions = tuple(self._pending_exit_actions.items())
        for exit_order_id, (action, discovered_at) in delayed_actions:
            if bar.instrument == action.intent.instrument and bar.timestamp > discovered_at:
                actions.append(action)
                self._pending_exit_actions.pop(exit_order_id, None)
        pending_entries = set(self._pending_exits.values())
        for order_id, (position, exits) in tuple(self._decision_positions.items()):
            if bar.instrument != position.instrument or order_id in pending_entries:
                continue
            if position.entry_fill_time is not None and bar.timestamp < position.entry_fill_time:
                continue
            if position.stop_policy == "or_vwap":
                vwap_bars = tuple(item for item in self._completed_bars if item.instrument == bar.instrument)
                vwap, source_bar = calculate_session_vwap(
                    cast(tuple[MarketAggregate, ...], vwap_bars),
                    cutoff=bar.timestamp, timezone=self.policy.config.timezone,
                    session_start=self.policy.config.market_session[0],
                )
                local_date = bar.timestamp.astimezone(ZoneInfo(self.policy.config.timezone)).date()
                session_bars = tuple(
                    item for item in vwap_bars
                    if item.timestamp.astimezone(ZoneInfo(self.policy.config.timezone)).date() == local_date
                )[-20:]
                ranges = [item.high - item.low for item in session_bars if item.high >= item.low]
                atr = sum(ranges) / len(ranges) if ranges else None
                result = calculate_vwap_stop(
                    direction="long" if position.side is OrderSide.BUY else "short",
                    entry_price=position.entry_price, vwap=vwap, atr=atr,
                    source_aggregate_id=getattr(source_bar, "aggregate_id", None),
                    source_observation_id=getattr(source_bar, "observation_id", None),
                )
                initial_risk = position.initial_risk_points or 0.0
                favorable = (
                    bar.high - position.entry_price if position.side is OrderSide.BUY
                    else position.entry_price - bar.low
                )
                post_3r = position.post_3r_activated or (
                    initial_risk > 0 and favorable >= 3.0 * initial_risk
                )
                if post_3r and result.candidate is not None and initial_risk > 0:
                    one_r = (
                        position.entry_price + initial_risk
                        if position.side is OrderSide.BUY
                        else position.entry_price - initial_risk
                    )
                    result = replace(
                        result,
                        candidate=(max(one_r, result.candidate) if position.side is OrderSide.BUY
                                   else min(one_r, result.candidate)),
                    )
                tightens = result.candidate is not None and (
                    result.candidate > position.stop_price if position.side is OrderSide.BUY
                    else result.candidate < position.stop_price
                )
                if tightens:
                    position = replace(
                        position, stop_price=result.candidate, stop_source="vwap",
                        post_3r_activated=post_3r,
                    )
                    self._decision_positions[order_id] = (position, exits)
                elif post_3r and not position.post_3r_activated:
                    position = replace(position, post_3r_activated=True)
                    self._decision_positions[order_id] = (position, exits)
                self._stop_update_evidence.append({
                    "event": "stop_updated", "vwap_stop_reference": result.vwap,
                    "active_stop": position.stop_price, "stop_source": position.stop_source,
                    "atr": result.atr, "source_aggregate_id": result.source_aggregate_id,
                    "source_observation_id": result.source_observation_id,
                    "reason": result.reason, "changed": tightens,
                    "observed_at": bar.timestamp.isoformat(),
                })
            action = exits.evaluate(
                position, timestamp=bar.timestamp, high=bar.high, low=bar.low,
                close=bar.close, open=bar.open, volume=bar.volume,
                client_order_id=f"exit-{order_id}-{bar.timestamp.isoformat()}",
            )
            if action is not None:
                self._pending_exits[action.intent.client_order_id] = order_id
                self._pending_exit_actions[action.intent.client_order_id] = (
                    action, bar.timestamp,
                )
        return tuple(actions)

    @property
    def stop_update_evidence(self) -> tuple[Mapping[str, object], ...]:
        return tuple(self._stop_update_evidence)

    def settle_exit(self, exit_order_id: str, *, filled: bool, **_kwargs: object) -> None:
        entry_order_id = self._pending_exits.pop(exit_order_id, None)
        self._pending_exit_actions.pop(exit_order_id, None)
        if filled and entry_order_id is not None:
            self._decision_positions.pop(entry_order_id, None)

    def snapshot(self) -> Mapping[str, object]:
        decision_positions = {
            order_id: {
                "position": _position_snapshot(position),
                "exit_state": exits.snapshot(),
            }
            for order_id, (position, exits) in self._decision_positions.items()
        }
        pending_actions = {
            order_id: {
                "action": _exit_action_snapshot(action),
                "discovered_at": discovered_at.isoformat(),
            }
            for order_id, (action, discovered_at) in self._pending_exit_actions.items()
        }
        return wrap_snapshot(
            strategy_id=ORDERFLOW_NAME,
            metadata={"name": self.metadata.name, "version": self.metadata.version,
                      "config_hash": self.metadata.config_hash},
            payload={
                **self.policy.snapshot(),
                "decision_positions": decision_positions,
                "pending_exits": dict(self._pending_exits),
                "pending_exit_actions": pending_actions,
                "completed_bars": [_bar_snapshot(item) for item in self._completed_bars],
            },
        )

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, object], *, capital_profile=None) -> "OrderflowStrategy":
        _, payload = unwrap_snapshot(snapshot)
        strategy = cls.__new__(cls)
        strategy.policy = OrderflowPolicy.from_snapshot(payload)
        strategy.coordinator = SharedDecisionCoordinator(strategy.policy)
        strategy._decision_positions = {}
        raw_positions = payload.get("decision_positions", {})
        if not isinstance(raw_positions, Mapping):
            raise ValueError("invalid orderflow decision position snapshot")
        for order_id, raw in raw_positions.items():
            if not isinstance(order_id, str) or not isinstance(raw, Mapping):
                raise ValueError("invalid orderflow decision position")
            position_raw = raw.get("position")
            exit_raw = raw.get("exit_state")
            if not isinstance(position_raw, Mapping) or not isinstance(exit_raw, Mapping):
                raise ValueError("incomplete orderflow decision position")
            position = _position_from_snapshot(position_raw)
            if position.entry_order_id != order_id:
                raise ValueError("orderflow position key does not match entry_order_id")
            strategy._decision_positions[order_id] = (position, ExitStateMachine.from_snapshot(exit_raw))
        raw_pending = payload.get("pending_exits", {})
        if not isinstance(raw_pending, Mapping) or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in raw_pending.items()
        ):
            raise ValueError("invalid orderflow pending exits")
        if any(value not in strategy._decision_positions for value in raw_pending.values()):
            raise ValueError("orderflow pending exit references unknown position")
        strategy._pending_exits = dict(raw_pending)
        raw_actions = payload.get("pending_exit_actions", {})
        if not isinstance(raw_actions, Mapping):
            raise ValueError("invalid orderflow pending exit actions")
        strategy._pending_exit_actions = {}
        for order_id, raw in raw_actions.items():
            if not isinstance(order_id, str) or not isinstance(raw, Mapping):
                raise ValueError("invalid orderflow pending exit action")
            action_raw = raw.get("action")
            discovered_at = raw.get("discovered_at")
            if not isinstance(action_raw, Mapping) or not isinstance(discovered_at, str):
                raise ValueError("incomplete orderflow pending exit action")
            if order_id not in strategy._pending_exits:
                raise ValueError("pending exit action is not pending")
            strategy._pending_exit_actions[order_id] = (
                _exit_action_from_snapshot(action_raw), datetime.fromisoformat(discovered_at),
            )
        raw_bars = payload.get("completed_bars", ())
        if not isinstance(raw_bars, (list, tuple)):
            raise ValueError("invalid orderflow completed bars")
        strategy._completed_bars = list(
            _bar_from_snapshot(item) for item in raw_bars
            if isinstance(item, Mapping)
        )
        return strategy


__all__ = [
    "OrderflowConfig", "OrderflowPolicy", "OrderflowStrategy",
    "ORDERFLOW_NAME", "ORDERFLOW_VERSION",
]
