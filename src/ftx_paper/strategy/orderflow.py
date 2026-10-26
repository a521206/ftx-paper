"""Diagnostic orderflow strategy for causal VWAP interaction observations."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
import hashlib
import json
import math
from dataclasses import replace

from ftx_paper.domain.decision_context import (
    CapitalSnapshot,
    DecisionContext,
    ExposureSnapshot,
    MarginSnapshot,
)
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.contracts import MarketRole
from ftx_paper.market.bundles import DecisionBundle

from .coordinator import SharedDecisionCoordinator
from .orderflow_contract import OrderflowObservation
from .orderflow_trigger import observe_trigger
from .policy_contract import ExecutionMode, FeatureRequirement, PolicyDecision
from .protocol import StrategyMetadata
from .snapshot_envelope import unwrap_snapshot, wrap_snapshot


ORDERFLOW_NAME = "ftx-paper-orderflow"
ORDERFLOW_VERSION = "0.1.0"


@dataclass(frozen=True, slots=True)
class OrderflowConfig:
    name: str = ORDERFLOW_NAME
    version: str = ORDERFLOW_VERSION
    enabled_timeframes: tuple[str, ...] = ("1m", "5m")
    session_windows: tuple[tuple[str, str], ...] = (("09:15", "15:30"),)
    response_timeframe: str = "5m"
    vwap_calculation_version: str = "typical_price_volume_v1"
    price_input: str = "typical_price"
    volume_input: str = "volume"
    session_reset: str = "trading_date"
    interaction_band: float = 5.0
    response_type: str = "confirmed_rejection"
    confirmation_rule: str = "completed_response_candle"
    invalidation_rule: str = "close_outside_frozen_band"
    trigger_type: str = "diagnostic_touch"
    admission_policy_version: str = "diagnostic_v1"
    stop_policy: str = "shared_adaptive_stop"
    exit_policy: str = "shared_exit_state_machine"
    feature_set: tuple[str, ...] = ("close", "vwap", "volume")
    enabled_vehicles: tuple[str, ...] = ("futures",)
    execution_mode: ExecutionMode = ExecutionMode.DIAGNOSTIC

    def __post_init__(self) -> None:
        if self.interaction_band <= 0:
            raise ValueError("interaction band must be positive")
        if self.response_timeframe not in self.enabled_timeframes:
            raise ValueError("response timeframe must be enabled")
        if not self.feature_set or len(self.feature_set) != len(set(self.feature_set)):
            raise ValueError("orderflow feature_set must be non-empty and unique")
        if "futures" not in self.enabled_vehicles:
            raise ValueError("orderflow requires futures as the enabled vehicle")
        if self.execution_mode is not ExecutionMode.DIAGNOSTIC:
            raise ValueError("executable orderflow policy is deferred to a later phase")

    @property
    def config_hash(self) -> str:
        payload = asdict(self)
        payload["execution_mode"] = self.execution_mode.value
        return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class InteractionState:
    interaction_id: str
    timeframe: str
    instrument: str
    reference_level: float
    lower_band: float
    upper_band: float
    source_observation_id: str
    state: str
    response_state: str
    generation: int | None

    def __post_init__(self) -> None:
        if self.state not in {"CREATED", "ACTIVE", "ENDED"}:
            raise ValueError("invalid orderflow interaction state")
        if self.response_state not in {"PENDING_RESPONSE", "CONFIRMED", "INVALIDATED", "EXPIRED"}:
            raise ValueError("invalid orderflow response state")
        if not self.source_observation_id:
            raise ValueError("orderflow interaction source observation is required")
        if not self.instrument.strip():
            raise ValueError("orderflow interaction instrument is required")
        if not all(math.isfinite(value) for value in (self.reference_level, self.lower_band, self.upper_band)):
            raise ValueError("orderflow interaction geometry must be finite")
        if self.lower_band > self.reference_level or self.reference_level > self.upper_band:
            raise ValueError("orderflow interaction band is inconsistent")


def _number(observation: OrderflowObservation, name: str) -> float | None:
    value = observation.features.get(name)
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


class OrderflowPolicy:
    def __init__(self, config: OrderflowConfig = OrderflowConfig()) -> None:
        self.config = config
        self._interactions: dict[str, InteractionState] = {}
        self._session_id: str | None = None
        self._processed_observation_ids: set[str] = set()

    @property
    def execution_mode(self) -> ExecutionMode:
        return self.config.execution_mode

    @property
    def required_features(self) -> tuple[FeatureRequirement, ...]:
        return tuple(
            FeatureRequirement(name, timeframe=self.config.response_timeframe)
            for name in self.config.feature_set
        )

    def evaluate(self, bundle: DecisionBundle, context: DecisionContext) -> tuple[PolicyDecision, ...]:
        orderflow = bundle.orderflow_context
        base = {
            "strategy_name": self.config.name,
            "strategy_version": self.config.version,
            "config_hash": self.config.config_hash,
            "bundle_id": bundle.bundle_id,
            "decision_at": context.as_of.isoformat(),
            "availability_cutoff": orderflow.cutoff.isoformat() if orderflow else None,
            "feature_version": self.config.vwap_calculation_version,
        }
        if orderflow is None or orderflow.session_id != bundle.trading_date:
            return (PolicyDecision({**base, "outcome": "rejected", "reason": "orderflow_context_unavailable"}),)
        if self._session_id != orderflow.session_id:
            self._interactions.clear()
            self._processed_observation_ids.clear()
            self._session_id = orderflow.session_id

        observations = orderflow.observations_for(self.config.response_timeframe)
        if not observations:
            return (PolicyDecision({**base, "outcome": "rejected", "reason": "required_features_unavailable"}),)

        results: list[PolicyDecision] = []
        futures = bundle.bars.get(MarketRole.FUTURES)
        expected_instrument = futures.instrument.symbol if futures is not None else None
        for observation in observations:
            if observation.observation_id in self._processed_observation_ids:
                continue
            if observation.timeframe != self.config.response_timeframe:
                continue
            if expected_instrument is not None and observation.instrument != expected_instrument:
                continue
            if observation.available_at > min(context.as_of, orderflow.cutoff):
                continue
            if observation.context is None or observation.context.session_id != orderflow.session_id:
                continue
            if observation.feature_version != self.config.vwap_calculation_version:
                continue
            if not orderflow.validate_complete_parent(observation):
                continue
            self._processed_observation_ids.add(observation.observation_id)
            vwap = _number(observation, "vwap")
            close = _number(observation, "close")
            if vwap is None or close is None:
                results.append(PolicyDecision({
                    **base,
                    "outcome": "rejected",
                    "reason": "required_feature_missing",
                    "candidate_id": observation.observation_id,
                    "source_observation_ids": [observation.observation_id],
                }))
                continue
            trigger = observe_trigger(
                observation,
                boundary=vwap,
                execution_mode=self.execution_mode.value,
                trigger_type=self.config.trigger_type,
            )
            provenance = {
                "source_observation_id": observation.source_observation_id,
                "source_observation_ids": [observation.observation_id],
                "response_evidence_ids": list(observation.evidence_ids),
                "timeframe": observation.timeframe,
                "instrument": observation.instrument,
                "generation": observation.context.generation if observation.context else None,
                "replay_provenance": dict(observation.context.replay_provenance) if observation.context else {},
                "trigger": asdict(trigger),
            }
            generation = observation.context.generation if observation.context else None
            previous = next(
                (
                    item for item in self._interactions.values()
                    if item.timeframe == observation.timeframe
                    and item.instrument == observation.instrument
                    and item.generation == generation
                    and item.state == "ACTIVE"
                ),
                None,
            )
            inside_band = (
                previous.lower_band <= close <= previous.upper_band
                if previous is not None
                else abs(close - vwap) <= self.config.interaction_band
            )
            if previous is None and not inside_band:
                continue
            interaction_id = previous.interaction_id if previous is not None else hashlib.sha256(
                f"{observation.timeframe}:{generation}:{observation.observation_id}:{self.config.config_hash}".encode()
            ).hexdigest()[:24]
            if previous is None:
                previous = InteractionState(
                    interaction_id=interaction_id,
                    timeframe=observation.timeframe,
                    instrument=observation.instrument,
                    reference_level=vwap,
                    lower_band=vwap - self.config.interaction_band,
                    upper_band=vwap + self.config.interaction_band,
                    source_observation_id=observation.observation_id,
                    state="CREATED",
                    response_state="PENDING_RESPONSE",
                    generation=observation.context.generation if observation.context else None,
                )
                self._interactions[interaction_id] = previous
            if not inside_band:
                ended = replace(previous, state="ENDED", response_state=(
                    "EXPIRED" if previous.response_state == "PENDING_RESPONSE" else previous.response_state
                ))
                self._interactions[interaction_id] = ended
                results.append(PolicyDecision({
                    **base,
                    **provenance,
                    "outcome": "rejected",
                    "reason": "interaction_ended",
                    "candidate_id": interaction_id,
                    "interaction_id": interaction_id,
                    "interaction_state": ended.state,
                    "response_state": ended.response_state,
                    "source_observation_ids": [observation.observation_id],
                }))
                continue
            previous = replace(previous, state="ACTIVE")
            if observation.response_status == "recovery":
                previous = replace(previous, response_state="INVALIDATED")
                self._interactions[interaction_id] = previous
                results.append(PolicyDecision({
                    **base,
                    **provenance,
                    "outcome": "rejected",
                    "reason": "recovery_response_not_in_initial_hypothesis",
                    "candidate_id": interaction_id,
                    "interaction_id": interaction_id,
                    "response_state": previous.response_state,
                }))
                continue
            if observation.response_status != "confirmed_rejection":
                self._interactions[interaction_id] = previous
                results.append(PolicyDecision({
                    **base,
                    **provenance,
                    "outcome": "rejected",
                    "reason": "response_pending",
                    "candidate_id": interaction_id,
                    "interaction_id": interaction_id,
                    "response_state": "PENDING_RESPONSE",
                }))
                continue
            previous = replace(previous, response_state="CONFIRMED")
            self._interactions[interaction_id] = previous
            payload = {
                **base,
                **provenance,
                "outcome": "accepted",
                "reason": "diagnostic_vwap_interaction",
                "candidate_id": interaction_id,
                "opportunity_id": interaction_id,
                "interaction_id": interaction_id,
                "source_observation_ids": [observation.observation_id],
                "response_evidence_ids": list(observation.evidence_ids),
                "template_id": "vwap_rejection_diagnostic",
                "template_version": "1",
                "feature_values": dict(observation.features),
                "trigger": asdict(trigger),
                "interaction_state": previous.state,
                "response_state": previous.response_state,
            }
            results.append(PolicyDecision(payload))
        if not results and all(observation.observation_id in self._processed_observation_ids for observation in observations):
            return ()
        return tuple(results) or (
            PolicyDecision({**base, "outcome": "rejected", "reason": "no_vwap_interaction"}),
        )

    def on_execution_event(self, event: ExecutionNotification) -> None:
        # Diagnostic mode cannot own execution state.
        return None

    def snapshot(self) -> Mapping[str, object]:
        return {
            "schema_version": 1,
            "config": asdict(self.config) | {"execution_mode": self.config.execution_mode.value},
            "interactions": [asdict(self._interactions[key]) for key in sorted(self._interactions)],
            "session_id": self._session_id,
            "processed_observation_ids": sorted(self._processed_observation_ids),
        }

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, object]) -> "OrderflowPolicy":
        if snapshot.get("schema_version") != 1:
            raise ValueError("unsupported orderflow snapshot schema")
        raw_config = snapshot.get("config")
        if not isinstance(raw_config, Mapping):
            raise ValueError("orderflow snapshot config must be an object")
        values = dict(raw_config)
        allowed_config = set(OrderflowConfig.__dataclass_fields__)
        if set(values) != allowed_config:
            raise ValueError("orderflow snapshot config fields are invalid")
        values["execution_mode"] = ExecutionMode(values.get("execution_mode", "diagnostic"))
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
        raw_interactions = snapshot.get("interactions", ())
        if not isinstance(raw_interactions, (list, tuple)):
            raise ValueError("orderflow snapshot interactions must be a sequence")
        seen_ids: set[str] = set()
        allowed_interaction = set(InteractionState.__dataclass_fields__)
        for item in raw_interactions:
            if not isinstance(item, Mapping):
                raise ValueError("orderflow snapshot interaction must be an object")
            if set(item) != allowed_interaction:
                raise ValueError("orderflow snapshot interaction fields are invalid")
            try:
                interaction = InteractionState(**dict(item))
            except (TypeError, ValueError) as exc:
                raise ValueError("invalid orderflow snapshot interaction") from exc
            if not interaction.interaction_id or interaction.interaction_id in seen_ids:
                raise ValueError("duplicate orderflow snapshot interaction ID")
            seen_ids.add(interaction.interaction_id)
            policy._interactions[interaction.interaction_id] = interaction
        return policy


class OrderflowStrategy:
    """Strategy facade preserving the public Strategy protocol."""

    def __init__(self, config: OrderflowConfig = OrderflowConfig()) -> None:
        self.policy = OrderflowPolicy(config)
        self.coordinator = SharedDecisionCoordinator(self.policy)

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

    def snapshot(self) -> Mapping[str, object]:
        return wrap_snapshot(
            strategy_id="orderflow-research",
            metadata={"name": self.metadata.name, "version": self.metadata.version,
                      "config_hash": self.metadata.config_hash},
            payload=self.policy.snapshot(),
        )

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, object], *, capital_profile=None) -> "OrderflowStrategy":
        _, payload = unwrap_snapshot(snapshot)
        strategy = cls.__new__(cls)
        strategy.policy = OrderflowPolicy.from_snapshot(payload)
        strategy.coordinator = SharedDecisionCoordinator(strategy.policy)
        return strategy


__all__ = [
    "InteractionState", "OrderflowConfig", "OrderflowPolicy", "OrderflowStrategy",
    "ORDERFLOW_NAME", "ORDERFLOW_VERSION",
]
