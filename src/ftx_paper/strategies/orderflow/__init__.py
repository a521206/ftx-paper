"""Minimal orderflow strategy boundary for the canonical port."""

from .boundary.factory import OrderflowStrategyFactory
from .boundary.orderflow import OrderflowStrategy
from .boundary.orderflow_config import ORDERFLOW_NAME, ORDERFLOW_VERSION, OrderflowConfig
from .boundary.source_adapter import LiveSourceAdapter, ReplaySourceAdapter, completed_observations
from .runtime_boundary import (
    BoundaryEvent,
    BoundaryState,
    CompletedObservation,
    LiveInputAdapter,
    ReplayInputAdapter,
    RuntimeBoundaryArtifact,
    build_runtime_boundary_artifact,
    transition_one,
)
from .market_observation import MarketObservation, ObservationGap, observe_market
from .features import daily_atr
from .state_machine_source import canonical_source_rows, source_fingerprint, source_metadata
from .value_publication import PriorSessionValueReference, ValuePublication
from .parity_artifacts import PHASE_STREAMS, build_phase4_artifact, compare_phase4_manifests, phase_facts_from_payload
from .context_location_replay import ContextLocationReplay, replay_context_location, replay_context_location_artifact
from .interaction_replay import InteractionResponseReplay, replay_interaction_response
from .coordinator import OrderflowAdvance, OrderflowClockInput, OrderflowCoordinator
from .replay_pipeline import OrderflowReplayEngine
from .evidence_publication import AtrPublication, EvidencePublication, publish_phase4a_evidence
from .evidence import EvidenceAdapter, PaperFeatureAdapter, SerializedEvidenceAdapter, evidence_payload, produce_evidence, serialize_evidence
from .state_machine import ReplayMachine, ReplayState
from .timestamps import NIFTY_ZONE, UTC, nifty_label, parse_nifty_label, parse_utc, require_utc, serialize_utc

STRATEGY_ID = ORDERFLOW_NAME

__all__ = ["ORDERFLOW_NAME", "ORDERFLOW_VERSION", "OrderflowConfig", "OrderflowStrategy", "OrderflowStrategyFactory", "STRATEGY_ID", "BoundaryEvent", "BoundaryState", "CompletedObservation", "LiveInputAdapter", "ReplayInputAdapter", "RuntimeBoundaryArtifact", "build_runtime_boundary_artifact", "transition_one", "ReplayMachine", "ReplayState", "MarketObservation", "ObservationGap", "observe_market", "daily_atr", "LiveSourceAdapter", "ReplaySourceAdapter", "completed_observations", "canonical_source_rows", "source_fingerprint", "source_metadata", "PriorSessionValueReference", "ValuePublication", "AtrPublication", "EvidencePublication", "publish_phase4a_evidence", "EvidenceAdapter", "PaperFeatureAdapter", "SerializedEvidenceAdapter", "evidence_payload", "produce_evidence", "serialize_evidence", "PHASE_STREAMS", "build_phase4_artifact", "compare_phase4_manifests", "phase_facts_from_payload", "ContextLocationReplay", "replay_context_location", "replay_context_location_artifact", "InteractionResponseReplay", "replay_interaction_response", "OrderflowAdvance", "OrderflowClockInput", "OrderflowCoordinator", "OrderflowReplayEngine", "UTC", "NIFTY_ZONE", "nifty_label", "parse_nifty_label", "parse_utc", "require_utc", "serialize_utc"]
