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
from .features import daily_atr, derive_session_levels
from .contextual import publish_market_contexts
from .state_machine_source import canonical_source_rows, source_fingerprint, source_metadata
from .value_publication import PriorSessionValueAdapter, PriorSessionValueReference, ValuePublication, publish_prior_session_value
from .prior_session_value import PriorSessionProfile, PriorSessionValueConfig, PriorSessionValueStore, build_prior_session_profile
from .parity_artifacts import PHASE_STREAMS, build_phase4_artifact, compare_phase4_manifests, phase_facts_from_payload
from .context_location_replay import ContextLocationReplay, replay_context_location, replay_context_location_artifact
from .evidence_publication import AtrPublication, EvidencePublication, publish_phase4a_evidence
from .evidence import EvidenceAdapter, PaperFeatureAdapter, SerializedEvidenceAdapter, evidence_payload, produce_evidence, serialize_evidence

STRATEGY_ID = ORDERFLOW_NAME

__all__ = ["ORDERFLOW_NAME", "ORDERFLOW_VERSION", "OrderflowConfig", "OrderflowStrategy", "OrderflowStrategyFactory", "STRATEGY_ID", "BoundaryEvent", "BoundaryState", "CompletedObservation", "LiveInputAdapter", "ReplayInputAdapter", "RuntimeBoundaryArtifact", "build_runtime_boundary_artifact", "transition_one", "MarketObservation", "ObservationGap", "observe_market", "daily_atr", "derive_session_levels", "publish_market_contexts", "LiveSourceAdapter", "ReplaySourceAdapter", "completed_observations", "canonical_source_rows", "source_fingerprint", "source_metadata", "PriorSessionValueAdapter", "PriorSessionValueReference", "ValuePublication", "publish_prior_session_value", "PriorSessionProfile", "PriorSessionValueConfig", "PriorSessionValueStore", "build_prior_session_profile", "AtrPublication", "EvidencePublication", "publish_phase4a_evidence", "EvidenceAdapter", "PaperFeatureAdapter", "SerializedEvidenceAdapter", "evidence_payload", "produce_evidence", "serialize_evidence", "PHASE_STREAMS", "build_phase4_artifact", "compare_phase4_manifests", "phase_facts_from_payload", "ContextLocationReplay", "replay_context_location", "replay_context_location_artifact"]
