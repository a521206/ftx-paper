"""Minimal orderflow strategy boundary for the canonical port."""

from .orderflow import OrderflowStrategy
from .orderflow_config import ORDERFLOW_NAME, ORDERFLOW_VERSION, OrderflowConfig
from .factory import OrderflowStrategyFactory
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
from .source_adapter import LiveSourceAdapter, ReplaySourceAdapter, completed_observations
from .state_machine_source import canonical_source_rows, source_fingerprint, source_metadata
from .value_publication import PriorSessionValueAdapter, PriorSessionValueReference, ValuePublication, publish_prior_session_value
from .parity_artifacts import PHASE_STREAMS, build_phase4_artifact, compare_phase4_manifests, phase_facts_from_payload

STRATEGY_ID = ORDERFLOW_NAME

__all__ = ["ORDERFLOW_NAME", "ORDERFLOW_VERSION", "OrderflowConfig", "OrderflowStrategy", "OrderflowStrategyFactory", "STRATEGY_ID", "BoundaryEvent", "BoundaryState", "CompletedObservation", "LiveInputAdapter", "ReplayInputAdapter", "RuntimeBoundaryArtifact", "build_runtime_boundary_artifact", "transition_one", "MarketObservation", "ObservationGap", "observe_market", "LiveSourceAdapter", "ReplaySourceAdapter", "completed_observations", "canonical_source_rows", "source_fingerprint", "source_metadata", "PriorSessionValueAdapter", "PriorSessionValueReference", "ValuePublication", "publish_prior_session_value", "PHASE_STREAMS", "build_phase4_artifact", "compare_phase4_manifests", "phase_facts_from_payload"]
