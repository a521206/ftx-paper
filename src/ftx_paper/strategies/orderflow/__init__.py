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

STRATEGY_ID = ORDERFLOW_NAME

__all__ = ["ORDERFLOW_NAME", "ORDERFLOW_VERSION", "OrderflowConfig", "OrderflowStrategy", "OrderflowStrategyFactory", "STRATEGY_ID", "BoundaryEvent", "BoundaryState", "CompletedObservation", "LiveInputAdapter", "ReplayInputAdapter", "RuntimeBoundaryArtifact", "build_runtime_boundary_artifact", "transition_one", "MarketObservation", "ObservationGap", "observe_market", "LiveSourceAdapter", "ReplaySourceAdapter", "completed_observations", "canonical_source_rows", "source_fingerprint", "source_metadata"]
