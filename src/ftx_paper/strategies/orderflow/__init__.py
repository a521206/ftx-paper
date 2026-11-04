"""Orderflow strategy boundary and Paper execution integration."""

# This module is a public re-export facade; imported names are intentional API.
# ruff: noqa: F401

from .boundary.factory import OrderflowStrategyFactory
from .boundary.orderflow import OrderflowStrategy
from .boundary.orderflow_config import ORDERFLOW_NAME, ORDERFLOW_VERSION, OrderflowConfig
from .boundary.source_adapter import LiveSourceAdapter, ReplaySourceAdapter, completed_observations
from .runtime_boundary import (
    BoundaryEvent, BoundaryState, CompletedObservation, LiveInputAdapter,
    ReplayInputAdapter, RuntimeBoundaryArtifact, build_runtime_boundary_artifact,
    transition_one,
)
from .market_observation import MarketObservation, ObservationGap, observe_market
from .state_machine_source import canonical_source_rows, source_fingerprint, source_metadata
from .value_publication import PriorSessionValueReference, ValuePublication
from .parity_artifacts import (
    ARTIFACT_TYPE, CONTRACT_ID, CONTRACT_VERSION, DECISION_STAGES, EXECUTION_KINDS,
    LAYERS, LEDGER_KINDS, PARITY_PAYLOAD_KEYS, PHASE_STREAMS, SCHEMA_VERSION,
    build_parity_artifact, build_parity_artifact_from_payload, build_phase4_artifact,
    compare_parity_artifacts, compare_phase4_manifests, read_parity_artifact,
    read_parity_page, phase_facts_from_payload, validate_contract_declarations,
    validate_parity_artifact, write_parity_artifact,
)
from .phase7_projection import PHASE7_STREAMS, build_phase7_projection
from .execution_adapter import PaperOrderProposal, OrderflowExecutionAdapter
from .host_adapter import (
    HOST_ADAPTER_CONTRACT_ID, HOST_ADAPTER_CONTRACT_VERSION, CanonicalInputAdapter,
    CanonicalOutputAdapter, HostOutputEnvelope, LiveHostInputAdapter, MarketEvidence,
    ReplayHostInputAdapter,
)
from .state_machine_types import CanonicalExecutionProposal
from .opportunity_coordinator import LifecycleFormation, OpportunityCoordinator
from .coordinator import OrderflowAdvance, OrderflowClockInput, OrderflowCoordinator
from .execution import ExecutionCoordinator
from .transition import OrderflowTransitionOwner, TransitionAdvance
from .paper_runtime import PaperOrderflowRuntime
from .providers import CanonicalFormationProvider, CanonicalInputEnvelope, OrderflowInputStream
from .feature_store import FeatureStore
from .evidence_publication import AtrPublication, EvidencePublication, publish_phase4a_evidence
from .evidence import EvidenceAdapter, PaperFeatureAdapter, SerializedEvidenceAdapter, evidence_payload, produce_evidence, serialize_evidence
from .state_machine import ReplayState
from .state_machine_reporting import build_replay_payload
from .timestamps import NIFTY_ZONE, UTC, nifty_label, parse_nifty_label, parse_utc, require_utc, serialize_utc

STRATEGY_ID = ORDERFLOW_NAME

__all__ = [
    "ARTIFACT_TYPE", "AtrPublication", "BoundaryEvent", "BoundaryState",
    "CONTRACT_ID", "CONTRACT_VERSION", "CanonicalExecutionProposal",
    "CanonicalFormationProvider", "CanonicalInputEnvelope", "CanonicalInputAdapter",
    "CanonicalOutputAdapter", "CompletedObservation", "DECISION_STAGES",
    "EXECUTION_KINDS", "EvidenceAdapter", "EvidencePublication", "ExecutionCoordinator",
    "FeatureStore", "HOST_ADAPTER_CONTRACT_ID", "HOST_ADAPTER_CONTRACT_VERSION",
    "HostOutputEnvelope", "LAYERS", "LEDGER_KINDS", "LifecycleFormation",
    "LiveHostInputAdapter", "LiveInputAdapter", "LiveSourceAdapter", "MarketEvidence",
    "MarketObservation", "NIFTY_ZONE", "ObservationGap", "OpportunityCoordinator",
    "OrderflowAdvance", "OrderflowClockInput", "OrderflowConfig", "OrderflowCoordinator",
    "OrderflowExecutionAdapter", "OrderflowInputStream", "OrderflowStrategy",
    "OrderflowStrategyFactory", "OrderflowTransitionOwner", "ORDERFLOW_NAME",
    "ORDERFLOW_VERSION", "PARITY_PAYLOAD_KEYS", "PHASE7_STREAMS", "PHASE_STREAMS",
    "PaperFeatureAdapter", "PaperOrderProposal", "PaperOrderflowRuntime",
    "PriorSessionValueReference", "ReplayHostInputAdapter", "ReplayInputAdapter",
    "ReplaySourceAdapter", "ReplayState", "RuntimeBoundaryArtifact", "SCHEMA_VERSION",
    "STRATEGY_ID", "SerializedEvidenceAdapter", "TransitionAdvance", "UTC",
    "ValuePublication", "build_parity_artifact", "build_parity_artifact_from_payload",
    "build_phase4_artifact", "build_phase7_projection", "build_replay_payload",
    "build_runtime_boundary_artifact", "canonical_source_rows", "compare_parity_artifacts",
    "compare_phase4_manifests", "completed_observations", "evidence_payload",
    "nifty_label", "observe_market", "parse_nifty_label", "parse_utc",
    "phase_facts_from_payload", "produce_evidence", "publish_phase4a_evidence",
    "read_parity_artifact", "read_parity_page", "require_utc", "serialize_evidence",
    "serialize_utc", "source_fingerprint", "source_metadata", "transition_one",
    "validate_contract_declarations", "validate_parity_artifact", "write_parity_artifact",
]
