"""Canonical orderflow strategy boundary and execution owner."""

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
from .parity_artifacts import (
    ARTIFACT_TYPE,
    CONTRACT_ID,
    CONTRACT_VERSION,
    DECISION_STAGES,
    EXECUTION_KINDS,
    LAYERS,
    LEDGER_KINDS,
    PARITY_PAYLOAD_KEYS,
    PHASE_STREAMS,
    SCHEMA_VERSION,
    build_parity_artifact,
    build_parity_artifact_from_payload,
    build_phase4_artifact,
    compare_parity_artifacts,
    compare_phase4_manifests,
    read_parity_artifact,
    read_parity_page,
    phase_facts_from_payload,
    validate_contract_declarations,
    validate_parity_artifact,
    write_parity_artifact,
)
from .phase7_projection import PHASE7_STREAMS, build_phase7_projection
from .execution_adapter import PaperOrderProposal, OrderflowExecutionAdapter
from .state_machine_types import CanonicalExecutionProposal
from .context_location_replay import ContextLocationReplay, replay_context_location, replay_context_location_artifact
from .interaction_replay import InteractionResponseReplay, replay_interaction_response
from .opportunity_coordinator import LifecycleFormation, OpportunityCoordinator
from .coordinator import OrderflowAdvance, OrderflowClockInput, OrderflowCoordinator
from .execution import ExecutionCoordinator
from .transition import OrderflowTransitionOwner, TransitionAdvance
from .evidence_publication import AtrPublication, EvidencePublication, publish_phase4a_evidence
from .evidence import EvidenceAdapter, PaperFeatureAdapter, SerializedEvidenceAdapter, evidence_payload, produce_evidence, serialize_evidence
from .state_machine import ReplayMachine, ReplayState
from .state_machine_reporting import build_replay_payload
from .timestamps import NIFTY_ZONE, UTC, nifty_label, parse_nifty_label, parse_utc, require_utc, serialize_utc

STRATEGY_ID = ORDERFLOW_NAME
__all__ = ["ORDERFLOW_NAME", "ORDERFLOW_VERSION", "OrderflowConfig", "OrderflowStrategy", "OrderflowStrategyFactory", "STRATEGY_ID", "BoundaryEvent", "BoundaryState", "CompletedObservation", "LiveInputAdapter", "ReplayInputAdapter", "RuntimeBoundaryArtifact", "build_runtime_boundary_artifact", "transition_one", "ReplayMachine", "ReplayState", "build_replay_payload", "MarketObservation", "ObservationGap", "observe_market", "daily_atr", "LiveSourceAdapter", "ReplaySourceAdapter", "completed_observations", "canonical_source_rows", "source_fingerprint", "source_metadata", "PriorSessionValueReference", "ValuePublication", "AtrPublication", "EvidencePublication", "publish_phase4a_evidence", "EvidenceAdapter", "PaperFeatureAdapter", "SerializedEvidenceAdapter", "evidence_payload", "produce_evidence", "serialize_evidence", "ARTIFACT_TYPE", "CONTRACT_ID", "CONTRACT_VERSION", "DECISION_STAGES", "EXECUTION_KINDS", "LAYERS", "LEDGER_KINDS", "PARITY_PAYLOAD_KEYS", "PHASE_STREAMS", "SCHEMA_VERSION", "build_parity_artifact", "build_parity_artifact_from_payload", "build_phase4_artifact", "compare_parity_artifacts", "compare_phase4_manifests", "read_parity_artifact", "read_parity_page", "phase_facts_from_payload", "validate_contract_declarations", "validate_parity_artifact", "write_parity_artifact", "PHASE7_STREAMS", "build_phase7_projection", "CanonicalExecutionProposal", "PaperOrderProposal", "OrderflowExecutionAdapter", "ExecutionCoordinator", "OrderflowTransitionOwner", "TransitionAdvance", "ContextLocationReplay", "replay_context_location", "replay_context_location_artifact", "InteractionResponseReplay", "replay_interaction_response", "LifecycleFormation", "OpportunityCoordinator", "OrderflowAdvance", "OrderflowClockInput", "OrderflowCoordinator", "UTC", "NIFTY_ZONE", "nifty_label", "parse_nifty_label", "parse_utc", "require_utc", "serialize_utc"]
