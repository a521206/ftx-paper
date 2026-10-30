"""Implementation package for the isolated orderflow strategy."""

from ftx_paper.strategies.orderflow.orderflow_config import ORDERFLOW_NAME, ORDERFLOW_VERSION, OrderflowConfig
from .policy import OrderflowPolicy, OrderflowStrategy, _admission_projection, _score_rejection
from .orderflow_admission import PredicateStatus, evaluate_admission
from .orderflow_opportunity import SetupOpportunity
from .orderflow_capture import CaptureMode, ReplayRecord, ReplayRecorder
from .orderflow_source import SourceIdentity, normalize_session_date, prepare_replay_input, source_fingerprint
from .orderflow_observation import observe_market
from .orderflow_producer import CausalOrderflowProducer
from .orderflow_contract import OrderflowContext, OrderflowObservation, OrderflowObservationContext, UnavailableReason, UnavailableValue, feature_values_to_dict, stable_orderflow_id
from .orderflow_location import LocationObservation
from .orderflow_context import ContextHistory, MarketContext, ContextVersionRef, Permission, RegimePermission, OpeningGapState, SessionLevels, ValueReference, advance_opening_gap, classify_opening_gap, classify_context_relation
from .orderflow_trigger import EntryPolicy, TriggerObservation, observe_trigger
from .orderflow_opportunity import IntentStatus, OpportunityStatus, PendingTrigger, TradeIntent, ExecutionGroupClaim, ExecutionGroupRegistry, OpportunityTransition, StructuralSetup, arm_intent, cancel_intent, record_trigger, advance_timeouts_before_formation
from .orderflow_vwap import MarketAggregate, VwapCalculationConfig, VwapMeasurement, VwapStopResult, VwapLocationProvider, calculate_session_vwap, calculate_vwap_stop, derive_vwap_band, evaluate_vwap_diagnostic_response, publish_vwap_locations
from ftx_paper.strategies.shared.policy_contract import ExecutionMode, FeatureRequirement, PolicyDecision, StrategyPolicy, feature_union
from ftx_paper.strategies.shared.coordinator import SharedDecisionCoordinator

STRATEGY_ID = "orderflow-research"

__all__ = [
    "ORDERFLOW_NAME",
    "ORDERFLOW_VERSION",
    "OrderflowConfig",
    "OrderflowPolicy",
    "OrderflowStrategy",
    "_score_rejection",
    "STRATEGY_ID",
    "PredicateStatus", "evaluate_admission", "SetupOpportunity", "CaptureMode", "ReplayRecord", "ReplayRecorder",
    "SourceIdentity", "normalize_session_date", "prepare_replay_input", "source_fingerprint", "observe_market", "CausalOrderflowProducer",
    "OrderflowContext", "OrderflowObservation", "OrderflowObservationContext", "UnavailableReason", "UnavailableValue", "feature_values_to_dict", "stable_orderflow_id", "LocationObservation",
    "ContextHistory", "MarketContext", "ContextVersionRef", "Permission", "RegimePermission", "OpeningGapState", "SessionLevels", "ValueReference", "advance_opening_gap", "classify_opening_gap", "classify_context_relation",
    "EntryPolicy", "TriggerObservation", "observe_trigger", "IntentStatus", "OpportunityStatus", "PendingTrigger", "TradeIntent", "ExecutionGroupClaim", "ExecutionGroupRegistry", "OpportunityTransition", "StructuralSetup", "arm_intent", "cancel_intent", "record_trigger", "advance_timeouts_before_formation",
    "MarketAggregate", "VwapCalculationConfig", "VwapMeasurement", "VwapStopResult", "VwapLocationProvider", "calculate_session_vwap", "calculate_vwap_stop", "derive_vwap_band", "evaluate_vwap_diagnostic_response", "publish_vwap_locations", "_admission_projection", "ExecutionMode", "FeatureRequirement", "PolicyDecision", "StrategyPolicy", "feature_union", "SharedDecisionCoordinator",
]
