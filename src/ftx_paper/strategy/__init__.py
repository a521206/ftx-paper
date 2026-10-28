from typing import TYPE_CHECKING

from .config import STRATEGY_NAME, STRATEGY_VERSION
from ftx_paper.market.requirements import FeatureRequirement


def feature_union(policies):
    from .policy_contract import feature_union as _feature_union
    return _feature_union(policies)

if TYPE_CHECKING:
    from .live import ConfiguredLiveStrategy
    from .factory import ConfiguredStrategyFactory, StrategyFactory
    from .coordinator import SharedDecisionCoordinator
    from .orderflow import OrderflowConfig, OrderflowPolicy, OrderflowStrategy
    from .orderflow_contract import (
        OrderflowContext, OrderflowObservation, OrderflowObservationContext,
        UnavailableReason, UnavailableValue, feature_values_to_dict, stable_orderflow_id,
    )
    from .orderflow_trigger import TriggerObservation, observe_trigger
    from .orderflow_producer import CausalOrderflowProducer
    from .orderflow_source import (
        OrderflowReplayInput, OrderflowSourceMetadata, SourceIdentity, normalize_session_date,
        prepare_replay_input, source_fingerprint,
    )
    from .orderflow_observation import (
        MarketObservation, ObservationBoundary, ObservationGap, ObservationIssue,
        observe_market, rows_available_for_observation,
    )
    from .orderflow_context import (
        ContextHistory, MarketContext, ContextVersionRef, ContextRelation,
        OpeningGapState, SessionLevels, ValueReference,
        advance_opening_gap, classify_opening_gap, derive_session_levels,
        permission_for_context, classify_context_relation, admission_recovery_available,
    )
    from .orderflow_location import LocationBand, LocationObservation, LocationRecord, LocationRegistry
    from .orderflow_vwap import (
        MarketAggregate, VwapCalculationConfig, VwapMeasurement, VwapStopResult, VwapLocationProvider,
        calculate_session_vwap, calculate_vwap_stop, derive_vwap_band,
        evaluate_vwap_diagnostic_response, publish_vwap_locations,
    )
    from .orderflow_interaction import (
        HypothesisState, InteractionCandle, InteractionCloseReason, InteractionDetector,
        InteractionStatus, LocationInteraction,
    )
    from .orderflow_response import (
        ResponseEvaluation, ResponseHypothesis, ResponseOutcomeStatus,
        evaluate_response_hypotheses, evaluate_response_window,
    )
    from .orderflow_opportunity import (
        ExecutionGroupClaim, ExecutionGroupRegistry, OpportunityStatus,
        OpportunityTransition, SetupOpportunity, StructuralSetup,
    )
    from .orderflow_admission import (
        AdmissionDecision, DiagnosticDecisionIntent, PredicateResult,
        PredicateStatus, evaluate_admission,
    )
    from .orderflow_capture import CaptureMode, REPLAY_RECORD_SCHEMA_VERSION, ReplayRecord, ReplayRecorder, validate_replay_scope
    from .orderflow_replay import CANONICAL_PHASE_ORDER, OrderflowRuntimeState, ReplayBarResult, ReplayMachine, ReplayPhase
    from .orderflow_reporting import CAUSAL_CHECKPOINT_PHASES, build_multi_session_report, build_session_summary, causal_actions, first_divergent_phase
    from .research_policy import ResearchPolicyAdapter
    from .snapshot_envelope import unwrap_snapshot, wrap_snapshot
    from .policy_contract import ExecutionMode, PolicyDecision, StrategyPolicy
    from .templates import Direction, LocationTemplate, OpeningInteractionKind, ResponseType, TradeTemplate, TriggerType
    from .response import ResponseCandle, ResponseSpec, build_response_spec
    from .opportunity import OpportunityContext, OpportunityOutcome, OpportunityOutcomeStatus, opportunity_identity


def __getattr__(name: str):
    if name == "ConfiguredLiveStrategy":
        from .live import ConfiguredLiveStrategy
        return ConfiguredLiveStrategy
    if name in {"ConfiguredStrategyFactory", "StrategyFactory"}:
        from .factory import ConfiguredStrategyFactory, StrategyFactory
        return {"ConfiguredStrategyFactory": ConfiguredStrategyFactory, "StrategyFactory": StrategyFactory}[name]
    if name in {"OrderflowContext", "OrderflowObservation", "OrderflowObservationContext", "UnavailableReason", "UnavailableValue", "feature_values_to_dict", "stable_orderflow_id"}:
        from .orderflow_contract import (
            OrderflowContext, OrderflowObservation, OrderflowObservationContext,
            UnavailableReason, UnavailableValue, feature_values_to_dict, stable_orderflow_id,
        )
        return {"OrderflowContext": OrderflowContext, "OrderflowObservation": OrderflowObservation,
                "OrderflowObservationContext": OrderflowObservationContext,
                "UnavailableReason": UnavailableReason, "UnavailableValue": UnavailableValue,
                "feature_values_to_dict": feature_values_to_dict,
                "stable_orderflow_id": stable_orderflow_id}[name]
    if name in {"ExecutionMode", "FeatureRequirement", "PolicyDecision", "StrategyPolicy"}:
        from .policy_contract import ExecutionMode, FeatureRequirement, PolicyDecision, StrategyPolicy
        return {"ExecutionMode": ExecutionMode, "FeatureRequirement": FeatureRequirement,
                "PolicyDecision": PolicyDecision, "StrategyPolicy": StrategyPolicy}[name]
    if name in {"Direction", "LocationTemplate", "OpeningInteractionKind", "ResponseType", "TradeTemplate", "TriggerType"}:
        from .templates import Direction, LocationTemplate, OpeningInteractionKind, ResponseType, TradeTemplate, TriggerType
        return {"Direction": Direction, "LocationTemplate": LocationTemplate, "OpeningInteractionKind": OpeningInteractionKind, "ResponseType": ResponseType, "TradeTemplate": TradeTemplate, "TriggerType": TriggerType}[name]
    if name in {"ResponseCandle", "ResponseSpec", "build_response_spec", "evaluate_policy_responses"}:
        from .response import ResponseCandle, ResponseSpec, build_response_spec
        from .orderflow_response import evaluate_policy_responses
        return {"ResponseCandle": ResponseCandle, "ResponseSpec": ResponseSpec, "build_response_spec": build_response_spec, "evaluate_policy_responses": evaluate_policy_responses}[name]
    if name in {"OpportunityContext", "OpportunityOutcome", "OpportunityOutcomeStatus", "opportunity_identity"}:
        from .opportunity import OpportunityContext, OpportunityOutcome, OpportunityOutcomeStatus, opportunity_identity
        return {"OpportunityContext": OpportunityContext, "OpportunityOutcome": OpportunityOutcome, "OpportunityOutcomeStatus": OpportunityOutcomeStatus, "opportunity_identity": opportunity_identity}[name]
    if name in {"LocationBand", "LocationObservation", "LocationRecord", "LocationRegistry"}:
        from .orderflow_location import LocationBand, LocationObservation, LocationRecord, LocationRegistry
        return {"LocationBand": LocationBand, "LocationObservation": LocationObservation,
                 "LocationRecord": LocationRecord, "LocationRegistry": LocationRegistry}[name]
    if name in {"MarketAggregate", "VwapCalculationConfig", "VwapMeasurement", "VwapStopResult", "VwapLocationProvider", "calculate_session_vwap", "calculate_vwap_stop", "derive_vwap_band", "evaluate_vwap_diagnostic_response", "publish_vwap_locations"}:
        from .orderflow_vwap import (
            MarketAggregate, VwapCalculationConfig, VwapMeasurement, VwapStopResult, VwapLocationProvider,
            calculate_session_vwap, calculate_vwap_stop, derive_vwap_band,
            evaluate_vwap_diagnostic_response, publish_vwap_locations,
        )
        return {
            "MarketAggregate": MarketAggregate, "VwapCalculationConfig": VwapCalculationConfig,
            "VwapMeasurement": VwapMeasurement, "VwapStopResult": VwapStopResult,
            "VwapLocationProvider": VwapLocationProvider,
            "calculate_session_vwap": calculate_session_vwap, "calculate_vwap_stop": calculate_vwap_stop,
            "derive_vwap_band": derive_vwap_band,
            "evaluate_vwap_diagnostic_response": evaluate_vwap_diagnostic_response,
            "publish_vwap_locations": publish_vwap_locations,
        }[name]
    if name == "SharedDecisionCoordinator":
        from .coordinator import SharedDecisionCoordinator
        return SharedDecisionCoordinator
    if name in {"TriggerObservation", "observe_trigger"}:
        from .orderflow_trigger import TriggerObservation, observe_trigger
        return {"TriggerObservation": TriggerObservation, "observe_trigger": observe_trigger}[name]
    if name == "CausalOrderflowProducer":
        from .orderflow_producer import CausalOrderflowProducer
        return CausalOrderflowProducer
    if name in {
        "OrderflowReplayInput", "OrderflowSourceMetadata", "SourceIdentity", "normalize_session_date",
        "prepare_replay_input", "source_fingerprint",
    }:
        from .orderflow_source import (
            OrderflowReplayInput, OrderflowSourceMetadata, SourceIdentity, normalize_session_date,
            prepare_replay_input, source_fingerprint,
        )
        return {
            "OrderflowReplayInput": OrderflowReplayInput,
            "OrderflowSourceMetadata": OrderflowSourceMetadata,
            "SourceIdentity": SourceIdentity,
            "normalize_session_date": normalize_session_date,
            "prepare_replay_input": prepare_replay_input,
            "source_fingerprint": source_fingerprint,
        }[name]
    if name in {"MarketObservation", "ObservationBoundary", "ObservationGap", "ObservationIssue", "observe_market", "rows_available_for_observation"}:
        from .orderflow_observation import (
            MarketObservation, ObservationBoundary, ObservationGap, ObservationIssue,
            observe_market, rows_available_for_observation,
        )
        return {
            "MarketObservation": MarketObservation,
            "ObservationBoundary": ObservationBoundary,
            "ObservationGap": ObservationGap,
            "ObservationIssue": ObservationIssue,
            "observe_market": observe_market,
            "rows_available_for_observation": rows_available_for_observation,
        }[name]
    if name in {"ContextHistory", "MarketContext", "ContextVersionRef", "ContextRelation", "OpeningGapState", "SessionLevels", "ValueReference", "advance_opening_gap", "classify_opening_gap", "derive_session_levels", "permission_for_context", "classify_context_relation", "admission_recovery_available"}:
        from .orderflow_context import (
            ContextHistory, MarketContext, ContextVersionRef, ContextRelation,
            OpeningGapState, SessionLevels, ValueReference,
            advance_opening_gap, classify_opening_gap, derive_session_levels,
            permission_for_context, classify_context_relation, admission_recovery_available,
        )
        return {
            "ContextHistory": ContextHistory,
            "MarketContext": MarketContext,
            "ContextVersionRef": ContextVersionRef,
            "ContextRelation": ContextRelation,
            "OpeningGapState": OpeningGapState,
            "SessionLevels": SessionLevels,
            "ValueReference": ValueReference,
            "advance_opening_gap": advance_opening_gap,
            "classify_opening_gap": classify_opening_gap,
            "derive_session_levels": derive_session_levels,
            "permission_for_context": permission_for_context,
            "classify_context_relation": classify_context_relation,
            "admission_recovery_available": admission_recovery_available,
        }[name]
    if name in {"LocationBand", "LocationObservation", "LocationRecord", "LocationRegistry"}:
        from .orderflow_location import LocationBand, LocationObservation, LocationRecord, LocationRegistry
        return {"LocationBand": LocationBand, "LocationObservation": LocationObservation,
                "LocationRecord": LocationRecord, "LocationRegistry": LocationRegistry}[name]
    if name in {"HypothesisState", "InteractionCandle", "InteractionCloseReason", "InteractionDetector", "InteractionStatus", "LocationInteraction"}:
        from .orderflow_interaction import (
            HypothesisState, InteractionCandle, InteractionCloseReason, InteractionDetector,
            InteractionStatus, LocationInteraction,
        )
        return {
            "HypothesisState": HypothesisState,
            "InteractionCandle": InteractionCandle,
            "InteractionCloseReason": InteractionCloseReason,
            "InteractionDetector": InteractionDetector,
            "InteractionStatus": InteractionStatus,
            "LocationInteraction": LocationInteraction,
        }[name]
    if name in {"ResponseEvaluation", "ResponseHypothesis", "ResponseOutcomeStatus", "evaluate_response_hypotheses", "evaluate_response_window", "select_response_hypotheses"}:
        from .orderflow_response import (
            ResponseEvaluation, ResponseHypothesis, ResponseOutcomeStatus,
            evaluate_response_hypotheses, evaluate_response_window, select_response_hypotheses,
        )
        return {
            "ResponseEvaluation": ResponseEvaluation,
            "ResponseHypothesis": ResponseHypothesis,
            "ResponseOutcomeStatus": ResponseOutcomeStatus,
            "evaluate_response_hypotheses": evaluate_response_hypotheses,
            "evaluate_response_window": evaluate_response_window,
            "select_response_hypotheses": select_response_hypotheses,
        }[name]
    if name in {"ExecutionGroupClaim", "ExecutionGroupRegistry", "OpportunityStatus", "OpportunityTransition", "SetupOpportunity", "StructuralSetup", "advance_timeouts_before_formation"}:
        from .orderflow_opportunity import (
            ExecutionGroupClaim, ExecutionGroupRegistry, OpportunityStatus,
            OpportunityTransition, SetupOpportunity, StructuralSetup, advance_timeouts_before_formation,
        )
        return {"ExecutionGroupClaim": ExecutionGroupClaim, "ExecutionGroupRegistry": ExecutionGroupRegistry, "OpportunityStatus": OpportunityStatus, "OpportunityTransition": OpportunityTransition, "SetupOpportunity": SetupOpportunity, "StructuralSetup": StructuralSetup, "advance_timeouts_before_formation": advance_timeouts_before_formation}[name]
    if name in {"AdmissionDecision", "DiagnosticDecisionIntent", "PredicateResult", "PredicateStatus", "can_requalify", "evaluate_admission"}:
        from .orderflow_admission import (
            AdmissionDecision, DiagnosticDecisionIntent, PredicateResult,
            PredicateStatus, can_requalify, evaluate_admission,
        )
        return {
            "AdmissionDecision": AdmissionDecision,
            "DiagnosticDecisionIntent": DiagnosticDecisionIntent,
            "PredicateResult": PredicateResult,
            "PredicateStatus": PredicateStatus,
            "can_requalify": can_requalify,
            "evaluate_admission": evaluate_admission,
        }[name]
    if name in {"CaptureMode", "REPLAY_RECORD_SCHEMA_VERSION", "ReplayRecord", "ReplayRecorder", "validate_replay_scope"}:
        from .orderflow_capture import CaptureMode, REPLAY_RECORD_SCHEMA_VERSION, ReplayRecord, ReplayRecorder, validate_replay_scope
        return {"CaptureMode": CaptureMode, "REPLAY_RECORD_SCHEMA_VERSION": REPLAY_RECORD_SCHEMA_VERSION, "ReplayRecord": ReplayRecord, "ReplayRecorder": ReplayRecorder, "validate_replay_scope": validate_replay_scope}[name]
    if name in {"CANONICAL_PHASE_ORDER", "OrderflowRuntimeState", "ReplayBarResult", "ReplayMachine", "ReplayPhase"}:
        from .orderflow_replay import CANONICAL_PHASE_ORDER, OrderflowRuntimeState, ReplayBarResult, ReplayMachine, ReplayPhase
        return {
            "CANONICAL_PHASE_ORDER": CANONICAL_PHASE_ORDER,
            "OrderflowRuntimeState": OrderflowRuntimeState,
            "ReplayBarResult": ReplayBarResult,
            "ReplayMachine": ReplayMachine,
            "ReplayPhase": ReplayPhase,
        }[name]
    if name in {"CAUSAL_CHECKPOINT_PHASES", "build_multi_session_report", "build_session_summary", "causal_actions", "first_divergent_phase"}:
        from .orderflow_reporting import CAUSAL_CHECKPOINT_PHASES, build_multi_session_report, build_session_summary, causal_actions, first_divergent_phase
        return {"CAUSAL_CHECKPOINT_PHASES": CAUSAL_CHECKPOINT_PHASES, "build_multi_session_report": build_multi_session_report, "build_session_summary": build_session_summary, "causal_actions": causal_actions, "first_divergent_phase": first_divergent_phase}[name]
    if name == "ResearchPolicyAdapter":
        from .research_policy import ResearchPolicyAdapter
        return ResearchPolicyAdapter
    if name in {"unwrap_snapshot", "wrap_snapshot"}:
        from .snapshot_envelope import unwrap_snapshot, wrap_snapshot
        return {"unwrap_snapshot": unwrap_snapshot, "wrap_snapshot": wrap_snapshot}[name]
    if name in {"OrderflowConfig", "OrderflowPolicy", "OrderflowStrategy"}:
        from .orderflow import OrderflowConfig, OrderflowPolicy, OrderflowStrategy
        return {"OrderflowConfig": OrderflowConfig, "OrderflowPolicy": OrderflowPolicy,
                "OrderflowStrategy": OrderflowStrategy}[name]
    raise AttributeError(name)

__all__ = [
    "ConfiguredLiveStrategy", "ConfiguredStrategyFactory", "StrategyFactory",
    "OrderflowContext", "OrderflowObservation", "OrderflowObservationContext",
    "UnavailableReason", "UnavailableValue", "feature_values_to_dict", "stable_orderflow_id",
    "ExecutionMode", "FeatureRequirement", "PolicyDecision", "StrategyPolicy",
    "SharedDecisionCoordinator", "feature_union",
    "OrderflowConfig", "OrderflowPolicy", "OrderflowStrategy",
    "TriggerObservation", "observe_trigger",
    "CausalOrderflowProducer",
    "OrderflowReplayInput", "OrderflowSourceMetadata", "SourceIdentity", "normalize_session_date",
    "prepare_replay_input", "source_fingerprint",
    "MarketObservation", "ObservationBoundary", "ObservationGap", "ObservationIssue",
    "observe_market", "rows_available_for_observation",
    "ContextHistory", "MarketContext", "ContextVersionRef", "ContextRelation",
    "OpeningGapState", "SessionLevels", "ValueReference",
    "advance_opening_gap", "classify_opening_gap", "derive_session_levels",
    "permission_for_context", "classify_context_relation", "admission_recovery_available",
    "LocationBand", "LocationObservation", "LocationRecord", "LocationRegistry",
    "MarketAggregate", "VwapCalculationConfig", "VwapMeasurement", "VwapStopResult", "VwapLocationProvider",
    "calculate_session_vwap", "calculate_vwap_stop",
    "derive_vwap_band", "evaluate_vwap_diagnostic_response", "publish_vwap_locations",
    "HypothesisState", "InteractionCandle", "InteractionCloseReason", "InteractionDetector",
    "InteractionStatus", "LocationInteraction",
    "ResponseEvaluation", "ResponseHypothesis", "ResponseOutcomeStatus",
    "evaluate_response_hypotheses", "evaluate_response_window",
    "ExecutionGroupClaim", "ExecutionGroupRegistry", "OpportunityStatus",
    "OpportunityTransition", "SetupOpportunity", "StructuralSetup",
    "AdmissionDecision", "DiagnosticDecisionIntent", "PredicateResult", "PredicateStatus", "evaluate_admission",
    "CaptureMode", "REPLAY_RECORD_SCHEMA_VERSION", "ReplayRecord", "ReplayRecorder", "validate_replay_scope",
    "CANONICAL_PHASE_ORDER", "OrderflowRuntimeState", "ReplayBarResult", "ReplayMachine", "ReplayPhase",
    "CAUSAL_CHECKPOINT_PHASES", "build_multi_session_report", "build_session_summary", "causal_actions", "first_divergent_phase",
    "Direction", "LocationTemplate", "OpeningInteractionKind", "ResponseType", "TradeTemplate", "TriggerType",
    "ResponseCandle", "ResponseSpec", "build_response_spec",
    "OpportunityContext", "OpportunityOutcome", "OpportunityOutcomeStatus", "opportunity_identity",
    "ResearchPolicyAdapter",
    "unwrap_snapshot", "wrap_snapshot",
    "STRATEGY_NAME", "STRATEGY_VERSION",
]
