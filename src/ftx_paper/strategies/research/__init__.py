"""Implementation package for the isolated FTX research strategy."""

from .live import ConfiguredLiveStrategy
from .factory import ConfiguredStrategyFactory, StrategyFactory
from .admission import admission_result, build_admission_features
from .risk_state import RiskGateState
from .risk import RiskAssessment, RiskConfig, RiskDecision, RiskEngine
from .sizing import SizingPipeline, SizingPipelineInput
from .decision import CandidateDecision, IndependentLiveDecisionEngine, LiveDecision
from ftx_paper.strategies.research.config import STRATEGY_NAME, STRATEGY_VERSION
from ftx_paper.strategies.orderflow.orderflow_capture import CaptureMode, ReplayRecord, ReplayRecorder
from ftx_paper.strategies.orderflow.orderflow_contract import (
    OrderflowContext, OrderflowObservation, OrderflowObservationContext, UnavailableReason,
    UnavailableValue, feature_values_to_dict, stable_orderflow_id,
)
from ftx_paper.strategies.orderflow.orderflow_observation import observe_market
from ftx_paper.strategies.orderflow.orderflow_source import prepare_replay_input, SourceIdentity
from ftx_paper.strategies.orderflow.orderflow_producer import CausalOrderflowProducer, source_fingerprint

STRATEGY_ID = STRATEGY_NAME

__all__ = [
    "ConfiguredLiveStrategy", "ConfiguredStrategyFactory", "StrategyFactory", "STRATEGY_ID", "STRATEGY_NAME", "STRATEGY_VERSION",
    "admission_result", "build_admission_features", "RiskGateState", "RiskAssessment", "RiskConfig", "RiskDecision", "RiskEngine",
    "SizingPipeline", "SizingPipelineInput", "CandidateDecision", "IndependentLiveDecisionEngine", "LiveDecision",
    "CaptureMode", "ReplayRecord", "ReplayRecorder", "observe_market", "prepare_replay_input", "SourceIdentity", "CausalOrderflowProducer", "source_fingerprint",
    "OrderflowContext", "OrderflowObservation", "OrderflowObservationContext", "UnavailableReason", "UnavailableValue", "feature_values_to_dict", "stable_orderflow_id",
]
