"""Implementation package for the isolated FTX research strategy."""

from .live import ConfiguredLiveStrategy
from .factory import ResearchStrategyFactory
from .admission import admission_result, build_admission_features
from .risk_state import RiskGateState
from .risk import RiskAssessment, RiskConfig, RiskDecision, RiskEngine
from .sizing import SizingPipeline, SizingPipelineInput
from .decision import CandidateDecision, IndependentLiveDecisionEngine, LiveDecision
from ftx_paper.strategies.research.config import STRATEGY_NAME, STRATEGY_VERSION

STRATEGY_ID = STRATEGY_NAME

__all__ = [
    "ConfiguredLiveStrategy", "ResearchStrategyFactory", "STRATEGY_ID", "STRATEGY_NAME", "STRATEGY_VERSION",
    "admission_result", "build_admission_features", "RiskGateState", "RiskAssessment", "RiskConfig", "RiskDecision", "RiskEngine",
    "SizingPipeline", "SizingPipelineInput", "CandidateDecision", "IndependentLiveDecisionEngine", "LiveDecision",
]
