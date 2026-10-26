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
    from .orderflow_contract import OrderflowContext, OrderflowObservation, OrderflowObservationContext
    from .orderflow_trigger import TriggerObservation, observe_trigger
    from .orderflow_producer import CausalOrderflowProducer
    from .research_policy import ResearchPolicyAdapter
    from .snapshot_envelope import unwrap_snapshot, wrap_snapshot
    from .policy_contract import ExecutionMode, PolicyDecision, StrategyPolicy


def __getattr__(name: str):
    if name == "ConfiguredLiveStrategy":
        from .live import ConfiguredLiveStrategy
        return ConfiguredLiveStrategy
    if name in {"ConfiguredStrategyFactory", "StrategyFactory"}:
        from .factory import ConfiguredStrategyFactory, StrategyFactory
        return {"ConfiguredStrategyFactory": ConfiguredStrategyFactory, "StrategyFactory": StrategyFactory}[name]
    if name in {"OrderflowContext", "OrderflowObservation", "OrderflowObservationContext"}:
        from .orderflow_contract import OrderflowContext, OrderflowObservation, OrderflowObservationContext
        return {"OrderflowContext": OrderflowContext, "OrderflowObservation": OrderflowObservation,
                "OrderflowObservationContext": OrderflowObservationContext}[name]
    if name in {"ExecutionMode", "FeatureRequirement", "PolicyDecision", "StrategyPolicy"}:
        from .policy_contract import ExecutionMode, FeatureRequirement, PolicyDecision, StrategyPolicy
        return {"ExecutionMode": ExecutionMode, "FeatureRequirement": FeatureRequirement,
                "PolicyDecision": PolicyDecision, "StrategyPolicy": StrategyPolicy}[name]
    if name == "SharedDecisionCoordinator":
        from .coordinator import SharedDecisionCoordinator
        return SharedDecisionCoordinator
    if name in {"TriggerObservation", "observe_trigger"}:
        from .orderflow_trigger import TriggerObservation, observe_trigger
        return {"TriggerObservation": TriggerObservation, "observe_trigger": observe_trigger}[name]
    if name == "CausalOrderflowProducer":
        from .orderflow_producer import CausalOrderflowProducer
        return CausalOrderflowProducer
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
    "ExecutionMode", "FeatureRequirement", "PolicyDecision", "StrategyPolicy",
    "SharedDecisionCoordinator", "feature_union",
    "OrderflowConfig", "OrderflowPolicy", "OrderflowStrategy",
    "TriggerObservation", "observe_trigger",
    "CausalOrderflowProducer",
    "ResearchPolicyAdapter",
    "unwrap_snapshot", "wrap_snapshot",
    "STRATEGY_NAME", "STRATEGY_VERSION",
]
