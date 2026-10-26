"""Runtime-owned conversion from policy decisions to public decision events."""

from __future__ import annotations

from collections.abc import Mapping
from hashlib import sha256
from typing import TYPE_CHECKING

from ftx_paper.domain.decision_context import DecisionContext
from ftx_paper.execution.events import ExecutionNotification
from .policy_contract import ExecutionMode, StrategyPolicy

if TYPE_CHECKING:
    from ftx_paper.market.bundles import DecisionBundle
    from ftx_paper.strategy.decision import DecisionEvent


class SharedDecisionCoordinator:
    """Apply shared policy boundary rules without owning strategy hypotheses."""

    def __init__(self, policy: StrategyPolicy) -> None:
        self.policy = policy

    def evaluate(
        self,
        bundle: DecisionBundle,
        context: DecisionContext,
    ) -> tuple[DecisionEvent, ...]:
        from ftx_paper.strategy.decision import CandidateDecision, LiveDecision

        decisions = self.policy.evaluate(bundle, context)
        result: list[DecisionEvent] = []
        for decision in decisions:
            if isinstance(decision, (LiveDecision, CandidateDecision)):
                order = getattr(decision, "order", None)
                payload_order = getattr(decision, "payload", {}).get("order")
                if self.policy.execution_mode is ExecutionMode.DIAGNOSTIC and (order is not None or payload_order is not None):
                    payload = dict(decision.payload)
                    payload.update({
                        "outcome": "rejected",
                        "reason": "diagnostic_policy_order_forbidden",
                    })
                    payload.pop("order", None)
                    if not payload.get("decision_id") and not payload.get("candidate_id"):
                        payload["decision_id"] = sha256(
                            f"{bundle.bundle_id}:diagnostic_policy_order_forbidden".encode()
                        ).hexdigest()[:24]
                    result.append(CandidateDecision(payload, event_type="CANDIDATEDECISION"))
                    continue
                result.append(decision)
                continue
            payload = dict(decision.payload)
            payload.setdefault("bundle_id", bundle.bundle_id)
            payload.setdefault("decision_at", context.as_of.isoformat())
            if not payload.get("decision_id") and not payload.get("candidate_id"):
                reason = str(payload.get("reason", decision.event_type))
                payload["decision_id"] = sha256(
                    f"{bundle.bundle_id}:{decision.event_type}:{reason}".encode()
                ).hexdigest()[:24]
            order = decision.order
            if self.policy.execution_mode is ExecutionMode.DIAGNOSTIC and (
                order is not None or payload.get("order") is not None
            ):
                payload["outcome"] = "rejected"
                payload["reason"] = "diagnostic_policy_order_forbidden"
                payload.pop("order", None)
                order = None
            result.append(CandidateDecision(payload, order=order, event_type=decision.event_type))
        return tuple(result)

    def on_execution_event(self, event: ExecutionNotification) -> None:
        self.policy.on_execution_event(event)

    def snapshot(self) -> Mapping[str, object]:
        return self.policy.snapshot()


__all__ = ["SharedDecisionCoordinator"]
