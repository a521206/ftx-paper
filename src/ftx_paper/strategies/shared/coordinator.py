"""Runtime-owned conversion from policy decisions to public decision events."""

from __future__ import annotations

from collections.abc import Mapping
from hashlib import sha256
from typing import TYPE_CHECKING

from ftx_paper.domain.decision_context import DecisionContext
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.execution.order_proposal import build_entry_order
from ftx_paper.contracts import MarketRole
from .policy_contract import ExecutionMode, PolicyDecision, StrategyPolicy

if TYPE_CHECKING:
    from ftx_paper.market.bundles import DecisionBundle


class SharedDecisionCoordinator:
    """Apply shared policy boundary rules without owning strategy hypotheses."""

    def __init__(self, policy: StrategyPolicy) -> None:
        self.policy = policy

    def evaluate(
        self,
        bundle: DecisionBundle,
        context: DecisionContext,
    ) -> tuple[PolicyDecision, ...]:
        decisions = self.policy.evaluate(bundle, context)
        result: list[PolicyDecision] = []

        for decision in decisions:
            payload = dict(decision.payload)
            payload.setdefault("bundle_id", bundle.bundle_id)
            payload.setdefault("decision_at", context.as_of.isoformat())
            if not payload.get("decision_id"):
                reason = str(payload.get("reason", decision.event_type))
                payload["decision_id"] = sha256(
                    f"{bundle.bundle_id}:{decision.event_type}:{reason}".encode()
                ).hexdigest()[:24]
            payload.pop("candidate" + "_id", None)
            proposal = payload.get("execution_proposal")
            order = getattr(decision, "order", None)
            if order is None and self.policy.execution_mode is ExecutionMode.EXECUTABLE:
                futures = bundle.bars.get(MarketRole.FUTURES)
                if isinstance(proposal, Mapping) and futures is None:
                    policy = payload.get("policy")
                    payload.update({
                        "outcome": "unavailable",
                        "reason": "futures_instrument_unavailable",
                        "policy": {
                            **(policy if isinstance(policy, Mapping) else {}),
                            "outcome": "unavailable",
                            "reason_code": "futures_instrument_unavailable",
                        },
                    })
                elif isinstance(proposal, Mapping) and futures is not None:
                    order = build_entry_order(
                        proposal, instrument=futures.instrument, context=context,
                    )
            if self.policy.execution_mode is ExecutionMode.DIAGNOSTIC and (
                order is not None or payload.get("order") is not None
            ):
                policy = payload.get("policy")
                payload["outcome"] = "rejected"
                payload["reason"] = "diagnostic_policy_order_forbidden"
                payload["policy"] = {
                    **(policy if isinstance(policy, Mapping) else {}),
                    "outcome": "rejected",
                    "reason_code": "diagnostic_policy_order_forbidden",
                }
                payload.pop("order", None)
                order = None
            result.append(PolicyDecision(payload, order=order, event_type=decision.event_type))
        return tuple(result)

    def on_execution_event(self, event: ExecutionNotification) -> None:
        self.policy.on_execution_event(event)

    def snapshot(self) -> Mapping[str, object]:
        return self.policy.snapshot()


__all__ = ["SharedDecisionCoordinator"]
