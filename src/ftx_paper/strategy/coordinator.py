"""Runtime-owned conversion from policy decisions to public decision events."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from hashlib import sha256
from typing import TYPE_CHECKING

from ftx_paper.domain.decision_context import DecisionContext
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.execution.order_proposal import build_entry_order
from ftx_paper.contracts import MarketRole
from ftx_paper.contracts import build_decision_view, validate_decision_view
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

        def canonical(payload: dict[str, object], event_type: str) -> dict[str, object]:
            evidence = payload.get("evidence")
            provenance = evidence.get("provenance") if isinstance(evidence, Mapping) else None
            available = payload.get("available_at")
            if available is None and isinstance(provenance, Mapping):
                available = provenance.get("available_at")
            decision_at = payload.get("decision_at")
            if isinstance(available, str) and isinstance(decision_at, str):
                available_time = datetime.fromisoformat(available.replace("Z", "+00:00"))
                decision_time = datetime.fromisoformat(decision_at.replace("Z", "+00:00"))
                if available_time > decision_time:
                    payload["decision_at"] = available_time.isoformat()
            return validate_decision_view(build_decision_view(payload, event_type=event_type))

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
            if isinstance(decision, (LiveDecision, CandidateDecision)):
                payload_order = payload.get("order")
                if self.policy.execution_mode is ExecutionMode.DIAGNOSTIC and (order is not None or payload_order is not None):
                    policy = payload.get("policy")
                    payload.update({
                        "outcome": "rejected",
                        "reason": "diagnostic_policy_order_forbidden",
                        "policy": {
                            **(policy if isinstance(policy, Mapping) else {}),
                            "outcome": "rejected",
                            "reason_code": "diagnostic_policy_order_forbidden",
                        },
                    })
                    payload.pop("order", None)
                    if not payload.get("decision_id"):
                        payload["decision_id"] = sha256(
                            f"{bundle.bundle_id}:diagnostic_policy_order_forbidden".encode()
                        ).hexdigest()[:24]
                    payload["decision_view"] = canonical(payload, decision.event_type)
                    result.append(CandidateDecision(payload, event_type=decision.event_type))
                    continue
                payload["decision_view"] = canonical(payload, decision.event_type)
                if isinstance(decision, CandidateDecision):
                    result.append(CandidateDecision(payload, order=order, event_type=decision.event_type))
                else:
                    result.append(LiveDecision(decision.event_type, payload, order=order))
                continue
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
            payload["decision_view"] = canonical(payload, decision.event_type)
            result.append(CandidateDecision(payload, order=order, event_type=decision.event_type))
        return tuple(result)

    def on_execution_event(self, event: ExecutionNotification) -> None:
        self.policy.on_execution_event(event)

    def snapshot(self) -> Mapping[str, object]:
        return self.policy.snapshot()


__all__ = ["SharedDecisionCoordinator"]
