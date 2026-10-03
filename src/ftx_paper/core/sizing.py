"""Paper-owned deterministic quantity sizing pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field

from ftx_paper.core.capital_context import CapitalRuntimeContext
from ftx_paper.core.risk import RiskAssessment


@dataclass(frozen=True, slots=True)
class SizingPipelineInput:
    risk: RiskAssessment
    requested_quantity: int
    score_multiplier: float = 1.0
    direction: str = "long"
    net_directional_lots: float = 0.0
    concurrency_limit_lots: float | None = None
    stability_multiplier: float = 1.0
    drawdown_multiplier: float = 1.0
    vehicle_limit_lots: int | None = None
    candidate_id: str = ""


@dataclass(frozen=True, slots=True)
class SizingPipelineDecision:
    final_quantity: int
    requested_quantity: int
    risk_ceiling: int
    score_multiplier: float
    concurrency_limit_lots: int
    stability_multiplier: float
    vehicle_limit_lots: int | None
    rationale: str
    candidate_id: str = ""
    stage_results: tuple[tuple[str, int], ...] = ()
    policy_version: int = 1
    metadata: dict[str, object] = field(default_factory=dict)


class SizingPipeline:
    """Apply Paper quantity stages in one deterministic order."""

    def __init__(self, context: CapitalRuntimeContext | None = None) -> None:
        self.context = context

    def decide(self, inputs: SizingPipelineInput) -> SizingPipelineDecision:
        requested = max(0, int(inputs.requested_quantity))
        risk_ceiling = max(0, int(inputs.risk.risk_ceiling))
        if not inputs.risk.approved or risk_ceiling <= 0:
            return self._blocked(inputs, requested, risk_ceiling, inputs.risk.reason)

        score = max(0.0, min(1.5, float(inputs.score_multiplier)))
        lots = min(risk_ceiling, int(round(risk_ceiling * score)))
        stages = [("risk_ceiling", risk_ceiling), ("score", lots)]

        limit = inputs.concurrency_limit_lots
        if limit is None and self.context is not None:
            limit = self.context.max_net_directional_lots
        limit = float(limit if limit is not None else 10.0)
        signed_headroom = limit - inputs.net_directional_lots if inputs.direction.lower() in {"long", "buy"} else limit + inputs.net_directional_lots
        concurrency = max(0, int(signed_headroom))
        lots = min(lots, concurrency)
        stages.append(("concurrency", lots))

        vehicle_limit = inputs.vehicle_limit_lots
        if vehicle_limit is None and self.context is not None:
            vehicle_limit = self.context.max_lots
        if vehicle_limit is not None:
            lots = min(lots, max(0, int(vehicle_limit)))
        stages.append(("vehicle", lots))

        drawdown = max(0.0, min(1.0, float(getattr(inputs, "drawdown_multiplier", 1.0))))
        lots = int(lots * drawdown)
        stages.append(("drawdown", lots))

        stability = max(0.0, min(1.0, float(inputs.stability_multiplier)))
        lots = int(lots * stability)
        stages.append(("stability", lots))
        rationale = "approved" if lots > 0 else "sub_one_lot_or_gate"
        return SizingPipelineDecision(
            final_quantity=lots,
            requested_quantity=requested,
            risk_ceiling=risk_ceiling,
            score_multiplier=score,
            concurrency_limit_lots=concurrency,
            stability_multiplier=stability,
            vehicle_limit_lots=vehicle_limit,
            rationale=rationale,
            candidate_id=inputs.candidate_id,
            stage_results=tuple(stages),
            policy_version=self.context.profile.schema_version if self.context is not None else 1,
            metadata={"candidate_id": inputs.candidate_id, "stage_results": tuple(stages), "drawdown_multiplier": drawdown},
        )

    @staticmethod
    def _blocked(inputs: SizingPipelineInput, requested: int, risk_ceiling: int, reason: str) -> SizingPipelineDecision:
        return SizingPipelineDecision(
            final_quantity=0,
            requested_quantity=requested,
            risk_ceiling=risk_ceiling,
            score_multiplier=1.0,
            concurrency_limit_lots=0,
            stability_multiplier=float(inputs.stability_multiplier),
            vehicle_limit_lots=inputs.vehicle_limit_lots,
            rationale=reason or "risk_blocked",
            candidate_id=inputs.candidate_id,
            stage_results=(("risk", 0),),
            metadata={"candidate_id": inputs.candidate_id, "block_reason": reason or "risk_blocked"},
        )


__all__ = ["SizingPipeline", "SizingPipelineDecision", "SizingPipelineInput"]
