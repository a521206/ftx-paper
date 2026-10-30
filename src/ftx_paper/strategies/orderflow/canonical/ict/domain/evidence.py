"""Immutable evidence records used by state transitions."""

from dataclasses import dataclass
from datetime import datetime

from ftx_paper.strategies.orderflow.canonical.ict.domain.enums import EvidenceStatus
from ftx_paper.strategies.orderflow.canonical.ict.domain.time import require_order


@dataclass(frozen=True, slots=True)
class Evidence:
    """One causal observation, which may also explicitly be unavailable."""

    name: str
    status: EvidenceStatus
    observed_at: datetime
    available_at: datetime
    value: float | str | bool | None = None
    threshold: float | str | bool | None = None
    units: str | None = None
    reason: str = ""
    source: str | None = None

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("evidence requires a name")
        require_order(observed_at=self.observed_at, available_at=self.available_at, subject="evidence")

    @classmethod
    def unavailable(cls, name: str, *, observed_at: datetime, reason: str) -> "Evidence":
        return cls(
            name=name,
            status=EvidenceStatus.UNAVAILABLE,
            observed_at=observed_at,
            available_at=observed_at,
            reason=reason,
        )
