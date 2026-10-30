"""Immutable established-state and unresolved-hypothesis records."""

from dataclasses import dataclass
from datetime import datetime
import math

from ftx_paper.strategies.orderflow.canonical.ict.domain.enums import (
    Condition,
    EvidenceStatus,
    EpisodeLifecycle,
    Movement,
    StructuralDirection,
    TransitionResolution,
)
from ftx_paper.strategies.orderflow.canonical.ict.domain.evidence import Evidence
from ftx_paper.strategies.orderflow.canonical.ict.domain.time import require_aware, require_order
from ftx_paper.strategies.orderflow.canonical.ict.scale.model import AtrReference


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    condition: Condition
    structural_direction: StructuralDirection
    current_movement: Movement
    timeframe: str
    observed_at: datetime
    available_at: datetime
    version: int = 0
    episode_id: str | None = None

    def __post_init__(self) -> None:
        if not self.timeframe:
            raise ValueError("market snapshots require a timeframe")
        require_order(observed_at=self.observed_at, available_at=self.available_at, subject="snapshot")
        if self.version < 0:
            raise ValueError("snapshot version cannot be negative")
        if self.episode_id is not None and not self.episode_id:
            raise ValueError("snapshot episode_id cannot be empty")


@dataclass(frozen=True, slots=True)
class BalanceEpisode:
    episode_id: str
    formation_start: datetime
    formation_end: datetime
    confirmed_at: datetime | None
    lower_boundary: float
    upper_boundary: float
    lifecycle: EpisodeLifecycle = EpisodeLifecycle.FORMING
    boundary_version: int = 1
    ended_at: datetime | None = None
    end_reason: str | None = None
    atr_reference: AtrReference | None = None

    def __post_init__(self) -> None:
        if not self.episode_id:
            raise ValueError("balance episodes require identity")
        require_aware(self.formation_start, name="formation_start")
        require_aware(self.formation_end, name="formation_end")
        if self.formation_end < self.formation_start:
            raise ValueError("formation interval is inverted")
        if self.upper_boundary <= self.lower_boundary:
            raise ValueError("balance boundaries must have positive width")
        if self.boundary_version < 1:
            raise ValueError("boundary version must be positive")
        if self.lifecycle is EpisodeLifecycle.FORMING and self.confirmed_at is not None:
            raise ValueError("forming episodes cannot have confirmed_at")
        if self.confirmed_at is not None:
            require_aware(self.confirmed_at, name="confirmed_at")
            if self.confirmed_at < self.formation_end:
                raise ValueError("balance cannot be confirmed before formation ends")
        if self.lifecycle is EpisodeLifecycle.ACTIVE and self.confirmed_at is None:
            raise ValueError("active episodes require confirmed_at")
        if self.lifecycle is EpisodeLifecycle.ENDED:
            if self.confirmed_at is None:
                raise ValueError("ended episodes require confirmed_at")
            if self.ended_at is None or not self.end_reason:
                raise ValueError("ended episodes require ended_at and end_reason")
            require_aware(self.ended_at, name="ended_at")
            if self.ended_at < self.formation_end:
                raise ValueError("balance cannot end before formation ends")
            if self.confirmed_at is not None and self.ended_at < self.confirmed_at:
                raise ValueError("balance cannot end before confirmation")


@dataclass(frozen=True, slots=True)
class BalanceExcursion:
    """A completed bar excursion beyond frozen balance boundaries."""

    episode_id: str
    observed_at: datetime
    available_at: datetime
    lower_boundary: float
    upper_boundary: float
    high: float
    low: float

    def __post_init__(self) -> None:
        if not self.episode_id:
            raise ValueError("balance excursions require episode identity")
        require_order(observed_at=self.observed_at, available_at=self.available_at, subject="excursion")
        if self.upper_boundary <= self.lower_boundary:
            raise ValueError("excursion boundaries must have positive width")
        if self.high < self.low:
            raise ValueError("excursion high must be >= low")


@dataclass(frozen=True, slots=True)
class CompressionObservation:
    episode_id: str
    observed_at: datetime
    available_at: datetime
    active_band_width: float | None
    outer_range_width: float | None
    volatility: float | None
    prior_volatility: float | None
    position_in_range: float | None
    contained: bool | None
    active_band_fraction: float | None = None
    volatility_ratio: float | None = None
    containment_duration: int | None = None
    status: EvidenceStatus = EvidenceStatus.UNAVAILABLE
    reason: str = ""

    def __post_init__(self) -> None:
        if not self.episode_id:
            raise ValueError("compression observations require episode identity")
        require_order(observed_at=self.observed_at, available_at=self.available_at, subject="compression")
        if self.position_in_range is not None and not math.isfinite(self.position_in_range):
            raise ValueError("position_in_range must be finite")
        measurements = (
            self.active_band_width,
            self.active_band_fraction,
            self.outer_range_width,
            self.volatility,
            self.prior_volatility,
            self.volatility_ratio,
            self.position_in_range,
            self.containment_duration,
            self.contained,
        )
        if self.status is EvidenceStatus.UNAVAILABLE and any(value is not None for value in measurements):
            raise ValueError("unavailable compression cannot contain measurements")
        if self.status is not EvidenceStatus.UNAVAILABLE:
            if any(value is None for value in measurements):
                raise ValueError("available compression requires all measurements")
            numeric = (
                self.active_band_width,
                self.active_band_fraction,
                self.outer_range_width,
                self.volatility,
                self.prior_volatility,
                self.volatility_ratio,
                )
            if any(not math.isfinite(value) or value < 0 for value in numeric if value is not None):
                raise ValueError("compression measurements must be finite and non-negative")
            active_band_width = self.active_band_width
            outer_range_width = self.outer_range_width
            if active_band_width is None or outer_range_width is None:
                raise ValueError("available compression requires all measurements")
            if outer_range_width == 0:
                raise ValueError("compression requires a positive outer range")
            if self.containment_duration is None or self.containment_duration < 0:
                raise ValueError("containment duration must be non-negative")

    @classmethod
    def unavailable(
        cls,
        *,
        episode_id: str,
        observed_at: datetime,
        available_at: datetime | None = None,
        reason: str,
    ) -> "CompressionObservation":
        return cls(
            episode_id=episode_id,
            observed_at=observed_at,
            available_at=observed_at if available_at is None else available_at,
            active_band_width=None,
            active_band_fraction=None,
            outer_range_width=None,
            volatility=None,
            prior_volatility=None,
            volatility_ratio=None,
            position_in_range=None,
            containment_duration=None,
            contained=None,
            reason=reason,
        )


@dataclass(frozen=True, slots=True)
class TransitionAttempt:
    attempt_id: str
    source_state: Condition
    proposed_destination: Condition
    observed_at: datetime
    available_at: datetime
    evidence: tuple[Evidence, ...] = ()
    resolution: TransitionResolution = TransitionResolution.UNRESOLVED
    resolved_at: datetime | None = None
    source_range_version: int | None = None
    episode_id: str | None = None
    direction: StructuralDirection | None = None
    atr_reference: AtrReference | None = None
    atr_qualification: str = "disabled"

    def __post_init__(self) -> None:
        if not self.attempt_id:
            raise ValueError("transition attempts require identity")
        require_order(observed_at=self.observed_at, available_at=self.available_at, subject="attempt")
        if self.resolution is not TransitionResolution.UNRESOLVED and not self.resolved_at:
            raise ValueError("resolved attempts require resolved_at")
        if self.resolved_at is not None:
            require_aware(self.resolved_at, name="resolved_at")
            if self.resolved_at < self.available_at:
                raise ValueError("transition cannot resolve before availability")
        if self.atr_qualification not in ("passed", "failed", "unavailable", "disabled"):
            raise ValueError("invalid ATR qualification")
