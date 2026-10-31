"""Registered-location interactions and their synthetic projections."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping

from .location_geometry import LocationBand
from .location_observation import LocationObservation
from .orderflow_common import Aggregate, canonical_timestamp


class CandleClassification(StrEnum):
    CONTAINED = "CONTAINED"
    OUTSIDE_ABOVE = "OUTSIDE_ABOVE"
    OUTSIDE_BELOW = "OUTSIDE_BELOW"


class InteractionStatus(StrEnum):
    ACTIVE = "ACTIVE"
    ENDED = "ENDED"


class LocationTemplate(StrEnum):
    VWAP = "VWAP"
    OR_HIGH = "OR_HIGH"
    OR_LOW = "OR_LOW"
    PD_HIGH = "PD_HIGH"
    PD_LOW = "PD_LOW"
    SESSION_HIGH = "SESSION_HIGH"
    SESSION_LOW = "SESSION_LOW"


class ExitMode(StrEnum):
    FIXED_TARGET = "fixed_target"
    CONDITIONAL = "conditional"


class CloseSide(StrEnum):
    ABOVE = "ABOVE"
    INSIDE_BAND = "INSIDE_BAND"
    BELOW = "BELOW"


class DepartureDirection(StrEnum):
    SAME_SIDE = "same_side"
    CROSS_BAND = "cross_band"


class InteractionCloseReason(StrEnum):
    CROSS_BAND_DEPARTURE = "CROSS_BAND_DEPARTURE"
    SAME_SIDE_DEPARTURE = "SAME_SIDE_DEPARTURE"
    OUTSIDE_BAND_DEPARTURE = "OUTSIDE_BAND_DEPARTURE"
    SESSION_END = "SESSION_END"
    DATA_GAP = "DATA_GAP"
    REFERENCE_REPLACED = "REFERENCE_REPLACED"


@dataclass(frozen=True, slots=True)
class InteractionCandle:
    candle: Aggregate
    classification: CandleClassification
    is_terminal: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "candle_id": self.candle.time,
            "time": self.candle.time,
            "end_time": self.candle.end_time,
            "available_at": self.candle.available_at,
            "timeframe": self.candle.timeframe,
            "open": self.candle.open,
            "high": self.candle.high,
            "low": self.candle.low,
            "close": self.candle.close,
            "volume": self.candle.volume,
            "classification": self.classification.value,
            "is_terminal": self.is_terminal,
        }


@dataclass(frozen=True, slots=True)
class ProjectionSnapshot:
    timeframe: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    source_candle_ids: tuple[str, ...]
    first_source_start: str
    last_source_end: str
    available_at: str

    def __post_init__(self) -> None:
        for name in ("first_source_start", "last_source_end", "available_at"):
            object.__setattr__(self, name, canonical_timestamp(getattr(self, name)))

    def to_dict(self) -> dict[str, Any]:
        return {
            "timeframe": self.timeframe,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "source_candle_ids": list(self.source_candle_ids),
            "first_source_start": self.first_source_start,
            "last_source_end": self.last_source_end,
            "available_at": self.available_at,
        }


@dataclass(frozen=True, slots=True)
class SyntheticScoreSnapshot:
    context_version: str
    observed_at: str
    available_at: str
    source_candle_ids: tuple[str, ...]
    long_score: float | None
    short_score: float | None


@dataclass(frozen=True, slots=True)
class InteractionSnapshot:
    interaction_id: str
    instrument: str
    session_date: str
    location: str
    timeframe: str
    template_version: str
    creation_candle: Aggregate
    reference_level: float
    band: LocationBand
    pre_contact_side: CloseSide | None
    interaction_started_at: str
    available_at: str
    reference_generation: int
    sessions_carried: int
    status: InteractionStatus
    departure_direction: DepartureDirection | None
    terminal_close_side: CloseSide | None
    terminal_candle_id: str | None
    close_reason: InteractionCloseReason | None
    candles: tuple[InteractionCandle, ...]
    synthetic_snapshots: tuple[tuple[str, tuple[ProjectionSnapshot, ...]], ...]
    pd_role: str | None = None

    @property
    def band_low(self) -> float:
        return self.band.low

    @property
    def band_high(self) -> float:
        return self.band.high

    def snapshot(self) -> dict[str, Any]:
        return {
            "interaction_id": self.interaction_id,
            "location": self.location,
            "timeframe": self.timeframe,
            "reference_level": self.reference_level,
            "reference_generation": self.reference_generation,
            "sessions_carried": self.sessions_carried,
            "band_low": self.band.low,
            "band_high": self.band.high,
            "band": {"low": self.band.low, "high": self.band.high},
            "status": self.status.value,
            "departure_direction": self.departure_direction.value
            if self.departure_direction
            else None,
            "terminal_close_side": self.terminal_close_side.value
            if self.terminal_close_side
            else None,
            "terminal_candle_id": self.terminal_candle_id,
            "close_reason": self.close_reason.value if self.close_reason else None,
            "candles": [item.to_dict() for item in self.candles],
            "pre_contact_side": self.pre_contact_side.value
            if self.pre_contact_side
            else None,
            "pd_role": self.pd_role,
            "synthetic_snapshots": {
                key: [item.to_dict() for item in values]
                for key, values in self.synthetic_snapshots
            },
        }


@dataclass
class SyntheticProjection:
    timeframe: str
    source_candle_ids: list[str] = field(default_factory=list)
    candles: list[Aggregate] = field(default_factory=list)
    snapshots: list[ProjectionSnapshot] = field(default_factory=list)

    def add(self, candle: Aggregate) -> bool:
        if candle.timeframe != self.timeframe:
            raise ValueError("projection source timeframe mismatch")
        identity = candle_identity(candle)
        if identity in self.source_candle_ids:
            return False
        self.source_candle_ids.append(identity)
        self.candles.append(candle)
        self.candles.sort(key=candle_identity)
        self.snapshots.append(self.snapshot())
        return True

    def snapshot(self) -> ProjectionSnapshot:
        if not self.candles:
            raise ValueError("cannot snapshot an empty projection")
        return ProjectionSnapshot(
            self.timeframe,
            self.candles[0].open,
            max(c.high for c in self.candles),
            min(c.low for c in self.candles),
            self.candles[-1].close,
            float(sum(c.volume for c in self.candles)),
            tuple(candle_identity(c) for c in self.candles),
            candle_identity(self.candles[0]),
            self.candles[-1].end_time,
            max(c.available_at for c in self.candles),
        )


def candle_identity(candle: Aggregate) -> str:
    return candle.time


def candle_end_identity(candle: Aggregate) -> str:
    return candle.end_time


@dataclass
class LocationInteraction:
    interaction_id: str
    instrument: str
    session_date: str
    location: LocationTemplate | str
    timeframe: str
    template_version: str
    creation_candle: Aggregate
    reference_level: float
    atr: float
    band: LocationBand
    reference_provenance: Mapping[str, Any]
    initial_close_side: CloseSide
    pre_contact_side: CloseSide | None
    interaction_started_at: str
    created_at: str
    available_at: str
    reference_generation: int = 0
    sessions_carried: int = 0
    status: InteractionStatus = InteractionStatus.ACTIVE
    departure_direction: DepartureDirection | None = None
    terminal_close_side: CloseSide | None = None
    terminal_candle_id: str | None = None
    close_reason: InteractionCloseReason | None = None
    synthetic: dict[str, SyntheticProjection] = field(default_factory=dict)
    candles: list[InteractionCandle] = field(default_factory=list)
    frozen_projections: dict[str, ProjectionSnapshot] = field(default_factory=dict)
    score_history: list[SyntheticScoreSnapshot] = field(default_factory=list)
    source_cursors: dict[str, str] = field(default_factory=dict, repr=False)
    pd_role: str | None = None
    pd_role_transition_candle_id: str | None = None
    pd_role_transition_available_at: str | None = None

    @property
    def band_low(self) -> float:
        return self.band.low

    @property
    def band_high(self) -> float:
        return self.band.high

    def __post_init__(self) -> None:
        self.interaction_started_at = canonical_timestamp(self.interaction_started_at)
        self.created_at = canonical_timestamp(self.created_at)
        self.available_at = canonical_timestamp(self.available_at)
        self.initial_close_side = CloseSide(self.initial_close_side)
        if self.departure_direction is not None:
            self.departure_direction = DepartureDirection(self.departure_direction)
        if self.terminal_close_side is not None:
            self.terminal_close_side = CloseSide(self.terminal_close_side)
        if self.close_reason is not None:
            self.close_reason = InteractionCloseReason(self.close_reason)
        if self.creation_candle.timeframe != self.timeframe:
            raise ValueError("interaction timeframe must match creation candle")
        if not self.synthetic:
            self.synthetic = {
                timeframe: SyntheticProjection(timeframe) for timeframe in ("5m", "1m")
            }
        if not self.candles:
            self.candles.append(
                InteractionCandle(
                    self.creation_candle, self.initial_close_classification()
                )
            )

    @classmethod
    def create(
        cls,
        *,
        instrument: str,
        session_date: str,
        location: LocationTemplate | str,
        template_version: str,
        candle: Aggregate,
        reference_level: float,
        atr: float,
        band: LocationBand,
        created_at: str,
        available_at: str,
        reference_provenance: Mapping[str, Any] | None = None,
        pre_contact_side: CloseSide | None = None,
        reference_generation: int = 0,
        timeframe: str | None = None,
        observation: LocationObservation | None = None,
    ) -> LocationInteraction | None:
        source_timeframe = timeframe or candle.timeframe
        if source_timeframe not in {"1m", "5m"}:
            raise ValueError("unsupported interaction timeframe")
        if (
            candle.timeframe != source_timeframe
            or candle.available_at < candle.end_time
        ):
            raise ValueError(
                f"interactions require causally completed {source_timeframe} candles"
            )
        high = observation.high if observation else candle.high
        low = observation.low if observation else candle.low
        close = observation.close if observation else candle.close
        if not band.intersects(high=high, low=low):
            return None
        return cls(
            f"{instrument}:{session_date}:{location}:{source_timeframe}:{candle.time}",
            instrument,
            session_date,
            location,
            source_timeframe,
            template_version,
            candle,
            reference_level,
            atr,
            band,
            dict(reference_provenance or {}),
            _close_side(close, band.low, band.high),
            pre_contact_side,
            candle.time,
            created_at,
            available_at,
            reference_generation=reference_generation,
        )

    def initial_close_classification(self) -> CandleClassification:
        return _classify_close(
            self.creation_candle.close, self.band_low, self.band_high
        )

    def add_projection_candle(self, candle: Aggregate) -> bool:
        if self.status is not InteractionStatus.ACTIVE:
            return False
        added = self.synthetic[candle.timeframe].add(candle)
        if added:
            self.source_cursors[candle.timeframe] = max(
                candle_identity(candle),
                self.source_cursors.get(candle.timeframe, candle_identity(candle)),
            )
        return added

    def record_score(
        self,
        *,
        snapshot: ProjectionSnapshot,
        context_version: str,
        long_score: float | None,
        short_score: float | None,
    ) -> None:
        if (
            self.score_history
            and self.score_history[-1].source_candle_ids == snapshot.source_candle_ids
        ):
            return
        self.score_history.append(
            SyntheticScoreSnapshot(
                context_version,
                snapshot.last_source_end,
                snapshot.available_at,
                snapshot.source_candle_ids,
                long_score,
                short_score,
            )
        )

    def advance_projections(self, sources: Mapping[str, list[Aggregate]]) -> None:
        for timeframe, candles in sources.items():
            cursor = self.source_cursors.get(timeframe)
            for candle in candles:
                if candle_identity(candle) >= candle_identity(
                    self.creation_candle
                ) and (cursor is None or candle_identity(candle) > cursor):
                    self.add_projection_candle(candle)

    def seed_projections(self, sources: Mapping[str, list[Aggregate]]) -> None:
        for timeframe, candles in sources.items():
            if timeframe not in self.synthetic:
                raise ValueError(f"unsupported projection timeframe: {timeframe}")
            eligible = [
                c
                for c in candles
                if c.available_at <= self.available_at
                and candle_identity(c)
                <= candle_identity(self.creation_candle)
                <= candle_end_identity(c)
            ]
            if not eligible:
                eligible = [
                    c
                    for c in candles
                    if c.available_at <= self.available_at
                    and candle_identity(c) >= candle_identity(self.creation_candle)
                ]
            if eligible:
                containing = min(eligible, key=candle_identity)
                for candle in candles:
                    if candle.available_at <= self.available_at and candle_identity(
                        candle
                    ) >= candle_identity(containing):
                        self.add_projection_candle(candle)

    def observe(
        self, candle: Aggregate, *, observation: LocationObservation | None = None
    ) -> InteractionStatus:
        if self.status is not InteractionStatus.ACTIVE:
            return self.status
        if candle.timeframe != self.timeframe:
            raise ValueError("interaction candle timeframe mismatch")
        if candle.available_at < candle.end_time:
            raise ValueError("interaction candle must be causally complete")
        if candle.available_at < self.available_at:
            raise ValueError(
                "interaction candle is unavailable at interaction creation"
            )
        if self.candles and candle_identity(candle) < candle_identity(self.candles[-1].candle):
            raise ValueError("interaction observations must be chronological")
        if candle_identity(candle) <= candle_identity(self.creation_candle) or any(
            candle_identity(i.candle) == candle_identity(candle) for i in self.candles
        ):
            return self.status
        close = observation.close if observation else candle.close
        classification = _classify_close(close, self.band_low, self.band_high)
        side = _close_side(close, self.band_low, self.band_high)
        terminal = classification is not CandleClassification.CONTAINED
        self.candles.append(InteractionCandle(candle, classification, terminal))
        self.add_projection_candle(candle)
        if terminal:
            departure_side = (
                self.initial_close_side
                if self.initial_close_classification()
                is not CandleClassification.CONTAINED
                else self.pre_contact_side
            )
            if departure_side is not None:
                self.departure_direction = (
                    DepartureDirection.SAME_SIDE
                    if side == departure_side
                    else DepartureDirection.CROSS_BAND
                )
            self.terminal_close_side = side
            self.terminal_candle_id = candle_identity(candle)
            reason = (
                InteractionCloseReason.OUTSIDE_BAND_DEPARTURE
                if self.departure_direction is None
                else InteractionCloseReason.CROSS_BAND_DEPARTURE
                if self.departure_direction is DepartureDirection.CROSS_BAND
                else InteractionCloseReason.SAME_SIDE_DEPARTURE
            )
            self.end(
                InteractionStatus.ENDED,
                available_at=candle.available_at,
                close_reason=reason,
            )
        return self.status

    def end(
        self,
        reason: InteractionStatus,
        *,
        available_at: str,
        close_reason: InteractionCloseReason | str | None = None,
    ) -> None:
        if reason is InteractionStatus.ACTIVE:
            raise ValueError("active is not a terminal interaction reason")
        if canonical_timestamp(available_at) < self.available_at:
            raise ValueError("interaction ending must be causal")
        self.status = reason
        self.close_reason = InteractionCloseReason(close_reason or reason.value)
        self.frozen_projections = {
            timeframe: projection.snapshot()
            for timeframe, projection in self.synthetic.items()
            if projection.candles
        }

    def finalize(
        self,
        reason: InteractionStatus,
        *,
        available_at: str,
        close_reason: InteractionCloseReason | str,
        completed_sources: Mapping[str, list[Aggregate]] | None = None,
    ) -> None:
        if completed_sources:
            for timeframe, candles in completed_sources.items():
                for candle in candles:
                    if candle.available_at <= available_at and candle_identity(
                        candle
                    ) >= candle_identity(self.creation_candle):
                        self.add_projection_candle(candle)
        self.end(reason, available_at=available_at, close_reason=close_reason)

    def snapshot(self) -> dict[str, Any]:
        return self.evaluation_snapshot().snapshot()

    def evaluation_snapshot(self) -> InteractionSnapshot:
        return InteractionSnapshot(
            self.interaction_id,
            self.instrument,
            self.session_date,
            str(self.location),
            self.timeframe,
            self.template_version,
            self.creation_candle,
            self.reference_level,
            self.band,
            self.pre_contact_side,
            self.interaction_started_at,
            self.available_at,
            self.reference_generation,
            self.sessions_carried,
            self.status,
            self.departure_direction,
            self.terminal_close_side,
            self.terminal_candle_id,
            self.close_reason,
            tuple(self.candles),
            tuple((tf, tuple(p.snapshots)) for tf, p in sorted(self.synthetic.items())),
            self.pd_role,
        )


def _classify_close(close: float, low: float, high: float) -> CandleClassification:
    return (
        CandleClassification.OUTSIDE_BELOW
        if close < low
        else CandleClassification.OUTSIDE_ABOVE
        if close > high
        else CandleClassification.CONTAINED
    )


def _close_side(close: float, low: float, high: float) -> CloseSide:
    return (
        CloseSide.BELOW
        if close < low
        else CloseSide.ABOVE
        if close > high
        else CloseSide.INSIDE_BAND
    )
