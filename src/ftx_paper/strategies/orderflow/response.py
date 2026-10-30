"""Runtime response geometry and candle evaluation."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Generic, TypeVar

from .location_geometry import LocationBand
from .templates import Direction, ResponseType, TradeTemplate
from .interactions import CloseSide

T = TypeVar("T")
H = TypeVar("H", bound=object)


class ResponseOutcomeStatus(StrEnum):
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    INVALIDATED = "INVALIDATED"
    EXPIRED = "EXPIRED"


@dataclass(frozen=True, slots=True)
class ResponseCandle:
    open: float
    high: float
    low: float
    close: float


@dataclass(frozen=True, slots=True)
class ResponseEvaluation(Generic[T]):
    status: str | None
    armed: bool
    evidence: tuple[T, ...]
    confirmation_candle: T | None
    direction: Direction | None = None

    @property
    def evidence_ids(self) -> tuple[str, ...]:
        values: list[str] = []
        for item in self.evidence:
            item_ids = getattr(item, "evidence_ids", ())
            if item_ids:
                values.extend(str(value) for value in item_ids)
            elif getattr(item, "observation_id", None):
                values.append(str(getattr(item, "observation_id")))
        return tuple(dict.fromkeys(values))

    @property
    def confirmation_observation_id(self) -> str | None:
        value = (
            getattr(self.confirmation_candle, "observation_id", None)
            if self.confirmation_candle is not None
            else None
        )
        return str(value) if value is not None else None

    @property
    def confirmation_available_at(self) -> datetime | None:
        return getattr(self.confirmation_candle, "available_at", None)

    @property
    def reason(self) -> str | None:
        return self.status.lower() if self.status is not None else None


@dataclass(frozen=True, slots=True)
class ResponseSpec:
    direction: Direction
    response_type: ResponseType
    band: LocationBand
    expiry_at: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "direction", Direction(self.direction))
        object.__setattr__(self, "response_type", ResponseType(self.response_type))

    @property
    def band_lower(self) -> float:
        return self.band.low

    @property
    def band_upper(self) -> float:
        return self.band.high

    @property
    def center(self) -> float:
        return self.band.center

    def touches(self, *, high: float, low: float) -> bool:
        return self.band.intersects(high=high, low=low)

    def confirmation_close(self, close: float) -> bool:
        if self.response_type is ResponseType.ACCEPTANCE:
            boundary = (
                self.band.high if self.direction is Direction.LONG else self.band.low
            )
            return (
                close > boundary
                if self.direction is Direction.LONG
                else close < boundary
            )
        return (
            close >= self.center
            if self.direction is Direction.LONG
            else close <= self.center
        )

    def confirms(
        self, *, open: float, high: float, low: float, close: float, armed: bool
    ) -> bool:
        if not armed:
            return False
        if self.response_type is ResponseType.ACCEPTANCE:
            return self.confirmation_close(close)
        approach_side = (
            open >= self.center
            if self.direction is Direction.LONG
            else open <= self.center
        )
        return (
            self.touches(high=high, low=low)
            and approach_side
            and self.confirmation_close(close)
        )

    def invalidation_close(self, close: float) -> bool:
        if self.response_type is ResponseType.ACCEPTANCE:
            return False
        return (
            close < self.center
            if self.direction is Direction.LONG
            else close > self.center
        )

    def cross_band_confirms(
        self, *, close: float, pre_contact_side: CloseSide | str | None
    ) -> bool:
        if self.response_type is not ResponseType.REJECTION or pre_contact_side is None:
            return False
        side = CloseSide(pre_contact_side)
        return (
            side is CloseSide.ABOVE
            and self.direction is Direction.SHORT
            and close < self.band.low
        ) or (
            side is CloseSide.BELOW
            and self.direction is Direction.LONG
            and close > self.band.high
        )

    def cross_band_possible(self, pre_contact_side: CloseSide | str | None) -> bool:
        if pre_contact_side is None:
            return False
        side = CloseSide(pre_contact_side)
        return self.response_type is ResponseType.REJECTION and (
            (self.direction is Direction.SHORT and side is CloseSide.ABOVE)
            or (self.direction is Direction.LONG and side is CloseSide.BELOW)
        )

    def advance(
        self, *, open: float, high: float, low: float, close: float, armed: bool
    ) -> tuple[bool, str | None]:
        armed = armed or self.touches(high=high, low=low)
        if not armed or open == close:
            return armed, None
        if self.invalidation_close(close):
            return armed, ResponseOutcomeStatus.INVALIDATED
        if self.confirms(open=open, high=high, low=low, close=close, armed=armed):
            return armed, ResponseOutcomeStatus.CONFIRMED
        return armed, None

    def expired(self, *, session_date: str, as_of: str) -> bool:
        return (
            self.expiry_at is not None and as_of >= f"{session_date}T{self.expiry_at}"
        )


def build_response_spec(
    template: TradeTemplate,
    *,
    band: LocationBand | None = None,
    band_lower: float | None = None,
    band_upper: float | None = None,
    direction: Direction | None = None,
) -> ResponseSpec:
    if band is None:
        if band_lower is None or band_upper is None:
            raise TypeError("band or band bounds are required")
        band = LocationBand(band_lower, band_upper)
    return ResponseSpec(
        direction or template.direction,
        template.response_type,
        band,
        template.expiry_at,
    )


def advance_response_candle(
    spec: ResponseSpec, candle: ResponseCandle, *, armed: bool
) -> tuple[bool, str | None]:
    return spec.advance(
        open=candle.open,
        high=candle.high,
        low=candle.low,
        close=candle.close,
        armed=armed,
    )


def evaluate_response_window(
    candles: Sequence[T],
    *,
    to_candle: Callable[[T], ResponseCandle],
    spec_for: Callable[[T], ResponseSpec],
    armed: bool,
    pre_contact_side: CloseSide | str | None = None,
    reset_before: Callable[[T], bool] | None = None,
    invalidation_allowed: Callable[[ResponseSpec, T], bool] | None = None,
) -> ResponseEvaluation[T]:
    evidence: list[T] = []
    for candle in candles:
        if reset_before is not None and reset_before(candle):
            evidence.clear()
            armed = False
            continue
        evidence.append(candle)
        spec = spec_for(candle)
        value = to_candle(candle)
        if spec.cross_band_confirms(
            close=value.close, pre_contact_side=pre_contact_side
        ):
            return ResponseEvaluation(
                ResponseOutcomeStatus.CONFIRMED,
                armed,
                tuple(evidence),
                candle,
                spec.direction,
            )
        armed, status = spec.advance(
            open=value.open,
            high=value.high,
            low=value.low,
            close=value.close,
            armed=armed,
        )
        if status == ResponseOutcomeStatus.INVALIDATED and spec.cross_band_possible(
            pre_contact_side
        ):
            status = None
        if (
            status == ResponseOutcomeStatus.INVALIDATED
            and invalidation_allowed is not None
            and not invalidation_allowed(spec, candle)
        ):
            status = None
        if status is not None:
            return ResponseEvaluation(
                status, armed, tuple(evidence), candle, spec.direction
            )
    return ResponseEvaluation(None, armed, tuple(evidence), None)


def evaluate_response_hypotheses(
    candles: Sequence[T],
    hypotheses: Sequence[H],
    *,
    to_candle: Callable[[T], ResponseCandle],
    spec_for: Callable[[H, T], ResponseSpec],
    armed: bool,
    pre_contact_side: CloseSide | str | None = None,
    reset_before: Callable[[T], bool] | None = None,
    invalidation_allowed: Callable[[ResponseSpec, T], bool] | None = None,
) -> tuple[ResponseEvaluation[T], ...]:
    states: dict[H, tuple[bool, list[T], str | None, T | None, Direction | None]] = {
        hypothesis: (armed, [], None, None, None) for hypothesis in hypotheses
    }
    for candle in candles:
        if reset_before is not None and reset_before(candle):
            states = {
                h: (False, [], None, None, None)
                for h in hypotheses
                if states[h][2] is None
            } | {h: state for h, state in states.items() if state[2] is not None}
            continue
        for hypothesis in hypotheses:
            current_armed, evidence, status, confirmation, direction = states[
                hypothesis
            ]
            if status is not None:
                continue
            evidence.append(candle)
            spec = spec_for(hypothesis, candle)
            value = to_candle(candle)
            if spec.cross_band_confirms(
                close=value.close, pre_contact_side=pre_contact_side
            ):
                states[hypothesis] = (
                    current_armed,
                    evidence,
                    ResponseOutcomeStatus.CONFIRMED,
                    candle,
                    spec.direction,
                )
                continue
            current_armed, next_status = spec.advance(
                open=value.open,
                high=value.high,
                low=value.low,
                close=value.close,
                armed=current_armed,
            )
            if (
                next_status == ResponseOutcomeStatus.INVALIDATED
                and spec.cross_band_possible(pre_contact_side)
            ):
                next_status = None
            if (
                next_status == ResponseOutcomeStatus.INVALIDATED
                and invalidation_allowed is not None
                and not invalidation_allowed(spec, candle)
            ):
                next_status = None
            states[hypothesis] = (
                current_armed,
                evidence,
                next_status,
                candle if next_status is not None else confirmation,
                spec.direction if next_status is not None else direction,
            )
    return tuple(
        ResponseEvaluation(state[2], state[0], tuple(state[1]), state[3], state[4])
        for state in (states[h] for h in hypotheses)
    )


__all__ = [
    "ResponseCandle",
    "ResponseEvaluation",
    "ResponseOutcomeStatus",
    "ResponseSpec",
    "advance_response_candle",
    "build_response_spec",
    "evaluate_response_hypotheses",
    "evaluate_response_window",
]
