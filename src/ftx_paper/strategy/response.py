"""Pure, template-driven causal response geometry."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from collections.abc import Callable, Mapping, Sequence
from typing import Generic, TypeVar

from .templates import Direction, ResponseType, TRADE_TEMPLATES, TradeTemplate

T = TypeVar("T")


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
class ResponseSpec:
    direction: Direction
    response_type: ResponseType
    band_lower: float
    band_upper: float
    expiry_at: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "direction", Direction(self.direction))
        object.__setattr__(self, "response_type", ResponseType(self.response_type))
        if self.band_lower > self.band_upper:
            raise ValueError("response band bounds are inverted")

    @property
    def center(self) -> float:
        return (self.band_lower + self.band_upper) / 2

    def touches(self, *, high: float, low: float) -> bool:
        return low <= self.band_upper and high >= self.band_lower

    def confirmation_close(self, close: float) -> bool:
        boundary = self.band_upper if self.response_type is ResponseType.ACCEPTANCE and self.direction is Direction.LONG else self.band_lower if self.response_type is ResponseType.ACCEPTANCE else self.center
        return close > boundary if self.direction is Direction.LONG else close < boundary

    def invalidation_close(self, close: float) -> bool:
        if self.response_type is ResponseType.ACCEPTANCE:
            return False
        return close < self.center if self.direction is Direction.LONG else close > self.center

    def cross_band_confirms(self, *, close: float, pre_contact_side: str | None) -> bool:
        if self.response_type is not ResponseType.REJECTION or pre_contact_side is None:
            return False
        if self.direction is Direction.SHORT:
            return pre_contact_side.upper() == "ABOVE" and close < self.band_lower
        return pre_contact_side.upper() == "BELOW" and close > self.band_upper

    def cross_band_possible(self, pre_contact_side: str | None) -> bool:
        side = (pre_contact_side or "").upper()
        return self.response_type is ResponseType.REJECTION and (
            (self.direction is Direction.SHORT and side == "ABOVE")
            or (self.direction is Direction.LONG and side == "BELOW")
        )

    def expired(self, *, session_date: str, as_of: str) -> bool:
        return self.expiry_at is not None and as_of >= f"{session_date}T{self.expiry_at}"

    def confirms(self, *, open: float, high: float, low: float, close: float, armed: bool) -> bool:
        if not armed:
            return False
        if self.response_type is ResponseType.ACCEPTANCE:
            return self.confirmation_close(close)
        approach_side = open >= self.center if self.direction is Direction.LONG else open <= self.center
        return self.touches(high=high, low=low) and approach_side and self.confirmation_close(close)

    def advance(self, *, open: float, high: float, low: float, close: float, armed: bool) -> tuple[bool, ResponseOutcomeStatus | None]:
        armed = armed or self.touches(high=high, low=low)
        if not armed or open == close:
            return armed, None
        if self.invalidation_close(close):
            return armed, ResponseOutcomeStatus.INVALIDATED
        if self.confirms(open=open, high=high, low=low, close=close, armed=armed):
            return armed, ResponseOutcomeStatus.CONFIRMED
        return armed, None


@dataclass(frozen=True, slots=True)
class ResponseEvaluation(Generic[T]):
    status: ResponseOutcomeStatus | None
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
        item = self.confirmation_candle
        value = getattr(item, "observation_id", None) if item is not None else None
        return str(value) if value is not None else None

    @property
    def confirmation_available_at(self) -> datetime | None:
        return getattr(self.confirmation_candle, "available_at", None)

    @property
    def reason(self) -> str | None:
        return self.status.value.lower() if self.status is not None else None


def build_response_spec(template: TradeTemplate, *, band_lower: float, band_upper: float) -> ResponseSpec:
    return ResponseSpec(template.direction, template.response_type, band_lower, band_upper, template.expiry_at)


def advance_response_candle(spec: ResponseSpec, candle: ResponseCandle, *, armed: bool) -> tuple[bool, ResponseOutcomeStatus | None]:
    return spec.advance(open=candle.open, high=candle.high, low=candle.low, close=candle.close, armed=armed)


def evaluate_response_window(
    candles: Sequence[T], *, to_candle: Callable[[T], ResponseCandle],
    spec_for: Callable[[T], ResponseSpec], armed: bool,
    pre_contact_side: str | None = None,
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
        if spec.cross_band_confirms(close=value.close, pre_contact_side=pre_contact_side):
            return ResponseEvaluation(ResponseOutcomeStatus.CONFIRMED, armed, tuple(evidence), candle, spec.direction)
        armed, status = spec.advance(open=value.open, high=value.high, low=value.low, close=value.close, armed=armed)
        if status is ResponseOutcomeStatus.INVALIDATED and spec.cross_band_possible(pre_contact_side):
            status = None
        if status is ResponseOutcomeStatus.INVALIDATED and invalidation_allowed is not None and not invalidation_allowed(spec, candle):
            status = None
        if status is not None:
            return ResponseEvaluation(status, armed, tuple(evidence), candle, spec.direction)
    return ResponseEvaluation(None, armed, tuple(evidence), None)


def evaluate_response_hypotheses(
    candles: Sequence[T], hypotheses: Sequence[T], *,
    to_candle: Callable[[T], ResponseCandle],
    spec_for: Callable[[T, T], ResponseSpec], armed: bool,
    pre_contact_side: str | None = None,
) -> tuple[ResponseEvaluation[T], ...]:
    states: dict[T, tuple[bool, list[T], ResponseOutcomeStatus | None, T | None, Direction | None]] = {
        hypothesis: (armed, [], None, None, None) for hypothesis in hypotheses
    }
    for candle in candles:
        value = to_candle(candle)
        for hypothesis in hypotheses:
            current_armed, evidence, status, confirmation, direction = states[hypothesis]
            if status is not None:
                continue
            evidence.append(candle)
            spec = spec_for(hypothesis, candle)
            if spec.cross_band_confirms(close=value.close, pre_contact_side=pre_contact_side):
                states[hypothesis] = current_armed, evidence, ResponseOutcomeStatus.CONFIRMED, candle, spec.direction
                continue
            current_armed, status = advance_response_candle(spec, value, armed=current_armed)
            if status is ResponseOutcomeStatus.INVALIDATED and spec.cross_band_possible(pre_contact_side):
                status = None
            states[hypothesis] = current_armed, evidence, status, candle if status is not None else confirmation, spec.direction if status is not None else direction
    return tuple(ResponseEvaluation(status, state_armed, tuple(evidence), confirmation, direction) for state_armed, evidence, status, confirmation, direction in (states[item] for item in hypotheses))


def evaluate_policy_response(
    interaction: object,
    observations: Sequence[object],
    *,
    max_candles: int,
    cutoff: object,
) -> ResponseEvaluation[object] | None:
    """Adapt Paper observations to the canonical template response contract."""
    alias = str(getattr(interaction, "location_alias", "")).upper()
    template = TRADE_TEMPLATES.get(("5m_recovery_breakout", alias))
    if template is None:
        return None
    if not isinstance(cutoff, datetime):
        return None
    candidates = tuple(
        item for item in observations
        if getattr(item, "timeframe", None) == getattr(interaction, "timeframe", None)
        and isinstance(getattr(item, "observed_at", None), datetime)
        and isinstance(getattr(interaction, "created_at", None), datetime)
        and getattr(item, "observed_at") > getattr(interaction, "created_at")
        and isinstance(getattr(item, "available_at", None), datetime)
        and getattr(item, "available_at") <= cutoff
    )[:max_candles]
    if not candidates:
        return ResponseEvaluation(None, False, (), None, template.direction)
    band_lower = getattr(interaction, "band_lower", None)
    band_upper = getattr(interaction, "band_upper", None)
    if not isinstance(band_lower, (int, float)) or not isinstance(band_upper, (int, float)):
        return None
    spec = build_response_spec(template, band_lower=float(band_lower), band_upper=float(band_upper))
    result = evaluate_response_window(
        candidates,
        to_candle=lambda item: ResponseCandle(
            float(getattr(item, "features")["open"]),
            float(getattr(item, "features")["high"]),
            float(getattr(item, "features")["low"]),
            float(getattr(item, "features")["close"]),
        ),
        spec_for=lambda _item: spec,
        armed=False,
        pre_contact_side=getattr(interaction, "pre_contact_side", None),
    )
    # Legacy Paper fixtures may omit template cell context.  Preserve causal
    # evidence while selecting direction from the pre-contact candle only.
    if result.status is not ResponseOutcomeStatus.CONFIRMED:
        first_features = getattr(candidates[0], "features", {})
        previous_close = first_features.get("previous_close") if isinstance(first_features, Mapping) else None
        reference = getattr(interaction, "reference_level", None)
        if isinstance(previous_close, (int, float)) and isinstance(reference, (int, float)) and previous_close != reference:
            inferred = Direction.LONG if previous_close < reference else Direction.SHORT
            template = replace(template, direction=inferred)
            spec = build_response_spec(template, band_lower=float(band_lower), band_upper=float(band_upper))
            result = evaluate_response_window(
                candidates,
                to_candle=lambda item: ResponseCandle(
                    float(getattr(item, "features")["open"]), float(getattr(item, "features")["high"]),
                    float(getattr(item, "features")["low"]), float(getattr(item, "features")["close"]),
                ),
                spec_for=lambda _item: spec,
                armed=False,
            )
    return result


def evaluate_policy_responses(
    interaction: object, observations: Sequence[object], *, max_candles: int, cutoff: object,
) -> tuple[tuple[TradeTemplate, ResponseEvaluation[object]], ...]:
    """Evaluate acceptance and rejection hypotheses independently."""
    alias = str(getattr(interaction, "location_alias", "")).upper()
    if not isinstance(cutoff, datetime):
        return ()
    templates = tuple(
        template for (family, location), template in TRADE_TEMPLATES.items()
        if location == alias and family in {"5m_recovery_breakout", "5m_acceptance_breakout"}
    )
    templates = tuple(sorted(templates, key=lambda item: item.response_type.value != "acceptance"))
    results: list[tuple[TradeTemplate, ResponseEvaluation[object]]] = []
    for template in templates:
        created_at = getattr(interaction, "created_at", None)
        candidates = tuple(
            item for item in observations
            if getattr(item, "timeframe", None) == getattr(interaction, "timeframe", None)
            and isinstance(getattr(item, "observed_at", None), datetime)
            and isinstance(created_at, datetime)
            and getattr(item, "observed_at") > created_at
            and isinstance(getattr(item, "available_at", None), datetime)
            and getattr(item, "available_at") <= cutoff
        )[:max_candles]
        lower, upper = getattr(interaction, "band_lower", None), getattr(interaction, "band_upper", None)
        if not isinstance(lower, (int, float)) or not isinstance(upper, (int, float)):
            continue
        spec = build_response_spec(template, band_lower=float(lower), band_upper=float(upper))
        evaluation = evaluate_response_window(
            candidates,
            to_candle=lambda item: ResponseCandle(
                float(getattr(item, "features")["open"]), float(getattr(item, "features")["high"]),
                float(getattr(item, "features")["low"]), float(getattr(item, "features")["close"]),
            ),
            spec_for=lambda _item: spec,
            armed=False,
            pre_contact_side=getattr(interaction, "pre_contact_side", None),
        )
        results.append((template, evaluation))
    return tuple(results)


__all__ = [
    "ResponseCandle", "ResponseEvaluation", "ResponseOutcomeStatus", "ResponseSpec",
    "advance_response_candle", "build_response_spec", "evaluate_policy_response", "evaluate_response_hypotheses", "evaluate_response_window",
]
