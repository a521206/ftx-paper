"""Declarative orderflow strategy templates.

The template registry is pure policy data.  Runtime modules consume these
objects to derive response geometry, trigger requirements, and exit policy;
they do not embed location-specific direction rules.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Mapping


class Direction(StrEnum):
    LONG = "long"
    SHORT = "short"


class LocationTemplate(StrEnum):
    VWAP = "VWAP"
    OR_HIGH = "OR_HIGH"
    OR_LOW = "OR_LOW"
    PD_HIGH = "PD_HIGH"
    PD_LOW = "PD_LOW"
    SESSION_HIGH = "SESSION_HIGH"
    SESSION_LOW = "SESSION_LOW"


class OpeningInteractionKind(StrEnum):
    OPENING_BREAKOUT = "opening_breakout"
    OPENING_REJECTION = "opening_rejection"
    OPENING_RETEST = "opening_retest"


class ResponseType(StrEnum):
    REJECTION = "rejection"
    ACCEPTANCE = "acceptance"


class TriggerType(StrEnum):
    BREAKOUT = "breakout"
    RECLAIM = "reclaim"
    PULLBACK = "pullback"
    IMMEDIATE = "immediate"


class StopPolicy(StrEnum):
    STRUCTURAL_TRAILING = "structural_trailing"
    OR_VWAP = "or_vwap"


class ExitMode(StrEnum):
    FIXED_TARGET = "fixed_target"
    CONDITIONAL = "conditional"


@dataclass(frozen=True, slots=True)
class ExitExecutionTiming:
    evidence: str = "touch"
    order: str = "next_bar_open"
    fill: str = "open"
    gap_policy: str = "fill_open"

    def __post_init__(self) -> None:
        if self.evidence not in {"touch", "completed_close"}:
            raise ValueError("unsupported execution evidence")
        if self.order not in {"next_bar_open", "same_bar"}:
            raise ValueError("unsupported execution order")
        if self.fill not in {"open", "level"}:
            raise ValueError("unsupported execution fill")
        if self.gap_policy not in {"fill_open", "reject"}:
            raise ValueError("unsupported execution gap policy")
        if self.evidence == "completed_close" and self.order != "next_bar_open":
            raise ValueError("completed-close exits require next-bar execution")
        if self.order == "same_bar":
            raise ValueError("same-bar exits are not supported by the replay")
        if self.fill != "open" or self.gap_policy != "fill_open":
            raise ValueError("the replay supports only next-bar-open fill_open execution")


@dataclass(frozen=True, slots=True)
class ExitResponseConfig:
    direction: Direction
    response_type: ResponseType

    def __post_init__(self) -> None:
        object.__setattr__(self, "direction", Direction(self.direction))
        object.__setattr__(self, "response_type", ResponseType(self.response_type))


@dataclass(frozen=True, slots=True)
class TradeExit:
    rule_id: str
    location: LocationTemplate
    mode: ExitMode
    required: bool
    execution: ExitExecutionTiming
    trigger: str
    score_direction: str | None = None
    score_margin: float | None = None
    response: ExitResponseConfig | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "location", LocationTemplate(self.location))
        object.__setattr__(self, "mode", ExitMode(self.mode))
        if not isinstance(self.execution, ExitExecutionTiming):
            raise TypeError("trade exits require structured execution timing")


@dataclass(frozen=True, slots=True)
class TradeTemplate:
    template_id: str
    version: str
    thesis: str
    locations: tuple[LocationTemplate, ...]
    direction: Direction
    response_type: ResponseType
    response_timeframe: str
    carry_sessions: int
    trigger_type: TriggerType
    exits: tuple[TradeExit, ...]
    stop_policy: StopPolicy = StopPolicy.STRUCTURAL_TRAILING
    expiry_at: str = "15:00"
    required_confirmation: tuple[str, ...] = ()
    displacement_required: bool | None = None
    execution_conflict_group: str | None = None

    def __post_init__(self) -> None:
        locations = tuple(dict.fromkeys(LocationTemplate(item) for item in self.locations))
        if not locations:
            raise ValueError("trade template requires at least one location")
        if self.carry_sessions < 0:
            raise ValueError("carry_sessions cannot be negative")
        object.__setattr__(self, "locations", locations)
        object.__setattr__(self, "direction", Direction(self.direction))
        object.__setattr__(self, "response_type", ResponseType(self.response_type))
        object.__setattr__(self, "trigger_type", TriggerType(self.trigger_type))
        object.__setattr__(self, "stop_policy", StopPolicy(self.stop_policy))

    @property
    def policy_version(self) -> str:
        return f"location-{self.response_type.value}-{self.response_timeframe}-v1"

    @property
    def requires_displacement(self) -> bool:
        return self.displacement_required if self.displacement_required is not None else self.response_type is ResponseType.ACCEPTANCE


TRADE_EXITS: Mapping[str, TradeExit] = {
    "short_to_or_low": TradeExit(
        rule_id="short-to-or-low-exit", location=LocationTemplate.OR_LOW,
        mode=ExitMode.CONDITIONAL, required=False, execution=ExitExecutionTiming(),
        trigger="touch_after_confirmation", response=ExitResponseConfig(Direction.LONG, ResponseType.REJECTION),
        score_direction="long", score_margin=0.0,
    ),
    "long_to_or_high": TradeExit(
        rule_id="long-to-or-high-exit", location=LocationTemplate.OR_HIGH,
        mode=ExitMode.CONDITIONAL, required=False, execution=ExitExecutionTiming(),
        trigger="touch_after_confirmation", response=ExitResponseConfig(Direction.SHORT, ResponseType.REJECTION),
        score_direction="short", score_margin=0.0,
    ),
    "pd_high_short": TradeExit(
        rule_id="pd_high-short-exit", location=LocationTemplate.PD_LOW,
        mode=ExitMode.CONDITIONAL, required=False, execution=ExitExecutionTiming(),
        trigger="touch_after_confirmation", response=ExitResponseConfig(Direction.LONG, ResponseType.REJECTION),
        score_direction="long", score_margin=0.0,
    ),
    "pd_low_long": TradeExit(
        rule_id="pd_low-long-exit", location=LocationTemplate.PD_HIGH,
        mode=ExitMode.CONDITIONAL, required=False, execution=ExitExecutionTiming(),
        trigger="touch_after_confirmation", response=ExitResponseConfig(Direction.SHORT, ResponseType.REJECTION),
        score_direction="short", score_margin=0.0,
    ),
}


LONG_RECOVERY_LOW = TradeTemplate(
    template_id="long-recovery-breakout-low", version="1",
    thesis="long recovery_breakout at low locations",
    locations=(LocationTemplate.OR_LOW, LocationTemplate.SESSION_LOW),
    direction=Direction.LONG, response_type=ResponseType.REJECTION,
    response_timeframe="5m", carry_sessions=1,
    trigger_type=TriggerType.BREAKOUT,
    exits=(TRADE_EXITS["long_to_or_high"],),
    execution_conflict_group="low-long-recovery",
)


TRADE_TEMPLATES: Mapping[tuple[str, str], TradeTemplate] = {
    ("5m_recovery_breakout", "OR_HIGH"): TradeTemplate(
        template_id="or_high-short-recovery_breakout", version="1",
        thesis="short recovery_breakout at OR_HIGH", locations=(LocationTemplate.OR_HIGH,),
        direction=Direction.SHORT, response_type=ResponseType.REJECTION,
        response_timeframe="5m", carry_sessions=1,
        trigger_type=TriggerType.BREAKOUT,
        exits=(),
        execution_conflict_group="high-short-recovery",
    ),
    ("5m_recovery_breakout", "OR_LOW"): LONG_RECOVERY_LOW,
    ("5m_recovery_breakout", "PD_HIGH"): TradeTemplate(
        template_id="pd_high-short-recovery_breakout", version="1",
        thesis="short recovery_breakout at PD_HIGH", locations=(LocationTemplate.PD_HIGH,),
        direction=Direction.SHORT, response_type=ResponseType.REJECTION,
        response_timeframe="5m", carry_sessions=1,
        trigger_type=TriggerType.PULLBACK,
        exits=(TRADE_EXITS["pd_high_short"],),
    ),
    ("5m_recovery_breakout", "PD_LOW"): TradeTemplate(
        template_id="pd_low-long-recovery_breakout", version="1",
        thesis="long recovery_breakout at PD_LOW", locations=(LocationTemplate.PD_LOW,),
        direction=Direction.LONG, response_type=ResponseType.REJECTION,
        response_timeframe="5m", carry_sessions=1,
        trigger_type=TriggerType.PULLBACK,
        exits=(TRADE_EXITS["pd_low_long"],),
    ),
    ("5m_recovery_breakout", "SESSION_HIGH"): TradeTemplate(
        template_id="session_high-short-recovery_breakout", version="1",
        thesis="short recovery_breakout at SESSION_HIGH", locations=(LocationTemplate.SESSION_HIGH,),
        direction=Direction.SHORT, response_type=ResponseType.REJECTION,
        response_timeframe="5m", carry_sessions=1,
        trigger_type=TriggerType.BREAKOUT,
        exits=(TRADE_EXITS["short_to_or_low"],),
        execution_conflict_group="high-short-recovery",
    ),
    ("5m_recovery_breakout", "SESSION_LOW"): LONG_RECOVERY_LOW,
    ("5m_acceptance_breakout", "OR_HIGH"): TradeTemplate(
        template_id="or_high-long-acceptance_breakout", version="1",
        thesis="long acceptance_breakout at OR_HIGH", locations=(LocationTemplate.OR_HIGH,),
        direction=Direction.LONG, response_type=ResponseType.ACCEPTANCE,
        response_timeframe="5m", carry_sessions=1,
        trigger_type=TriggerType.BREAKOUT,
        exits=(),
        stop_policy=StopPolicy.OR_VWAP,
    ),
    ("5m_acceptance_breakout", "OR_LOW"): TradeTemplate(
        template_id="or_low-short-acceptance_breakout", version="1",
        thesis="short acceptance_breakout at OR_LOW", locations=(LocationTemplate.OR_LOW,),
        direction=Direction.SHORT, response_type=ResponseType.ACCEPTANCE,
        response_timeframe="5m", carry_sessions=1,
        trigger_type=TriggerType.BREAKOUT,
        exits=(),
        stop_policy=StopPolicy.OR_VWAP,
    ),
    ("5m_acceptance_breakout", "PD_HIGH"): TradeTemplate(
        template_id="pd_high-short-acceptance_breakout", version="1",
        thesis="short acceptance_breakout at PD_HIGH", locations=(LocationTemplate.PD_HIGH,),
        direction=Direction.SHORT, response_type=ResponseType.ACCEPTANCE,
        response_timeframe="5m", carry_sessions=1,
        trigger_type=TriggerType.PULLBACK,
        exits=(TRADE_EXITS["pd_high_short"],),
    ),
    ("5m_acceptance_breakout", "PD_LOW"): TradeTemplate(
        template_id="pd_low-long-acceptance_breakout", version="1",
        thesis="long acceptance_breakout at PD_LOW", locations=(LocationTemplate.PD_LOW,),
        direction=Direction.LONG, response_type=ResponseType.ACCEPTANCE,
        response_timeframe="5m", carry_sessions=1,
        trigger_type=TriggerType.PULLBACK,
        exits=(TRADE_EXITS["pd_low_long"],),
    ),
    ("5m_acceptance_breakout", "SESSION_HIGH"): TradeTemplate(
        template_id="session_high-long-acceptance_breakout", version="1",
        thesis="long acceptance_breakout at SESSION_HIGH", locations=(LocationTemplate.SESSION_HIGH,),
        direction=Direction.LONG, response_type=ResponseType.ACCEPTANCE,
        response_timeframe="5m", carry_sessions=1,
        trigger_type=TriggerType.BREAKOUT,
        exits=(TRADE_EXITS["short_to_or_low"],),
        stop_policy=StopPolicy.OR_VWAP,
    ),
    ("5m_acceptance_breakout", "SESSION_LOW"): TradeTemplate(
        template_id="session_low-short-acceptance_breakout", version="1",
        thesis="short acceptance_breakout at SESSION_LOW", locations=(LocationTemplate.SESSION_LOW,),
        direction=Direction.SHORT, response_type=ResponseType.ACCEPTANCE,
        response_timeframe="5m", carry_sessions=1,
        trigger_type=TriggerType.BREAKOUT,
        exits=(TRADE_EXITS["long_to_or_high"],),
        stop_policy=StopPolicy.OR_VWAP,
    ),
}

# Keep the canonical default subset rule even though every current template
# uses one of these registered locations.
DEFAULT_TEMPLATES: Mapping[str, TradeTemplate] = {
    template.template_id: template
    for template in TRADE_TEMPLATES.values()
    if template.response_type is ResponseType.REJECTION
    or any(location.value in {
        "OR_LOW", "OR_HIGH", "PD_LOW", "PD_HIGH", "SESSION_LOW", "SESSION_HIGH",
    } for location in template.locations)
}
DEFAULT_REJECTION_TEMPLATES: Mapping[str, TradeTemplate] = {
    key: item for key, item in DEFAULT_TEMPLATES.items() if item.response_type is ResponseType.REJECTION
}

__all__ = [
    "DEFAULT_REJECTION_TEMPLATES", "DEFAULT_TEMPLATES", "Direction",
    "ExitExecutionTiming", "ExitMode", "LocationTemplate", "OpeningInteractionKind",
    "ResponseType", "StopPolicy", "TradeExit", "ExitResponseConfig",
    "TradeTemplate", "TRADE_EXITS", "TRADE_TEMPLATES", "TriggerType",
]
