from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from math import isfinite

from .market import Instrument


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(StrEnum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class OrderRole(StrEnum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"


class OrderLifecycleState(StrEnum):
    PROPOSED = "PROPOSED"
    VALIDATED = "VALIDATED"
    AUTHORIZED = "AUTHORIZED"
    SUBMITTING = "SUBMITTING"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"
    RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"


@dataclass(frozen=True, slots=True)
class ExitPlanRule:
    """Frozen, strategy-neutral representation of a canonical exit rule."""

    rule_id: str
    location: str
    mode: str
    required: bool
    trigger: str
    score_direction: str | None = None
    score_margin: float | None = None
    response_direction: str | None = None
    response_type: str | None = None
    evidence: str = "touch"
    order: str = "next_bar_open"
    fill: str = "open"
    gap_policy: str = "fill_open"
    level: float | None = None
    band_lower: float | None = None
    band_upper: float | None = None
    interaction_id: str | None = None

    def __post_init__(self) -> None:
        if not self.rule_id or not self.location:
            raise ValueError("exit plan rule identity is required")
        if self.mode not in {"fixed_target", "conditional"}:
            raise ValueError("unsupported exit plan mode")
        if self.trigger not in {"touch_after_confirmation", "touch", "completed_close"}:
            raise ValueError("unsupported exit plan trigger")
        if self.evidence not in {"touch", "completed_close"}:
            raise ValueError("unsupported exit plan evidence")
        if self.order != "next_bar_open" or self.fill != "open" or self.gap_policy != "fill_open":
            raise ValueError("unsupported exit plan execution timing")
        if self.score_margin is not None and self.score_margin < 0:
            raise ValueError("exit plan score margin cannot be negative")
        if self.score_direction is not None and self.score_direction not in {"long", "short"}:
            raise ValueError("unsupported exit plan score direction")
        if self.response_direction is not None and self.response_direction not in {"long", "short"}:
            raise ValueError("unsupported exit plan response direction")
        if self.response_type is not None and self.response_type not in {"rejection", "acceptance"}:
            raise ValueError("unsupported exit plan response type")
        for value in (self.score_margin, self.level, self.band_lower, self.band_upper):
            if value is not None and not isfinite(value):
                raise ValueError("exit plan numeric fields must be finite")
        if self.level is not None and self.level <= 0:
            raise ValueError("exit plan level must be positive")
        if self.band_lower is not None and self.band_upper is not None and self.band_lower > self.band_upper:
            raise ValueError("exit plan band is inverted")


def exit_plan_from_payload(raw: object) -> tuple[ExitPlanRule, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, (list, tuple)):
        raise ValueError("exit plan must be a sequence")
    rules: list[ExitPlanRule] = []
    for item in raw:
        if not isinstance(item, Mapping):
            raise ValueError("exit plan rule must be an object")
        level = item.get("level")
        margin = item.get("score_margin")
        def optional_number(name: str, value: object) -> float | None:
            if value is None:
                return None
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value):
                raise ValueError(f"exit plan {name} is invalid")
            return float(value)
        required = item.get("required")
        if not isinstance(required, bool):
            raise ValueError("exit plan required must be boolean")
        for name in ("rule_id", "location", "mode", "trigger"):
            if not isinstance(item.get(name), str) or not item[name]:
                raise ValueError(f"exit plan {name} is invalid")
        for name in ("score_direction", "response_direction", "response_type", "interaction_id"):
            if item.get(name) is not None and not isinstance(item.get(name), str):
                raise ValueError(f"exit plan {name} is invalid")
        rules.append(ExitPlanRule(
            rule_id=item["rule_id"], location=item["location"],
            mode=item["mode"], required=required,
            trigger=str(item["trigger"]),
            score_direction=str(item["score_direction"]) if isinstance(item.get("score_direction"), str) else None,
            score_margin=optional_number("score margin", margin),
            response_direction=str(item["response_direction"]) if isinstance(item.get("response_direction"), str) else None,
            response_type=str(item["response_type"]) if isinstance(item.get("response_type"), str) else None,
            evidence=str(item.get("evidence", "touch")), order=str(item.get("order", "next_bar_open")),
            fill=str(item.get("fill", "open")), gap_policy=str(item.get("gap_policy", "fill_open")),
            level=optional_number("level", level),
            band_lower=optional_number("band lower", item.get("band_lower")),
            band_upper=optional_number("band upper", item.get("band_upper")),
            interaction_id=str(item["interaction_id"]) if isinstance(item.get("interaction_id"), str) else None,
        ))
    return tuple(rules)


_ORDER_LIFECYCLE_TRANSITIONS: dict[OrderLifecycleState, frozenset[OrderLifecycleState]] = {
    OrderLifecycleState.PROPOSED: frozenset({
        OrderLifecycleState.VALIDATED,
        OrderLifecycleState.AUTHORIZED,
        # Preserve the pre-state-machine submission path during migration.
        OrderLifecycleState.SUBMITTING,
        OrderLifecycleState.REJECTED,
        OrderLifecycleState.CANCELLED,
    }),
    OrderLifecycleState.VALIDATED: frozenset({
        OrderLifecycleState.AUTHORIZED,
        OrderLifecycleState.REJECTED,
        OrderLifecycleState.CANCELLED,
    }),
    OrderLifecycleState.AUTHORIZED: frozenset({
        OrderLifecycleState.SUBMITTING,
        OrderLifecycleState.REJECTED,
        OrderLifecycleState.CANCELLED,
    }),
    OrderLifecycleState.SUBMITTING: frozenset({
        OrderLifecycleState.ACKNOWLEDGED,
        OrderLifecycleState.FILLED,
        OrderLifecycleState.UNKNOWN,
        OrderLifecycleState.REJECTED,
    }),
    OrderLifecycleState.ACKNOWLEDGED: frozenset({
        OrderLifecycleState.PARTIALLY_FILLED,
        OrderLifecycleState.FILLED,
        OrderLifecycleState.CANCEL_REQUESTED,
        OrderLifecycleState.UNKNOWN,
    }),
    OrderLifecycleState.PARTIALLY_FILLED: frozenset({
        OrderLifecycleState.FILLED,
        OrderLifecycleState.CANCEL_REQUESTED,
        OrderLifecycleState.UNKNOWN,
    }),
    OrderLifecycleState.CANCEL_REQUESTED: frozenset({
        OrderLifecycleState.FILLED,
        OrderLifecycleState.PARTIALLY_FILLED,
        OrderLifecycleState.CANCELLED,
        OrderLifecycleState.UNKNOWN,
    }),
    OrderLifecycleState.UNKNOWN: frozenset({
        OrderLifecycleState.ACKNOWLEDGED,
        OrderLifecycleState.PARTIALLY_FILLED,
        OrderLifecycleState.FILLED,
        OrderLifecycleState.CANCELLED,
        OrderLifecycleState.REJECTED,
    }),
    OrderLifecycleState.FILLED: frozenset(),
    OrderLifecycleState.CANCELLED: frozenset(),
    OrderLifecycleState.REJECTED: frozenset(),
    OrderLifecycleState.RECONCILIATION_REQUIRED: frozenset(),
}


def is_legal_order_transition(current: str, new: str) -> bool:
    """Return whether an order lifecycle state can advance to ``new``."""
    try:
        current_state = OrderLifecycleState(str(current).upper())
        new_state = OrderLifecycleState(str(new).upper())
    except ValueError:
        return False
    return current_state == new_state or new_state in _ORDER_LIFECYCLE_TRANSITIONS[current_state]


@dataclass(slots=True)
class OrderLifecycle:
    """In-memory lifecycle aggregate for observed order and fill events."""

    requested_quantity: int
    state: OrderLifecycleState = OrderLifecycleState.PROPOSED
    filled_quantity: int = 0
    cancellation_requested: bool = False
    _fill_ids: set[str] = field(default_factory=set, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.requested_quantity <= 0:
            raise ValueError("requested quantity must be positive")
        if self.filled_quantity < 0 or self.filled_quantity > self.requested_quantity:
            raise ValueError("filled quantity is outside requested quantity")

    @property
    def remaining_quantity(self) -> int:
        return self.requested_quantity - self.filled_quantity

    @property
    def fill_ids(self) -> frozenset[str]:
        return frozenset(self._fill_ids)

    def transition(self, state: OrderLifecycleState) -> bool:
        if self.state is state:
            return False
        if not is_legal_order_transition(self.state.value, state.value):
            raise ValueError(f"illegal order transition: {self.state} -> {state}")
        self.state = state
        if state is OrderLifecycleState.CANCEL_REQUESTED:
            self.cancellation_requested = True
        return True

    def request_cancel(self) -> bool:
        if self.state in {
            OrderLifecycleState.FILLED,
            OrderLifecycleState.CANCELLED,
            OrderLifecycleState.REJECTED,
        }:
            return False
        if self.state in {
            OrderLifecycleState.PROPOSED,
            OrderLifecycleState.VALIDATED,
            OrderLifecycleState.AUTHORIZED,
        }:
            self.state = OrderLifecycleState.CANCELLED
            return True
        self.transition(OrderLifecycleState.CANCEL_REQUESTED)
        return True

    def mark_unknown(self) -> bool:
        if self.state is OrderLifecycleState.UNKNOWN:
            return False
        self.transition(OrderLifecycleState.UNKNOWN)
        return True

    def apply_fill(self, fill_id: str, quantity: int) -> bool:
        """Apply one observed fill exactly once and preserve cancel intent."""
        if not fill_id:
            raise ValueError("fill ID is required")
        if quantity <= 0:
            raise ValueError("fill quantity must be positive")
        if fill_id in self._fill_ids:
            return False
        if self.state in {
            OrderLifecycleState.FILLED,
            OrderLifecycleState.CANCELLED,
            OrderLifecycleState.REJECTED,
        }:
            raise ValueError(f"cannot fill terminal order in state {self.state}")
        if quantity > self.remaining_quantity:
            raise ValueError("fill quantity exceeds remaining quantity")
        self._fill_ids.add(fill_id)
        self.filled_quantity += quantity
        if self.remaining_quantity == 0:
            self.state = OrderLifecycleState.FILLED
        elif not self.cancellation_requested:
            self.state = OrderLifecycleState.PARTIALLY_FILLED
        return True

    def cancel_remaining(self) -> bool:
        if self.remaining_quantity == 0 or self.state in {
            OrderLifecycleState.CANCELLED,
            OrderLifecycleState.REJECTED,
        }:
            return False
        if self.state is not OrderLifecycleState.CANCEL_REQUESTED:
            self.request_cancel()
        self.state = OrderLifecycleState.CANCELLED
        return True


@dataclass(frozen=True, slots=True)
class OrderAck:
    client_order_id: str
    broker_order_id: str
    status: str


@dataclass(frozen=True, slots=True)
class OrderIntent:
    client_order_id: str
    instrument: Instrument
    side: OrderSide
    quantity: int
    order_type: OrderType = OrderType.MARKET
    limit_price: float | None = None
    reason: str = ""
    # Entry provenance used to reconstruct live protective exits.
    cell: str | None = None
    stop_price: float | None = None
    stop_policy: str | None = None
    exit_mode: str | None = None
    target_price: float | None = None
    exit_plan: tuple[ExitPlanRule, ...] = ()
    entry_bar: int | None = None
    trigger_price: float | None = field(default=None, kw_only=True)
    requested_fill_price: float | None = field(default=None, kw_only=True)
    role: OrderRole = field(kw_only=True)
    # Execution vehicle is a property of this order, never of the strategy.
    # Synthetic orders retain the futures instrument as their logical mark
    # and carry the selected CE/PE legs explicitly for the execution layer.
    vehicle: str = field(default="futures", kw_only=True)
    synthetic_legs: tuple[Instrument, Instrument] | None = field(default=None, kw_only=True)
    entry_order_id: str | None = field(default=None, kw_only=True)
    request_id: str | None = field(default=None, kw_only=True)
    strategy_id: str | None = field(default=None, kw_only=True)
    account_id: str | None = field(default=None, kw_only=True)
    execution_group_id: str | None = field(default=None, kw_only=True)
    lifecycle_id: str | None = field(default=None, kw_only=True)
    decision_id: str | None = field(default=None, kw_only=True)
    setup_availability: str | None = field(default=None, kw_only=True)
    structural_rank: int = field(default=0, kw_only=True)

    @property
    def lifecycle_decision_id(self) -> str:
        """Decision identifier used to link entry lifecycle events."""
        return self.decision_id or self.client_order_id

    def __post_init__(self) -> None:
        vehicle = str(self.vehicle).lower()
        if vehicle not in {"futures", "synthetic"}:
            raise ValueError("order vehicle must be 'futures' or 'synthetic'")
        for name, value in (("trigger_price", self.trigger_price), ("requested_fill_price", self.requested_fill_price)):
            if value is not None and (not isfinite(value) or value <= 0):
                raise ValueError(f"{name} must be a positive finite number")
        if vehicle == "synthetic":
            if self.synthetic_legs is None or len(self.synthetic_legs) != 2:
                raise ValueError("synthetic orders must contain a CE/PE leg pair")
            call, put = self.synthetic_legs
            if {call.instrument_type.upper(), put.instrument_type.upper()} != {"CE", "PE"}:
                raise ValueError("synthetic orders must contain one CE and one PE leg")
            if (call.expiry, call.strike) != (put.expiry, put.strike):
                raise ValueError("synthetic CE/PE legs must share expiry and strike")
        object.__setattr__(self, "vehicle", vehicle)
        if any(not isinstance(rule, ExitPlanRule) for rule in self.exit_plan):
            raise TypeError("order exit_plan must contain ExitPlanRule values")
        if self.structural_rank < 0:
            raise ValueError("structural rank must be non-negative")
