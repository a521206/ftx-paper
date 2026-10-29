"""Typed codecs for orderflow instrument, position, bar, and exit snapshots."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import datetime
from typing import Protocol, cast

from ftx_paper.contracts import Instrument, MarketBar, OptionType, OrderIntent, OrderRole, OrderSide, exit_plan_from_payload

from .exits import ExitAction, ExitStateMachine, PositionState
from .orderflow_config import OrderflowConfig
from .orderflow_context import ContextHistory
from .orderflow_interaction import InteractionDetector
from .orderflow_opportunity import SetupOpportunity
from .orderflow_location import LocationRegistry
from .orderflow_exit import exit_plan_payload


class _PolicySnapshotSource(Protocol):
    config: OrderflowConfig
    _session_id: str | None
    _processed_observation_ids: set[str]
    _processed_location_observation_ids: set[str]
    @property
    def _location_detectors(self) -> Mapping[str, InteractionDetector]: ...
    _context_history: ContextHistory
    _location_registry: LocationRegistry
    _context_version: int
    _session_high: float | None
    _session_low: float | None
    _admission_suspended: bool
    _gap_end: datetime | None
    _opening_gap_last_evaluated_id: str | None
    _opening_seed: dict[str, object] | None
    _pending_triggers: dict[str, dict[str, object]]
    @property
    def _opportunities(self) -> Mapping[str, SetupOpportunity]: ...

    def snapshot(self) -> Mapping[str, object]: ...


class _StrategySnapshotSource(Protocol):
    @property
    def policy(self) -> _PolicySnapshotSource: ...

    @property
    def _decision_positions(self) -> Mapping[str, tuple[PositionState, ExitStateMachine]]: ...

    @property
    def _pending_exits(self) -> Mapping[str, str]: ...

    @property
    def _pending_exit_actions(self) -> Mapping[str, tuple[ExitAction, datetime]]: ...

    @property
    def _completed_bars(self) -> Sequence[MarketBar]: ...
    @property
    def _exit_scores(self) -> Mapping[str, tuple[float | None, float | None]]: ...


def policy_snapshot_payload(policy: _PolicySnapshotSource) -> dict[str, object]:
    """Serialize policy state without taking ownership of its lifecycle."""
    config = asdict(policy.config)
    config["execution_mode"] = policy.config.execution_mode.value
    config["capture_mode"] = policy.config.capture_mode.value
    return {
        "schema_version": 3,
        "config": config,
        "session_id": policy._session_id,
        "processed_observation_ids": sorted(policy._processed_observation_ids),
        "processed_location_observation_ids": sorted(policy._processed_location_observation_ids),
        "location_detectors": {
            timeframe: detector.snapshot() for timeframe, detector in policy._location_detectors.items()
        },
        "context_history": policy._context_history.to_dict(),
        "location_registry": [
            {
                **asdict(item),
                "created_at": item.created_at.isoformat(),
                "available_at": item.available_at.isoformat(),
                "band": asdict(item.band),
            }
            for item in policy._location_registry.records.values()
        ],
        "context_version": policy._context_version,
        "session_high": policy._session_high,
        "session_low": policy._session_low,
        "admission_suspended": policy._admission_suspended,
        "gap_end": policy._gap_end.isoformat() if policy._gap_end else None,
        "opening_gap_last_evaluated_id": policy._opening_gap_last_evaluated_id,
        "opening_seed": dict(policy._opening_seed) if policy._opening_seed is not None else None,
        "opportunities": [policy._opportunities[key].snapshot() for key in sorted(policy._opportunities)],
        "pending_triggers": {
            key: {
                "direction": value.get("direction"),
                "template_id": getattr(value.get("template"), "template_id", None),
                "interaction_id": getattr(value.get("interaction"), "interaction_id", None),
                "retest_touched": value.get("retest_touched", False),
            }
            for key, value in policy._pending_triggers.items()
        },
    }


def strategy_snapshot_payload(strategy: _StrategySnapshotSource) -> dict[str, object]:
    """Assemble the strategy wrapper while preserving policy ownership."""
    decision_positions = {
        order_id: {"position": position_snapshot(position), "exit_state": exits.snapshot()}
        for order_id, (position, exits) in strategy._decision_positions.items()
    }
    pending_actions = {
        order_id: {"action": exit_action_snapshot(action), "discovered_at": discovered_at.isoformat()}
        for order_id, (action, discovered_at) in strategy._pending_exit_actions.items()
    }
    return {
        **strategy.policy.snapshot(),
        "decision_positions": decision_positions,
        "pending_exits": dict(strategy._pending_exits),
        "pending_exit_actions": pending_actions,
        "completed_bars": [bar_snapshot(item) for item in strategy._completed_bars],
        "exit_scores": {
            location: list(scores) for location, scores in strategy._exit_scores.items()
        },
    }


def instrument_snapshot(instrument: Instrument) -> dict[str, object]:
    return {
        "symbol": instrument.symbol, "exchange": instrument.exchange,
        "instrument_type": instrument.instrument_type, "expiry": instrument.expiry,
        "strike": instrument.strike,
        "option_type": instrument.option_type.value if instrument.option_type is not None else None,
    }


def instrument_from_snapshot(raw: Mapping[str, object]) -> Instrument:
    option_type = raw.get("option_type")
    strike_value = raw.get("strike")
    strike = float(strike_value) if isinstance(strike_value, (int, float)) and not isinstance(strike_value, bool) else None
    return Instrument(
        str(raw["symbol"]), str(raw["exchange"]), str(raw["instrument_type"]),
        cast(str | None, raw.get("expiry")) if isinstance(raw.get("expiry"), str) else None, strike,
        OptionType(option_type) if isinstance(option_type, str) else None,
    )


def position_snapshot(position: PositionState) -> dict[str, object]:
    return {
        "instrument": instrument_snapshot(position.instrument), "entry_price": position.entry_price,
        "stop_price": position.stop_price, "quantity": position.quantity, "side": position.side.value,
        "stop_policy": position.stop_policy, "cell": position.cell, "exit_mode": position.exit_mode,
        "entry_fill_time": position.entry_fill_time.isoformat() if position.entry_fill_time else None,
        "exit_reference_price": position.exit_reference_price, "target_price": position.target_price,
        "vehicle": position.vehicle,
        "synthetic_legs": [instrument_snapshot(item) for item in position.synthetic_legs] if position.synthetic_legs else None,
        "entry_bar": position.entry_bar, "entry_order_id": position.entry_order_id,
        "exit_plan": exit_plan_payload(position.exit_plan),
        "initial_risk_points": position.initial_risk_points, "partial_exit_enabled": position.partial_exit_enabled,
        "stop_source": position.stop_source, "post_3r_activated": position.post_3r_activated,
    }


def position_from_snapshot(raw: Mapping[str, object]) -> PositionState:
    instrument = raw.get("instrument")
    if not isinstance(instrument, Mapping):
        raise ValueError("orderflow position instrument is invalid")
    legs_raw = raw.get("synthetic_legs")
    legs = None
    if isinstance(legs_raw, (list, tuple)):
        if len(legs_raw) != 2 or any(not isinstance(item, Mapping) for item in legs_raw):
            raise ValueError("orderflow synthetic legs are invalid")
        legs = (instrument_from_snapshot(legs_raw[0]), instrument_from_snapshot(legs_raw[1]))
    entry_fill_time = raw.get("entry_fill_time")
    if entry_fill_time is not None and not isinstance(entry_fill_time, str):
        raise ValueError("orderflow entry fill time is invalid")
    entry_price_value, stop_price_value = raw.get("entry_price"), raw.get("stop_price")
    if (not isinstance(entry_price_value, (int, float)) or isinstance(entry_price_value, bool)
            or not isinstance(stop_price_value, (int, float)) or isinstance(stop_price_value, bool)):
        raise ValueError("orderflow position prices are invalid")
    quantity_value = raw.get("quantity")
    if not isinstance(quantity_value, int) or isinstance(quantity_value, bool):
        raise ValueError("orderflow position quantity is invalid")

    def optional_float(name: str) -> float | None:
        value = raw.get(name)
        return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None

    return PositionState(
        instrument_from_snapshot(instrument), float(entry_price_value), float(stop_price_value),
        quantity_value, OrderSide(str(raw["side"])),
        stop_policy=cast(str, raw["stop_policy"]) if isinstance(raw.get("stop_policy"), str) else None,
        cell=cast(str, raw["cell"]) if isinstance(raw.get("cell"), str) else None,
        exit_mode=str(raw.get("exit_mode", "signal")),
        entry_fill_time=datetime.fromisoformat(entry_fill_time) if entry_fill_time else None,
        exit_reference_price=optional_float("exit_reference_price"), target_price=optional_float("target_price"),
        vehicle=str(raw.get("vehicle", "futures")), synthetic_legs=legs,
        exit_plan=exit_plan_from_payload(raw.get("exit_plan")),
        entry_bar=cast(int, raw["entry_bar"]) if isinstance(raw.get("entry_bar"), int) else None,
        entry_order_id=cast(str, raw["entry_order_id"]) if isinstance(raw.get("entry_order_id"), str) else None,
        initial_risk_points=optional_float("initial_risk_points"), partial_exit_enabled=bool(raw.get("partial_exit_enabled", False)),
        stop_source=str(raw.get("stop_source", "initial")), post_3r_activated=bool(raw.get("post_3r_activated", False)),
    )


def bar_snapshot(bar: MarketBar) -> dict[str, object]:
    return {"instrument": instrument_snapshot(bar.instrument), "timestamp": bar.timestamp.isoformat(),
            "open": bar.open, "high": bar.high, "low": bar.low, "close": bar.close,
            "volume": bar.volume, "open_interest": bar.open_interest}


def bar_from_snapshot(raw: Mapping[str, object]) -> MarketBar:
    instrument = raw.get("instrument")
    timestamp = raw.get("timestamp")
    if not isinstance(instrument, Mapping):
        raise ValueError("orderflow completed bar instrument is invalid")
    if not isinstance(timestamp, str):
        raise ValueError("orderflow completed bar timestamp is invalid")
    values = tuple(raw.get(name) for name in ("open", "high", "low", "close"))
    if any(not isinstance(value, (int, float)) or isinstance(value, bool) for value in values):
        raise ValueError("orderflow completed bar prices are invalid")
    numeric_values = cast(tuple[int | float, int | float, int | float, int | float], values)
    volume = raw.get("volume")
    open_interest = raw.get("open_interest")
    open_price, high_price, low_price, close_price = (float(value) for value in numeric_values)
    volume_value = float(volume) if isinstance(volume, (int, float)) and not isinstance(volume, bool) else None
    open_interest_value = float(open_interest) if isinstance(open_interest, (int, float)) and not isinstance(open_interest, bool) else None
    return MarketBar(instrument_from_snapshot(instrument), datetime.fromisoformat(timestamp),
                     open_price, high_price, low_price, close_price, volume_value, open_interest_value)


def exit_action_snapshot(action: ExitAction) -> dict[str, object]:
    intent = action.intent
    return {"reason": action.reason, "price": action.price, "cell": action.cell, "bars_held": action.bars_held,
            "mae_bp": action.mae_bp, "mfe_bp": action.mfe_bp,
            "intent": {"client_order_id": intent.client_order_id, "instrument": instrument_snapshot(intent.instrument),
                       "side": intent.side.value, "quantity": intent.quantity, "role": intent.role.value,
                       "reason": intent.reason, "entry_order_id": intent.entry_order_id, "vehicle": intent.vehicle,
                        "exit_mode": intent.exit_mode, "synthetic_legs": [instrument_snapshot(item) for item in intent.synthetic_legs] if intent.synthetic_legs else None,
                        "entry_bar": intent.entry_bar, "exit_plan": exit_plan_payload(intent.exit_plan)}}


def exit_action_from_snapshot(raw: Mapping[str, object]) -> ExitAction:
    intent_raw = raw.get("intent")
    if not isinstance(intent_raw, Mapping):
        raise ValueError("orderflow exit intent is invalid")
    instrument_raw = intent_raw.get("instrument")
    if not isinstance(instrument_raw, Mapping):
        raise ValueError("orderflow exit instrument is invalid")
    intent = OrderIntent(str(intent_raw["client_order_id"]), instrument_from_snapshot(instrument_raw),
                        OrderSide(str(intent_raw["side"])), int(intent_raw["quantity"]),
                        role=OrderRole(str(intent_raw["role"])), reason=str(intent_raw.get("reason", "")),
                        entry_order_id=intent_raw.get("entry_order_id") if isinstance(intent_raw.get("entry_order_id"), str) else None,
                         vehicle=str(intent_raw.get("vehicle", "futures")), exit_mode=intent_raw.get("exit_mode") if isinstance(intent_raw.get("exit_mode"), str) else None,
                         entry_bar=int(intent_raw["entry_bar"]) if isinstance(intent_raw.get("entry_bar"), int) else None,
                         exit_plan=exit_plan_from_payload(intent_raw.get("exit_plan")))
    cell = cast(str, raw["cell"]) if isinstance(raw.get("cell"), str) else None
    return ExitAction(str(raw["reason"]), float(cast(int | float, raw["price"])), intent,
                      cell, int(cast(int, raw.get("bars_held", 0))),
                      float(cast(int | float, raw.get("mae_bp", 0.0))),
                      float(cast(int | float, raw.get("mfe_bp", 0.0))))


__all__ = ["bar_from_snapshot", "bar_snapshot", "exit_action_from_snapshot", "exit_action_snapshot",
           "instrument_from_snapshot", "instrument_snapshot", "position_from_snapshot", "position_snapshot"]
