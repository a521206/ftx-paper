"""Snapshot validation and restoration for the orderflow strategy."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import TYPE_CHECKING

from .orderflow_codec import (
    bar_from_snapshot,
    exit_action_from_snapshot,
    position_from_snapshot,
)
from .exits import ExitStateMachine
from .orderflow_config import OrderflowConfig
from .orderflow_capture import CaptureMode
from .orderflow_context import ContextHistory
from .orderflow_interaction import InteractionDetector
from .orderflow_location import LocationRegistry
from .orderflow_opportunity import SetupOpportunity
from .orderflow_position_manager import OrderflowPositionManager
from .policy_contract import ExecutionMode
from .snapshot_envelope import unwrap_snapshot

if TYPE_CHECKING:
    from .orderflow import OrderflowPolicy, OrderflowStrategy


def config_from_snapshot(snapshot: Mapping[str, object]) -> OrderflowConfig:
    raw_config = snapshot.get("config")
    if not isinstance(raw_config, Mapping):
        raise ValueError("orderflow snapshot config must be an object")
    values = dict(raw_config)
    allowed_config = set(OrderflowConfig.__dataclass_fields__)
    values.setdefault("timezone", "Asia/Kolkata")
    values.setdefault("market_session", ("09:15", "15:30"))
    values.setdefault("vehicle", "futures")
    values.setdefault("replay_mode", "causal")
    values.setdefault("capture_mode", CaptureMode.FULL)
    if set(values) != allowed_config:
        raise ValueError("orderflow snapshot config fields are invalid")
    values["execution_mode"] = ExecutionMode(values.get("execution_mode", "diagnostic"))
    values["capture_mode"] = CaptureMode(values.get("capture_mode", CaptureMode.FULL))
    try:
        return OrderflowConfig(**values)
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid orderflow snapshot config") from exc


def restore_policy_state(policy: OrderflowPolicy, snapshot: Mapping[str, object]) -> None:
    session_id = snapshot.get("session_id")
    if session_id is not None and not isinstance(session_id, str):
        raise ValueError("invalid orderflow snapshot session_id")
    policy._session_id = session_id

    raw_processed = snapshot.get("processed_observation_ids", ())
    if not isinstance(raw_processed, (list, tuple)) or any(not isinstance(item, str) for item in raw_processed):
        raise ValueError("invalid processed orderflow observation IDs")
    policy._processed_observation_ids = set(raw_processed)

    raw_location_processed = snapshot.get("processed_location_observation_ids")
    if not isinstance(raw_location_processed, (list, tuple)) or any(
        not isinstance(item, str) for item in raw_location_processed
    ):
        raise ValueError("invalid processed location observation IDs")
    policy._processed_location_observation_ids = set(raw_location_processed)

    raw_detectors = snapshot.get("location_detectors")
    if not isinstance(raw_detectors, Mapping):
        raise ValueError("invalid orderflow location detector snapshots")
    if set(raw_detectors) != {"1m", "5m"}:
        raise ValueError("orderflow location detector timeframes are invalid")
    if any(not isinstance(value, Mapping) for value in raw_detectors.values()):
        raise ValueError("invalid orderflow location detector snapshot")
    policy._location_detectors = {
        str(timeframe): InteractionDetector.from_snapshot(value)
        for timeframe, value in raw_detectors.items()
    }

    admission_suspended = snapshot.get("admission_suspended")
    gap_end = snapshot.get("gap_end")
    if "gap_end" not in snapshot or not isinstance(admission_suspended, bool):
        raise ValueError("invalid orderflow admission suspension state")
    if gap_end is not None and not isinstance(gap_end, str):
        raise ValueError("invalid orderflow gap end")
    policy._admission_suspended = admission_suspended
    policy._gap_end = datetime.fromisoformat(gap_end) if gap_end is not None else None

    last_gap_observation = snapshot.get("opening_gap_last_evaluated_id")
    if last_gap_observation is not None and not isinstance(last_gap_observation, str):
        raise ValueError("invalid opening gap evaluation observation ID")
    policy._opening_gap_last_evaluated_id = last_gap_observation
    raw_opening_seed = snapshot.get("opening_seed")
    if raw_opening_seed is not None and not isinstance(raw_opening_seed, Mapping):
        raise ValueError("invalid opening seed snapshot")
    policy._opening_seed = dict(raw_opening_seed) if isinstance(raw_opening_seed, Mapping) else None

    raw_context_version = snapshot.get("context_version", 0)
    if not isinstance(raw_context_version, int) or raw_context_version < 0:
        raise ValueError("invalid orderflow context version")
    policy._context_version = raw_context_version
    raw_session_high = snapshot.get("session_high")
    raw_session_low = snapshot.get("session_low")
    policy._session_high = float(raw_session_high) if isinstance(raw_session_high, (int, float)) else None
    policy._session_low = float(raw_session_low) if isinstance(raw_session_low, (int, float)) else None

    raw_context_history = snapshot.get("context_history")
    if isinstance(raw_context_history, Mapping):
        policy._context_history = ContextHistory.from_dict(raw_context_history)
    raw_location_registry = snapshot.get("location_registry")
    if raw_location_registry is not None:
        policy._location_registry = LocationRegistry.from_dict(raw_location_registry)
    raw_opportunities = snapshot.get("opportunities")
    if not isinstance(raw_opportunities, (list, tuple)):
        raise ValueError("invalid orderflow opportunities")
    for item in raw_opportunities:
        if not isinstance(item, Mapping):
            raise ValueError("invalid orderflow opportunity")
        opportunity = SetupOpportunity.from_snapshot(item)
        if opportunity.lifecycle_id in policy._opportunities:
            raise ValueError("duplicate orderflow opportunity ID")
        policy._opportunities[opportunity.lifecycle_id] = opportunity


def policy_from_snapshot(
    cls: type[OrderflowPolicy], snapshot: Mapping[str, object],
) -> OrderflowPolicy:
    schema_version = snapshot.get("schema_version")
    if schema_version not in {2, 3}:
        raise ValueError("unsupported orderflow snapshot schema")
    if schema_version == 2:
        legacy_detector = snapshot.get("location_detector")
        if not isinstance(legacy_detector, Mapping):
            raise ValueError("invalid legacy orderflow location detector snapshot")
        snapshot = {
            **snapshot,
            "schema_version": 3,
            "location_detectors": {
                "1m": InteractionDetector("1m").snapshot(),
                "5m": legacy_detector,
            },
        }
    policy = cls(config_from_snapshot(snapshot))
    restore_policy_state(policy, snapshot)
    return policy


def strategy_from_snapshot(
    cls: type[OrderflowStrategy], snapshot: Mapping[str, object],
) -> OrderflowStrategy:
    from .coordinator import SharedDecisionCoordinator

    _, payload = unwrap_snapshot(snapshot)
    strategy = cls.__new__(cls)
    from .orderflow import OrderflowPolicy

    strategy.policy = policy_from_snapshot(OrderflowPolicy, payload)
    strategy.coordinator = SharedDecisionCoordinator(strategy.policy)
    strategy._position_manager = OrderflowPositionManager(strategy.policy.config)

    raw_positions = payload.get("decision_positions", {})
    if not isinstance(raw_positions, Mapping):
        raise ValueError("invalid orderflow decision position snapshot")
    for order_id, raw in raw_positions.items():
        if not isinstance(order_id, str) or not isinstance(raw, Mapping):
            raise ValueError("invalid orderflow decision position")
        position_raw = raw.get("position")
        exit_raw = raw.get("exit_state")
        if not isinstance(position_raw, Mapping) or not isinstance(exit_raw, Mapping):
            raise ValueError("incomplete orderflow decision position")
        position = position_from_snapshot(position_raw)
        if position.entry_order_id != order_id:
            raise ValueError("orderflow position key does not match entry_order_id")
        strategy._position_manager.positions[order_id] = (
            position, ExitStateMachine.from_snapshot(exit_raw),
        )

    raw_pending = payload.get("pending_exits", {})
    if not isinstance(raw_pending, Mapping) or any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in raw_pending.items()
    ):
        raise ValueError("invalid orderflow pending exits")
    if any(value not in strategy._position_manager.positions for value in raw_pending.values()):
        raise ValueError("orderflow pending exit references unknown position")
    strategy._position_manager.pending_exits = dict(raw_pending)
    strategy.policy.set_active_position(bool(strategy._position_manager.positions))

    raw_actions = payload.get("pending_exit_actions", {})
    if not isinstance(raw_actions, Mapping):
        raise ValueError("invalid orderflow pending exit actions")
    for order_id, raw in raw_actions.items():
        if not isinstance(order_id, str) or not isinstance(raw, Mapping):
            raise ValueError("invalid orderflow pending exit action")
        action_raw = raw.get("action")
        discovered_at = raw.get("discovered_at")
        if not isinstance(action_raw, Mapping) or not isinstance(discovered_at, str):
            raise ValueError("incomplete orderflow pending exit action")
        if order_id not in strategy._position_manager.pending_exits:
            raise ValueError("pending exit action is not pending")
        strategy._position_manager.pending_exit_actions[order_id] = (
            exit_action_from_snapshot(action_raw), datetime.fromisoformat(discovered_at),
        )

    raw_bars = payload.get("completed_bars", ())
    if not isinstance(raw_bars, (list, tuple)):
        raise ValueError("invalid orderflow completed bars")
    strategy._position_manager.completed_bars = [
        bar_from_snapshot(item) for item in raw_bars if isinstance(item, Mapping)
    ]
    return strategy


__all__ = [
    "config_from_snapshot", "policy_from_snapshot", "restore_policy_state",
    "strategy_from_snapshot",
]
