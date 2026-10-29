"""Strategy-local position and protective-exit state for orderflow."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from typing import TYPE_CHECKING, cast
from zoneinfo import ZoneInfo

from ftx_paper.config import SESSION_EXIT_DEADLINE_TIME
from ftx_paper.contracts import MarketBar, OrderIntent, OrderSide

from .exits import ExitAction, ExitStateMachine, PositionState
from .orderflow_vwap import MarketAggregate, calculate_session_vwap, calculate_vwap_stop

if TYPE_CHECKING:
    from .orderflow_config import OrderflowConfig


class OrderflowPositionManager:
    """Maintain strategy-local position management state.

    This object calculates exit actions from completed bars. Portfolio mutation,
    authorization, fills, and settlement remain owned by the runtime execution
    boundary.
    """

    def __init__(self, config: OrderflowConfig) -> None:
        self.config = config
        self.positions: dict[str, tuple[PositionState, ExitStateMachine]] = {}
        self.pending_exits: dict[str, str] = {}
        self.pending_exit_actions: dict[str, tuple[ExitAction, datetime]] = {}
        self.completed_bars: list[MarketBar] = []
        self.stop_update_evidence: list[dict[str, object]] = []

    def register_entry(
        self,
        order: OrderIntent,
        *,
        fill_price: float | None = None,
        entry_fill_time: datetime | str | None = None,
        reference_price: float | None = None,
    ) -> None:
        if order.stop_price is None or order.client_order_id in self.positions:
            return
        if isinstance(entry_fill_time, str):
            entry_fill_time = datetime.fromisoformat(entry_fill_time)
        entry = fill_price if fill_price is not None else 0.0
        position = PositionState(
            order.instrument, entry, order.stop_price, order.quantity, order.side,
            stop_policy=order.stop_policy, cell=order.cell,
            exit_mode=order.exit_mode or "signal", entry_fill_time=entry_fill_time,
            exit_reference_price=reference_price, vehicle=order.vehicle,
            synthetic_legs=order.synthetic_legs, entry_bar=order.entry_bar,
            entry_order_id=order.client_order_id,
            initial_risk_points=abs(entry - order.stop_price),
        )
        self.positions[order.client_order_id] = (
            position, ExitStateMachine(close_time=SESSION_EXIT_DEADLINE_TIME),
        )

    def _release_delayed_actions(self, bar: MarketBar) -> list[ExitAction]:
        actions: list[ExitAction] = []
        for exit_order_id, (action, discovered_at) in tuple(self.pending_exit_actions.items()):
            if bar.instrument == action.intent.instrument and bar.timestamp > discovered_at:
                actions.append(action)
                self.pending_exit_actions.pop(exit_order_id, None)
        return actions

    def _update_vwap_stop(
        self,
        order_id: str,
        position: PositionState,
        exits: ExitStateMachine,
        bar: MarketBar,
    ) -> PositionState:
        vwap_bars = tuple(item for item in self.completed_bars if item.instrument == bar.instrument)
        vwap, source_bar = calculate_session_vwap(
            cast(tuple[MarketAggregate, ...], vwap_bars), cutoff=bar.timestamp,
            timezone=self.config.timezone, session_start=self.config.market_session[0],
        )
        timezone = ZoneInfo(self.config.timezone)
        local_date = bar.timestamp.astimezone(timezone).date()
        session_bars = tuple(
            item for item in vwap_bars
            if item.timestamp.astimezone(timezone).date() == local_date
        )[-20:]
        ranges = [item.high - item.low for item in session_bars if item.high >= item.low]
        atr = sum(ranges) / len(ranges) if ranges else None
        result = calculate_vwap_stop(
            direction="long" if position.side is OrderSide.BUY else "short",
            entry_price=position.entry_price, vwap=vwap, atr=atr,
            source_aggregate_id=getattr(source_bar, "aggregate_id", None),
            source_observation_id=getattr(source_bar, "observation_id", None),
        )
        initial_risk = position.initial_risk_points or 0.0
        favorable = (
            bar.high - position.entry_price if position.side is OrderSide.BUY
            else position.entry_price - bar.low
        )
        post_3r = position.post_3r_activated or (initial_risk > 0 and favorable >= 3.0 * initial_risk)
        if post_3r and result.candidate is not None and initial_risk > 0:
            one_r = (
                position.entry_price + initial_risk
                if position.side is OrderSide.BUY else position.entry_price - initial_risk
            )
            result = replace(
                result,
                candidate=(max(one_r, result.candidate) if position.side is OrderSide.BUY
                           else min(one_r, result.candidate)),
            )
        tightens = result.candidate is not None and (
            result.candidate > position.stop_price if position.side is OrderSide.BUY
            else result.candidate < position.stop_price
        )
        if tightens:
            position = replace(
                position, stop_price=result.candidate, stop_source="vwap",
                post_3r_activated=post_3r,
            )
            self.positions[order_id] = (position, exits)
        elif post_3r and not position.post_3r_activated:
            position = replace(position, post_3r_activated=True)
            self.positions[order_id] = (position, exits)
        self.stop_update_evidence.append({
            "event": "stop_updated", "vwap_stop_reference": result.vwap,
            "active_stop": position.stop_price, "stop_source": position.stop_source,
            "atr": result.atr, "source_aggregate_id": result.source_aggregate_id,
            "source_observation_id": result.source_observation_id,
            "reason": result.reason, "changed": tightens,
            "observed_at": bar.timestamp.isoformat(),
        })
        return position

    def _evaluate_position_bar(
        self, order_id: str, position: PositionState, exits: ExitStateMachine, bar: MarketBar,
    ) -> None:
        if position.stop_policy == "or_vwap":
            position = self._update_vwap_stop(order_id, position, exits, bar)
        action = exits.evaluate(
            position, timestamp=bar.timestamp, high=bar.high, low=bar.low,
            close=bar.close, open=bar.open, volume=bar.volume,
            client_order_id=f"exit-{order_id}-{bar.timestamp.isoformat()}",
        )
        if action is not None:
            self.pending_exits[action.intent.client_order_id] = order_id
            self.pending_exit_actions[action.intent.client_order_id] = action, bar.timestamp

    def on_closed_bar(self, bar: MarketBar) -> tuple[ExitAction, ...]:
        self.completed_bars.append(bar)
        actions = self._release_delayed_actions(bar)
        pending_entries = set(self.pending_exits.values())
        for order_id, (position, exits) in tuple(self.positions.items()):
            if bar.instrument != position.instrument or order_id in pending_entries:
                continue
            if position.entry_fill_time is not None and bar.timestamp < position.entry_fill_time:
                continue
            self._evaluate_position_bar(order_id, position, exits, bar)
        return tuple(actions)

    def settle_exit(self, exit_order_id: str, *, filled: bool) -> None:
        entry_order_id = self.pending_exits.pop(exit_order_id, None)
        self.pending_exit_actions.pop(exit_order_id, None)
        if filled and entry_order_id is not None:
            self.positions.pop(entry_order_id, None)


__all__ = ["OrderflowPositionManager"]
