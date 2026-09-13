from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Iterable
from typing import Literal
from ftx_paper.contracts import MarketBar, OrderIntent, OrderRole, OrderSide
from .engine import PaperEngine
from ftx_paper.config import NIFTY_LOT_SIZE


@dataclass(frozen=True, slots=True)
class ReplayTrade:
    """One deterministic replay trade, including its eventual settlement."""

    entry_order_id: str
    instrument: str
    side: OrderSide
    quantity: int
    entry_timestamp: str
    entry_price: float
    exit_order_id: str | None = None
    exit_timestamp: str | None = None
    exit_price: float | None = None
    exit_reason: str | None = None
    realized_pnl: float | None = None
    status: Literal["open", "closed"] = "open"
    vehicle: str = "futures"


@dataclass(frozen=True, slots=True)
class ReplayResult:
    orders: tuple[OrderIntent, ...]
    events: tuple[dict[str, object], ...]
    bars_seen: int
    trades: tuple[ReplayTrade, ...] = ()


def replay(engine: PaperEngine, bars: Iterable[MarketBar]) -> ReplayResult:
    """Run a completed-bar transcript through the full deterministic lifecycle.

    Replay fills are deliberately simple and reproducible: every order emitted
    while processing a bar is filled at that bar's close.  Exit evaluation is
    performed before the current bar's strategy evaluation, so a newly opened
    position cannot exit on its entry bar.  The ordering mirrors the live
    lifecycle while keeping broker timestamps and network behavior out of
    parity comparisons.
    """
    previous = None
    orders: list[OrderIntent] = []
    events: list[dict[str, object]] = []
    open_trades: list[ReplayTrade] = []
    trades: list[ReplayTrade] = []
    for bar in bars:
        if previous is not None and bar.timestamp <= previous:
            raise ValueError("replay bars must be strictly increasing")
        previous = bar.timestamp

        # Protective/signal exits for positions carried into this bar are
        # evaluated before accepting a new entry on the same completed bar.
        exit_actions = engine.on_closed_bar(bar)
        for action in exit_actions:
            orders.append(action.intent)
            events.append({
                "event_type": "EXITDECISION",
                "decision_id": action.intent.client_order_id,
                "vehicle": action.intent.vehicle,
                "timestamp": bar.timestamp.isoformat(),
                "reason": action.reason,
                "price": action.price,
            })
            events.append({
                "event_type": "FILL",
                "decision_id": action.intent.client_order_id,
                "vehicle": action.intent.vehicle,
                "timestamp": bar.timestamp.isoformat(),
                "symbol": action.intent.instrument.symbol,
                "side": action.intent.side.value,
                "quantity": action.intent.quantity,
                "price": action.price,
            })
            _settle_trade(
                open_trades, trades, action.intent, bar,
                exit_price=action.price, exit_reason=action.reason,
            )
            engine.settle_exit(action.intent.client_order_id, filled=True)

        result = engine.on_bar(bar)
        orders.extend(result.orders)
        events.extend(result.events)
        for order in result.orders:
            fill_price = bar.close
            if _is_exit(order):
                events.append({
                    "event_type": "EXITDECISION",
                    "decision_id": order.client_order_id,
                    "vehicle": order.vehicle,
                    "timestamp": bar.timestamp.isoformat(),
                    "reason": order.reason,
                    "price": fill_price,
                })
            events.append({
                "event_type": "FILL",
                "decision_id": order.client_order_id,
                "timestamp": bar.timestamp.isoformat(),
                "symbol": order.instrument.symbol,
                "side": order.side.value,
                "quantity": order.quantity,
                "price": fill_price,
            })
            if _is_exit(order):
                _settle_trade(
                    open_trades, trades, order, bar,
                    exit_price=fill_price, exit_reason=order.reason,
                )
                engine.settle_exit(order.client_order_id, filled=True)
            else:
                open_trades.append(ReplayTrade(
                    entry_order_id=order.client_order_id,
                    instrument=order.instrument.symbol,
                    side=order.side,
                    quantity=order.quantity,
                    entry_timestamp=bar.timestamp.isoformat(),
                    entry_price=fill_price,
                    vehicle=order.vehicle,
                ))
                engine.register_entry(order, fill_price=fill_price)

    return ReplayResult(tuple(orders), tuple(events), engine.bars_seen, tuple(trades + open_trades))


def _is_exit(order: OrderIntent) -> bool:
    return order.role == OrderRole.EXIT


def _settle_trade(
    open_trades: list[ReplayTrade],
    trades: list[ReplayTrade],
    order: OrderIntent,
    bar: MarketBar,
    *,
    exit_price: float,
    exit_reason: str,
) -> None:
    """Close the matching FIFO position and retain unmatched exits as events."""
    match_index = next(
        (index for index, trade in enumerate(open_trades)
         if trade.instrument == order.instrument.symbol
         and trade.vehicle == order.vehicle
         and trade.side != order.side),
        None,
    )
    if match_index is None:
        return
    trade = open_trades.pop(match_index)
    signed = 1.0 if trade.side is OrderSide.BUY else -1.0
    settled = ReplayTrade(
        entry_order_id=trade.entry_order_id,
        instrument=trade.instrument,
        side=trade.side,
        quantity=trade.quantity,
        entry_timestamp=trade.entry_timestamp,
        entry_price=trade.entry_price,
        exit_order_id=order.client_order_id,
        exit_timestamp=bar.timestamp.isoformat(),
        exit_price=exit_price,
        exit_reason=exit_reason,
        realized_pnl=(exit_price - trade.entry_price) * NIFTY_LOT_SIZE * trade.quantity * signed,
        status="closed",
    )
    trades.append(settled)
