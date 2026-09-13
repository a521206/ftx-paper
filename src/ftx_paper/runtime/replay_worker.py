from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from queue import Empty, Full, Queue
from threading import Event, Lock, Thread
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from ftx_paper.contracts import Instrument, MarketBar, MarketRole, OptionRole, OrderRole, OrderSide, Role, SyntheticFutureQuote, synthetic_future_quote
from ftx_paper.capital_config import FtxCapitalConfig
from ftx_paper.core import AggregatorConfig, Cell, CompletedBarAggregator, InstrumentKey, PaperEngine
from ftx_paper.core.cost import futures_cost, synthetic_futures_cost
from ftx_paper.execution import PaperExecutionCoordinator
from ftx_paper.core.settlement import ExitValidationError, validate_exit_order
from ftx_paper.runtime.store import RuntimeStore
from ftx_paper.strategy import ConfiguredLiveStrategy
from ftx_paper.strategy.config import (
    AFTERNOON_ENTRY_MINUTES,
    MORNING_ENTRY_MINUTES,
)
from ftx_paper.config import NIFTY_LOT_SIZE


IST = ZoneInfo("Asia/Kolkata")
SESSION_OPEN_MINUTES = 9 * 60 + 15


@dataclass(frozen=True, slots=True)
class TradeSettlement:
    cell: Cell
    entry_bar: int
    exit_bar: int
    realized_pnl: float


def _execution_cost(trade: dict[str, Any]) -> float:
    """Return the isolated Paper equivalent of the canonical FTX cost model."""
    lots = float(trade["quantity"])
    if trade.get("vehicle") == "futures":
        return futures_cost(lots)
    ce = float(trade["ce_entry_price"])
    pe = float(trade["pe_entry_price"])
    return synthetic_futures_cost(
        ce, pe, lots, is_short=str(trade.get("side", "")).upper() == "SELL",
    )


class ReplayWorker:
    """Bounded in-process replay queue with a fresh engine per job."""

    def __init__(self, store: RuntimeStore, *, capital_config: FtxCapitalConfig, max_queue_size: int = 8) -> None:
        self.store = store
        self.capital_config = capital_config
        self._queue: Queue[tuple[str, dict[str, Any], Event]] = Queue(maxsize=max_queue_size)
        self._lock = Lock()
        self._cancel: dict[str, Event] = {}
        self._thread: Thread | None = None
        self._stopped = Event()

    def _ensure_started(self) -> None:
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                self._stopped.clear()
                self._thread = Thread(target=self._run, name="ftx-paper-replay", daemon=True)
                self._thread.start()

    def stop(self) -> None:
        """Stop the worker when the owning application is shutting down."""
        self._stopped.set()

    def submit(self, request: dict[str, Any]) -> str:
        run_id = uuid4().hex
        cancel = Event()
        session_date = str(request.get("session_date") or request.get("date") or "").strip()[:10]
        if session_date:
            # The replay page is a date-level diagnostic view.  Re-running a
            # date replaces its prior stored result so stale trades cannot
            # remain visible beside the fresh run.
            self.store.clear_replay_runs_for_date(session_date)
        with self._lock:
            self._cancel[run_id] = cancel
        self.store.create_replay_run(run_id, request)
        self._ensure_started()
        try:
            self._queue.put_nowait((run_id, request, cancel))
        except Full:
            self.store.update_replay_run(run_id, status="rejected", error="replay queue is full")
            raise RuntimeError("replay queue is full") from None
        return run_id

    def cancel(self, run_id: str) -> bool:
        run = self.store.read_replay_run(run_id)
        if run is None or run["status"] in {"completed", "failed", "cancelled", "rejected"}:
            return False
        self.store.update_replay_run(run_id, status="cancelled", error="cancelled by user")
        with self._lock:
            event = self._cancel.get(run_id)
            if event:
                event.set()
        return True

    def _run(self) -> None:
        while not self._stopped.is_set():
            try:
                run_id, request, cancel = self._queue.get(timeout=0.2)
            except Empty:
                continue
            try:
                run = self.store.read_replay_run(run_id)
                if run is None or run.get("status") == "cancelled":
                    continue
                self.store.update_replay_run(run_id, status="running")
                result = self._execute(request, cancel)
                run = self.store.read_replay_run(run_id)
                if run is not None and run.get("status") != "cancelled":
                    self.store.update_replay_run(run_id, status="completed", result=result)
            except Exception as exc:
                run = self.store.read_replay_run(run_id)
                if run is not None and run.get("status") != "cancelled":
                    self.store.update_replay_run(run_id, status="failed", error=str(exc))
            finally:
                with self._lock:
                    self._cancel.pop(run_id, None)
                self._queue.task_done()

    def _execute(self, request: dict[str, Any], cancel: Event) -> dict[str, Any]:
        session_date = str(request.get("session_date") or request.get("date") or "")[:10]
        raw_vehicles = request.get("vehicles")
        if raw_vehicles is None:
            vehicles = ("futures", "synthetic")
        elif isinstance(raw_vehicles, (list, tuple)):
            vehicles = tuple(dict.fromkeys(str(item).lower() for item in raw_vehicles))
        else:
            raise ValueError("replay vehicles must be an array containing 'futures' and/or 'synthetic'")
        if not vehicles or any(item not in {"futures", "synthetic"} for item in vehicles):
            raise ValueError(f"unsupported replay vehicles: {vehicles}")
        dates = [session_date] if session_date else self._dates()
        all_events: list[dict[str, Any]] = []
        diagnostic_trades: list[dict[str, Any]] = []
        bars_seen = 0
        initial_capital = self.capital_config.initial_capital
        current_equity = initial_capital
        peak_equity = initial_capital
        max_drawdown = 0.0
        # This engine/strategy is private to this replay run and never shared
        # with RuntimeSession, its broker, ledger, or risk state.
        for date in dates:
            if cancel.is_set():
                break
            # A date is an independent simulation session. No positions,
            # cooldowns, or risk state may leak into the next date.
            strategy = ConfiguredLiveStrategy(capital_config=self.capital_config, enabled_vehicles=vehicles)
            coordinator = PaperExecutionCoordinator(strategy.portfolio)
            engine = PaperEngine(strategy)
            strategy.update_portfolio_state(equity=current_equity, peak_equity=peak_equity)
            bars = self._bars(date)
            aggregator = self._aggregator(bars)
            open_trades: list[dict[str, Any]] = []
            last_quotes: dict[str, SyntheticFutureQuote] = {}
            for bar in bars:
                if cancel.is_set():
                    break
                bundle = aggregator.ingest(bar)
                if bundle is None:
                    continue
                futures_bar = bundle.bars[MarketRole.FUTURES]
                # Synthetic contract selection is anchored to the spot/index
                # level, matching the Paper entry contract rule. Futures is
                # still the decision and exit-state instrument.
                raw_supporting_bars = (bundle.supporting_inputs or {}).get("bars", {})
                quote_selection_bar = raw_supporting_bars.get(MarketRole.SPOT, futures_bar)
                option_bars: dict[Role, MarketBar] = {
                    role: bar for role, bar in raw_supporting_bars.items()
                    if isinstance(role, (MarketRole, OptionRole))
                }
                for trade in open_trades:
                    if trade.get("vehicle") == "futures":
                        continue
                    quote = synthetic_future_quote(
                        quote_selection_bar, option_bars,
                        symbols=(trade["ce_symbol"], trade["pe_symbol"]),
                    )
                    if quote is not None:
                        last_quotes[trade["entry_order_id"]] = quote
                exit_actions = engine.on_closed_bar(futures_bar)
                for action in exit_actions:
                    try:
                        open_trade = self._matching_trade(open_trades, action.intent)
                    except ExitValidationError as exc:
                        all_events.append({"event_type": "EXECUTION_ERROR", "decision_id": action.intent.client_order_id,
                                           "reason": exc.reason, "source": "replay", "session_date": date})
                        engine.settle_exit(action.intent.client_order_id, filled=False)
                        continue
                    exit_quote = synthetic_future_quote(
                        quote_selection_bar, option_bars,
                        symbols=(open_trade["ce_symbol"], open_trade["pe_symbol"]),
                    ) if open_trade and action.intent.vehicle == "synthetic" else None
                    if exit_quote is None and action.intent.vehicle == "synthetic":
                        all_events.append({"event_type": "EXECUTION_ERROR", "decision_id": action.intent.client_order_id,
                                           "reason": "missing_synthetic_future_quote", "source": "replay", "session_date": date})
                        engine.settle_exit(action.intent.client_order_id, filled=False)
                        continue
                    # Futures exits fill at the canonical protective trigger
                    # (including gap handling), not at the bar close.  A
                    # synthetic position uses the same futures trigger time,
                    # then settles at its same-minute synthetic quote.
                    exit_price = exit_quote.price if exit_quote is not None else action.price
                    exit_event = {"event_type": "EXITDECISION", "decision_id": action.intent.client_order_id,
                                  "vehicle": action.intent.vehicle,
                                  "entry_decision_id": open_trade["entry_order_id"] if open_trade else None,
                                  "timestamp": futures_bar.timestamp.isoformat(), "reason": action.reason,
                                  "price": exit_price, "source": "replay", "session_date": date}
                    if exit_quote is not None:
                        exit_event.update({"synthetic_future_price": exit_quote.price, "ce_strike": exit_quote.strike,
                                           "ce_exit_price": exit_quote.ce.close, "pe_exit_price": exit_quote.pe.close})
                    all_events.append(exit_event)
                    realized = self._settle_trade(open_trades, diagnostic_trades, action.intent, futures_bar,
                                                  exit_quote, action.reason, exit_price=exit_price,
                                                  trade=open_trade)
                    coordinator.fill(action.intent, price=exit_price, timestamp=futures_bar.timestamp.isoformat())
                    engine.settle_exit(action.intent.client_order_id, filled=True)
                    if realized is not None:
                        engine.record_exit(cell=realized.cell.name, reason=action.reason,
                                           entry_bar=realized.entry_bar, exit_bar=engine.bars_seen,
                                           date=date, vehicle=action.intent.vehicle,
                                           direction="long" if action.intent.side.value == "SELL" else "short",
                                           quantity=action.intent.quantity)
                        current_equity += realized.realized_pnl
                        peak_equity = max(peak_equity, current_equity)
                        max_drawdown = min(max_drawdown, current_equity - peak_equity)
                        strategy.update_portfolio_state(equity=current_equity, peak_equity=peak_equity)
                result = engine.on_bundle(bundle)
                bars_seen += 1
                result_events = [{**event, "source": "replay", "session_date": date} for event in result.events]
                all_events.extend(result_events)
                for order in result.orders:
                    all_events.append({"event_type": "ORDER_SUPPRESSED", "decision_id": order.client_order_id,
                                       "vehicle": order.vehicle,
                                       "timestamp": futures_bar.timestamp.isoformat(), "source": "replay",
                                       "session_date": date, "execution_allowed": False})
                    if order.role is OrderRole.ENTRY:
                        entry_quote = synthetic_future_quote(
                            quote_selection_bar, option_bars,
                            symbols=(order.synthetic_legs[0].symbol, order.synthetic_legs[1].symbol)
                            if order.synthetic_legs is not None else None,
                        ) if order.vehicle == "synthetic" else None
                        if entry_quote is None and order.vehicle == "synthetic":
                            all_events.append({"event_type": "EXECUTION_ERROR", "decision_id": order.client_order_id,
                                               "vehicle": order.vehicle,
                                               "reason": "missing_synthetic_future_quote", "source": "replay", "session_date": date})
                            continue
                        fill_price = entry_quote.price if entry_quote is not None else futures_bar.close
                        for event in reversed(all_events):
                            if event.get("decision_id") == order.client_order_id:
                                event.update({"vehicle": order.vehicle, "entry_price": fill_price})
                                if entry_quote is not None:
                                    event.update({"ce_symbol": entry_quote.ce.instrument.symbol,
                                                  "pe_symbol": entry_quote.pe.instrument.symbol, "ce_strike": entry_quote.strike,
                                                  "ce_entry_price": entry_quote.ce.close, "pe_entry_price": entry_quote.pe.close,
                                                  "synthetic_entry_price": entry_quote.price})
                                break
                        all_events.append({"event_type": "FILL", "decision_id": order.client_order_id,
                                           "fill_timestamp": futures_bar.timestamp.isoformat(),
                                           "price": fill_price, "quantity": order.quantity,
                                           "vehicle": order.vehicle, "source": "replay",
                                           "session_date": date})
                        engine.register_entry(order, fill_price=fill_price,
                                              entry_fill_time=futures_bar.timestamp,
                                              reference_price=futures_bar.close)
                        coordinator.fill(order, price=fill_price, timestamp=futures_bar.timestamp.isoformat(),
                                         synthetic_entry_prices=(entry_quote.ce.close, entry_quote.pe.close)
                                         if entry_quote is not None else None)
                        open_trades.append({"entry_order_id": order.client_order_id,
                                            "instrument": order.instrument.symbol,
                                            "side": order.side.value, "quantity": order.quantity,
                                            "entry_timestamp": futures_bar.timestamp.isoformat(),
                                            "entry_price": fill_price, "vehicle": order.vehicle,
                                            "cell": order.cell,
                                            "entry_bar": order.entry_bar or engine.bars_seen})
                        if entry_quote is not None:
                            open_trades[-1].update({"ce_symbol": entry_quote.ce.instrument.symbol, "pe_symbol": entry_quote.pe.instrument.symbol,
                                                     "ce_expiry": entry_quote.ce.instrument.expiry,
                                                     "pe_expiry": entry_quote.pe.instrument.expiry,
                                                     "ce_strike": entry_quote.strike, "ce_entry_price": entry_quote.ce.close,
                                                     "pe_entry_price": entry_quote.pe.close,
                                                     "synthetic_entry_price": entry_quote.price,
                                                     "quote_timestamp": entry_quote.ce.timestamp.isoformat(),
                                                     "quote_source": "same_minute_bundle"})
                            last_quotes[order.client_order_id] = entry_quote
                    elif order.role is OrderRole.EXIT:
                        try:
                            open_trade = self._matching_trade(open_trades, order)
                        except ExitValidationError as exc:
                            all_events.append({"event_type": "EXECUTION_ERROR", "decision_id": order.client_order_id,
                                               "reason": exc.reason, "source": "replay", "session_date": date})
                            engine.settle_exit(order.client_order_id, filled=False)
                            continue
                        exit_quote = synthetic_future_quote(
                            quote_selection_bar, option_bars,
                            symbols=(open_trade["ce_symbol"], open_trade["pe_symbol"]),
                        ) if open_trade and order.vehicle == "synthetic" else None
                        if exit_quote is None and order.vehicle == "synthetic":
                            all_events.append({"event_type": "EXECUTION_ERROR", "decision_id": order.client_order_id,
                                               "reason": "missing_synthetic_future_quote", "source": "replay", "session_date": date})
                            engine.settle_exit(order.client_order_id, filled=False)
                            continue
                        realized = self._settle_trade(open_trades, diagnostic_trades, order, futures_bar,
                                                      exit_quote, order.reason,
                                                      exit_price=exit_quote.price if exit_quote is not None else futures_bar.close,
                                                      trade=open_trade)
                        coordinator.fill(order, price=exit_quote.price if exit_quote is not None else futures_bar.close,
                                         timestamp=futures_bar.timestamp.isoformat())
                        engine.record_exit(cell=realized.cell.name, reason=order.reason,
                                           entry_bar=realized.entry_bar, exit_bar=engine.bars_seen,
                                           date=date, vehicle=order.vehicle,
                                           direction="long" if order.side.value == "SELL" else "short",
                                           quantity=order.quantity)
                        current_equity += realized.realized_pnl
                        peak_equity = max(peak_equity, current_equity)
                        max_drawdown = min(max_drawdown, current_equity - peak_equity)
                        strategy.update_portfolio_state(equity=current_equity, peak_equity=peak_equity)
            # Preserve open positions as mark-to-market/open replay results.
            diagnostic_trades.extend({**trade, "status": "open"} for trade in open_trades)
        scores = [int(event["score"]) for event in all_events
                  if isinstance(event.get("score"), (int, float))]
        normalized_trades = [self._normalize_trade(trade) for trade in diagnostic_trades]
        return {
            "result_schema_version": 2,
            "source": "replay",
            "vehicles": list(vehicles),
            "vehicle_semantics": "shared_portfolio_directional_and_margin",
            "strategy": {"name": ConfiguredLiveStrategy.name, "version": ConfiguredLiveStrategy.version},
            "configuration": {"initial_capital": initial_capital,
                               "max_daily_loss": self.capital_config.max_daily_loss,
                               "max_net_directional_lots": self.capital_config.max_net_directional_lots,
                               "lot_size": NIFTY_LOT_SIZE},
            "bars_seen": bars_seen,
            "events": all_events,
            "decisions": [event for event in all_events if str(event.get("event_type", "")).endswith("DECISION")],
            "first_divergent_stage": None,
            # Diagnostic-only trades live inside the replay run result. They
            # are never written to runtime_events, the broker ledger, or the
            # live trades/capital projections.
            "trades": normalized_trades,
            "capital": {
                "initial_capital": initial_capital,
                "current_equity": current_equity,
                "realized_pnl": current_equity - initial_capital,
                "peak_equity": peak_equity,
                "max_drawdown": max_drawdown,
                "lot_size": NIFTY_LOT_SIZE,
            },
            "scoring": {
                "decisions_scored": len(scores),
                "score_min": min(scores) if scores else None,
                "score_max": max(scores) if scores else None,
                "score_average": sum(scores) / len(scores) if scores else None,
            },
        }

    @staticmethod
    def _normalize_trade(trade: dict[str, Any]) -> dict[str, Any]:
        """Expose the replay engine's authoritative entry-to-exit record."""
        entry_at = datetime.fromisoformat(str(trade["entry_timestamp"]))
        exit_at = (
            datetime.fromisoformat(str(trade["exit_timestamp"]))
            if trade.get("exit_timestamp") else None
        )
        entry_ist = entry_at.astimezone(IST)
        exit_ist = exit_at.astimezone(IST) if exit_at else None
        entry_minutes = entry_ist.hour * 60 + entry_ist.minute - SESSION_OPEN_MINUTES
        return {
            "status": str(trade.get("status", "open")),
            "date": entry_ist.date().isoformat(),
            "session": ReplayWorker._session_for_minutes(entry_minutes),
            "vehicle": str(trade.get("vehicle", "synthetic")),
            "cell": str(trade.get("cell", "")),
            "direction": "long" if str(trade.get("side", "")).upper() == "BUY" else "short",
            "entry_time": entry_ist.strftime("%H:%M"),
            "exit_time": exit_ist.strftime("%H:%M") if exit_ist else "",
            "quantity": float(trade.get("quantity", 0.0)),
            "entry_price": float(trade["entry_price"]),
            "exit_price": float(trade["exit_price"]) if trade.get("exit_price") is not None else None,
            "gross_pnl_rs": float(trade["gross_pnl_rs"]) if trade.get("gross_pnl_rs") is not None else None,
            "cost_rs": float(trade["cost_rs"]) if trade.get("cost_rs") is not None else None,
            "net_pnl_rs": float(trade["net_pnl_rs"]) if trade.get("net_pnl_rs") is not None else None,
            # The normalized replay API uses the canonical audit vocabulary;
            # Paper’s internal state machine keeps its more descriptive name.
            "exit_reason": {
                "trailing_stop": "trail_stop",
            }.get(str(trade.get("exit_reason")), trade.get("exit_reason")),
            # Keep the normalized replay record self-contained for API-only
            # comparators investigating synthetic quote selection.
            "ce_strike": trade.get("ce_strike"),
            "ce_entry_price": trade.get("ce_entry_price"),
            "pe_entry_price": trade.get("pe_entry_price"),
            "ce_exit_price": trade.get("ce_exit_price"),
            "pe_exit_price": trade.get("pe_exit_price"),
            "synthetic_entry_price": trade.get("synthetic_entry_price"),
            "synthetic_exit_price": trade.get("synthetic_exit_price"),
            "ce_symbol": trade.get("ce_symbol"),
            "pe_symbol": trade.get("pe_symbol"),
            "expiry": trade.get("ce_expiry") or trade.get("pe_expiry"),
            "quote_timestamp": trade.get("quote_timestamp"),
            "quote_source": trade.get("quote_source"),
            "ce_exit_symbol": trade.get("ce_exit_symbol"),
            "pe_exit_symbol": trade.get("pe_exit_symbol"),
            "exit_quote_timestamp": trade.get("exit_quote_timestamp"),
            "exit_quote_source": trade.get("exit_quote_source"),
        }

    @staticmethod
    def _session_for_minutes(minutes_from_open: int) -> str:
        """Classify an entry with the canonical half-open session windows."""
        if MORNING_ENTRY_MINUTES[0] <= minutes_from_open < MORNING_ENTRY_MINUTES[1]:
            return "morning"
        if AFTERNOON_ENTRY_MINUTES[0] <= minutes_from_open < AFTERNOON_ENTRY_MINUTES[1]:
            return "afternoon"
        return "unknown"

    @staticmethod
    def _matching_trade(open_trades: list[dict[str, Any]], order: Any) -> dict[str, Any]:
        entry_order_id = order.entry_order_id
        if entry_order_id is None:
            raise ExitValidationError("exit_without_entry_order_id")
        trade = next((item for item in open_trades
                      if item["entry_order_id"] == entry_order_id), None)
        if trade is None:
            raise ExitValidationError("exit_without_matching_entry")
        validate_exit_order(
            order,
            entry_instrument=trade["instrument"],
            entry_vehicle=trade["vehicle"],
            entry_side=OrderSide(trade["side"]),
            entry_quantity=trade["quantity"],
            entry_leg_symbols=(trade["ce_symbol"], trade["pe_symbol"])
            if trade["vehicle"] == "synthetic" else None,
        )
        return trade

    @staticmethod
    def _settle_trade(open_trades: list[dict[str, Any]], trades: list[dict[str, Any]],
                      order: Any, bar: MarketBar, quote: SyntheticFutureQuote | None,
                      reason: str, *, exit_price: float, trade: dict[str, Any]) -> TradeSettlement:
        open_trades.remove(trade)
        signed = 1.0 if trade["side"] == "BUY" else -1.0
        gross_pnl = (exit_price - trade["entry_price"]) * NIFTY_LOT_SIZE * trade["quantity"] * signed
        cost_rs = _execution_cost(trade)
        net_pnl = gross_pnl - cost_rs
        settled = {**trade, "exit_order_id": order.client_order_id,
                       "exit_timestamp": bar.timestamp.isoformat(), "exit_price": exit_price,
                       "exit_reason": reason,
                       "realized_pnl": net_pnl,
                       "gross_pnl_rs": gross_pnl,
                       "cost_rs": cost_rs,
                       "net_pnl_rs": net_pnl,
                       "status": "closed"}
        if quote is not None:
            settled.update({"synthetic_exit_price": exit_price, "ce_exit_price": quote.ce.close,
                            "pe_exit_price": quote.pe.close,
                            "ce_exit_symbol": quote.ce.instrument.symbol,
                            "pe_exit_symbol": quote.pe.instrument.symbol,
                            "exit_quote_timestamp": quote.ce.timestamp.isoformat(),
                            "exit_quote_source": "same_minute_bundle"})
        trades.append(settled)
        cell = trade.get("cell")
        if not isinstance(cell, str):
            raise TypeError("settled trade cell must be a canonical name")
        parsed_cell = Cell.parse(cell)
        entry_bar = trade.get("entry_bar", 0)
        if not isinstance(entry_bar, int) or isinstance(entry_bar, bool):
            raise TypeError("settled trade entry_bar must be an integer")
        return TradeSettlement(parsed_cell, entry_bar, len(trades), net_pnl)

    def _dates(self) -> list[str]:
        return self.store.read_market_dates()

    def _bars(self, session_date: str) -> tuple[MarketBar, ...]:
        rows = self.store.read_market_bars(session_date)
        bars = []
        for row in rows:
            instrument_type = str(row.get("instrument_type", "")).upper()
            if instrument_type not in {"FUT", "FUTURES", "INDEX", "EQ", "CE", "PE"}:
                continue
            bars.append(MarketBar(
                instrument=Instrument(symbol=str(row["symbol"]), exchange=str(row["exchange"]),
                                      instrument_type=instrument_type, expiry=row.get("expiry"),
                                      strike=row.get("strike")),
                timestamp=datetime.fromisoformat(str(row["minute"])),
                open=float(row["open"]), high=float(row["high"]), low=float(row["low"]), close=float(row["close"]),
                volume=row.get("volume"), open_interest=row.get("open_interest"),
            ))
        # The futures bar is the decision clock and is emitted immediately by
        # the aggregator. Ingest same-minute supporting bars first so spot,
        # VIX, and option inputs are available in that completed bundle.
        return tuple(sorted(
            bars,
            key=lambda bar: (
                bar.timestamp,
                str(bar.instrument.instrument_type).upper() in {"FUT", "FUTURES"},
            ),
        ))

    @staticmethod
    def _aggregator(bars: tuple[MarketBar, ...]) -> CompletedBarAggregator:
        roles = {}
        for bar in bars:
            kind = str(bar.instrument.instrument_type).upper()
            role = (MarketRole.FUTURES if kind in {"FUT", "FUTURES"}
                    else MarketRole.VIX if bar.instrument.symbol.upper() in {"INDIA VIX", "INDIAVIX"}
                    else OptionRole(bar.instrument.symbol) if kind in {"CE", "PE"}
                    else MarketRole.SPOT)
            roles[InstrumentKey(bar.instrument.exchange, bar.instrument.symbol)] = role
        return CompletedBarAggregator(roles, AggregatorConfig(required_roles=(MarketRole.FUTURES,), deadline_seconds=0))

__all__ = ["ReplayWorker"]
