from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from queue import Empty, Full, Queue
from threading import Event, Lock, Thread
from typing import Any
from uuid import uuid4

from ftx_paper.contracts import Instrument, MarketBar, MarketRole, OptionRole, OrderRole, Role, SyntheticFutureQuote, synthetic_future_quote
from ftx_paper.core import AggregatorConfig, Cell, CompletedBarAggregator, InstrumentKey, PaperEngine
from ftx_paper.runtime.store import RuntimeStore
from ftx_paper.strategy import ConfiguredLiveStrategy
from ftx_paper.strategy.config import CAPITAL
from ftx_paper.config import NIFTY_LOT_SIZE


@dataclass(frozen=True, slots=True)
class TradeSettlement:
    cell: Cell
    entry_bar: int
    exit_bar: int
    realized_pnl: float


class ReplayWorker:
    """Bounded in-process replay queue with a fresh engine per job."""

    def __init__(self, store: RuntimeStore, *, max_queue_size: int = 8) -> None:
        self.store = store
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
        dates = [session_date] if session_date else self._dates()
        all_events: list[dict[str, Any]] = []
        diagnostic_trades: list[dict[str, Any]] = []
        bars_seen = 0
        initial_capital = float(CAPITAL)
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
            strategy = ConfiguredLiveStrategy(capital=CAPITAL)
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
                raw_supporting_bars = (bundle.supporting_inputs or {}).get("bars", {})
                option_bars: dict[Role, MarketBar] = {
                    role: bar for role, bar in raw_supporting_bars.items()
                    if isinstance(role, (MarketRole, OptionRole))
                }
                for trade in open_trades:
                    quote = synthetic_future_quote(
                        futures_bar, option_bars,
                        symbols=(trade["ce_symbol"], trade["pe_symbol"]),
                    )
                    if quote is not None:
                        last_quotes[trade["entry_order_id"]] = quote
                exit_actions = engine.on_closed_bar(futures_bar)
                for action in exit_actions:
                    open_trade = next((trade for trade in open_trades
                                       if trade["instrument"] == action.intent.instrument.symbol), None)
                    exit_quote = synthetic_future_quote(
                        futures_bar, option_bars,
                        symbols=(open_trade["ce_symbol"], open_trade["pe_symbol"]),
                    ) if open_trade else None
                    if exit_quote is None and open_trade:
                        exit_quote = last_quotes.get(open_trade["entry_order_id"])
                    if exit_quote is None:
                        all_events.append({"event_type": "EXECUTION_ERROR", "decision_id": action.intent.client_order_id,
                                           "reason": "missing_synthetic_future_quote", "source": "replay", "session_date": date})
                        continue
                    all_events.append({"event_type": "EXITDECISION", "decision_id": action.intent.client_order_id,
                                       "timestamp": futures_bar.timestamp.isoformat(), "reason": action.reason,
                                       "price": exit_quote.price, "synthetic_future_price": exit_quote.price,
                                       "ce_strike": exit_quote.strike, "ce_exit_price": exit_quote.ce.close,
                                       "pe_exit_price": exit_quote.pe.close, "source": "replay", "session_date": date})
                    engine.settle_exit(action.intent.client_order_id, filled=True)
                    realized = self._settle_trade(open_trades, diagnostic_trades, action.intent, futures_bar,
                                                  exit_quote, action.reason)
                    if realized is not None:
                        engine.record_exit(cell=realized.cell.name, reason=action.reason,
                                           entry_bar=realized.entry_bar, exit_bar=engine.bars_seen,
                                           date=date)
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
                                       "timestamp": futures_bar.timestamp.isoformat(), "source": "replay",
                                       "session_date": date, "execution_allowed": False})
                    if order.role is OrderRole.ENTRY:
                        entry_quote = synthetic_future_quote(futures_bar, option_bars)
                        if entry_quote is None:
                            all_events.append({"event_type": "EXECUTION_ERROR", "decision_id": order.client_order_id,
                                               "reason": "missing_synthetic_future_quote", "source": "replay", "session_date": date})
                            continue
                        fill_price = entry_quote.price
                        for event in reversed(all_events):
                            if event.get("decision_id") == order.client_order_id:
                                event.update({"vehicle": "synthetic", "ce_symbol": entry_quote.ce.instrument.symbol,
                                              "pe_symbol": entry_quote.pe.instrument.symbol, "ce_strike": entry_quote.strike,
                                              "ce_entry_price": entry_quote.ce.close, "pe_entry_price": entry_quote.pe.close,
                                              "synthetic_entry_price": entry_quote.price, "entry_price": entry_quote.price})
                                break
                        engine.register_entry(order, fill_price=fill_price,
                                              entry_fill_time=futures_bar.timestamp)
                        open_trades.append({"entry_order_id": order.client_order_id,
                                            "instrument": order.instrument.symbol,
                                            "side": order.side.value, "quantity": order.quantity,
                                            "entry_timestamp": futures_bar.timestamp.isoformat(),
                                            "entry_price": fill_price, "vehicle": "synthetic",
                                            "ce_symbol": entry_quote.ce.instrument.symbol, "pe_symbol": entry_quote.pe.instrument.symbol,
                                            "ce_strike": entry_quote.strike, "ce_entry_price": entry_quote.ce.close,
                                            "pe_entry_price": entry_quote.pe.close, "cell": order.cell,
                                            "entry_bar": order.entry_bar or engine.bars_seen})
                        last_quotes[order.client_order_id] = entry_quote
                    elif order.role is OrderRole.EXIT:
                        open_trade = next((trade for trade in open_trades
                                           if trade["instrument"] == order.instrument.symbol), None)
                        exit_quote = synthetic_future_quote(
                            futures_bar, option_bars,
                            symbols=(open_trade["ce_symbol"], open_trade["pe_symbol"]),
                        ) if open_trade else None
                        if exit_quote is None and open_trade:
                            exit_quote = last_quotes.get(open_trade["entry_order_id"])
                        if exit_quote is None:
                            all_events.append({"event_type": "EXECUTION_ERROR", "decision_id": order.client_order_id,
                                               "reason": "missing_synthetic_future_quote", "source": "replay", "session_date": date})
                            continue
                        realized = self._settle_trade(open_trades, diagnostic_trades, order, futures_bar,
                                                      exit_quote, order.reason)
                        if realized is not None:
                            engine.record_exit(cell=realized.cell.name, reason=order.reason,
                                               entry_bar=realized.entry_bar, exit_bar=engine.bars_seen,
                                               date=date)
                            current_equity += realized.realized_pnl
                            peak_equity = max(peak_equity, current_equity)
                            max_drawdown = min(max_drawdown, current_equity - peak_equity)
                            strategy.update_portfolio_state(equity=current_equity, peak_equity=peak_equity)
            # Preserve open positions as mark-to-market/open replay results.
            diagnostic_trades.extend({**trade, "status": "open"} for trade in open_trades)
        scores = [int(event["score"]) for event in all_events
                  if isinstance(event.get("score"), (int, float))]
        return {
            "source": "replay",
            "bars_seen": bars_seen,
            "events": all_events,
            # Diagnostic-only trades live inside the replay run result. They
            # are never written to runtime_events, the broker ledger, or the
            # live trades/capital projections.
            "diagnostic_trades": diagnostic_trades,
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
    def _settle_trade(open_trades: list[dict[str, Any]], trades: list[dict[str, Any]],
                      order: Any, bar: MarketBar, quote: SyntheticFutureQuote, reason: str) -> TradeSettlement | None:
        opposite = "SELL" if order.side.value == "BUY" else "BUY"
        index = next((i for i, trade in enumerate(open_trades)
                      if trade["instrument"] == order.instrument.symbol and trade["side"] == opposite), None)
        if index is None:
            return None
        trade = open_trades.pop(index)
        signed = 1.0 if trade["side"] == "BUY" else -1.0
        exit_price = quote.price
        realized_pnl = (exit_price - trade["entry_price"]) * NIFTY_LOT_SIZE * trade["quantity"] * signed
        trades.append({**trade, "exit_order_id": order.client_order_id,
                       "exit_timestamp": bar.timestamp.isoformat(), "exit_price": exit_price,
                       "synthetic_exit_price": exit_price, "ce_exit_price": quote.ce.close,
                       "pe_exit_price": quote.pe.close,
                       "exit_reason": reason,
                       "realized_pnl": realized_pnl,
                       "status": "closed"})
        cell = trade.get("cell")
        if not isinstance(cell, str):
            raise TypeError("settled trade cell must be a canonical name")
        parsed_cell = Cell.parse(cell)
        entry_bar = trade.get("entry_bar", 0)
        if not isinstance(entry_bar, int) or isinstance(entry_bar, bool):
            raise TypeError("settled trade entry_bar must be an integer")
        return TradeSettlement(parsed_cell, entry_bar, len(trades), realized_pnl)

    def _dates(self) -> list[str]:
        return self.store.read_market_dates()

    def _bars(self, session_date: str) -> tuple[MarketBar, ...]:
        rows = self.store.read_market_bars(session_date)
        bars = []
        for row in rows:
            instrument_type = str(row.get("instrument_type", "")).upper()
            if instrument_type not in {"FUT", "FUTURES", "INDEX", "CE", "PE"}:
                continue
            bars.append(MarketBar(
                instrument=Instrument(symbol=str(row["symbol"]), exchange=str(row["exchange"]),
                                      instrument_type=instrument_type, expiry=row.get("expiry"),
                                      strike=row.get("strike")),
                timestamp=datetime.fromisoformat(str(row["minute"])),
                open=float(row["open"]), high=float(row["high"]), low=float(row["low"]), close=float(row["close"]),
                volume=row.get("volume"), open_interest=row.get("open_interest"),
            ))
        return tuple(sorted(bars, key=lambda bar: bar.timestamp))

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
