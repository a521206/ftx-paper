from __future__ import annotations

from datetime import datetime
from queue import Empty, Full, Queue
from threading import Event, Lock, Thread
from typing import Any
from uuid import uuid4

from ftx_paper.contracts import Instrument, MarketBar, MarketRole, OptionRole, OrderRole
from ftx_paper.core import AggregatorConfig, CompletedBarAggregator, InstrumentKey, PaperEngine
from ftx_paper.runtime.store import RuntimeStore
from ftx_paper.strategy import ConfiguredLiveStrategy
from ftx_paper.strategy.config import CAPITAL


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
                if self.store.read_replay_run(run_id).get("status") == "cancelled":
                    continue
                self.store.update_replay_run(run_id, status="running")
                result = self._execute(request, cancel)
                if self.store.read_replay_run(run_id).get("status") != "cancelled":
                    self.store.update_replay_run(run_id, status="completed", result=result)
            except Exception as exc:
                if self.store.read_replay_run(run_id).get("status") != "cancelled":
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
        # This engine/strategy is private to this replay run and never shared
        # with RuntimeSession, its broker, ledger, or risk state.
        for date in dates:
            if cancel.is_set():
                break
            # A date is an independent simulation session. No positions,
            # cooldowns, or risk state may leak into the next date.
            engine = PaperEngine(ConfiguredLiveStrategy(capital=CAPITAL))
            bars = self._bars(date)
            aggregator = self._aggregator(bars)
            open_trades: list[dict[str, Any]] = []
            for bar in bars:
                if cancel.is_set():
                    break
                bundle = aggregator.ingest(bar)
                if bundle is None:
                    continue
                futures_bar = bundle.bars[MarketRole.FUTURES]
                exit_actions = engine.on_closed_bar(futures_bar)
                for action in exit_actions:
                    all_events.append({"event_type": "EXITDECISION", "decision_id": action.intent.client_order_id,
                                       "timestamp": futures_bar.timestamp.isoformat(), "reason": action.reason,
                                       "price": action.price, "source": "replay", "session_date": date})
                    engine.settle_exit(action.intent.client_order_id, filled=True)
                    self._settle_trade(open_trades, diagnostic_trades, action.intent, futures_bar,
                                       action.price, action.reason)
                result = engine.on_bundle(bundle)
                bars_seen += 1
                all_events.extend({**event, "source": "replay", "session_date": date} for event in result.events)
                for order in result.orders:
                    all_events.append({"event_type": "ORDER_SUPPRESSED", "decision_id": order.client_order_id,
                                       "timestamp": futures_bar.timestamp.isoformat(), "source": "replay",
                                       "session_date": date, "execution_allowed": False})
                    if order.role is OrderRole.ENTRY:
                        fill_price = futures_bar.close
                        engine.register_entry(order, fill_price=fill_price,
                                              entry_fill_time=futures_bar.timestamp)
                        open_trades.append({"entry_order_id": order.client_order_id,
                                            "instrument": order.instrument.symbol,
                                            "side": order.side.value, "quantity": order.quantity,
                                            "entry_timestamp": futures_bar.timestamp.isoformat(),
                                            "entry_price": fill_price})
                    elif order.role is OrderRole.EXIT:
                        self._settle_trade(open_trades, diagnostic_trades, order, futures_bar,
                                           futures_bar.close, order.reason)
            # Preserve open positions as mark-to-market/open replay results.
            diagnostic_trades.extend({**trade, "status": "open"} for trade in open_trades)
        return {
            "source": "replay",
            "bars_seen": bars_seen,
            "events": all_events,
            # Diagnostic-only trades live inside the replay run result. They
            # are never written to runtime_events, the broker ledger, or the
            # live trades/capital projections.
            "diagnostic_trades": diagnostic_trades,
        }

    @staticmethod
    def _settle_trade(open_trades: list[dict[str, Any]], trades: list[dict[str, Any]],
                      order: Any, bar: MarketBar, exit_price: float, reason: str) -> None:
        opposite = "SELL" if order.side.value == "BUY" else "BUY"
        index = next((i for i, trade in enumerate(open_trades)
                      if trade["instrument"] == order.instrument.symbol and trade["side"] == opposite), None)
        if index is None:
            return
        trade = open_trades.pop(index)
        signed = 1.0 if trade["side"] == "BUY" else -1.0
        trades.append({**trade, "exit_order_id": order.client_order_id,
                       "exit_timestamp": bar.timestamp.isoformat(), "exit_price": exit_price,
                       "exit_reason": reason,
                       "realized_pnl": (exit_price - trade["entry_price"]) * trade["quantity"] * signed,
                       "status": "closed"})

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
