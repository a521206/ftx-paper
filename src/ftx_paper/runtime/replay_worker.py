from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
import hashlib
import json
from queue import Empty, Full, Queue
from threading import Event, Lock, Thread
from typing import Any, cast
from uuid import uuid4
from zoneinfo import ZoneInfo

from ftx_paper.contracts import Instrument, MarketBar, MarketRole, OptionRole, OrderRole, OrderSide, Role, SyntheticFutureQuote, role_to_key, synthetic_future_quote
from ftx_paper.domain.capital import ResearchCapitalProfile, RESEARCH_CAPITAL_PROFILE, CapitalRuntimeContext
from ftx_paper.market import AggregatorConfig, Cell, CompletedBarAggregator, InstrumentKey, LocationDetector
from ftx_paper.market.features import option_pcr_at_event, vix_open_and_event
from ftx_paper.domain import AccountAggregate, PortfolioState
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.runtime.engine import PaperEngine
from ftx_paper.runtime.futures_source import FuturesSessionSourceResolver
from ftx_paper.runtime.parity_artifacts import build_parity_artifact
from ftx_paper.execution.cost import futures_cost, synthetic_futures_cost
from ftx_paper.execution import PaperExecutionCoordinator
from ftx_paper.execution.settlement import ExitValidationError, validate_exit_order
from ftx_paper.strategy.exits import ExitAction
from ftx_paper.strategy import ConfiguredStrategyFactory, StrategyFactory
from ftx_paper.strategy.config import (
    AFTERNOON_ENTRY_MINUTES,
    MORNING_ENTRY_MINUTES,
)
from ftx_paper.config import NIFTY_LOT_SIZE


IST = ZoneInfo("Asia/Kolkata")
SESSION_OPEN_MINUTES = 9 * 60 + 15
SESSION_CLOSE_MINUTES = 15 * 60 + 10


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

    def __init__(self, store: Any, *, capital_profile: ResearchCapitalProfile = RESEARCH_CAPITAL_PROFILE,
                 max_queue_size: int = 1024, strategy_factory: StrategyFactory | None = None,
                 futures_source_resolver: Any | None = None) -> None:
        self.store = store
        self.capital_profile = capital_profile
        self.strategy_factory = strategy_factory or ConfiguredStrategyFactory(capital_profile=capital_profile)
        self._futures_sources = futures_source_resolver or FuturesSessionSourceResolver(store)
        self._source_cache: dict[str, Any] = {}
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
        session_date = str(request.get("session_date") or "").strip()[:10]
        if not session_date:
            raise ValueError("replay requires one session_date")
        # One stored result per trading day. Re-running a date replaces its
        # prior result so stale trades cannot remain visible.
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

    def input_manifest(self, session_date: str) -> dict[str, Any]:
        """Return the deterministic, API-safe inputs used by a replay date."""
        rows = self.store.read_market_bars(session_date)
        futures_source = self._resolve_source(session_date)
        # Reuse the rows already loaded above.  A manifest is metadata-only,
        # but _bars still applies the canonical contract and session filters.
        try:
            bars = self._bars(session_date, rows=rows, futures_source=futures_source)
        except TypeError as exc:
            # Preserve compatibility with lightweight test doubles that still
            # expose the original one-argument _bars hook.
            if "unexpected keyword argument 'rows'" not in str(exc):
                raise
            bars = self._bars(session_date)
        replay_rows = [
            {
                "symbol": bar.instrument.symbol,
                "exchange": bar.instrument.exchange,
                "minute": bar.timestamp.isoformat(),
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "open_interest": bar.open_interest,
                "instrument_type": bar.instrument.instrument_type,
                "expiry": bar.instrument.expiry,
                "strike": bar.instrument.strike,
                "option_type": bar.instrument.option_type,
            }
            for bar in bars
        ]
        strategy_rows = [
            row for row in replay_rows
            if str(row.get("instrument_type", "")).upper() in {"FUT", "FUTURES", "INDEX"}
            and (
                str(row.get("instrument_type", "")).upper() in {"FUT", "FUTURES"}
                or str(row.get("symbol", "")).upper() in {"NIFTY", "NIFTY 50", "INDIA VIX"}
            )
        ]
        by_instrument: dict[str, int] = {}
        for row in strategy_rows:
            key = ":".join(str(row.get(field) or "").upper() for field in ("exchange", "symbol", "instrument_type"))
            if key:
                by_instrument[key] = by_instrument.get(key, 0) + 1
        vix_rows = [row for row in replay_rows if str(row.get("symbol", "")).upper() == "INDIA VIX"]
        option_rows = [row for row in replay_rows if str(row.get("instrument_type", "")).upper() in {"CE", "PE"}]
        option_minutes = {str(row.get("minute")) for row in option_rows}
        futures_minutes = {
            str(row.get("minute")) for row in rows
            if str(row.get("instrument_type", "")).upper() in {"FUT", "FUTURES"}
        }
        source_provenance = {
            "current": futures_source.provenance() if futures_source is not None else None,
            "prior": self._prior_source_provenance(session_date),
        }
        payload = {
            "available_date": session_date,
            "date": session_date,
            "session_date": session_date,
            "bar_count": len(strategy_rows),
            "raw_bar_count": len(rows),
            "supporting_bar_count": len(bars) - len(strategy_rows),
            "futures_source": futures_source.provenance() if futures_source is not None else None,
            "provenance": source_provenance,
            "per_instrument_counts": dict(sorted(by_instrument.items())),
            "availability": {
                "vix": bool(vix_rows),
                "pcr": bool(option_rows and option_minutes.intersection(futures_minutes)),
            },
            "expiry_calendar": sorted({
                str(item)[:10] for item in self.store.read_expiry_dates()
                if str(item)[:10] >= session_date
            } | {
                str(row["expiry"])[:10] for row in option_rows
                if row.get("expiry") and str(row["expiry"])[:10] >= session_date
            }),
            "session_configuration": {
                "timezone": "Asia/Kolkata",
                "open": "09:15",
                "close": "15:10",
                "morning_entry_minutes": list(MORNING_ENTRY_MINUTES),
                "afternoon_entry_minutes": list(AFTERNOON_ENTRY_MINUTES),
                "aggregator": {"required_roles": ["futures"], "deadline_seconds": 0},
            },
        }
        fingerprint_source = {**payload, "rows": sorted(
            strategy_rows,
            key=lambda row: tuple(str(row.get(field) or "") for field in (
                "minute", "exchange", "symbol", "instrument_type", "expiry", "strike", "option_type",
                "open", "high", "low", "close", "volume", "open_interest",
            )),
        )}
        canonical = json.dumps(fingerprint_source, sort_keys=True, separators=(",", ":"), default=str).encode()
        payload["input_fingerprint"] = hashlib.sha256(canonical).hexdigest()
        return payload

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
                result = self._execute({**request, "run_id": run_id}, cancel)
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
        session_date = str(request.get("session_date") or "")[:10]
        if not session_date:
            raise ValueError("replay requires one session_date")
        raw_vehicles = request.get("vehicles")
        if raw_vehicles is None:
            vehicles = ("futures", "synthetic")
        elif isinstance(raw_vehicles, (list, tuple)):
            vehicles = tuple(dict.fromkeys(str(item).lower() for item in raw_vehicles))
        else:
            raise ValueError("replay vehicles must be an array containing 'futures' and/or 'synthetic'")
        if not vehicles or any(item not in {"futures", "synthetic"} for item in vehicles):
            raise ValueError(f"unsupported replay vehicles: {vehicles}")
        if "futures" not in vehicles:
            raise ValueError("synthetic replay is reporting-only; futures must be enabled")
        emit_rejected_decisions = request.get("emit_rejected_decisions", False)
        if not isinstance(emit_rejected_decisions, bool):
            raise ValueError("emit_rejected_decisions must be a boolean")
        dates = [session_date]
        all_events: list[dict[str, Any]] = []
        trace: list[dict[str, Any]] = []
        unavailable_fields: set[str] = set()
        diagnostic_trades: list[dict[str, Any]] = []
        bars_seen = 0
        initial_capital = self.capital_profile.initial_capital
        current_equity = initial_capital
        peak_equity = initial_capital
        max_drawdown = 0.0
        last_portfolio: PortfolioState | None = None
        last_strategy_metadata = None
        available_dates = self._dates()
        expiry_dates = self.store.read_expiry_dates()
        # This engine/strategy is private to this replay run and never shared
        # with RuntimeSession, its broker, ledger, or risk state.
        self._source_cache = {}
        for date in dates:
            if cancel.is_set():
                break
            # A date is an independent simulation session. No positions,
            # cooldowns, or risk state may leak into the next date.
            portfolio = PortfolioState.from_snapshot({
                "schema_version": 1,
                "capital": {
                    "initial_capital": initial_capital,
                    "current_equity": current_equity,
                    "realized_pnl": current_equity - initial_capital,
                },
                "peak_equity": peak_equity,
                "daily_baseline": current_equity,
                "reservations": {},
                "positions": {},
                "pending_orders": {},
                "settled_orders": [],
            })
            bars = self._bars(date)
            prior_day_high, prior_day_low = self._prior_day_levels(date, available_dates)
            # Historical replay databases may not contain the imported expiry
            # calendar. Infer an expiry session from the option bars themselves
            # so adaptive stops use the same expiry widening as canonical.
            effective_expiry_dates = set(expiry_dates)
            if any(
                str(bar.instrument.expiry or "")[:10] == date
                and str(bar.instrument.instrument_type).upper() in {"CE", "PE"}
                for bar in bars
            ):
                effective_expiry_dates.add(date)
            capital_context = CapitalRuntimeContext(self.capital_profile, environment="replay")
            strategy = self.strategy_factory.create(
                enabled_vehicles=vehicles,
                portfolio=portfolio,
                capital_context=capital_context,
                prior_day_high=prior_day_high,
                prior_day_low=prior_day_low,
                expiry_dates=frozenset(effective_expiry_dates),
            )
            last_portfolio = portfolio
            last_strategy_metadata = strategy.metadata
            coordinator = PaperExecutionCoordinator(portfolio, capital_context)
            account = AccountAggregate(portfolio, capital_context)
            engine = PaperEngine(
                strategy, emit_rejected_decisions=emit_rejected_decisions,
            )
            # Each date gets a fresh position/risk session while cumulative
            # capital is carried by the authoritative PortfolioState.
            aggregator = self._aggregator(bars)
            trace_detector = LocationDetector()
            trace_detector.reset(prior_day_high=prior_day_high, prior_day_low=prior_day_low)
            trace_vix_history: list[MarketBar] = []
            trace_vix_open: float | None = None
            open_trades: list[dict[str, Any]] = []
            # Synthetic is a reporting-only view of the accepted futures
            # decision.  Keep its diagnostic lifecycle separate so it cannot
            # consume risk, margin, cooldown, or execution state.
            synthetic_open_trades: list[dict[str, Any]] = []
            last_quotes: dict[str, SyntheticFutureQuote] = {}
            for bar in bars:
                if cancel.is_set():
                    break
                bundle = aggregator.ingest(bar)
                if bundle is None:
                    continue
                futures_bar = bundle.bars[MarketRole.FUTURES]
                trace_snapshot = trace_detector.observe(futures_bar)
                trace_features: dict[str, Any] = {}
                trace_locations: list[str] = []
                trace_transitions: list[dict[str, str]] = []
                trace_unavailable: list[str] = []
                current_vix = bundle.bars.get(MarketRole.VIX) or (
                    bundle.supporting_inputs or {}
                ).get("bars", {}).get(MarketRole.VIX)
                if current_vix is not None:
                    trace_vix_history.append(current_vix)
                    opening, _ = vix_open_and_event(tuple(trace_vix_history), current_vix)
                    if opening is not None:
                        trace_vix_open = opening
                if trace_snapshot is not None:
                    trace_features = {
                        "vwap": trace_snapshot.features.vwap,
                        "session_high": trace_snapshot.features.session_high,
                        "session_low": trace_snapshot.features.session_low,
                        "opening_range_high": trace_snapshot.features.opening_range_high,
                        "opening_range_low": trace_snapshot.features.opening_range_low,
                        "atr": trace_snapshot.features.atr,
                        "prior_day_high": trace_snapshot.features.prior_day_high,
                        "prior_day_low": trace_snapshot.features.prior_day_low,
                        "vix": current_vix.close if current_vix is not None else None,
                        "vix_open": trace_vix_open,
                        "pcr": self._bundle_pcr(bundle, effective_expiry_dates),
                    }
                    trace_locations = [item.value for item in trace_snapshot.locations]
                    trace_transitions = [
                        {"reference": item.reference.value, "kind": item.kind.value,
                         "from": item.from_side.value, "to": item.to_side.value}
                        for item in trace_snapshot.transitions
                    ]
                    trace_unavailable = [key for key, value in trace_features.items() if value is None]
                    unavailable_fields.update(trace_unavailable)
                trace_bar_index = bars_seen + 1
                trace.append({
                    "date": date,
                    "bar_index": trace_bar_index,
                    "timestamp": futures_bar.timestamp.isoformat(),
                    "features": trace_features,
                    "locations": trace_locations,
                    "transitions": trace_transitions,
                    "unavailable_fields": trace_unavailable,
                })
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
                exit_actions = cast(tuple[ExitAction, ...], engine.on_closed_bar(futures_bar))
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
                        all_events.append({"event_type": "ORDER_UNFILLED", "decision_id": action.intent.client_order_id,
                                           "vehicle": action.intent.vehicle, "timestamp": futures_bar.timestamp.isoformat(),
                                           "source": "replay", "session_date": date, "reason": "missing_synthetic_future_quote"})
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
                                  "decision_at": futures_bar.timestamp.isoformat(), "reason": action.reason,
                                  "exit_mode": action.intent.exit_mode,
                                  "bars_held": action.bars_held, "mae_bp": action.mae_bp, "mfe_bp": action.mfe_bp,
                                  "exit_price": action.price, "trigger_price": action.price,
                                  "price": exit_price, "source": "replay", "session_date": date}
                    if exit_quote is not None:
                        exit_event.update({"synthetic_future_price": exit_quote.price, "ce_strike": exit_quote.strike,
                                           "ce_exit_price": exit_quote.ce.close, "pe_exit_price": exit_quote.pe.close})
                    all_events.append(exit_event)
                    all_events.append({"event_type": "ORDER_INTENT", "decision_id": action.intent.client_order_id,
                                       "client_order_id": action.intent.client_order_id,
                                       "vehicle": action.intent.vehicle, "direction": "long" if action.intent.side is OrderSide.SELL else "short",
                                       "quantity": action.intent.quantity, "entry_bar": action.intent.entry_bar,
                                       "decision_at": futures_bar.timestamp.isoformat(), "source": "replay", "session_date": date})
                    all_events.append({"event_type": "ORDER_ACK", "decision_id": action.intent.client_order_id,
                                       "client_order_id": action.intent.client_order_id, "vehicle": action.intent.vehicle,
                                       "timestamp": futures_bar.timestamp.isoformat(), "source": "replay",
                                       "session_date": date, "status": "ACKNOWLEDGED"})
                    realized = self._settle_trade(open_trades, diagnostic_trades, action.intent, futures_bar,
                                                  exit_quote, action.reason, exit_price=exit_price,
                                                  trade=open_trade)
                    if action.intent.vehicle == "futures" and "synthetic" in vehicles:
                        synthetic_trade = next(
                            (item for item in synthetic_open_trades
                             if item["futures_entry_order_id"] == action.intent.entry_order_id),
                            None,
                        )
                        synthetic_exit_quote = (
                            synthetic_future_quote(
                                quote_selection_bar, option_bars,
                                symbols=(synthetic_trade["ce_symbol"], synthetic_trade["pe_symbol"]),
                            )
                            if synthetic_trade is not None else None
                        )
                        if synthetic_trade is not None and synthetic_exit_quote is not None:
                            synthetic_order = replace(
                                action.intent,
                                client_order_id=f"{action.intent.client_order_id}:synthetic-report",
                                vehicle="synthetic",
                                synthetic_legs=(
                                    synthetic_exit_quote.ce.instrument,
                                    synthetic_exit_quote.pe.instrument,
                                ),
                            )
                            self._settle_trade(
                                synthetic_open_trades, diagnostic_trades,
                                synthetic_order, futures_bar, synthetic_exit_quote,
                                action.reason, exit_price=synthetic_exit_quote.price,
                                trade=synthetic_trade,
                            )
                    if action.intent.vehicle == "futures":
                        coordinator.fill(action.intent, price=exit_price,
                                         timestamp=futures_bar.timestamp.isoformat(),
                                         cost=_execution_cost(open_trade))
                    all_events.append({"event_type": "FILL", "decision_id": action.intent.client_order_id,
                                       "client_order_id": action.intent.client_order_id,
                                       "fill_timestamp": futures_bar.timestamp.isoformat(), "price": exit_price,
                                       "quantity": action.intent.quantity, "vehicle": action.intent.vehicle,
                                       "source": "replay", "session_date": date})
                    all_events.append({"event_type": "EXECUTEDDECISION", "decision_id": action.intent.client_order_id,
                                       "client_order_id": action.intent.client_order_id,
                                       "fill_timestamp": futures_bar.timestamp.isoformat(), "price": exit_price,
                                       "quantity": action.intent.quantity, "vehicle": action.intent.vehicle,
                                       "source": "replay", "session_date": date, "status": "FILLED"})
                    engine.on_execution_event(ExecutionNotification(
                        event_type="FILLED", client_order_id=action.intent.client_order_id,
                        status="FILLED", quantity=action.intent.quantity,
                        filled_quantity=action.intent.quantity, price=exit_price,
                    ))
                    engine.settle_exit(action.intent.client_order_id, filled=True)
                    if realized is not None:
                        engine.record_exit(cell=realized.cell.name, reason=action.reason,
                                           entry_bar=realized.entry_bar, exit_bar=engine.bars_seen,
                                           date=date, vehicle=action.intent.vehicle,
                                           direction="long" if action.intent.side.value == "SELL" else "short",
                                           quantity=action.intent.quantity)
                        current_equity = portfolio.equity
                        peak_equity = portfolio.peak_equity
                        max_drawdown = min(max_drawdown, current_equity - peak_equity)
                try:
                    result = engine.on_bundle(bundle, context=account.context(as_of=futures_bar.timestamp))
                except TypeError as exc:
                    if "context" not in str(exc):
                        raise
                    result = engine.on_bundle(bundle)
                bars_seen += 1
                result_events = [{**event, "source": "replay", "session_date": date} for event in result.events]
                all_events.extend(result_events)
                for order in result.orders:
                    all_events.append({"event_type": "ORDER_INTENT",
                                       "decision_id": order.client_order_id.rsplit(":", 1)[0],
                                       "client_order_id": order.client_order_id,
                                       "instrument": order.instrument.symbol,
                                       "direction": "long" if order.side is OrderSide.BUY else "short",
                                       "quantity": order.quantity, "cell": order.cell,
                                       "stop": order.stop_price, "exit_mode": order.exit_mode,
                                       "entry_bar": order.entry_bar, "decision_at": futures_bar.timestamp.isoformat(),
                                       "source": "replay",
                                       "session_date": date})
                    if order.role is OrderRole.ENTRY:
                        if order.vehicle == "futures":
                            try:
                                account.authorize_and_reserve(order)
                            except ValueError as exc:
                                all_events.append({"event_type": "RISK_REJECTED",
                                                   "decision_id": order.client_order_id,
                                                   "reason": str(exc), "source": "replay", "session_date": date})
                                all_events.append({"event_type": "ORDER_SUPPRESSED", "decision_id": order.client_order_id,
                                                   "vehicle": order.vehicle, "timestamp": futures_bar.timestamp.isoformat(),
                                                   "source": "replay", "session_date": date, "execution_allowed": False,
                                                   "reason": str(exc)})
                                continue
                            all_events.append({"event_type": "ORDER_AUTHORIZED",
                                               "decision_id": order.client_order_id,
                                               "account_revision": account.revision,
                                                "source": "replay", "session_date": date})
                        all_events.append({"event_type": "ORDER_ACK", "decision_id": order.client_order_id,
                                           "client_order_id": order.client_order_id, "vehicle": order.vehicle,
                                           "timestamp": futures_bar.timestamp.isoformat(), "source": "replay",
                                           "session_date": date, "status": "ACKNOWLEDGED"})
                        entry_quote = synthetic_future_quote(
                            quote_selection_bar, option_bars,
                            symbols=(order.synthetic_legs[0].symbol, order.synthetic_legs[1].symbol)
                            if order.synthetic_legs is not None else None,
                        ) if order.vehicle == "synthetic" else None
                        if entry_quote is None and order.vehicle == "synthetic":
                            all_events.append({"event_type": "EXECUTION_ERROR", "decision_id": order.client_order_id,
                                               "vehicle": order.vehicle,
                                               "reason": "missing_synthetic_future_quote", "source": "replay", "session_date": date})
                            all_events.append({"event_type": "ORDER_UNFILLED", "decision_id": order.client_order_id,
                                               "vehicle": order.vehicle, "timestamp": futures_bar.timestamp.isoformat(),
                                               "source": "replay", "session_date": date, "reason": "missing_synthetic_future_quote"})
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
                        all_events.append({"event_type": "EXECUTEDDECISION", "decision_id": order.client_order_id,
                                           "client_order_id": order.client_order_id, "fill_timestamp": futures_bar.timestamp.isoformat(),
                                           "price": fill_price, "quantity": order.quantity, "vehicle": order.vehicle,
                                           "source": "replay", "session_date": date, "status": "FILLED"})
                        engine.register_entry(order, fill_price=fill_price,
                                              entry_fill_time=futures_bar.timestamp,
                                              reference_price=futures_bar.close)
                        if order.vehicle == "futures":
                            coordinator.fill(order, price=fill_price,
                                              timestamp=futures_bar.timestamp.isoformat())
                        engine.on_execution_event(ExecutionNotification(
                            event_type="FILLED", client_order_id=order.client_order_id,
                            status="FILLED", quantity=order.quantity,
                            filled_quantity=order.quantity, price=fill_price,
                        ))
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
                        elif order.vehicle == "futures" and "synthetic" in vehicles:
                            report_quote = synthetic_future_quote(
                                quote_selection_bar, option_bars,
                            )
                            if report_quote is not None:
                                synthetic_open_trades.append({
                                    "futures_entry_order_id": order.client_order_id,
                                    "entry_order_id": f"{order.client_order_id}:synthetic-report",
                                    "instrument": order.instrument.symbol,
                                    "side": order.side.value,
                                    "quantity": order.quantity,
                                    "entry_timestamp": futures_bar.timestamp.isoformat(),
                                    "entry_price": report_quote.price,
                                    "vehicle": "synthetic",
                                    "cell": order.cell,
                                    "entry_bar": order.entry_bar or engine.bars_seen,
                                    "ce_symbol": report_quote.ce.instrument.symbol,
                                    "pe_symbol": report_quote.pe.instrument.symbol,
                                    "ce_expiry": report_quote.ce.instrument.expiry,
                                    "pe_expiry": report_quote.pe.instrument.expiry,
                                    "ce_strike": report_quote.strike,
                                    "ce_entry_price": report_quote.ce.close,
                                    "pe_entry_price": report_quote.pe.close,
                                    "synthetic_entry_price": report_quote.price,
                                    "quote_timestamp": report_quote.ce.timestamp.isoformat(),
                                    "quote_source": "same_minute_bundle",
                                })
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
                        if order.vehicle == "futures":
                            coordinator.fill(
                                order,
                                price=exit_quote.price if exit_quote is not None else futures_bar.close,
                                timestamp=futures_bar.timestamp.isoformat(),
                                cost=_execution_cost(open_trade),
                            )
                        engine.record_exit(cell=realized.cell.name, reason=order.reason,
                                           entry_bar=realized.entry_bar, exit_bar=engine.bars_seen,
                                           date=date, vehicle=order.vehicle,
                                           direction="long" if order.side.value == "SELL" else "short",
                                           quantity=order.quantity)
                        current_equity = portfolio.equity
                        peak_equity = portfolio.peak_equity
                        max_drawdown = min(max_drawdown, current_equity - peak_equity)
            # Preserve open positions as mark-to-market/open replay results.
            diagnostic_trades.extend({**trade, "status": "open"} for trade in open_trades)
            diagnostic_trades.extend({**trade, "status": "open"} for trade in synthetic_open_trades)
        scores = [int(event["score"]) for event in all_events
                  if isinstance(event.get("score"), (int, float))]
        normalized_trades = [self._normalize_trade(trade) for trade in diagnostic_trades]
        ledger_by_date: dict[str, dict[str, Any]] = {}
        for trade in normalized_trades:
            if trade.get("vehicle") != "futures":
                continue
            date = str(trade.get("date", ""))[:10]
            if not date:
                continue
            row = ledger_by_date.setdefault(date, {
                "date": date, "gross_pnl_rs": 0.0, "cost_rs": 0.0,
                "net_pnl_rs": 0.0, "daily_pnl": 0.0,
            })
            row["gross_pnl_rs"] += float(trade.get("gross_pnl_rs") or 0.0)
            row["cost_rs"] += float(trade.get("cost_rs") or 0.0)
            row["net_pnl_rs"] += float(trade.get("net_pnl_rs") or 0.0)
            row["daily_pnl"] += float(trade.get("net_pnl_rs") or 0.0)
        if ledger_by_date:
            if last_portfolio is None or last_strategy_metadata is None:
                raise RuntimeError("replay completed without a strategy session")
            portfolio = last_portfolio
            final_date = max(ledger_by_date)
            final_row = ledger_by_date[final_date]
            final_row.update({
                "equity": current_equity,
                "drawdown": max(0.0, -max_drawdown),
                "open_margin": max(
                    float(item.get("quantity") or 0.0)
                    * self.capital_profile.vehicle_limit("futures").margin_per_lot
                    for item in normalized_trades
                    if item.get("vehicle") == "futures"
                ) if normalized_trades else 0.0,
                "reservations": {
                    order_id: asdict(reservation)
                    for order_id, reservation in portfolio.reservations.items()
                },
                "directional_exposure": sum(
                    position.quantity * (1 if position.side is OrderSide.BUY else -1)
                    for position in portfolio.positions.values()
                ),
                "rejection_counts": {},
            })
        if last_strategy_metadata is None:
            raise RuntimeError("replay completed without a strategy session")
        directional_exposure = sum(
            position.quantity * (1 if position.side is OrderSide.BUY else -1)
            for position in (last_portfolio.positions.values() if last_portfolio is not None else ())
        )
        input_manifest = self.input_manifest(session_date)
        metadata = {
            "api_version": "1.1",
            "strategy_version": last_strategy_metadata.version,
            "configuration_hash": last_strategy_metadata.config_hash,
            "input_fingerprint": input_manifest["input_fingerprint"],
            "input_provenance": input_manifest["provenance"],
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "directional_exposure": directional_exposure,
            "requested_parameters": {
                "session_date": session_date,
                "vehicles": list(vehicles),
                "emit_rejected_decisions": emit_rejected_decisions,
            },
        }
        normalized_decisions = []
        for event in all_events:
            if not str(event.get("event_type", "")).endswith("DECISION"):
                continue
            normalized = self._normalize_decision(event)
            decision_id = event.get("decision_id")
            linked = [item for item in all_events if decision_id and item.get("decision_id") == decision_id]
            execution_events = [item for item in linked if str(item.get("event_type", "")) in {
                "ORDER_INTENT", "ORDER_AUTHORIZED", "ORDER_ACK", "FILL", "EXECUTEDDECISION",
                "ORDER_UNFILLED", "ORDER_SUPPRESSED", "RISK_REJECTED",
            }]
            normalized["execution"].update({
                "event_types": [str(item.get("event_type")) for item in execution_events],
                "client_order_id": next((item.get("client_order_id") for item in execution_events
                                          if item.get("client_order_id")), None),
            })
            normalized_decisions.append(normalized)
        replay_result = {
            "result_schema_version": 2,
            "source": "replay",
            "vehicles": list(vehicles),
            "vehicle_semantics": "shared_portfolio_directional_and_margin",
            "strategy": {"name": last_strategy_metadata.name, "version": last_strategy_metadata.version,
                         "config_hash": last_strategy_metadata.config_hash},
            "metadata": metadata,
            "configuration": {"initial_capital": initial_capital,
                               "max_daily_loss": self.capital_profile.max_daily_loss,
                               "max_net_directional_lots": self.capital_profile.max_net_directional_lots,
                               "risk_per_trade": self.capital_profile.risk_per_trade,
                               "max_lots": self.capital_profile.max_lots,
                               "lot_size": NIFTY_LOT_SIZE},
            "bars_seen": bars_seen,
            "directional_exposure": directional_exposure,
            "events": all_events,
            "decisions": [event for event in all_events if str(event.get("event_type", "")).endswith("DECISION")],
            "decision_trace_schema_version": 1,
            "decision_trace": normalized_decisions,
            "trace_schema_version": 1,
            "trace_bar_index_base": 1,
            "trace": trace,
            "unavailable_fields": sorted(unavailable_fields),
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
            "ledger": ledger_by_date,
            "scoring": {
                "decisions_scored": len(scores),
                "score_min": min(scores) if scores else None,
                "score_max": max(scores) if scores else None,
                "score_average": sum(scores) / len(scores) if scores else None,
            },
        }
        replay_result["parity_artifact"] = build_parity_artifact(replay_result, request=request)
        return replay_result

    @staticmethod
    def _normalize_decision(event: dict[str, Any]) -> dict[str, Any]:
        payload = dict(event)
        event_type = str(payload.get("event_type", ""))
        return {
            "candidate_identity": payload.get("candidate_id") or payload.get("decision_id"),
            "decision_id": payload.get("decision_id"),
            "decision_stage": payload.get("decision_stage") or event_type.lower().removesuffix("decision"),
            "outcome": payload.get("outcome") or event_type.lower(),
            "rejection_reason": payload.get("reason"),
            "risk": {
                key: payload.get(key) for key in ("stop_basis", "risk_per_trade", "risk_amount", "quantity", "score_multiplier")
                if payload.get(key) is not None
            },
            "sizing": {
                key: payload.get(key) for key in ("quantity", "max_lots", "lot_size", "entry_price")
                if payload.get(key) is not None
            },
            "execution": {
                key: payload.get(key) for key in ("vehicle", "client_order_id", "execution_status", "execution_event_type")
                if payload.get(key) is not None
            },
            "date": payload.get("session_date"),
            "timestamp": payload.get("decision_at"),
            "bar_index": payload.get("sequence"),
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

    @staticmethod
    def _bundle_pcr(bundle: Any, expiry_dates: Any) -> float | None:
        supporting = bundle.supporting_inputs or {}
        bars = supporting.get("bars", {})
        sources = supporting.get("sources", {})
        if not isinstance(bars, dict):
            return None
        if isinstance(sources, dict):
            bars = {
                key: bar for key, bar in bars.items()
                if sources.get(role_to_key(key) if isinstance(key, (MarketRole, OptionRole)) else str(key), "same_minute") == "same_minute"
            }
        futures = bundle.bars.get(MarketRole.FUTURES)
        return option_pcr_at_event(bars, futures, expiry_dates=expiry_dates) if futures is not None else None

    def _prior_day_levels(
        self, session_date: str, available_dates: list[str] | None = None,
    ) -> tuple[float | None, float | None]:
        """Return the preceding stored futures high/low for replay context.

        Pipeline replays seed each date with the immediately preceding
        trading day's futures range. Paper dates are evaluated independently
        for positions and risk, but their feature context must retain this
        causal cross-date input.
        """
        dates = sorted(available_dates or self._dates())
        prior = [value for value in dates if value < session_date]
        if not prior:
            return None, None
        source = self._resolve_source(prior[-1])
        if source is None:
            return None, None
        return max(float(row["high"]) for row in source.bars), min(float(row["low"]) for row in source.bars)

    def _resolve_source(self, session_date: str) -> Any | None:
        if session_date not in self._source_cache:
            self._source_cache[session_date] = self._futures_sources.resolve(session_date)
        return self._source_cache[session_date]

    def _prior_source_provenance(self, session_date: str) -> dict[str, Any] | None:
        dates = sorted(value for value in self._dates() if value < session_date)
        if not dates:
            return None
        source = self._resolve_source(dates[-1])
        return source.provenance() if source is not None else None

    def _bars(
        self,
        session_date: str,
        *,
        rows: list[dict[str, Any]] | None = None,
        futures_source: Any | None = None,
    ) -> tuple[MarketBar, ...]:
        loaded_rows = rows if rows is not None else self.store.read_market_bars(session_date)
        futures_source = futures_source if futures_source is not None else self._resolve_source(session_date)
        if any(str(row.get("instrument_type", "")).upper() in {"FUT", "FUTURES"} for row in loaded_rows):
            loaded_rows = [
                row for row in loaded_rows
                if str(row.get("instrument_type", "")).upper() not in {"FUT", "FUTURES"}
                or (
                    futures_source is not None
                    and str(row.get("symbol")) == futures_source.symbol
                    and str(row.get("expiry"))[:10] == futures_source.expiry
                )
            ]
        bars = []
        for row in loaded_rows:
            instrument_type = str(row.get("instrument_type", "")).upper()
            if instrument_type not in {"FUT", "FUTURES", "INDEX", "EQ", "CE", "PE"}:
                continue
            timestamp = datetime.fromisoformat(str(row["minute"]))
            minute_of_day = timestamp.astimezone(IST).hour * 60 + timestamp.astimezone(IST).minute
            if not SESSION_OPEN_MINUTES <= minute_of_day <= SESSION_CLOSE_MINUTES:
                continue
            bars.append(MarketBar(
                instrument=Instrument(symbol=str(row["symbol"]), exchange=str(row["exchange"]),
                                      instrument_type=instrument_type, expiry=row.get("expiry"),
                                      strike=row.get("strike")),
                timestamp=timestamp,
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
                    else MarketRole.VIX if bar.instrument.symbol.upper() == "INDIA VIX"
                    else OptionRole(bar.instrument.symbol) if kind in {"CE", "PE"}
                    else MarketRole.SPOT)
            roles[InstrumentKey(bar.instrument.exchange, bar.instrument.symbol)] = role
        return CompletedBarAggregator(roles, AggregatorConfig(required_roles=(MarketRole.FUTURES,), deadline_seconds=0))

__all__ = ["ReplayWorker"]
