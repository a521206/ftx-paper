from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
import traceback
from queue import Empty, Full, Queue
from threading import Event, Lock, Thread, current_thread
from collections.abc import Mapping
from typing import Any, cast
from uuid import uuid4

from ftx_paper.contracts import MarketBar, MarketRole, OptionRole, OrderRole, OrderSide, Role, SyntheticFutureQuote, build_decision_view, synthetic_future_quote, validate_decision_view
from ftx_paper.contracts.replay import (
    ExecutionScope, ReplayLedgerEntry, ReplayRequest, ReplayResult,
    ReplaySummary, ReplayTrade, REPLAY_SCHEMA_VERSION,
)
from ftx_paper.domain.capital import ResearchCapitalProfile, RESEARCH_CAPITAL_PROFILE, CapitalRuntimeContext
from ftx_paper.domain import AccountAggregate, PortfolioState
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.runtime.engine import PaperEngine
from ftx_paper.runtime.decision_turn import DecisionTurn, evaluate_turn
from ftx_paper.runtime.replay_input import ReplayInput
from ftx_paper.runtime.replay_projection import (
    build_summary, canonical_events, is_parity_decision_event, normalize_decision, normalize_trade,
)
from ftx_paper.runtime.state import ReplayRuntimeState
from ftx_paper.runtime.replay_runners import (
    OrderflowReplayRunner,
    ResearchReplayRunner,
    StrategyReplayRunner,
)
from ftx_paper.execution.lifecycle import (
    ExecutionLifecycleCoordinator,
    ExecutionPhase,
    ExecutionTurn,
)
from ftx_paper.execution.cost import futures_cost, synthetic_futures_cost
from ftx_paper.execution import PaperExecutionCoordinator
from ftx_paper.execution.entry_fill import entry_fill_decision, validate_exit_plan
from ftx_paper.execution.settlement import ExitValidationError, validate_exit_order
from ftx_paper.strategies.shared.exits import ExitAction
from ftx_paper.strategies.shared.factory import StrategyContext, StrategySelector
from ftx_paper.config import NIFTY_LOT_SIZE
from ftx_paper.strategies.orderflow.timestamps import nifty_label

_EXECUTION_EVENT_TYPES = frozenset({
    "ORDER_INTENT", "ORDER_AUTHORIZED", "ORDER_ACK", "FILL",
    "ORDER_UNFILLED", "EXECUTION_PROPOSAL_SUPPRESSED", "ENTRY_BLOCKED_RISK",
    "ENTRY_BLOCKED_EXIT_PLAN", "EXECUTION_ERROR",
    "EXECUTION_GROUP_CANDIDATE_DENIED", "TRIGGER_DENIED_PORTFOLIO",
    "RESERVATION", "AUTHORIZATION", "SUBMISSION", "PENDING_EXIT",
    "EXIT_FILL", "CANCELLATION", "SETTLEMENT",
})


def _source_replay_records(
    bars: tuple[MarketBar, ...], session_date: str,
) -> list[dict[str, Any]]:
    """Project selected futures bars into the canonical replay input shape."""
    return [
        {
            "bar": {
                "time": nifty_label(bar.timestamp)[-5:],
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": int(bar.volume or 0),
                "oi": int(bar.open_interest or 0),
                "symbol": "NIFTY",
                "expiry": "",
                "contract_type": "CONTINUOUS",
                "session_date": session_date,
            },
        }
        for bar in bars
    ]


class _ScopedEvents(list[dict[str, Any]]):
    """Attach replay ownership when an event enters the replay stream."""

    def __init__(self, *, run_id: str, execution_scope: str, ledger_scope: str) -> None:
        super().__init__()
        self._scope = execution_scope
        self._ledger_scope = ledger_scope
        self._run_id = run_id

    def append(self, event: dict[str, Any]) -> None:
        scoped = dict(event)
        scoped.setdefault("run_id", self._run_id)
        scoped.setdefault("execution_scope", self._scope)
        scoped.setdefault("ledger_scope", self._ledger_scope)
        scoped.setdefault(
            "neutral_event_key",
            ":".join(str(scoped.get(field) or "") for field in (
                "event_type", "decision_id", "client_order_id", "timestamp", "decision_at",
            )),
        )
        super().append(scoped)

    def extend(self, events: Any) -> None:
        for event in events:
            self.append(event)


@dataclass(frozen=True, slots=True)
class TradeSettlement:
    cell: str
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
                 max_queue_size: int = 1024, strategy_selector: StrategySelector | None = None,
                 futures_source_resolver: Any | None = None, parity_projector: Any | None = None) -> None:
        self.store = store
        self.capital_profile = capital_profile
        self.strategy_selector = strategy_selector
        self.parity_projector = parity_projector
        self.inputs = ReplayInput(store, futures_source_resolver)
        self.runners: dict[str, StrategyReplayRunner] = {
            "ftx-paper-orderflow": OrderflowReplayRunner(self),
            "ftx-paper-research": ResearchReplayRunner(self),
        }
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

    def stop(self, *, wait: bool = True) -> None:
        """Cancel queued work and stop the worker during application shutdown."""
        self._stopped.set()
        with self._lock:
            run_ids = tuple(self._cancel)
        for run_id in run_ids:
            self.cancel(run_id)
        thread = self._thread
        if wait and thread is not None and thread is not current_thread():
            thread.join()

    def submit(self, request: dict[str, Any]) -> str:
        request = ReplayRequest.from_mapping(request).to_dict()
        run_id = uuid4().hex
        cancel = Event()
        run_id, created = self.store.create_or_get_replay_run(run_id, request)
        if not created:
            return run_id
        with self._lock:
            self._cancel[run_id] = cancel
        self._ensure_started()
        try:
            self._queue.put_nowait((run_id, request, cancel))
        except Full:
            self.store.update_replay_run(run_id, status="rejected", error="replay queue is full")
            raise RuntimeError("replay queue is full") from None
        return run_id

    def execute_detached(self, request: dict[str, Any]) -> dict[str, Any]:
        """Run an isolated replay without creating or updating a stored run."""
        request = ReplayRequest.from_mapping(request).to_dict()
        return self._execute({**request, "run_id": request.get("run_id") or uuid4().hex}, Event())

    def input_manifest(
        self,
        session_date: str,
        *,
        rows: list[dict[str, Any]] | None = None,
        bars: tuple[MarketBar, ...] | None = None,
        futures_source: Any | None = None,
    ) -> Mapping[str, Any]:
        """Return the deterministic, API-safe inputs used by a replay date."""
        return self.inputs.manifest(
            session_date, rows=rows, bars=bars, futures_source=futures_source,
        )

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
                    persist = getattr(self.store, "write_replay_artifact", None)
                    if callable(persist):
                        persist(run_id, result)
                    else:
                        self.store.update_replay_run(run_id, status="running", result=result)
                    self.store.update_replay_run(run_id, status="completed")
            except Exception:
                run = self.store.read_replay_run(run_id)
                if run is not None and run.get("status") != "cancelled":
                    self.store.update_replay_run(run_id, status="failed", error=traceback.format_exc())
            finally:
                with self._lock:
                    self._cancel.pop(run_id, None)
                self._queue.task_done()

    def _execute(self, request: dict[str, Any], cancel: Event) -> dict[str, Any]:
        strategy_id = request.get("strategy_id")
        runner = self.runners.get(str(strategy_id))
        if runner is not None:
            return runner.run(request, cancel)
        return self._execute_research(request, cancel)

    def _execute_research(self, request: dict[str, Any], cancel: Event) -> dict[str, Any]:
        lifecycle = ExecutionLifecycleCoordinator()
        session_date_value = request.get("session_date")
        if not isinstance(session_date_value, str):
            raise ValueError("replay requires session_date as a string")
        session_date = session_date_value[:10]
        if not session_date:
            raise ValueError("replay requires one session_date")
        raw_vehicles = request.get("vehicles")
        if raw_vehicles is None:
            vehicles = ("futures", "synthetic")
        elif isinstance(raw_vehicles, (list, tuple)):
            if any(not isinstance(item, str) for item in raw_vehicles):
                raise ValueError("replay vehicles must contain strings")
            vehicles = tuple(dict.fromkeys(item.lower() for item in raw_vehicles))
        else:
            raise ValueError("replay vehicles must be an array containing 'futures' and/or 'synthetic'")
        if not vehicles or any(item not in {"futures", "synthetic"} for item in vehicles):
            raise ValueError(f"unsupported replay vehicles: {vehicles}")
        if "futures" not in vehicles:
            raise ValueError("synthetic replay is reporting-only; futures must be enabled")
        mode_value = request.get("mode", "replay")
        if not isinstance(mode_value, str):
            raise ValueError("mode must be 'replay'")
        mode = mode_value.lower()
        if mode != "replay":
            raise ValueError("mode must be 'replay'")
        include_parity_layers = request.get("include_parity_layers", False)
        if not isinstance(include_parity_layers, bool):
            raise ValueError("include_parity_layers must be a boolean")
        if include_parity_layers and self.parity_projector is None:
            raise RuntimeError("parity projector is required when parity layers are requested")
        dates = [session_date]
        run_id = str(request.get("run_id") or uuid4().hex)
        scope = cast(ExecutionScope, mode)
        all_events = _ScopedEvents(
            run_id=run_id, execution_scope=scope, ledger_scope="replay",
        )
        parity_records: dict[str, list[dict[str, Any]]] = {
            "decision": [], "execution_settlement": [], "portfolio_ledger": [],
        }
        ledger_records = parity_records["portfolio_ledger"]
        order_parent_ids: dict[str, str] = {}
        shadow_trades: list[dict[str, Any]] = []
        bars_seen = 0
        initial_capital = self.capital_profile.initial_capital
        current_equity = initial_capital
        peak_equity = initial_capital
        max_drawdown = 0.0
        last_portfolio: PortfolioState | None = None
        last_strategy_metadata = None
        expiry_dates = self.store.read_expiry_dates()
        # This engine/strategy is private to this replay run and never shared
        # with RuntimeSession, its broker, ledger, or risk state.
        self.inputs.reset()
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
            rows = self.store.read_market_bars(date)
            futures_source = self.inputs.resolve_source(date, rows=rows)
            bars = self.inputs.bars(date, rows=rows, futures_source=futures_source)
            futures_bars = tuple(
                bar for bar in bars
                if str(bar.instrument.instrument_type).upper() in {"FUT", "FUTURES"}
            )
            capital_context = CapitalRuntimeContext(self.capital_profile, environment="replay")
            replay_state = ReplayRuntimeState(
                portfolio=portfolio, capital_context=capital_context, engine=None,
            )
            strategy_id = request.get("strategy_id")
            if not isinstance(strategy_id, str):
                raise ValueError("strategy_id is required")
            if self.strategy_selector is None:
                raise RuntimeError("replay strategy selector was not configured")
            strategy = self.strategy_selector.create(
                strategy_id,
                StrategyContext(
                    enabled_vehicles=tuple(vehicles),
                    portfolio=replay_state.portfolio,
                    capital_context=replay_state.capital_context,
                    expiry_dates=frozenset(expiry_dates),
                ),
            )
            last_portfolio = portfolio
            last_strategy_metadata = strategy.metadata
            coordinator = PaperExecutionCoordinator(replay_state.portfolio, replay_state.capital_context)
            account = AccountAggregate(replay_state.portfolio, replay_state.capital_context)
            engine = PaperEngine(
                strategy,
            )
            replay_state.engine = engine
            replay_state.coordinator = coordinator
            replay_state.account = account
            portfolio = replay_state.portfolio
            capital_context = replay_state.capital_context
            last_portfolio = replay_state.portfolio
            # Each date gets a fresh position/risk session while cumulative
            # capital is carried by the authoritative PortfolioState.
            aggregator = ReplayInput.aggregator(bars)
            open_trades: list[dict[str, Any]] = []
            daily_reset_emitted = False
            # Synthetic is a reporting-only view of the accepted futures
            # decision.  Keep its diagnostic lifecycle separate so it cannot
            # consume risk, margin, cooldown, or execution state.
            synthetic_open_trades: list[dict[str, Any]] = []
            lifecycle.pending_entries.clear()

            next_available_at = {
                item.timestamp: (
                    futures_bars[index + 1].timestamp
                    if index + 1 < len(futures_bars) else item.timestamp
                )
                for index, item in enumerate(futures_bars)
            }

            for bar in bars:
                if cancel.is_set():
                    break
                bundle = aggregator.ingest(bar)
                if bundle is None:
                    continue
                ready_bundle = bundle
                event_start = len(all_events)
                futures_bar = bundle.bars[MarketRole.FUTURES]
                turn = ExecutionTurn(
                    turn_id=f"{date}:{futures_bar.timestamp.isoformat()}",
                    observed_at=futures_bar.timestamp.isoformat(),
                    available_at=next_available_at.get(
                        futures_bar.timestamp, futures_bar.timestamp,
                    ).isoformat(),
                    decision_at=next_available_at.get(
                        futures_bar.timestamp, futures_bar.timestamp,
                    ).isoformat(),
                    source="replay",
                    payload={"bundle_id": bundle.bundle_id},
                )
                # Synthetic contract selection is anchored to the spot/index
                # level, matching the Paper entry contract rule. Futures is
                # still the decision and exit-state instrument.
                raw_supporting_bars = (bundle.supporting_inputs or {}).get("bars", {})
                quote_selection_bar = raw_supporting_bars.get(MarketRole.SPOT, futures_bar)
                option_bars: dict[Role, MarketBar] = {
                    role: bar for role, bar in raw_supporting_bars.items()
                    if isinstance(role, (MarketRole, OptionRole))
                }
                # Replay advances private exit state for trade reporting without
                # mutating the live runtime portfolio.
                exit_actions: tuple[ExitAction, ...] = ()
                replay_exit_events: list[Mapping[str, Any]] = []

                def execute_exit(action: ExitAction) -> dict[str, Any] | None:
                    try:
                        open_trade = self._matching_trade(open_trades, action.intent)
                    except ExitValidationError as exc:
                        all_events.append({"event_type": "EXECUTION_ERROR", "decision_id": action.intent.client_order_id,
                                           "reason": exc.reason, "source": "replay", "session_date": date})
                        return {"filled": False}
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
                        return {"filled": False}
                    # Futures exits fill at the canonical protective trigger
                    # (including gap handling), not at the bar close.  A
                    # synthetic position uses the same futures trigger time,
                    # then settles at its same-minute synthetic quote.
                    exit_price = exit_quote.price if exit_quote is not None else action.price
                    exit_event = {"event_type": "EXITDECISION", "decision_id": action.intent.client_order_id,
                                  "vehicle": action.intent.vehicle,
                                  "entry_decision_id": open_trade["entry_order_id"] if open_trade else None,
                                  "decision_at": futures_bar.timestamp.isoformat(), "outcome": "recorded",
                                  "reason": action.reason,
                                  "exit_mode": action.intent.exit_mode,
                                  "bars_held": action.bars_held, "mae_bp": action.mae_bp, "mfe_bp": action.mfe_bp,
                                  "exit_price": action.price, "trigger_price": action.price,
                                  "price": exit_price, "source": "replay", "session_date": date}
                    if exit_quote is not None:
                        exit_event.update({"synthetic_future_price": exit_quote.price, "ce_strike": exit_quote.strike,
                                           "ce_exit_price": exit_quote.ce.close, "pe_exit_price": exit_quote.pe.close})
                    exit_event["decision_view"] = validate_decision_view(build_decision_view(
                        exit_event,
                        event_type="EXITDECISION",
                        strategy_id=action.intent.strategy_id,
                    ))
                    replay_exit_events.append(exit_event)
                    all_events.append({"event_type": "ORDER_INTENT", "decision_id": action.intent.client_order_id,
                                       "client_order_id": action.intent.client_order_id,
                                       "vehicle": action.intent.vehicle, "direction": "long" if action.intent.side is OrderSide.SELL else "short",
                                       "quantity": action.intent.quantity, "entry_bar": action.intent.entry_bar,
                                       "decision_at": futures_bar.timestamp.isoformat(), "source": "replay", "session_date": date})
                    all_events.append({"event_type": "ORDER_ACK", "decision_id": action.intent.client_order_id,
                                       "client_order_id": action.intent.client_order_id, "vehicle": action.intent.vehicle,
                                       "timestamp": futures_bar.timestamp.isoformat(), "source": "replay",
                                       "session_date": date, "status": "ACKNOWLEDGED"})
                    realized = self._settle_trade(open_trades, shadow_trades, action.intent, futures_bar,
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
                                synthetic_open_trades, shadow_trades,
                                synthetic_order, futures_bar, synthetic_exit_quote,
                                action.reason, exit_price=synthetic_exit_quote.price,
                                trade=synthetic_trade,
                            )
                    settlement = None
                    if action.intent.vehicle == "futures":
                        had_reservation = (
                            action.intent.entry_order_id in portfolio.reservations
                            if action.intent.entry_order_id is not None else False
                        )
                        settlement = lifecycle.apply_fill(
                            action.intent.client_order_id,
                            lambda: coordinator.fill(
                                action.intent, price=exit_price,
                                timestamp=futures_bar.timestamp.isoformat(),
                                cost=_execution_cost(open_trade),
                            ),
                        )
                        if isinstance(settlement, Mapping):
                            ledger_records.append({
                                "event_type": "EXIT_SETTLEMENT", "kind": "exit_settlement",
                                "decision_id": action.intent.client_order_id,
                                "timestamp": futures_bar.timestamp.isoformat(),
                                "quantity": action.intent.quantity,
                                "gross_pnl": settlement.get("gross_pnl"),
                                "costs": settlement.get("costs"),
                                "net_pnl": settlement.get("net_pnl"),
                            })
                            if had_reservation:
                                ledger_records.append({
                                    "event_type": "RESERVATION_RELEASE", "kind": "reservation_release",
                                    "decision_id": action.intent.client_order_id,
                                    "entry_order_id": action.intent.entry_order_id,
                                    "timestamp": futures_bar.timestamp.isoformat(),
                                    "quantity": action.intent.quantity,
                                })
                    all_events.append({"event_type": "FILL", "decision_id": action.intent.client_order_id,
                                       "client_order_id": action.intent.client_order_id,
                                       "fill_timestamp": futures_bar.timestamp.isoformat(), "price": exit_price,
                                       "quantity": action.intent.quantity, "vehicle": action.intent.vehicle,
                                       "source": "replay", "session_date": date})
                    engine.on_execution_event(ExecutionNotification(
                        event_type="FILLED", client_order_id=action.intent.client_order_id,
                        status="FILLED", quantity=action.intent.quantity,
                        filled_quantity=action.intent.quantity, price=exit_price,
                        occurred_at=futures_bar.timestamp.isoformat(),
                        observed_at=futures_bar.timestamp.isoformat(),
                        available_at=futures_bar.timestamp.isoformat(), source="replay",
                    ))
                    return {"filled": True, "realized": realized}

                def settle_exit(action: ExitAction, filled: bool) -> None:
                    engine.settle_exit(action.intent.client_order_id, filled=filled,
                                       strategy_id=action.intent.strategy_id)

                def record_exit(action: ExitAction, result: dict[str, Any] | None) -> None:
                    realized = result.get("realized") if result is not None else None
                    if realized is None:
                        return
                    engine.record_exit(strategy_id=action.intent.strategy_id,
                                       cell=realized.cell.name, reason=action.reason,
                                       entry_bar=realized.entry_bar, exit_bar=engine.bars_seen,
                                       date=date, vehicle=action.intent.vehicle,
                                       direction="long" if action.intent.side.value == "SELL" else "short",
                                       quantity=action.intent.quantity)
                    nonlocal current_equity, peak_equity, max_drawdown
                    current_equity = portfolio.equity
                    peak_equity = portfolio.peak_equity
                    max_drawdown = min(max_drawdown, current_equity - peak_equity)

                def process_replay_exits(actions: tuple[ExitAction, ...]) -> int:
                    for action in actions:
                        result = execute_exit(action)
                        filled = bool(result and result.get("filled"))
                        settle_exit(action, filled)
                        if filled:
                            record_exit(action, result)
                    return len(actions)

                def manage_existing(_turn) -> Mapping[str, Any]:
                    nonlocal exit_actions
                    exit_actions = cast(tuple[ExitAction, ...], engine.on_closed_bar(futures_bar))
                    return {"exit_count": process_replay_exits(exit_actions)}

                selected: list[Any] = []
                execution_orders: list[Any] = []
                result: Any = None
                pending_candidates = lifecycle.due_entries(futures_bar.timestamp.isoformat())
                candidate_orders: tuple[Any, ...] = ()

                def form_opportunity(_turn) -> Mapping[str, Any]:
                    nonlocal result
                    result = evaluate_turn(
                        engine,
                        DecisionTurn(
                            ready_bundle,
                            "replay",
                            account.context(as_of=futures_bar.timestamp),
                        ),
                    )
                    return {"proposal_count": len(result.orders)}

                def arbitrate(_turn) -> Mapping[str, Any]:
                    nonlocal candidate_orders
                    candidate_orders = pending_candidates + tuple(result.orders)
                    selected.extend(coordinator.arbitrate(candidate_orders, max_open_positions=1))
                    return {"accepted": len(selected)}

                def queue_execution(_turn) -> Mapping[str, Any]:
                    execution_orders.extend(selected)
                    for order in tuple(execution_orders):
                        order_parent_ids[order.client_order_id] = (
                            order.lifecycle_id or order.client_order_id.rsplit(":", 1)[0]
                        )
                        if order.role is OrderRole.ENTRY:
                            if order.client_order_id not in {item.client_order_id for item in pending_candidates}:
                                lifecycle.defer_entry(
                                    order, available_at=futures_bar.timestamp.isoformat(),
                                    reason="next_observed_bar",
                                )
                                coordinator.release_deferred_entry(order)
                                continue
                            if order.setup_availability is not None and futures_bar.timestamp.isoformat() <= order.setup_availability:
                                lifecycle.defer_entry(
                                    order, available_at=futures_bar.timestamp.isoformat(),
                                    reason="setup_not_yet_available",
                                )
                                coordinator.release_deferred_entry(order)
                                continue
                            try:
                                account.authorize_and_reserve(order)
                            except ValueError as exc:
                                all_events.append({"event_type": "ENTRY_BLOCKED_RISK", "decision_id": order.lifecycle_decision_id,
                                                   "reason": str(exc), "timestamp": futures_bar.timestamp.isoformat(),
                                                   "source": "replay", "session_date": date})
                                continue
                            entry_quote = synthetic_future_quote(
                                quote_selection_bar, option_bars,
                                symbols=(order.synthetic_legs[0].symbol, order.synthetic_legs[1].symbol)
                                if order.synthetic_legs is not None else None,
                            ) if order.vehicle == "synthetic" else None
                            if order.vehicle == "synthetic" and entry_quote is None:
                                coordinator.cancel(order)
                                all_events.append({"event_type": "ORDER_UNFILLED", "decision_id": order.lifecycle_decision_id,
                                                   "reason": "missing_synthetic_future_quote", "source": "replay",
                                                   "session_date": date})
                                continue
                            execution_order = order
                            if entry_quote is None:
                                requested_fill = order.requested_fill_price or order.limit_price or futures_bar.open
                                trigger_price = order.trigger_price or order.limit_price or requested_fill
                                fill_decision = entry_fill_decision(
                                    direction="long" if order.side is OrderSide.BUY else "short",
                                    trigger_price=trigger_price,
                                    requested_fill_price=requested_fill,
                                    bar_open=futures_bar.open,
                                    failure_boundary=float(order.stop_price) if order.stop_price is not None else futures_bar.open,
                                )
                                if fill_decision.hard_stop is None:
                                    all_events.append({"event_type": "ENTRY_BLOCKED_RISK", "decision_id": order.lifecycle_decision_id,
                                                       "reason": "failure_boundary_not_protective", "timestamp": futures_bar.timestamp.isoformat(),
                                                       "source": "replay", "session_date": date})
                                    continue
                                execution_order = replace(order, stop_price=fill_decision.hard_stop)
                                fill_price = fill_decision.fill_price
                            else:
                                fill_price = entry_quote.price
                            exit_decision = validate_exit_plan(
                                order.exit_plan,
                                direction="long" if order.side is OrderSide.BUY else "short",
                                fill_price=fill_price,
                            )
                            if exit_decision.required_rejections:
                                all_events.append({"event_type": "ENTRY_BLOCKED_EXIT_PLAN", "decision_id": order.lifecycle_decision_id,
                                                   "reason": "required_exit_plan_invalid", "rejected_rule_ids": tuple(
                                                       rule.rule_id for rule in exit_decision.required_rejections
                                                   ), "timestamp": futures_bar.timestamp.isoformat(),
                                                   "source": "replay", "session_date": date})
                                continue
                            execution_order = replace(execution_order, exit_plan=exit_decision.accepted)
                            fill_id = f"replay-entry:{order.client_order_id}:{futures_bar.timestamp.isoformat()}"
                            fill_result = lifecycle.apply_fill(fill_id, lambda: coordinator.fill(
                                execution_order, price=fill_price, timestamp=futures_bar.timestamp.isoformat(), fill_id=fill_id,
                            ))
                            if fill_result is None:
                                coordinator.cancel(execution_order)
                                all_events.append({
                                    "event_type": "TRIGGER_DENIED_PORTFOLIO",
                                    "decision_id": order.lifecycle_decision_id,
                                    "client_order_id": order.client_order_id,
                                    "reason": coordinator.denied_proposals.get(
                                        order.client_order_id, "portfolio_position_limit",
                                    ),
                                    "timestamp": futures_bar.timestamp.isoformat(),
                                    "source": "replay",
                                    "session_date": date,
                                })
                                continue
                            engine.register_entry(execution_order, fill_price=fill_price,
                                                  entry_fill_time=futures_bar.timestamp,
                                                  reference_price=futures_bar.close)
                            all_events.extend((
                                {"event_type": "RESERVATION", "decision_id": order.lifecycle_decision_id,
                                 "client_order_id": order.client_order_id, "quantity": order.quantity,
                                 "timestamp": futures_bar.timestamp.isoformat(), "source": "replay", "session_date": date},
                                {"event_type": "FILL", "decision_id": order.lifecycle_decision_id,
                                 "client_order_id": order.client_order_id, "fill_id": fill_id,
                                 "fill_timestamp": futures_bar.timestamp.isoformat(), "price": fill_price,
                                 "quantity": order.quantity, "vehicle": order.vehicle,
                                  "source": "replay", "session_date": date},
                            ))
                            if order.vehicle == "futures":
                                ledger_records.extend((
                                    {"event_type": "RESERVATION", "kind": "reservation",
                                     "decision_id": order.lifecycle_decision_id,
                                     "timestamp": futures_bar.timestamp.isoformat(),
                                     "quantity": order.quantity, "vehicle": order.vehicle},
                                    {"event_type": "AUTHORIZATION", "kind": "authorization",
                                     "decision_id": order.lifecycle_decision_id,
                                     "timestamp": futures_bar.timestamp.isoformat(),
                                     "quantity": order.quantity, "vehicle": order.vehicle},
                                ))
                            ledger_records.append({
                                "event_type": "FILL", "kind": "fill",
                                "decision_id": order.lifecycle_decision_id,
                                "timestamp": futures_bar.timestamp.isoformat(),
                                "quantity": order.quantity, "price": fill_price,
                            })
                            engine.on_execution_event(ExecutionNotification(
                                event_type="FILLED", client_order_id=order.client_order_id,
                                status="FILLED", quantity=order.quantity, filled_quantity=order.quantity,
                                price=fill_price, occurred_at=futures_bar.timestamp.isoformat(),
                                observed_at=futures_bar.timestamp.isoformat(),
                                available_at=futures_bar.timestamp.isoformat(), source="replay",
                            ))
                            open_trades.append({"entry_order_id": order.client_order_id,
                                                "instrument": order.instrument.symbol, "side": order.side.value,
                                                "quantity": order.quantity, "entry_timestamp": futures_bar.timestamp.isoformat(),
                                                "entry_price": fill_price, "vehicle": order.vehicle,
                                                "stop_price": execution_order.stop_price, "cell": order.cell,
                                                "entry_bar": order.entry_bar or engine.bars_seen})
                        elif order.role is OrderRole.EXIT:
                            try:
                                open_trade = self._matching_trade(open_trades, order)
                            except ExitValidationError as exc:
                                all_events.append({"event_type": "EXECUTION_ERROR", "decision_id": order.client_order_id,
                                                   "reason": exc.reason, "source": "replay", "session_date": date})
                                engine.settle_exit(order.client_order_id, filled=False)
                                continue
                            exit_price = futures_bar.close
                            had_reservation = (
                                order.entry_order_id in portfolio.reservations
                                if order.entry_order_id is not None else False
                            )
                            settlement = lifecycle.apply_fill(
                                f"replay-exit:{order.client_order_id}:{futures_bar.timestamp.isoformat()}",
                                lambda: coordinator.fill(order, price=exit_price,
                                                          timestamp=futures_bar.timestamp.isoformat(),
                                                          cost=_execution_cost(open_trade)),
                            )
                            realized = self._settle_trade(
                                open_trades, shadow_trades, order, futures_bar, None, order.reason,
                                exit_price=exit_price, trade=open_trade,
                            )
                            engine.settle_exit(order.client_order_id, filled=settlement is not None)
                            if settlement is not None:
                                engine.record_exit(strategy_id=order.strategy_id, cell=realized.cell,
                                                   reason=order.reason, entry_bar=realized.entry_bar,
                                                   exit_bar=engine.bars_seen, date=date, vehicle=order.vehicle,
                                                   direction="long" if order.side is OrderSide.SELL else "short",
                                                   quantity=order.quantity)
                            all_events.extend((
                                {"event_type": "EXIT_FILL", "decision_id": order.client_order_id,
                                 "client_order_id": order.client_order_id, "fill_timestamp": futures_bar.timestamp.isoformat(),
                                 "price": exit_price, "quantity": order.quantity, "source": "replay", "session_date": date},
                                {"event_type": "SETTLEMENT", "decision_id": order.client_order_id,
                                  "timestamp": futures_bar.timestamp.isoformat(), "source": "replay", "session_date": date},
                            ))
                            if settlement is not None:
                                settlement_values = settlement if isinstance(settlement, Mapping) else {}
                                ledger_records.append({
                                    "event_type": "EXIT_SETTLEMENT", "kind": "exit_settlement",
                                    "decision_id": order.client_order_id,
                                    "timestamp": futures_bar.timestamp.isoformat(),
                                    "quantity": order.quantity,
                                    "gross_pnl": settlement_values.get("gross_pnl"),
                                    "costs": settlement_values.get("costs"),
                                    "net_pnl": settlement_values.get("net_pnl"),
                                })
                                if had_reservation:
                                    ledger_records.append({
                                        "event_type": "RESERVATION_RELEASE", "kind": "reservation_release",
                                        "decision_id": order.client_order_id,
                                        "entry_order_id": order.entry_order_id,
                                        "timestamp": futures_bar.timestamp.isoformat(),
                                        "quantity": order.quantity,
                                    })
                            engine.on_execution_event(ExecutionNotification(
                                event_type="FILLED", client_order_id=order.client_order_id,
                                status="FILLED", quantity=order.quantity, filled_quantity=order.quantity,
                                price=exit_price, occurred_at=futures_bar.timestamp.isoformat(),
                                observed_at=futures_bar.timestamp.isoformat(),
                                available_at=futures_bar.timestamp.isoformat(), source="replay",
                            ))
                    return {"executed": len(execution_orders)}

                def record_replay_events(_turn) -> Mapping[str, Any]:
                    all_events.extend(replay_exit_events)
                    if result is not None:
                        for event in result.events:
                            if event not in all_events[event_start:]:
                                all_events.append({**event, "source": "replay", "session_date": date})
                    for event in all_events[event_start:]:
                        event_type = str(event.get("event_type", "")).upper()
                        if event_type in _EXECUTION_EVENT_TYPES or event_type in {"EXITDECISION", "EXIT", "CLOSE"}:
                            parity_records["execution_settlement"].append(dict(event))
                    for event in all_events[event_start:]:
                        if is_parity_decision_event(str(event.get("event_type", "")), event):
                            parity_records["decision"].append(dict(event))
                    return {"recorded": len(all_events) - event_start}

                lifecycle.run(
                    turn,
                    {
                        ExecutionPhase.REFERENCE_PUBLICATION: lambda _turn: {},
                        ExecutionPhase.PERMISSION_AND_GAP: lambda _turn: {},
                        ExecutionPhase.INTERACTION_ADVANCEMENT: lambda _turn: {},
                        ExecutionPhase.OPPORTUNITY_TIMEOUT: lambda _turn: {},
                        ExecutionPhase.EXISTING_POSITION_MANAGEMENT: manage_existing,
                        ExecutionPhase.OPPORTUNITY_FORMATION: form_opportunity,
                        ExecutionPhase.PROPOSAL_ARBITRATION: arbitrate,
                        ExecutionPhase.EXECUTION_SETTLEMENT: queue_execution,
                        ExecutionPhase.EVENT_RECORDING: record_replay_events,
                    },
                )
                if not daily_reset_emitted:
                    ledger_records.append({
                        "event_type": "DAILY_RESET",
                        "kind": "daily_reset",
                        "timestamp": futures_bar.timestamp.isoformat(),
                        "date": date,
                        "daily_baseline": portfolio.daily_baseline,
                    })
                    daily_reset_emitted = True
                bars_seen += 1
            for pending in lifecycle.pending_entries.values():
                order = pending.order
                all_events.append({
                    "event_type": "ORDER_UNFILLED",
                    "decision_id": order.lifecycle_decision_id,
                    "vehicle": order.vehicle,
                    "timestamp": pending.available_at or bars[-1].timestamp.isoformat(),
                    "source": "replay",
                    "session_date": date,
                    "reason": "no_next_available_bar",
                })
            # Preserve open positions as mark-to-market/open replay results.
            shadow_trades.extend({**trade, "status": "open"} for trade in open_trades)
            shadow_trades.extend({**trade, "status": "open"} for trade in synthetic_open_trades)
        for records in parity_records.values():
            for event in records:
                event_type = str(event.get("event_type") or event.get("kind") or "event").lower()
                event_key = event.get("client_order_id") or event.get("decision_id") or event.get("date") or "run"
                timestamp = event.get("timestamp") or event.get("occurred_at") or event.get("fill_timestamp") or ""
                event.setdefault("neutral_event_key", f"{event_type}:{event_key}:{timestamp}")
                order_id = event.get("client_order_id") or event.get("decision_id")
                parent = order_parent_ids.get(str(order_id)) if order_id is not None else None
                if parent is not None:
                    event.setdefault("causal_parent_id", parent)
                    event.setdefault("lifecycle_id", parent)
        scores = [int(event["score"]) for event in all_events
                  if isinstance(event.get("score"), (int, float))]
        normalized_trades = [normalize_trade(trade) for trade in shadow_trades]
        summary = {
            **build_summary(all_events, normalized_trades),
            "bars_seen": bars_seen,
            "decisions_scored": len(scores),
        }
        summary = {
            **summary,
            "accepted_decisions": sum(
                1 for event in all_events
                if str(event.get("event_type", "")).upper() == "CANDIDATEDECISION"
                and event.get("outcome") == "accepted"
            ),
            "rejected_decisions": sum(
                1 for event in all_events
                if str(event.get("event_type", "")).upper() == "CANDIDATEDECISION"
                and event.get("outcome") == "rejected"
            ),
            "risk_blocks": sum(
                1 for event in all_events
                if str(event.get("event_type", "")).upper() in {"ORDER_SUPPRESSED", "ENTRY_BLOCKED_RISK"}
            ),
            "closed_trades": sum(1 for trade in normalized_trades if trade.get("status") == "closed"),
        }
        closed_pnls = [float(trade.get("net_pnl_rs") or 0.0) for trade in normalized_trades
                       if trade.get("status") == "closed"]
        summary.update({
            "net_pnl": round(sum(closed_pnls), 4),
            "win_count": sum(1 for pnl in closed_pnls if pnl > 0),
            "loss_count": sum(1 for pnl in closed_pnls if pnl < 0),
            "win_rate": (round(sum(1 for pnl in closed_pnls if pnl > 0) / len(closed_pnls), 4)
                         if closed_pnls else None),
            "pnl_series": closed_pnls,
        })
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
        input_manifest = self.input_manifest(
            session_date, rows=rows, bars=bars, futures_source=futures_source,
        )
        metadata = {
            "api_version": "1.1",
            "strategy_version": last_strategy_metadata.version,
            "configuration_hash": last_strategy_metadata.config_hash,
            "normalized_fingerprint": input_manifest["normalized_fingerprint"],
            "input_provenance": input_manifest["provenance"],
            "input_availability": input_manifest["availability"],
            "selected_source_rows": int(input_manifest["selected_source_rows"]),
            "filtered_source_rows": int(input_manifest["filtered_source_rows"]),
            "completed_source_rows": int(input_manifest["completed_source_rows"]),
            "gap_rows": int(input_manifest["gap_rows"]),
            "processed_decision_bars": int(bars_seen),
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "directional_exposure": directional_exposure,
                "requested_parameters": {
                    "session_date": session_date,
                    "vehicles": list(vehicles),
                    "mode": mode,
                    "include_parity_layers": include_parity_layers,
                },
                "mode": mode,
                "include_parity_layers": include_parity_layers,
            }
        execution_by_decision: dict[Any, list[dict[str, Any]]] = {}
        for item in all_events:
            decision_id = item.get("decision_id")
            if decision_id and str(item.get("event_type", "")) in _EXECUTION_EVENT_TYPES:
                execution_by_decision.setdefault(decision_id, []).append(item)
        normalized_decisions = []
        for event in all_events:
            if not is_parity_decision_event(str(event.get("event_type", "")), event):
                continue
            normalized = normalize_decision(event)
            decision_id = event.get("decision_id")
            execution_events = execution_by_decision.get(decision_id, [])
            normalized["execution"].update({
                "event_types": [str(item.get("event_type")) for item in execution_events],
                "client_order_id": next((item.get("client_order_id") for item in execution_events
                                          if item.get("client_order_id")), None),
            })
            normalized_decisions.append(normalized)
        projection_input = {
            "strategy": {"name": last_strategy_metadata.name, "version": last_strategy_metadata.version,
                         "config_hash": last_strategy_metadata.config_hash},
            "bars_seen": bars_seen,
            "metadata": metadata,
            "events": all_events,
            "decisions": [event for event in all_events if is_parity_decision_event(str(event.get("event_type", "")), event)],
            "decision_trace": normalized_decisions,
            "summary": summary,
            "shadow_trades": normalized_trades,
            "shadow_ledger": ledger_records,
            "execution_scope": mode,
            "ledger_scope": "replay",
        }
        report = {
            "schema_version": 1,
            "report_type": "paper_replay",
            "strategy_id": str(request.get("strategy_id") or ""),
            "session_date": session_date,
            "summary": summary,
            "metrics": summary,
            "extensions": {
                "ftx-paper-research": {
                    "trades": normalized_trades,
                    "ledger": ledger_records,
                },
            },
        }
        parity_artifact = None
        if include_parity_layers and self.parity_projector is not None:
            parity_artifact = self.parity_projector(
                projection_input, request=request, layer_records=parity_records,
            )
        canonical_replay_events = canonical_events(
            all_events, run_id=run_id, execution_scope=cast(ExecutionScope, mode),
        )
        canonical = ReplayResult(
            run_id=run_id,
            schema_version=REPLAY_SCHEMA_VERSION,
            execution_scope=cast(ExecutionScope, mode),
            ledger_scope="replay",
            summary=ReplaySummary(summary),
            events=tuple(canonical_replay_events),
            shadow_trades=tuple(ReplayTrade(item) for item in normalized_trades),
            shadow_ledger=tuple(ReplayLedgerEntry(item) for item in ledger_records),
            parity_artifact=parity_artifact,
            report=report,
        )
        return canonical.to_dict()

    def _execute_orderflow_impl(
        self,
        request: dict[str, Any],
        cancel: Event,
        *,
        session_date: str,
        vehicles: tuple[str, ...],
        include_parity_layers: bool,
    ) -> dict[str, Any]:
        """Replay orderflow through its canonical transition owner.

        Orderflow deliberately does not use the generic replay execution loop:
        that loop owns Paper strategy fills and settlement, which would create a
        second orderflow state machine.  The strategy boundary remains the sole
        owner here; this method only loads bounded input and projects its facts.
        """
        if self.strategy_selector is None:
            raise RuntimeError("replay strategy selector was not configured")
        rows = self.store.read_market_bars(session_date)
        futures_source = self.inputs.resolve_source(session_date, rows=rows)
        bars = self.inputs.bars(session_date, rows=rows, futures_source=futures_source)
        source_futures_bars = tuple(
            bar for bar in bars
            if str(bar.instrument.instrument_type).upper() in {"FUT", "FUTURES"}
        )
        warmup_bars: list[Any] = []
        warmup_dates = [date for date in self.store.read_dates() if date < session_date]
        # Wilder ATR is recursively materialized; retaining only the lookback
        # window would reseed it from a different history than canonical replay.
        for warmup_date in sorted(warmup_dates):
            warmup_rows = self.store.read_market_bars(warmup_date)
            warmup_bars.extend(self.inputs.bars(
                warmup_date,
                rows=warmup_rows,
                futures_source=self.inputs.resolve_source(warmup_date, rows=warmup_rows),
            ))
        aggregator = ReplayInput.aggregator(bars)
        runtime = self.strategy_selector.create(
            "ftx-paper-orderflow",
            StrategyContext(enabled_vehicles=vehicles),
        )
        bind_replay_bars = getattr(runtime, "bind_replay_bars", None)
        if callable(bind_replay_bars):
            bind_replay_bars(bars, historical_bars=tuple(warmup_bars))
        # The selector composes the same PaperOrderflowRuntime used by live
        # hosting.  Replay owns only the bounded clock and capture loop.
        engine = PaperEngine(runtime)
        run_id = str(request.get("run_id") or uuid4().hex)
        events: _ScopedEvents = _ScopedEvents(
            run_id=run_id,
            execution_scope="replay",
            ledger_scope="replay",
        )
        bundles_seen = 0
        for bar in bars:
            if cancel.is_set():
                break
            bundle = aggregator.ingest(bar)
            if bundle is None:
                continue
            bundles_seen += 1
            result = engine.on_bundle(bundle)
            events.extend(
                {"event_type": event.get("event_type", "ORDERFLOW_FACT"), **event}
                for event in result.events
            )

        snapshot = getattr(runtime, "snapshot", lambda: {})()
        input_manifest = self.input_manifest(
            session_date, rows=rows, bars=bars, futures_source=futures_source,
        )
        summary = dict(build_summary(list(events), []))
        current_source = input_manifest["provenance"].get("current")
        storage_identity = (current_source or {}).get("storage_identity") or {}
        source_symbol = str(storage_identity.get("symbol") or "")
        source_expiry = str(storage_identity.get("expiry") or "")
        if source_symbol and source_expiry:
            summary["source"] = f"{source_symbol} FUT {source_expiry}"
        summary.update({
            "bars_seen": bundles_seen,
            "accepted_decisions": sum(1 for event in events if event.get("outcome") == "accepted"),
            "rejected_decisions": sum(1 for event in events if event.get("outcome") == "rejected"),
            "risk_blocks": sum(1 for event in events if str(event.get("event_type", "")).upper() in {"ORDER_SUPPRESSED", "ENTRY_BLOCKED_RISK"}),
            "closed_trades": 0,
        })
        metadata = {
            "api_version": "1.1",
            "strategy_version": runtime.metadata.version,
            "configuration_hash": runtime.metadata.config_hash,
            "normalized_fingerprint": input_manifest["normalized_fingerprint"],
            "input_provenance": input_manifest["provenance"],
            "input_availability": input_manifest["availability"],
            "selected_source_rows": int(input_manifest["selected_source_rows"]),
            "filtered_source_rows": int(input_manifest["filtered_source_rows"]),
            "completed_source_rows": int(input_manifest["completed_source_rows"]),
            "gap_rows": int(input_manifest["gap_rows"]),
            "processed_decision_bars": bundles_seen,
            "mode": "replay",
            "include_parity_layers": include_parity_layers,
        }
        report = {
            "schema_version": 1,
            "report_type": "paper_replay",
            "strategy_id": "ftx-paper-orderflow",
            "session_date": session_date,
            "summary": summary,
            "metrics": summary,
            "extensions": {},
        }
        parity_payload = getattr(runtime, "parity_payload", None)
        if not callable(parity_payload):
            raise RuntimeError("orderflow runtime does not expose canonical parity facts")
        orderflow_payload = dict(cast(dict[str, Any], parity_payload(rows)))
        orderflow_payload["records"] = list(events)
        orderflow_payload["replay_records"] = _source_replay_records(
            source_futures_bars, session_date,
        )
        orderflow_payload["session_date"] = session_date
        orderflow_payload["source_bar_count"] = len(source_futures_bars)
        report["extensions"]["ftx-paper-orderflow"] = {
            "orderflow_payload": orderflow_payload,
            "facts": orderflow_payload.get("records", []),
        }
        parity_artifact = None
        if include_parity_layers:
            from ftx_paper.strategies.orderflow.parity_artifacts import build_parity_artifact_from_payload

            try:
                parity_artifact = build_parity_artifact_from_payload(
                    orderflow_payload,
                    run_id=run_id,
                    producer={"name": runtime.metadata.name, "version": runtime.metadata.version},
                )
            except ValueError as exc:
                # The replay payload remains useful for phase diagnostics even
                # when final parity layers cannot be published yet.
                report["extensions"]["ftx-paper-orderflow"]["parity_artifact_unavailable"] = str(exc)
        canonical = ReplayResult(
            run_id=run_id,
            schema_version=REPLAY_SCHEMA_VERSION,
            execution_scope="replay",
            ledger_scope="replay",
            summary=ReplaySummary(summary),
            events=tuple(canonical_events(list(events), run_id=run_id, execution_scope="replay")),
            shadow_trades=(),
            shadow_ledger=(),
            parity_artifact=parity_artifact,
            report={
                **report,
                "extensions": {
                    **report["extensions"],
                    "ftx-paper-orderflow": {
                        **report["extensions"]["ftx-paper-orderflow"],
                        "metadata": metadata,
                        "strategy_snapshot": snapshot,
                    },
                },
            },
        )
        return canonical.to_dict()

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
        entry_bar = trade.get("entry_bar", 0)
        if not isinstance(entry_bar, int) or isinstance(entry_bar, bool):
            raise TypeError("settled trade entry_bar must be an integer")
        return TradeSettlement(cell, entry_bar, len(trades), net_pnl)

__all__ = ["ReplayWorker"]
