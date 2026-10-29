from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
import hashlib
import json
from queue import Empty, Full, Queue
from threading import Event, Lock, Thread, current_thread
from collections.abc import Mapping, Sequence
from typing import Any, cast
from uuid import uuid4

from ftx_paper.contracts import Instrument, MarketBar, MarketRole, OptionRole, OrderRole, OrderSide, Role, SyntheticFutureQuote, build_decision_view, synthetic_future_quote, validate_decision_view
from ftx_paper.contracts.replay import ExecutionScope, LEGACY_REPLAY_FIELDS, ReplayEvent, ReplayLedgerEntry, ReplayRequest, ReplayResult, ReplaySummary, ReplayTrade, REPLAY_SCHEMA_VERSION
from ftx_paper.domain.capital import ResearchCapitalProfile, RESEARCH_CAPITAL_PROFILE, CapitalRuntimeContext
from ftx_paper.market import (
    AggregatorConfig, Cell, CompletedBarAggregator, FeatureService, InstrumentKey,
    FeatureSnapshot, ORDERFLOW_DAILY_ATR_FEATURE_SET,
)
from ftx_paper.market.requirements import FeatureRequirement
from ftx_paper.domain import AccountAggregate, PortfolioState
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.runtime.engine import PaperEngine
from ftx_paper.runtime.decision_turn import DecisionTurn, evaluate_turn
from ftx_paper.runtime.orderflow_pipeline import ingest_orderflow_bar
from ftx_paper.runtime.events import is_decision_event
from ftx_paper.strategy.orderflow_contract import feature_values_to_dict, stable_orderflow_id
from ftx_paper.strategy.orderflow_observation import observe_market
from ftx_paper.strategy.orderflow_producer import CausalOrderflowProducer
from ftx_paper.strategy.orderflow_replay import ReplayMachine
from ftx_paper.strategy.orderflow_source import prepare_replay_input
from ftx_paper.runtime.futures_source import (
    FuturesSessionSourceResolver, materialize_orderflow_daily_atr, virtual_continuous_bar,
)
from ftx_paper.runtime.parity_artifacts import (
    NORMALIZED_BAR_FINGERPRINT_RECIPE,
    build_parity_artifact,
    normalized_bar_fingerprint,
)
from ftx_paper.runtime.state import ReplayRuntimeState
from ftx_paper.execution.lifecycle import (
    ExecutionLifecycleCoordinator,
    ExecutionPhase,
    ExecutionTurn,
)
from ftx_paper.execution.cost import futures_cost, synthetic_futures_cost
from ftx_paper.execution import PaperExecutionCoordinator
from ftx_paper.execution.settlement import ExitValidationError, validate_exit_order
from ftx_paper.strategy.exits import ExitAction
from ftx_paper.strategy import ConfiguredStrategyFactory, StrategyFactory
from ftx_paper.strategy.orderflow import OrderflowConfig
from ftx_paper.strategy.orderflow_reporting import detached_replay_records
from ftx_paper.strategy.policy_contract import ExecutionMode
from ftx_paper.strategy.config import (
    AFTERNOON_ENTRY_MINUTES,
    MORNING_ENTRY_MINUTES,
)
from ftx_paper.config import (
    IST,
    NIFTY_LOT_SIZE,
    SESSION_DATA_END,
    SESSION_DATA_END_MINUTES,
    SESSION_OPEN_MINUTES,
)


CANONICAL_INTERACTION_LOCATIONS = frozenset({
    "OR_HIGH", "OR_LOW", "SESSION_HIGH", "SESSION_LOW",
})

_EXECUTION_EVENT_TYPES = frozenset({
    "ORDER_INTENT", "ORDER_AUTHORIZED", "ORDER_ACK", "FILL",
    "ORDER_UNFILLED", "ORDER_SUPPRESSED", "RISK_REJECTED",
    "RESERVATION", "AUTHORIZATION", "SUBMISSION", "PENDING_EXIT",
    "EXIT_FILL", "CANCELLATION", "SETTLEMENT",
})
REPLAY_SUMMARY_SCHEMA_VERSION = 1


def _timestamp_string(value: Any) -> str:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _strip_legacy_fields(value: Any) -> Any:
    if isinstance(value, Mapping):
        legacy = {str(key) for key in value if str(key) in LEGACY_REPLAY_FIELDS}
        if legacy:
            raise ValueError("legacy replay fields are not accepted: " + ", ".join(sorted(legacy)))
        return {
            str(key): _strip_legacy_fields(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_strip_legacy_fields(item) for item in value]
    return value


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


def _available_orderflow_trigger_events(
    bundle: Any,
    as_of: datetime,
    emitted_ids: set[str],
) -> list[dict[str, Any]]:
    """Project completed 1m evidence at the replay decision boundary."""
    context = getattr(bundle, "orderflow_context", None)
    if context is None:
        return []
    events: list[dict[str, Any]] = []
    for observation in context.observations_for("1m"):
        if observation.available_at > as_of or observation.observation_id in emitted_ids:
            continue
        emitted_ids.add(observation.observation_id)
        events.append({
            "event_type": "POLICY_DECISION",
            "strategy_id": "ftx-paper-orderflow",
            "strategy_name": "ftx-paper-orderflow",
            "decision_id": stable_orderflow_id("trigger", observation.observation_id),
            "decision_at": observation.available_at.isoformat(),
            "available_at": observation.available_at.isoformat(),
            "observed_at": observation.observed_at.isoformat(),
            "outcome": "recorded",
            "reason": "trigger_observation",
            "decision_stage": "trigger",
            "timeframe": "1m",
            "source_observation_id": observation.source_observation_id,
            "source_observation_ids": [observation.observation_id],
            "feature_values": feature_values_to_dict(observation.features),
            "replay_provenance": dict(observation.context.replay_provenance) if observation.context else {},
            "session_date": observation.context.session_id if observation.context else None,
        })
    return events


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
        self.lifecycle = ExecutionLifecycleCoordinator()

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

    def execute_detached(self, request: dict[str, Any]) -> dict[str, Any]:
        """Run an isolated replay without creating or updating a stored run."""
        request = ReplayRequest.from_mapping(request).to_dict()
        return self._execute({**request, "run_id": request.get("run_id") or uuid4().hex}, Event())

    @staticmethod
    def compare_paired_replay(
        diagnostic: Mapping[str, Any], executable: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Compare the complete Paper projection without comparing object identity."""
        def decisions(result: Mapping[str, Any]) -> dict[str, tuple[Any, ...]]:
            values: dict[str, tuple[Any, ...]] = {}
            for event in result.get("events", ()):
                if not isinstance(event, Mapping) or not event.get("decision_id"):
                    continue
                decision_id = str(event["decision_id"])
                values[decision_id] = tuple(
                    event.get(field)
                    for field in (
                        "decision_stage", "outcome", "reason", "response_state",
                        "response_evidence_ids", "source_observation_ids",
                        "observed_at", "available_at", "decision_at",
                    )
                )
            return values

        diagnostic_decisions = decisions(diagnostic)
        executable_decisions = decisions(executable)
        mismatches = [
            {
                "decision_id": decision_id,
                "diagnostic": diagnostic_decisions.get(decision_id),
                "executable": executable_decisions.get(decision_id),
            }
            for decision_id in sorted(set(diagnostic_decisions) | set(executable_decisions))
            if diagnostic_decisions.get(decision_id) != executable_decisions.get(decision_id)
        ]
        def lifecycle(result: Mapping[str, Any]) -> tuple[tuple[Any, ...], ...]:
            values: list[tuple[Any, ...]] = []
            for event in result.get("events", ()):
                if not isinstance(event, Mapping):
                    continue
                event_type = str(event.get("event_type", "")).upper()
                if event_type not in _EXECUTION_EVENT_TYPES and event_type not in {"EXIT", "CLOSE"}:
                    continue
                values.append(tuple(event.get(field) for field in (
                    "event_type", "decision_id", "client_order_id", "entry_decision_id",
                    "outcome", "reason", "status", "quantity", "price", "fill_timestamp",
                    "timestamp", "available_at", "expected_available_at", "reservation_amount",
                    "gross_pnl", "net_pnl", "costs",
                )))
            return tuple(values)

        def projection(result: Mapping[str, Any], name: str) -> tuple[Any, ...]:
            values = result.get(name, ())
            if not isinstance(values, (list, tuple)):
                return (values,)
            return tuple(_strip_legacy_fields(item) for item in values)

        diagnostic_mutations = bool(
            diagnostic.get("authoritative_trades") or diagnostic.get("authoritative_ledger")
        )
        lifecycle_mismatches = []
        diagnostic_lifecycle, executable_lifecycle = lifecycle(diagnostic), lifecycle(executable)
        if diagnostic_lifecycle != executable_lifecycle:
            lifecycle_mismatches.append({
                "diagnostic": diagnostic_lifecycle,
                "executable": executable_lifecycle,
            })
        projection_mismatches = []
        compare_projections = "execution_scope" in diagnostic and "execution_scope" in executable
        for name in ("shadow_trades", "shadow_ledger", "authoritative_trades", "authoritative_ledger"):
            left, right = projection(diagnostic, name), projection(executable, name)
            if (compare_projections or name.startswith("authoritative_")) and left != right:
                projection_mismatches.append({"projection": name, "diagnostic": left, "executable": right})
        diagnostic_projections = {
            name: projection(diagnostic, name)
            for name in ("shadow_trades", "shadow_ledger", "authoritative_trades", "authoritative_ledger")
            if projection(diagnostic, name)
        }
        result = {
            "matched": not mismatches and not diagnostic_mutations and not diagnostic_lifecycle and not diagnostic_projections,
            "mismatches": mismatches,
            "diagnostic_mutations": diagnostic_mutations,
        }
        # Executable lifecycle and accounting facts are intentionally absent
        # from diagnostic replay. Keep them visible for reporting, but do not
        # treat their expected presence on only the executable side as a
        # decision mismatch.
        if lifecycle_mismatches:
            result["executable_lifecycle"] = executable_lifecycle
        if projection_mismatches:
            result["executable_projections"] = {
                name: projection(executable, name)
                for name in ("shadow_trades", "shadow_ledger", "authoritative_trades", "authoritative_ledger")
                if projection(executable, name)
            }
        if diagnostic_lifecycle or diagnostic_projections:
            result["unexpected_diagnostic_execution"] = {
                "lifecycle": diagnostic_lifecycle,
                "projections": diagnostic_projections,
            }
        return result

    def input_manifest(
        self,
        session_date: str,
        *,
        rows: list[dict[str, Any]] | None = None,
        bars: tuple[MarketBar, ...] | None = None,
        futures_source: Any | None = None,
    ) -> dict[str, Any]:
        """Return the deterministic, API-safe inputs used by a replay date."""
        loaded_rows = rows if rows is not None else self.store.read_market_bars(session_date)
        futures_source = futures_source if futures_source is not None else self._resolve_source(session_date)
        # Reuse the rows already loaded above.  A manifest is metadata-only,
        # but _bars still applies the canonical contract and session filters.
        if bars is None:
            try:
                bars = self._bars(session_date, rows=loaded_rows, futures_source=futures_source)
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
        selected_futures_rows = [
            row for row in strategy_rows
            if str(row.get("instrument_type", "")).upper() in {"FUT", "FUTURES"}
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
            str(row.get("minute")) for row in loaded_rows
            if str(row.get("instrument_type", "")).upper() in {"FUT", "FUTURES"}
        }
        source_provenance = {
            "current": futures_source.provenance() if futures_source is not None else None,
            "prior": self._prior_source_provenance(session_date),
        }
        completed_source_rows = len({
            str(row.get("minute")) for row in replay_rows
            if str(row.get("instrument_type", "")).upper() in {"FUT", "FUTURES"}
        })
        current_source = source_provenance["current"]
        selected_source_rows = int(
            current_source["source_bar_count"]
            if current_source is not None and "source_bar_count" in current_source
            else completed_source_rows
        )
        expected_source_rows = SESSION_DATA_END_MINUTES - SESSION_OPEN_MINUTES
        payload = {
            "available_date": session_date,
            "date": session_date,
            "session_date": session_date,
            "bar_count": len(strategy_rows),
            "raw_bar_count": len(loaded_rows),
            "supporting_bar_count": len(bars) - len(strategy_rows),
            "selected_source_rows": selected_source_rows,
            "filtered_source_rows": max(0, selected_source_rows - completed_source_rows),
            "completed_source_rows": completed_source_rows,
            "gap_rows": max(0, expected_source_rows - completed_source_rows),
            "futures_source": futures_source.provenance() if futures_source is not None else None,
            "provenance": source_provenance,
            "per_instrument_counts": dict(sorted(by_instrument.items())),
            "availability": {
                "vix": bool(vix_rows),
                "pcr": bool(option_rows and option_minutes.intersection(futures_minutes)) or None,
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
                "data_end": SESSION_DATA_END,
                "morning_entry_minutes": list(MORNING_ENTRY_MINUTES),
                "afternoon_entry_minutes": list(AFTERNOON_ENTRY_MINUTES),
                "aggregator": {"required_roles": ["futures"], "deadline_seconds": 0},
            },
        }
        # The parity input identity is a versioned hash of normalized selected
        # futures bars. Contract labels remain provenance and do not alter it.
        payload["fingerprint_recipe"] = NORMALIZED_BAR_FINGERPRINT_RECIPE
        payload["normalized_fingerprint"] = normalized_bar_fingerprint(selected_futures_rows)
        if isinstance(current_source, Mapping):
            source_provenance["current"] = {
                **current_source,
                "normalized_fingerprint": payload["normalized_fingerprint"],
            }
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
        self.lifecycle = ExecutionLifecycleCoordinator()
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
        emit_rejected_decisions = request.get("emit_rejected_decisions", False)
        if not isinstance(emit_rejected_decisions, bool):
            raise ValueError("emit_rejected_decisions must be a boolean")
        mode_value = request.get("mode", "replay")
        if not isinstance(mode_value, str):
            raise ValueError("mode must be 'replay'")
        mode = mode_value.lower()
        if mode != "replay":
            raise ValueError("mode must be 'replay'")
        include_parity_layers = request.get("include_parity_layers", False)
        if not isinstance(include_parity_layers, bool):
            raise ValueError("include_parity_layers must be a boolean")
        dates = [session_date]
        run_id = str(request.get("run_id") or uuid4().hex)
        all_events = _ScopedEvents(
            run_id=run_id,
            execution_scope=cast(ExecutionScope, mode),
            ledger_scope="replay",
        )
        parity_records: dict[str, list[dict[str, Any]]] = {
            "decision": [], "execution_settlement": [], "portfolio_ledger": [],
        }
        ledger_records = parity_records["portfolio_ledger"]
        emitted_orderflow_trigger_ids: set[str] = set()
        order_parent_ids: dict[str, str] = {}
        unavailable_fields: set[str] = set()
        shadow_trades: list[dict[str, Any]] = []
        bars_seen = 0
        causal_bars_seen = 0
        initial_capital = self.capital_profile.initial_capital
        current_equity = initial_capital
        peak_equity = initial_capital
        max_drawdown = 0.0
        last_portfolio: PortfolioState | None = None
        last_strategy_metadata = None
        runtime_replay_records: list[dict[str, Any]] = []
        atr_artifacts: list[dict[str, Any]] = []
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
            rows = self.store.read_market_bars(date)
            futures_source = self._resolve_source(date)
            bars = tuple(
                virtual_continuous_bar(bar)
                for bar in self._bars(date, rows=rows, futures_source=futures_source)
            )
            futures_bars = tuple(
                bar for bar in bars
                if str(bar.instrument.instrument_type).upper() in {"FUT", "FUTURES"}
            )
            replay_machine = None
            if futures_bars:
                replay_input = prepare_replay_input(
                    futures_bars,
                    date,
                    session_partition="09:15-15:30",
                    selection_rule="continuous_identity",
                )
                replay_machine = ReplayMachine(
                    observe_market(replay_input),
                    capture_mode=request.get("capture_mode", "full"),
                )
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
            replay_state = ReplayRuntimeState(
                portfolio=portfolio,
                capital_context=capital_context,
                engine=None,
                shadow_trades=shadow_trades,
                replay_ledger=ledger_records,
                parity_records=parity_records,
            )
            strategy_kwargs = {}
            strategy_id = request.get("strategy_id")
            if strategy_id is not None:
                strategy_kwargs["strategy"] = strategy_id
            if strategy_id == "ftx-paper-orderflow":
                strategy_kwargs["config"] = OrderflowConfig(
                    enabled_vehicles=tuple(vehicles),
                    execution_mode=(
                        ExecutionMode.EXECUTABLE
                        if mode == "replay" else ExecutionMode.DIAGNOSTIC
                    ),
                )
            strategy = self.strategy_factory.create(
                **strategy_kwargs,
                enabled_vehicles=vehicles,
                portfolio=replay_state.portfolio,
                capital_context=replay_state.capital_context,
                prior_day_high=prior_day_high,
                prior_day_low=prior_day_low,
                expiry_dates=frozenset(effective_expiry_dates),
            )
            last_portfolio = portfolio
            last_strategy_metadata = strategy.metadata
            coordinator = PaperExecutionCoordinator(replay_state.portfolio, replay_state.capital_context)
            account = AccountAggregate(replay_state.portfolio, replay_state.capital_context)
            engine = PaperEngine(
                strategy, emit_rejected_decisions=emit_rejected_decisions,
            )
            replay_state.engine = engine
            replay_state.coordinator = coordinator
            replay_state.account = account
            portfolio = replay_state.portfolio
            capital_context = replay_state.capital_context
            last_portfolio = replay_state.portfolio
            # Each date gets a fresh position/risk session while cumulative
            # capital is carried by the authoritative PortfolioState.
            native_orderflow = strategy.metadata.name == "ftx-paper-orderflow"
            orderflow_producer = (
                CausalOrderflowProducer()
                if native_orderflow else None
            )
            if orderflow_producer is not None:
                orderflow_producer.set_prior_value_bounds(prior_day_high, prior_day_low)
                orderflow_producer.set_frozen_atr(self._prior_daily_atr(date, available_dates))
                atr_artifact = self._orderflow_atr_artifact(date)
                if atr_artifact is not None:
                    atr_artifacts.append(atr_artifact)
                prior_dates = [item for item in available_dates if item < date]
                if prior_dates:
                    prior_date = max(prior_dates)
                    prior_rows = self.store.read_market_bars(prior_date)
                    prior_source = self._resolve_source(prior_date)
                    prior_bars = self._bars(
                        prior_date, rows=prior_rows, futures_source=prior_source,
                    )
                    if prior_bars:
                        orderflow_producer.set_prior_bars(prior_bars)
                        orderflow_producer.set_prior_close(prior_bars[-1].close)
            aggregator = self._aggregator(
                bars,
                orderflow_producer,
                tuple(getattr(getattr(strategy, "policy", None), "required_features", ())),
            )
            feature_service = FeatureService(None)
            open_trades: list[dict[str, Any]] = []
            daily_reset_emitted = False
            # Synthetic is a reporting-only view of the accepted futures
            # decision.  Keep its diagnostic lifecycle separate so it cannot
            # consume risk, margin, cooldown, or execution state.
            synthetic_open_trades: list[dict[str, Any]] = []
            last_quotes: dict[str, SyntheticFutureQuote] = {}
            self.lifecycle.pending_entries.clear()

            next_available_at = {
                item.timestamp: (
                    futures_bars[index + 1].timestamp
                    if index + 1 < len(futures_bars) else item.timestamp
                )
                for index, item in enumerate(futures_bars)
            }

            def replay_state_snapshot() -> dict[str, Any]:
                strategy_snapshot = getattr(engine.strategy, "snapshot", None)
                raw_strategy_state = strategy_snapshot() if callable(strategy_snapshot) else {}

                def scrub_legacy_fields(value: Any) -> Any:
                    if isinstance(value, Mapping):
                        return {
                            key: scrub_legacy_fields(item)
                            for key, item in value.items()
                            if key != "execution_mode"
                        }
                    if isinstance(value, tuple):
                        return tuple(scrub_legacy_fields(item) for item in value)
                    if isinstance(value, list):
                        return [scrub_legacy_fields(item) for item in value]
                    return value

                strategy_state = scrub_legacy_fields(raw_strategy_state)
                if not isinstance(strategy_state, Mapping):
                    strategy_state = {}
                return {
                    "engine": {"bars_seen": engine.bars_seen},
                    "portfolio": portfolio.snapshot(),
                    "strategy": strategy_state,
                }

            for bar in bars:
                if cancel.is_set():
                    break
                if orderflow_producer is not None:
                    # The producer ignores supporting instruments while the
                    # shared pipeline keeps feature and bundle ordering equal
                    # to live processing.
                    _, bundle = ingest_orderflow_bar(
                        bar,
                        producer=orderflow_producer,
                        feature_service=feature_service,
                        aggregator=aggregator,
                    )
                else:
                    feature_service.ingest(bar)
                    bundle = aggregator.ingest(bar)
                if bundle is None:
                    continue
                ready_bundle = bundle
                event_start = len(all_events)
                futures_bar = bundle.bars[MarketRole.FUTURES]
                before_state = replay_state_snapshot()
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
                bundle = feature_service.enrich(
                    bundle,
                    prior_day_high=prior_day_high,
                    prior_day_low=prior_day_low,
                    expiry_dates=tuple(str(item) for item in effective_expiry_dates),
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
                for trade in open_trades:
                    if trade.get("vehicle") == "futures":
                        continue
                    quote = synthetic_future_quote(
                        quote_selection_bar, option_bars,
                        symbols=(trade["ce_symbol"], trade["pe_symbol"]),
                    )
                    if quote is not None:
                        last_quotes[trade["entry_order_id"]] = quote
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
                        settlement = self.lifecycle.apply_fill(
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
                pending_candidates = self.lifecycle.due_entries(futures_bar.timestamp.isoformat())
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
                    selected.extend(coordinator.arbitrate(candidate_orders))
                    return {"accepted": len(selected)}

                def queue_execution(_turn) -> Mapping[str, Any]:
                    execution_orders.extend(selected)
                    for order in tuple(execution_orders):
                        order_parent_ids[order.client_order_id] = (
                            order.lifecycle_id or order.client_order_id.rsplit(":", 1)[0]
                        )
                        if order.role is OrderRole.ENTRY:
                            if order.client_order_id not in {item.client_order_id for item in pending_candidates}:
                                self.lifecycle.defer_entry(
                                    order, available_at=futures_bar.timestamp.isoformat(),
                                    reason="next_observed_bar",
                                )
                                continue
                            if order.setup_availability is not None and futures_bar.timestamp.isoformat() <= order.setup_availability:
                                self.lifecycle.defer_entry(
                                    order, available_at=futures_bar.timestamp.isoformat(),
                                    reason="setup_not_yet_available",
                                )
                                continue
                            try:
                                account.authorize_and_reserve(order)
                            except ValueError as exc:
                                all_events.append({"event_type": "RISK_REJECTED", "decision_id": order.client_order_id,
                                                   "reason": str(exc), "timestamp": futures_bar.timestamp.isoformat(),
                                                   "source": "replay", "session_date": date})
                                continue
                            entry_quote = synthetic_future_quote(
                                quote_selection_bar, option_bars,
                                symbols=(order.synthetic_legs[0].symbol, order.synthetic_legs[1].symbol)
                                if order.synthetic_legs is not None else None,
                            ) if order.vehicle == "synthetic" else None
                            if order.vehicle == "synthetic" and entry_quote is None:
                                if order.vehicle == "futures":
                                    coordinator.cancel(order)
                                all_events.append({"event_type": "ORDER_UNFILLED", "decision_id": order.client_order_id,
                                                   "reason": "missing_synthetic_future_quote", "source": "replay",
                                                   "session_date": date})
                                continue
                            fill_price = entry_quote.price if entry_quote is not None else futures_bar.open
                            fill_id = f"replay-entry:{order.client_order_id}:{futures_bar.timestamp.isoformat()}"
                            self.lifecycle.apply_fill(fill_id, lambda: coordinator.fill(
                                order, price=fill_price, timestamp=futures_bar.timestamp.isoformat(), fill_id=fill_id,
                            ))
                            engine.register_entry(order, fill_price=fill_price,
                                                  entry_fill_time=futures_bar.timestamp,
                                                  reference_price=futures_bar.close)
                            all_events.extend((
                                {"event_type": "RESERVATION", "decision_id": order.client_order_id,
                                 "client_order_id": order.client_order_id, "quantity": order.quantity,
                                 "timestamp": futures_bar.timestamp.isoformat(), "source": "replay", "session_date": date},
                                {"event_type": "FILL", "decision_id": order.client_order_id,
                                 "client_order_id": order.client_order_id, "fill_id": fill_id,
                                 "fill_timestamp": futures_bar.timestamp.isoformat(), "price": fill_price,
                                 "quantity": order.quantity, "vehicle": order.vehicle,
                                  "source": "replay", "session_date": date},
                            ))
                            if order.vehicle == "futures":
                                ledger_records.extend((
                                    {"event_type": "RESERVATION", "kind": "reservation",
                                     "decision_id": order.client_order_id,
                                     "timestamp": futures_bar.timestamp.isoformat(),
                                     "quantity": order.quantity, "vehicle": order.vehicle},
                                    {"event_type": "AUTHORIZATION", "kind": "authorization",
                                     "decision_id": order.client_order_id,
                                     "timestamp": futures_bar.timestamp.isoformat(),
                                     "quantity": order.quantity, "vehicle": order.vehicle},
                                ))
                            ledger_records.append({
                                "event_type": "FILL", "kind": "fill",
                                "decision_id": order.client_order_id,
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
                                                "stop_price": order.stop_price, "cell": order.cell,
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
                            settlement = self.lifecycle.apply_fill(
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
                                engine.record_exit(strategy_id=order.strategy_id, cell=realized.cell.name,
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
                    for event in result.events if result is not None else ():
                        if is_decision_event(str(event.get("event_type", "")), event):
                            parity_records["decision"].append(dict(event))
                    return {"recorded": len(all_events) - event_start}

                trigger_events = () if native_orderflow else _available_orderflow_trigger_events(
                    bundle, futures_bar.timestamp, emitted_orderflow_trigger_ids,
                )
                all_events.extend(trigger_events)
                phase_events = self.lifecycle.run(
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
                if replay_machine is not None and str(futures_bar.instrument.instrument_type).upper() in {"FUT", "FUTURES"}:
                    replay_machine.process_bar(
                        futures_bar.timestamp,
                        available_at=next_available_at.get(
                            futures_bar.timestamp, futures_bar.timestamp,
                        ),
                        before_state=before_state,
                        after_state=replay_state_snapshot(),
                        decisions=tuple(
                            str(event.get("decision_id"))
                            for event in all_events[event_start:]
                            if event.get("decision_id") is not None
                        ),
                        events=tuple(
                            dict(event) for event in all_events[event_start:]
                        ),
                        phase_results=tuple({
                            "phase": event.phase,
                            "sequence": event.sequence,
                            "payload": dict(event.payload),
                        } for event in phase_events),
                    )
                    runtime_replay_records.append(
                        replay_machine.recorder.records[-1].to_dict()
                    )
                if mode == "replay" and not daily_reset_emitted:
                    ledger_records.append({
                        "event_type": "DAILY_RESET",
                        "kind": "daily_reset",
                        "timestamp": futures_bar.timestamp.isoformat(),
                        "date": date,
                        "daily_baseline": portfolio.daily_baseline,
                    })
                    daily_reset_emitted = True
                bars_seen += 1
                # Scope parity by evidence actually available to orderflow,
                # rather than by every futures-clocked bundle.  A source row
                # carrying a gap status is retained for diagnostics but is not
                # a processed causal decision bar.
                orderflow_context = bundle.orderflow_context
                if orderflow_context is not None:
                    current_observation = next(
                        (
                            item for item in reversed(orderflow_context.observations_for("1m"))
                            if item.observed_at == futures_bar.timestamp
                        ),
                        None,
                    )
                    if (
                        current_observation is not None
                        and current_observation.context is not None
                        and current_observation.context.replay_provenance.get("gap_status") == "none"
                    ):
                        causal_bars_seen += 1
                result_events = ()
                due_entry_ids = {item.client_order_id for item in pending_candidates}
                # Phase 8 owns execution; this local projection contains no
                # executable orders and is retained only for denial metadata.
                phase8_orders: tuple[Any, ...] = ()
                selected_ids = {item.client_order_id for item in phase8_orders}
                for denied in candidate_orders:
                    if denied.client_order_id in selected_ids:
                        continue
                    all_events.append({
                        "event_type": "ORDER_DENIED",
                        "decision_id": denied.client_order_id,
                        "client_order_id": denied.client_order_id,
                        "strategy_id": denied.strategy_id,
                        "account_id": denied.account_id,
                        "reason": coordinator.denied_proposals.get(
                            denied.client_order_id, "proposal_not_selected",
                        ),
                        "decision_at": futures_bar.timestamp.isoformat(),
                        "source": "replay",
                        "session_date": date,
                    })
                for order in phase8_orders:
                    order_parent_ids[order.client_order_id] = (
                        order.lifecycle_id or order.client_order_id.rsplit(":", 1)[0]
                    )
                    if order.client_order_id not in due_entry_ids:
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
                        if order.client_order_id not in due_entry_ids:
                            self.lifecycle.defer_entry(
                                order, available_at=futures_bar.timestamp.isoformat(),
                                reason="next_observed_bar",
                            )
                            continue
                        if order.setup_availability is not None and futures_bar.timestamp.isoformat() <= order.setup_availability:
                            self.lifecycle.defer_entry(
                                order, available_at=futures_bar.timestamp.isoformat(),
                                reason="setup_not_yet_available",
                            )
                            continue
                        entry_bar = futures_bar
                        entry_timestamp = entry_bar.timestamp.isoformat()
                        if order.vehicle == "futures":
                            try:
                                account.authorize_and_reserve(order)
                            except ValueError as exc:
                                all_events.append({"event_type": "RISK_REJECTED",
                                                    "decision_id": order.client_order_id,
                                                    "reason": str(exc), "timestamp": futures_bar.timestamp.isoformat(),
                                                    "source": "replay", "session_date": date})
                                all_events.append({"event_type": "ORDER_SUPPRESSED", "decision_id": order.client_order_id,
                                                   "vehicle": order.vehicle, "timestamp": futures_bar.timestamp.isoformat(),
                                                   "source": "replay", "session_date": date, "execution_allowed": False,
                                                   "reason": str(exc)})
                                continue
                            reservation_event = {"event_type": "RESERVATION", "decision_id": order.client_order_id,
                                                 "client_order_id": order.client_order_id,
                                                 "timestamp": futures_bar.timestamp.isoformat(),
                                                 "quantity": order.quantity, "vehicle": order.vehicle,
                                                 "source": "replay", "session_date": date}
                            all_events.append(reservation_event)
                            ledger_records.append({**reservation_event, "kind": "reservation"})
                            all_events.append({"event_type": "ORDER_AUTHORIZED",
                                               "decision_id": order.client_order_id,
                                               "account_revision": account.revision,
                                               "timestamp": futures_bar.timestamp.isoformat(),
                                                 "source": "replay", "session_date": date})
                            ledger_records.append({
                                "event_type": "AUTHORIZATION", "kind": "authorization",
                                "decision_id": order.client_order_id,
                                "timestamp": futures_bar.timestamp.isoformat(),
                                "quantity": order.quantity, "vehicle": order.vehicle,
                            })
                        all_events.append({"event_type": "SUBMISSION", "decision_id": order.client_order_id,
                                           "client_order_id": order.client_order_id,
                                           "timestamp": futures_bar.timestamp.isoformat(),
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
                            ledger_records.append({"event_type": "CANCELLATION", "decision_id": order.client_order_id,
                                                   "timestamp": futures_bar.timestamp.isoformat(), "reason": "missing_synthetic_future_quote"})
                            continue
                        requested_fill = (
                            float(order.limit_price)
                            if order.limit_price is not None else entry_bar.open
                        )
                        fill_price = entry_quote.price if entry_quote is not None else (
                            max(requested_fill, entry_bar.open)
                            if order.side is OrderSide.BUY
                            else min(requested_fill, entry_bar.open)
                        )
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
                                            "fill_timestamp": entry_timestamp,
                                           "price": fill_price, "quantity": order.quantity,
                                           "vehicle": order.vehicle, "source": "replay",
                                             "session_date": date})
                        ledger_records.append({"event_type": "FILL", "decision_id": order.client_order_id,
                                                "timestamp": entry_timestamp,
                                               "quantity": order.quantity, "price": fill_price})
                        engine.register_entry(order, fill_price=fill_price,
                                               entry_fill_time=entry_bar.timestamp,
                                               reference_price=entry_bar.close)
                        if order.vehicle == "futures":
                            self.lifecycle.apply_fill(
                                order.client_order_id,
                                lambda: coordinator.fill(
                                    order, price=fill_price,
                                    timestamp=entry_timestamp,
                                ),
                            )
                        engine.on_execution_event(ExecutionNotification(
                            event_type="FILLED", client_order_id=order.client_order_id,
                            status="FILLED", quantity=order.quantity,
                            filled_quantity=order.quantity, price=fill_price,
                            occurred_at=entry_timestamp,
                            observed_at=entry_timestamp,
                            available_at=entry_timestamp, source="replay",
                        ))
                        open_trades.append({"entry_order_id": order.client_order_id,
                                            "instrument": order.instrument.symbol,
                                            "side": order.side.value, "quantity": order.quantity,
                                             "entry_timestamp": entry_timestamp,
                                            "entry_price": fill_price, "vehicle": order.vehicle,
                                            "stop_price": order.stop_price,
                                            "initial_risk_points": abs(fill_price - order.stop_price)
                                            if order.stop_price is not None else None,
                                            "initial_risk_rs": abs(fill_price - order.stop_price)
                                            * NIFTY_LOT_SIZE * order.quantity
                                            if order.stop_price is not None else None,
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
                        all_events.append({"event_type": "PENDING_EXIT", "decision_id": order.client_order_id,
                                           "client_order_id": order.client_order_id,
                                           "timestamp": futures_bar.timestamp.isoformat(),
                                           "source": "replay", "session_date": date})
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
                        realized = self._settle_trade(open_trades, shadow_trades, order, futures_bar,
                                                      exit_quote, order.reason,
                                                      exit_price=exit_quote.price if exit_quote is not None else futures_bar.close,
                                                      trade=open_trade)
                        settlement = None
                        had_reservation = (
                            order.entry_order_id in portfolio.reservations
                            if order.entry_order_id is not None else False
                        )
                        if order.vehicle == "futures":
                            settlement = self.lifecycle.apply_fill(
                                order.client_order_id,
                                lambda: coordinator.fill(
                                    order,
                                    price=exit_quote.price if exit_quote is not None else futures_bar.close,
                                    timestamp=futures_bar.timestamp.isoformat(),
                                    cost=_execution_cost(open_trade),
                                ),
                            )
                        settlement_values = settlement if isinstance(settlement, Mapping) else {}
                        exit_timestamp = futures_bar.timestamp.isoformat()
                        all_events.append({"event_type": "EXIT_FILL", "decision_id": order.client_order_id,
                                           "client_order_id": order.client_order_id, "fill_timestamp": exit_timestamp,
                                           "quantity": order.quantity,
                                           "price": exit_quote.price if exit_quote is not None else futures_bar.close,
                                           "source": "replay", "session_date": date})
                        all_events.append({"event_type": "SETTLEMENT", "decision_id": order.client_order_id,
                                           "timestamp": exit_timestamp, "source": "replay", "session_date": date})
                        if settlement is not None:
                            ledger_records.append({
                                "event_type": "EXIT_SETTLEMENT", "kind": "exit_settlement",
                                "decision_id": order.client_order_id, "timestamp": exit_timestamp,
                                "quantity": order.quantity,
                                "gross_pnl": settlement_values.get("gross_pnl"),
                                "costs": settlement_values.get("costs"),
                                "net_pnl": settlement_values.get("net_pnl"),
                            })
                        if had_reservation and settlement is not None:
                            ledger_records.append({
                                "event_type": "RESERVATION_RELEASE", "kind": "reservation_release",
                                "decision_id": order.client_order_id, "entry_order_id": order.entry_order_id,
                                "timestamp": exit_timestamp, "quantity": order.quantity,
                            })
                        engine.record_exit(strategy_id=order.strategy_id,
                                           cell=realized.cell.name, reason=order.reason,
                                           entry_bar=realized.entry_bar, exit_bar=engine.bars_seen,
                                           date=date, vehicle=order.vehicle,
                                           direction="long" if order.side.value == "SELL" else "short",
                                           quantity=order.quantity)
                        current_equity = portfolio.equity
                        peak_equity = portfolio.peak_equity
                        max_drawdown = min(max_drawdown, current_equity - peak_equity)
                del result_events
            for pending in self.lifecycle.pending_entries.values():
                order = pending.order
                all_events.append({
                    "event_type": "ORDER_UNFILLED",
                    "decision_id": order.client_order_id,
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
        normalized_trades = [self._normalize_trade(trade) for trade in shadow_trades]
        summary = {
            **self._build_summary(all_events, normalized_trades),
            "bars_seen": bars_seen,
            "decisions_scored": len(scores),
        }
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
            # Decision scope excludes futures-clocked bars whose orderflow
            # observation is incomplete or carries a source gap.
            "processed_decision_bars": int(causal_bars_seen if native_orderflow else bars_seen),
            "incomplete_causal_bars": max(0, int(bars_seen) - int(causal_bars_seen)),
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "directional_exposure": directional_exposure,
            "orderflow_daily_atr": atr_artifacts,
                "requested_parameters": {
                    "session_date": session_date,
                    "vehicles": list(vehicles),
                    "emit_rejected_decisions": emit_rejected_decisions,
                    "capture_mode": request.get("capture_mode", "full"),
                    "mode": mode,
                    "include_parity_layers": include_parity_layers,
                },
                "capture_mode": request.get("capture_mode", "full"),
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
            if not is_decision_event(str(event.get("event_type", "")), event):
                continue
            normalized = self._normalize_decision(event)
            decision_id = event.get("decision_id")
            execution_events = execution_by_decision.get(decision_id, [])
            normalized["execution"].update({
                "event_types": [str(item.get("event_type")) for item in execution_events],
                "client_order_id": next((item.get("client_order_id") for item in execution_events
                                          if item.get("client_order_id")), None),
            })
            normalized_decisions.append(normalized)
        replay_result = {
            "run_id": run_id,
            "schema_version": REPLAY_SCHEMA_VERSION,
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
            "causal_bars_seen": causal_bars_seen,
            "directional_exposure": directional_exposure,
            "events": all_events,
            "decisions": [event for event in all_events if is_decision_event(str(event.get("event_type", "")), event)],
            "decision_trace_schema_version": 1,
            "decision_trace": normalized_decisions,
            "unavailable_fields": sorted(unavailable_fields),
            "summary_schema_version": REPLAY_SUMMARY_SCHEMA_VERSION,
            "summary": summary,
            "shadow_trades": normalized_trades,
            "shadow_ledger": ledger_records,
            "authoritative_trades": [],
            "authoritative_ledger": [],
            "first_divergent_stage": None,
            # Diagnostic-only trades live inside the replay run result. They
            # are never written to runtime_events, the broker ledger, or the
            # live trades/capital projections.
            "execution_scope": mode,
            "ledger_scope": "replay",
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
        detached_scope = {
            "source_contract": "orderflow-replay-v1",
            "input_identity": metadata["normalized_fingerprint"],
            "requested_dates": [session_date],
            "effective_dates": [session_date],
            "timezone": request.get("timezone", "Asia/Kolkata"),
            "market_session": list(request.get("market_session", ["09:15", "15:30"])),
            "session_end_cutoff": request.get("session_end_cutoff", "15:30"),
            "decision_clock": request.get("decision_clock", {"start": "inclusive", "end": "exclusive"}),
            "vehicles": list(vehicles),
            "capital_profile": {"name": "research"},
            "strategy_configuration": request.get("strategy_configuration", {
                "candidate_states_are_actionable": False,
                "cost_basis": "gross_before_costs",
                "dynamic_session_extrema": "updated from completed 5m aggregates at their available_at",
                "entry_basis": "5m structural candidate plus causal 1m displacement or 5m recovery retest",
                "exit_basis": "owner-authorized 1m or 5m policy signal",
                "registered_locations": ["OR_LOW", "OR_HIGH", "PD_LOW", "PD_HIGH", "SESSION_HIGH", "SESSION_LOW"],
                "research_only": True,
            }),
            "counts": {
                "selected_source_rows": metadata["selected_source_rows"],
                "filtered_source_rows": metadata["filtered_source_rows"],
                "completed_source_rows": metadata["completed_source_rows"],
                "gap_rows": metadata["gap_rows"],
                "processed_decision_bars": metadata["processed_decision_bars"],
            },
        }
        replay_result["replay_records"] = runtime_replay_records or [
            record.to_dict() for record in detached_replay_records(tuple(all_events), scope=detached_scope)
        ]
        parity_artifact = build_parity_artifact(
            replay_result, request=request, layer_records=parity_records,
        ) if include_parity_layers else None
        canonical_events = self._canonical_events(
            all_events, run_id=run_id, execution_scope=cast(ExecutionScope, mode),
        )
        canonical = ReplayResult(
            run_id=run_id,
            schema_version=REPLAY_SCHEMA_VERSION,
            execution_scope=cast(ExecutionScope, mode),
            ledger_scope="replay",
            summary=ReplaySummary(summary),
            events=tuple(canonical_events),
            shadow_trades=tuple(ReplayTrade(item) for item in normalized_trades),
            shadow_ledger=tuple(ReplayLedgerEntry(item) for item in ledger_records),
            authoritative_trades=(),
            authoritative_ledger=(),
            parity_artifact=parity_artifact,
            replay_records=tuple(runtime_replay_records),
        )
        return canonical.to_dict()

    @staticmethod
    def _canonical_events(events: Sequence[Mapping[str, Any]], *, run_id: str,
                           execution_scope: ExecutionScope) -> list[ReplayEvent]:
        """Wrap producer events in one strict, producer-neutral event envelope."""
        result: list[ReplayEvent] = []
        for sequence, event in enumerate(events):
            raw_timestamp = (
                event.get("occurred_at") or event.get("timestamp") or event.get("decision_at")
                or event.get("fill_timestamp") or f"{event.get('session_date', '1970-01-01')}T00:00:00Z"
            )
            occurred_at = _timestamp_string(raw_timestamp)
            identity_source = json.dumps(
                {"sequence": sequence, "event_type": event.get("event_type"),
                 "decision_id": event.get("decision_id"), "occurred_at": occurred_at},
                sort_keys=True, separators=(",", ":"), default=str,
            ).encode()
            identity = hashlib.sha256(identity_source).hexdigest()
            result.append(ReplayEvent(
                schema_version=REPLAY_SCHEMA_VERSION, run_id=run_id,
                execution_scope=cast(ExecutionScope, execution_scope), ledger_scope="replay",
                sequence=sequence, occurred_at=occurred_at, identity=identity,
                values=_strip_legacy_fields(event),
            ))
        return result

    @staticmethod
    def _build_summary(
        events: list[dict[str, Any]], trades: list[dict[str, Any]],
    ) -> dict[str, Any]:
        def reason_identities(
            reason: str, *, identity_fields: tuple[str, ...],
        ) -> set[str]:
            identities = {
                str(next((event.get(field) for field in identity_fields if event.get(field) is not None), index))
                for index, event in enumerate(events)
                if event.get("reason") == reason
            }
            return identities

        futures = [trade for trade in trades if trade.get("vehicle") == "futures"]
        gross_pnl_rs = sum(float(trade.get("gross_pnl_rs") or 0.0) for trade in futures)
        risk_reward_values: list[float] = []
        missing_initial_risk = 0
        for trade in futures:
            initial_risk_rs = float(trade.get("initial_risk_rs") or 0.0)
            if initial_risk_rs:
                risk_reward_values.append(float(trade.get("gross_pnl_rs") or 0.0) / initial_risk_rs)
            else:
                missing_initial_risk += 1
        setup_ids = reason_identities(
            "setup_candidate_formed", identity_fields=("opportunity_id", "setup_id", "decision_id"),
        )
        all_interaction_ids = reason_identities(
            "location_interaction_observed", identity_fields=("interaction_id",),
        )
        interaction_events = [
            event for event in events if event.get("reason") == "location_interaction_observed"
        ]
        structural_interaction_ids = {
            str(event.get("interaction_id") or index)
            for index, event in enumerate(interaction_events)
            if str(event.get("location") or "").upper() in CANONICAL_INTERACTION_LOCATIONS
        }
        interaction_ids_by_timeframe: dict[str, set[str]] = {}
        interaction_ids_by_location: dict[str, set[str]] = {}
        for event in interaction_events:
            timeframe = str(event.get("timeframe") or "unknown")
            location = str(event.get("location") or "unknown").upper()
            interaction_id = str(event.get("interaction_id") or "")
            interaction_ids_by_timeframe.setdefault(timeframe, set()).add(interaction_id)
            interaction_ids_by_location.setdefault(location, set()).add(interaction_id)
        interaction_by_timeframe = {
            timeframe: len(ids) for timeframe, ids in interaction_ids_by_timeframe.items()
        }
        interaction_by_location = {
            location: len(ids) for location, ids in interaction_ids_by_location.items()
        }
        vwap_interaction_ids = interaction_ids_by_location.get("VWAP", set())
        return {
            "setup_candidates": len(setup_ids),
            "location_interactions": len(structural_interaction_ids),
            "total_location_interactions": len(all_interaction_ids),
            "vwap_interactions": len(vwap_interaction_ids),
            "entries_filled": len(futures),
            "gross_reward_points": gross_pnl_rs / NIFTY_LOT_SIZE,
            "achieved_risk_reward": round(sum(risk_reward_values), 5) if risk_reward_values else None,
            "metric_lineage": {
                "setup_candidate_event_count": sum(
                    event.get("reason") == "setup_candidate_formed" for event in events
                ),
                "setup_candidate_unique_count": len(setup_ids),
                "location_interaction_event_count": sum(
                    event.get("reason") == "location_interaction_observed" for event in events
                ),
                "location_interaction_unique_count": len(all_interaction_ids),
                "canonical_location_interaction_unique_count": len(structural_interaction_ids),
                "location_interaction_by_timeframe": dict(sorted(interaction_by_timeframe.items())),
                "location_interaction_by_location": dict(sorted(interaction_by_location.items())),
                "futures_trade_count": len(futures),
                "futures_trades_missing_initial_risk": missing_initial_risk,
                "risk_reward_component_count": len(risk_reward_values),
            },
        }

    @staticmethod
    def _normalize_decision(event: dict[str, Any]) -> dict[str, Any]:
        payload = dict(event)
        event_type = str(payload.get("event_type", ""))
        raw_view = payload.get("decision_view")
        view = validate_decision_view(raw_view) if isinstance(raw_view, Mapping) else build_decision_view(
            payload, event_type=event_type,
        )
        subject = dict(view["subject"])
        policy = dict(view["policy"])
        evidence = dict(view["evidence"])
        metrics = dict(evidence.get("metrics") or {})
        return {
            "decision_id": view.get("decision_id"),
            "decision_stage": view.get("stage") or event_type.lower().removesuffix("decision"),
            "outcome": policy.get("outcome") or event_type.lower(),
            "rejection_reason": policy.get("reason_code"),
            "risk": {
                key: metrics.get(key) for key in ("stop_basis", "risk_per_trade", "risk_amount", "quantity", "score_multiplier")
                if metrics.get(key) is not None
            },
            "sizing": {
                key: metrics.get(key) for key in ("quantity", "max_lots", "lot_size", "entry_price")
                if metrics.get(key) is not None
            },
            "execution": {
                key: payload.get(key) for key in ("client_order_id", "execution_status", "execution_event_type")
                if payload.get(key) is not None
            },
            "subject": subject,
            "evidence": evidence,
            "date": evidence.get("provenance", {}).get("session_date"),
            "timestamp": view.get("at"),
            "bar_index": metrics.get("sequence"),
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
        stop_price = float(trade["stop_price"]) if trade.get("stop_price") is not None else None
        initial_risk_points = (
            float(trade["initial_risk_points"])
            if trade.get("initial_risk_points") is not None
            else abs(float(trade["entry_price"]) - stop_price) if stop_price is not None else None
        )
        initial_risk_rs = (
            float(trade["initial_risk_rs"])
            if trade.get("initial_risk_rs") is not None
            else initial_risk_points * NIFTY_LOT_SIZE * float(trade.get("quantity", 0.0))
            if initial_risk_points is not None else None
        )
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
            "stop_price": stop_price,
            "initial_risk_points": initial_risk_points,
            "initial_risk_rs": initial_risk_rs,
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
        return self.store.read_dates()

    def _prior_daily_atr(
        self, session_date: str, available_dates: list[str],
    ) -> float | None:
        """Read or materialize Paper's prior-session ATR artifact."""
        del available_dates
        return materialize_orderflow_daily_atr(self.store, session_date)

    def _orderflow_atr_artifact(self, session_date: str) -> dict[str, Any] | None:
        repository_factory = getattr(self.store, "feature_repository", None)
        if not callable(repository_factory):
            return None
        reader = getattr(repository_factory(), "read_feature_snapshot_for_session", None)
        if not callable(reader):
            return None
        snapshot = reader(
            feature_set=ORDERFLOW_DAILY_ATR_FEATURE_SET,
            instrument="NIFTY",
            session_date=session_date,
        )
        if not isinstance(snapshot, FeatureSnapshot):
            return None
        key = snapshot.key
        return {
            "session_date": key.session_date,
            "value": snapshot.values.get("atr_14"),
            "feature_set": key.feature_set,
            "calculator_version": key.calculator_version,
            "parameters": dict(key.parameters),
            "source_fingerprint": key.input_fingerprint,
            "source_bar_count": len(snapshot.source_bar_ids),
            "availability": snapshot.availability,
        }

    def _prior_day_levels(
        self, session_date: str, available_dates: list[str] | None = None,
    ) -> tuple[float | None, float | None]:
        """Return the preceding stored futures high/low for replay context.

        Pipeline replays seed each date with the immediately preceding
        trading day's futures range. Paper dates are evaluated independently
        for positions and risk, but their feature context must retain this
        causal cross-date input.
        """
        dates = sorted(self._dates() if available_dates is None else available_dates)
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
                    and str(row.get("expiry", ""))[:10] == futures_source.expiry
                )
            ]
        bars = []
        for row in loaded_rows:
            instrument_type = str(row.get("instrument_type", "")).upper()
            if instrument_type not in {"FUT", "FUTURES", "INDEX", "EQ", "CE", "PE"}:
                continue
            timestamp = datetime.fromisoformat(str(row["minute"]))
            local_timestamp = timestamp.astimezone(IST)
            minute_of_day = local_timestamp.hour * 60 + local_timestamp.minute
            if not SESSION_OPEN_MINUTES <= minute_of_day < SESSION_DATA_END_MINUTES:
                continue
            is_selected_futures = (
                futures_source is not None
                and instrument_type in {"FUT", "FUTURES"}
                and str(row.get("symbol")) == futures_source.symbol
                and str(row.get("expiry", ""))[:10] == futures_source.expiry
            )
            raw_bar = MarketBar(
                instrument=Instrument(symbol=str(row["symbol"]), exchange=str(row["exchange"]),
                                      instrument_type=instrument_type, expiry=row.get("expiry"),
                                      strike=row.get("strike")),
                timestamp=timestamp,
                open=float(row["open"]), high=float(row["high"]), low=float(row["low"]), close=float(row["close"]),
                volume=row.get("volume"), open_interest=row.get("open_interest"),
            )
            bars.append(virtual_continuous_bar(raw_bar) if is_selected_futures else raw_bar)
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
    def _aggregator(
        bars: tuple[MarketBar, ...],
        orderflow_producer: CausalOrderflowProducer | None = None,
        feature_requirements: tuple[FeatureRequirement, ...] = (),
    ) -> CompletedBarAggregator:
        roles = {}
        for bar in bars:
            kind = str(bar.instrument.instrument_type).upper()
            role = (MarketRole.FUTURES if kind in {"FUT", "FUTURES"}
                    else MarketRole.VIX if bar.instrument.symbol.upper() == "INDIA VIX"
                    else OptionRole(bar.instrument.symbol) if kind in {"CE", "PE"}
                    else MarketRole.SPOT)
            roles[InstrumentKey(bar.instrument.exchange, bar.instrument.symbol)] = role
        return CompletedBarAggregator(
            roles,
            AggregatorConfig(
                required_roles=(MarketRole.FUTURES,), deadline_seconds=0,
                feature_requirements=feature_requirements,
            ),
            orderflow_context_provider=(orderflow_producer.context if orderflow_producer else None),
        )

__all__ = ["ReplayWorker"]
