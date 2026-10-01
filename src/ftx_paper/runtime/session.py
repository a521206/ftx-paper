from __future__ import annotations

import threading
import time
import logging
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, NotRequired, TypedDict, cast
from zoneinfo import ZoneInfo

from ftx_paper.broker import Broker, Fill, PaperBroker
from ftx_paper.capital_config import ResearchCapitalProfile, RESEARCH_CAPITAL_PROFILE
from ftx_paper.contracts import (
    Instrument, MarketBar, MarketRole, OptionRole, OptionType, OrderRole, OrderSide, normalize_exchange_timestamp, parse_role,
    role_to_key,
)
from ftx_paper.core import AggregatorConfig, CompletedBarAggregator, InstrumentKey, PaperEngine
from ftx_paper.core.strategy import Strategy
from ftx_paper.core.cost import futures_cost
from ftx_paper.core.settlement import ExitValidationError, validate_exit_order
from ftx_paper.execution import PaperExecutionCoordinator
from .events import is_decision_event, serialize_datetime
from .store import RuntimeStore


logger = logging.getLogger("ftx-paper")


class LastExecutionFill(TypedDict):
    price: float
    fill_timestamp: str
    symbol: str
    quantity: int
    realized_pnl: NotRequired[float]

if TYPE_CHECKING:
    from ftx_paper.broker.zerodha import ZerodhaInstrument


def _bundle_timestamp(session_date: str, minute: str) -> datetime:
    """Return the timezone-aware instant at which a bundle was evaluated."""
    value = minute if "T" in minute else f"{session_date}T{minute}"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
    return parsed


class RuntimeSession:
    """Single in-process owner of live market, execution, and runtime state."""

    LIVE_FEED_LEASE = "zerodha-live-feed"

    def __init__(self, store: RuntimeStore, auth: Any, specifications: list[dict[str, object]],
                 *, engine: PaperEngine | None = None,
                 capital_profile: ResearchCapitalProfile = RESEARCH_CAPITAL_PROFILE,
                 feed_factory: Callable[..., Any] | None = None, broker_factory: Callable[[Any], Broker] | None = None,
                 client_factory: Callable[[], Any] | None = None, normalize_payload: Callable[..., Any] | None = None,
                 market_clock: Callable[[], datetime] | None = None) -> None:
        self.store, self.auth, self.specifications = store, auth, specifications
        self.capital_profile = capital_profile
        self.engine = engine or PaperEngine()
        self._restore_strategy_state()
        strategy = getattr(self.engine, "strategy", None)
        portfolio = getattr(strategy, "portfolio", None)
        self.portfolio = portfolio
        self.coordinator = (
            PaperExecutionCoordinator(portfolio, getattr(strategy, "capital_context", None))
            if portfolio is not None else None
        )
        self.feed_factory, self.broker_factory = feed_factory, broker_factory
        self.client_factory, self.normalize_payload = client_factory, normalize_payload
        self.market_clock = market_clock or (lambda: datetime.now(ZoneInfo("Asia/Kolkata")))
        self.feed = None
        self.broker = None
        self._thread = None
        self._lock = threading.RLock()
        self._stopping = False
        self._started = threading.Event()
        self._aggregator: CompletedBarAggregator | None = None
        self._last_execution_fill: LastExecutionFill | None = None
        self._feed_lease_id: str | None = None

    def _restore_strategy_state(self) -> None:
        """Restore the strategy-owned portfolio and risk state before execution wiring."""
        strategy = getattr(self.engine, "strategy", None)
        restore = getattr(type(strategy), "from_snapshot", None) if strategy is not None else None
        raw_snapshot = self.store.read_status().get("strategy_snapshot")
        if not callable(restore) or not isinstance(raw_snapshot, Mapping) or self.capital_profile is None:
            return
        restored = restore(raw_snapshot, capital_profile=self.capital_profile)
        self.engine.strategy = cast(Strategy, restored)

    def _persist_strategy_state(self) -> None:
        strategy = getattr(self.engine, "strategy", None)
        snapshot = getattr(strategy, "snapshot", None)
        if callable(snapshot):
            self.store.patch_status({"strategy_snapshot": snapshot()})

    def start(self) -> None:
        with self._lock:
            state = self.store.read_status().get("state")
            if state in {"STARTING", "RUNNING"} and self._feed_lease_id is not None:
                return
            self._assert_flat_startup()
            # Claim this before launching the worker. This closes the race where
            # two processes both observe a stale STARTING/RUNNING status and
            # create live WebSocket connections concurrently.
            if self._feed_lease_id is None:
                self._feed_lease_id = self.store.acquire_process_lease(self.LIVE_FEED_LEASE)
            self._stopping = False
            self._started.clear()
            self.store.patch_status({"state": "STARTING", "health_state": "STARTING",
                                     "feed_connected": False, "feed_health": None,
                                     "error": None, "started_at": datetime.now(timezone.utc).isoformat(),
                                     "initial_capital": self.capital_profile.initial_capital if self.capital_profile else None,
                                     "max_daily_loss": self.capital_profile.max_daily_loss if self.capital_profile else None,
                                     "max_net_directional_lots": self.capital_profile.max_net_directional_lots if self.capital_profile else None,
                                     "capital": (
                                         self.portfolio.equity
                                         if self.portfolio is not None
                                         else None
                                     ),
                                     "pending_bundle_minutes": [], "pending_bundle_details": []})
            self._thread = threading.Thread(target=self._start_impl, daemon=True, name="ftx-paper-runtime")
            self._thread.start()

    def _start_impl(self) -> None:
        try:
            if self.auth is None or self.auth.access_token() is None:
                raise ValueError("Zerodha authentication required")
            from ftx_paper.broker.zerodha import ZerodhaFeed, classify_runtime_roles, create_kite_socket, discover_option_surface_contracts, is_nse_market_open, load_startup_backfill, resolve_instruments
            client = self.client_factory() if self.client_factory else self.auth.authenticated_client()
            resolved: list[ZerodhaInstrument] = resolve_instruments(
                client, self.specifications, contract_store=self.store,
            )
            discovered_options = discover_option_surface_contracts(client, underlying="NIFTY")
            resolved_keys = {(str(item["exchange"]), str(item["symbol"])) for item in resolved}
            resolved.extend(
                item for item in discovered_options
                if (str(item["exchange"]), str(item["symbol"])) not in resolved_keys
            )
            logger.info("Discovered %d nearest-expiry option contracts", len(discovered_options))
            roles = classify_runtime_roles(resolved)
            inferred_roles = {
                InstrumentKey(str(roles.futures["exchange"]), str(roles.futures["symbol"])): MarketRole.FUTURES,
                InstrumentKey(str(roles.vix["exchange"]), str(roles.vix["symbol"])): MarketRole.VIX,
            }
            role_map = {
                InstrumentKey(str(item["exchange"]), str(item["symbol"])): inferred_roles.get(
                    InstrumentKey(str(item["exchange"]), str(item["symbol"])),
                    (OptionRole(str(item["symbol"])) if str(item.get("instrument_type", "")).upper() in {"CE", "PE"}
                     else parse_role(item.get("role") or MarketRole.SPOT))
                )
                for item in resolved
            }
            self._aggregator = CompletedBarAggregator(
                role_map, AggregatorConfig(required_roles=(MarketRole.FUTURES,), deadline_seconds=10.0),
            )
            if self.broker_factory:
                self.broker = self.broker_factory(client)
            else:
                from ftx_paper.broker.zerodha import ZerodhaBroker
                self.broker = ZerodhaBroker(client)
            backfill = load_startup_backfill(client, resolved)
            self.store.append_market_bars(backfill, source="historical_backfill")
            if not is_nse_market_open(self.market_clock()):
                if self.broker is not None:
                    self.broker.close()
                self.store.patch_status({
                    "state": "WAITING_FOR_MARKET",
                    "health_state": "WAITING",
                    "phase": "BACKFILL",
                    "execution_enabled": False,
                    "feed_connected": False,
                    "feed_health": {
                        "connected": False,
                        "market_open": False,
                        "message": "NSE/NFO market closed; historical backfill completed",
                    },
                })
                self._release_feed_lease()
                self._started.set()
                return
            normalize = self.normalize_payload or self._make_normalizer(resolved)
            socket = create_kite_socket(self.auth.api_key, self.auth.access_token())
            feed_type = self.feed_factory or ZerodhaFeed
            self.feed = feed_type(
                socket, [int(item["instrument_token"]) for item in resolved], normalize,
                self.on_closed_bar,
                on_tick=self.on_tick,
                on_health=self.on_feed_health,
                expected_instruments={
                    f"{item['exchange']}:{item['symbol']}": (str(item["exchange"]), str(item["symbol"]))
                    for item in resolved
                },
            )
            with self._lock:
                if self._stopping:
                    self.feed.stop()
                    self.broker.close()
                    self._release_feed_lease()
                    return
                self.feed.start()
                self.store.patch_status({"state": "RUNNING", "health_state": "DEGRADED",
                                         "phase": "LIVE", "execution_enabled": True})
                health_snapshot = getattr(self.feed, "health_snapshot", None)
                if callable(health_snapshot):
                    self.on_feed_health(health_snapshot())
                self._started.set()
        except Exception as exc:
            self._replaying = False
            if self.broker is not None:
                self.broker.close()
            logger.exception("Runtime worker failed during startup")
            self.store.patch_status({"state": "ERROR", "health_state": "FAILED",
                                     "feed_connected": False, "error": str(exc)})
            self._release_feed_lease()
            self._started.set()

    def _assert_flat_startup(self) -> None:
        portfolio = self.portfolio
        if portfolio is None:
            raise RuntimeError("intraday runtime cannot start without a verifiable flat portfolio")
        active_orders = {"submitted", "reserved"}
        if portfolio.positions or portfolio.reservations or getattr(portfolio, "scoped_risk_reservations", {}) or any(
            state in active_orders for state in portfolio.pending_orders.values()
        ):
            raise RuntimeError("intraday runtime must start flat with no pending orders")

    def _release_feed_lease(self) -> None:
        lease_id = self._feed_lease_id
        if lease_id is None:
            return
        self._feed_lease_id = None
        self.store.release_process_lease(self.LIVE_FEED_LEASE, lease_id)

    @staticmethod
    def _make_normalizer(resolved: list[ZerodhaInstrument]):
        from ftx_paper.broker.zerodha import instrument_expiry_iso

        by_token = {int(item["instrument_token"]): item for item in resolved}

        def normalize(payload):
            from datetime import datetime
            item = by_token[int(payload["instrument_token"])]
            # The exchange timestamp can lag for a live derivative quote even
            # while the WebSocket is delivering fresh packets.  Use packet
            # receipt time for the live aggregation clock. The broker timestamp
            # remains available to the feed layer for diagnostics.
            received = payload.get("timestamp")
            if received is not None:
                try:
                    received_value = float(received)
                    if received_value > 100_000_000_000:
                        received_value /= 1000
                    timestamp = normalize_exchange_timestamp(received_value)
                except (OverflowError, TypeError, ValueError):
                    timestamp = normalize_exchange_timestamp(datetime.now(ZoneInfo("Asia/Kolkata")))
            else:
                timestamp = normalize_exchange_timestamp(datetime.now(ZoneInfo("Asia/Kolkata")))
            price = float(payload["last_price"])
            instrument_type = str(item.get("instrument_type", "INDEX"))
            if str(item["symbol"]).upper() in {"NIFTY", "NIFTY 50", "INDIA VIX", "INDIAVIX"}:
                instrument_type = "INDEX"
            expiry = instrument_expiry_iso(item.get("expiry")) if instrument_type in {"FUT", "CE", "PE"} else None
            raw_strike = item.get("strike")
            strike = float(raw_strike) if raw_strike is not None and instrument_type in {"CE", "PE"} else None
            instrument = Instrument(
                str(item["symbol"]), str(item["exchange"]), instrument_type,
                expiry=expiry,
                strike=strike,
                option_type=OptionType(instrument_type) if instrument_type in {"CE", "PE"} else None,
            )
            open_interest = payload.get("oi")
            if instrument_type == "FUT" and open_interest is None:
                open_interest = 0.0
            return MarketBar(instrument, timestamp, price, price, price, price, payload.get("volume_traded"), open_interest)

        return normalize

    def _persist_engine_event(self, event: dict[str, object], *, source: str, bundle_id: str,
                              session_date: str, decision_at: datetime, timestamp: str,
                              engine: PaperEngine | None = None) -> None:
        event_type = str(event.get("event_type", "ENGINE_EVENT"))
        payload = {**event, "decision_source": source, "session_date": session_date,
                   "execution_allowed": source == "live"}
        payload.pop("event_type", None)
        if is_decision_event(event_type):
            payload["decision_at"] = serialize_datetime(decision_at)
        metadata = (engine or self.engine).strategy_metadata
        if metadata:
            payload.update(strategy_version=metadata.version, config_hash=metadata.config_hash)
        self.store.append_event(event_type, payload,
                                f"{event_type}:{source}:{bundle_id}:{payload.get('decision_id', '')}",
                                timestamp=timestamp)

    def _process_bundle(self, bundle, *, source: str, engine: PaperEngine | None = None) -> None:
        active_engine = engine or self.engine
        self._process_bundle_for_source(bundle, source=source, engine=active_engine)

    def _process_bundle_for_source(self, bundle, *, source: str, engine: PaperEngine) -> None:
        """Persist a bundle result, with live execution kept behind the source gate."""
        active_engine = engine
        result = active_engine.on_bundle(bundle)
        decision_at = _bundle_timestamp(bundle.trading_date, bundle.minute)
        timestamp = serialize_datetime(decision_at)
        for event in result.events:
            self._persist_engine_event(event, source=source, bundle_id=bundle.bundle_id,
                                        session_date=bundle.trading_date, decision_at=decision_at, timestamp=timestamp,
                                        engine=active_engine)
        outcome_types = [str(event.get("event_type", "ENGINE_EVENT")) for event in result.events]
        self.store.append_event("BUNDLE_COMPLETE", {
            "bundle_id": bundle.bundle_id, "minute": bundle.minute,
            "required_roles": [role_to_key(role) for role in bundle.required_roles],
            "roles_present": [role_to_key(role) for role in bundle.bars],
            "decision_source": source, "session_date": bundle.trading_date,
            "execution_allowed": source == "live",
        }, f"bundle_complete:{source}:{bundle.bundle_id}", timestamp=timestamp)
        self.store.append_event("STRATEGY_EVALUATION", {
            "bundle_id": bundle.bundle_id, "minute": bundle.minute, "outcomes": outcome_types,
            "decision_source": source, "session_date": bundle.trading_date,
            "execution_allowed": source == "live",
        }, f"strategy_evaluation:{source}:{bundle.bundle_id}", timestamp=timestamp)
        for order in result.orders:
            order_decision_id = order.client_order_id.rsplit(":", 1)[0]
            self.store.append_event("ORDER_INTENT", {
                "decision_id": order_decision_id,
                "client_order_id": order.client_order_id,
                "instrument": order.instrument.symbol,
                "direction": "long" if order.side is OrderSide.BUY else "short",
                "quantity": order.quantity,
                "cell": order.cell,
                "stop": order.stop_price,
                "exit_mode": order.exit_mode,
                "entry_bar": order.entry_bar,
                "decision_at": timestamp,
                "decision_source": source,
                "session_date": bundle.trading_date,
                "execution_allowed": source == "live",
            }, f"order_intent:{source}:{bundle.bundle_id}:{order.client_order_id}", timestamp=timestamp)
            if source == "replay":
                self.store.append_event("ORDER_SUPPRESSED", {
                    "decision_id": order.client_order_id, "client_order_id": order.client_order_id,
                    "vehicle": order.vehicle,
                    "decision_source": "replay", "session_date": bundle.trading_date,
                    "execution_allowed": False, "reason": "startup_recovery",
                }, f"order_suppressed:{bundle.trading_date}:{order.client_order_id}", timestamp=timestamp)
            else:
                futures_bar = bundle.bars.get(MarketRole.FUTURES)
                self._execute_paper_order(order, session_date=bundle.trading_date, timestamp=timestamp,
                                           reference_price=futures_bar.close if isinstance(futures_bar, MarketBar) else None)

    def _execute_paper_order(self, order, *, session_date: str | None = None,
                             timestamp: str | None = None,
                             reference_price: float | None = None) -> bool:
        self._last_execution_fill = None
        def cancel_reservation() -> None:
            if self.coordinator is not None:
                if order.role is OrderRole.ENTRY:
                    self.coordinator.cancel(order)
                else:
                    self.coordinator.failed_exit(order)
                self._persist_strategy_state()
            if order.role is OrderRole.ENTRY and order.cell and session_date:
                self.engine.cancel_entry(cell=order.cell,
                                         direction="long" if order.side is OrderSide.BUY else "short",
                                         quantity=order.quantity, date=session_date, vehicle=order.vehicle)
        def compensate_fills(fills: tuple, broker_order_id: str) -> bool:
            handler = getattr(self.broker, "abort_partial", None)
            if not callable(handler):
                return False
            try:
                return bool(handler(order, broker_order_id, fills))
            except Exception as exc:
                self.store.append_event("EXECUTION_ERROR", {**context, "outcome": "execution_error",
                    "phase": "compensation", "error_type": type(exc).__name__, "reason": str(exc)},
                    f"execution_error:compensation:{order.client_order_id}", timestamp=timestamp)
                return False
        def fail_closed(reason: str) -> None:
            self._stopping = True
            self.store.patch_status({"state": "ERROR", "error": reason})
            self.store.append_event(
                "EXECUTION_ERROR", {**context, "outcome": "execution_error",
                                     "phase": "compensation", "reason": reason},
                f"execution_error:halt:{order.client_order_id}", timestamp=timestamp,
            )
        context = {
            "decision_id": order.client_order_id,
            "client_order_id": order.client_order_id,
            "decision_source": "live",
            "session_date": session_date,
            "execution_allowed": True,
            "reason": order.reason,
            "vehicle": order.vehicle,
        }
        if order.vehicle != "futures":
            self.store.append_event(
                "EXECUTION_ERROR",
                {**context, "outcome": "execution_error", "phase": "order_validation",
                 "reason": "synthetic_execution_disabled"},
                f"execution_error:vehicle:{order.client_order_id}", timestamp=timestamp,
            )
            return False
        if self.coordinator is not None:
            self.coordinator.submit(order)
            self._persist_strategy_state()
        entry_order_id = order.entry_order_id
        if order.role is OrderRole.EXIT:
            if entry_order_id is None:
                self.store.append_event(
                    "EXECUTION_ERROR",
                    {**context, "outcome": "execution_error", "phase": "order_validation",
                     "reason": "exit_without_entry_order_id"},
                    f"execution_error:exit_identity:{order.client_order_id}", timestamp=timestamp,
                )
                if self.coordinator is not None:
                    self.coordinator.failed_exit(order)
                return False
            position = self.portfolio.positions.get(entry_order_id) if self.portfolio is not None else None
            if position is None:
                self.store.append_event(
                    "EXECUTION_ERROR",
                    {**context, "outcome": "execution_error", "phase": "order_validation",
                     "reason": "exit_without_matching_entry", "entry_order_id": entry_order_id},
                    f"execution_error:exit_entry:{order.client_order_id}", timestamp=timestamp,
                )
                if self.coordinator is not None:
                    self.coordinator.failed_exit(order)
                return False
            try:
                validate_exit_order(
                    order,
                    entry_instrument=position.instrument,
                    entry_vehicle=position.vehicle,
                    entry_side=position.side,
                    entry_quantity=position.quantity,
                    entry_leg_symbols=position.synthetic_legs,
                )
            except ExitValidationError as exc:
                self.store.append_event(
                    "EXECUTION_ERROR",
                    {**context, "outcome": "execution_error", "phase": "order_validation",
                     "reason": exc.reason, "entry_order_id": entry_order_id},
                    f"execution_error:exit_contract:{order.client_order_id}", timestamp=timestamp,
                )
                if self.coordinator is not None:
                    self.coordinator.failed_exit(order)
                return False
        if self.broker is None:
            self.store.append_event("EXECUTION_ERROR", {**context,
                                                         "outcome": "execution_error",
                                                         "phase": "submit",
                                                         "reason": "broker_unavailable"},
                                    f"execution_error:submit:{order.client_order_id}", timestamp=timestamp)
            cancel_reservation()
            return False
        try:
            ack = self.broker.submit(order)
        except Exception as exc:
            self.store.append_event("EXECUTION_ERROR", {**context,
                                                         "outcome": "execution_error",
                                                         "phase": "submit",
                                                         "error_type": type(exc).__name__,
                                                         "reason": str(exc)},
                                    f"execution_error:submit:{order.client_order_id}", timestamp=timestamp)
            cancel_reservation()
            return False
        self.store.append_event("ORDER_ACK", {**context,
                                               "client_order_id": ack.client_order_id,
                                               "broker_order_id": ack.broker_order_id,
                                               "status": ack.status,
                                               "outcome": "acknowledged"}, f"order_ack:{ack.client_order_id}", timestamp=timestamp)
        try:
            poll_fills = getattr(self.broker, "poll_fills", None)
            if callable(poll_fills):
                raw_fills = poll_fills(order, ack.broker_order_id)
                if not isinstance(raw_fills, Iterable):
                    raise TypeError("broker poll_fills() must return an iterable")
                fills = tuple(raw_fills)
                if any(not isinstance(item, Fill) for item in fills):
                    raise TypeError("broker poll_fills() returned an invalid fill")
            else:
                fill = self.broker.poll_fill(order, ack.broker_order_id)
                fills = (fill,) if fill is not None else ()
        except Exception as exc:
            self.store.append_event("EXECUTION_ERROR", {**context,
                                                         "broker_order_id": ack.broker_order_id,
                                                         "outcome": "execution_error",
                                                         "phase": "fill_poll",
                                                         "error_type": type(exc).__name__,
                                                         "reason": str(exc)},
                                    f"execution_error:fill_poll:{order.client_order_id}", timestamp=timestamp)
            cancel_reservation()
            return False
        fill = fills[0] if fills else None
        if fills:
            fill = fills[0]
            if any(item.vehicle != order.vehicle for item in fills):
                self.store.append_event(
                    "EXECUTION_ERROR",
                    {**context, "outcome": "execution_error",
                     "phase": "fill_validation",
                     "reason": "fill_vehicle_mismatch",
                     "fill_vehicle": fills[0].vehicle},
                    f"execution_error:fill_vehicle:{order.client_order_id}",
                    timestamp=timestamp,
                )
                cancel_reservation()
                return False
            if order.vehicle == "synthetic" and len(fills) != 2:
                compensated = compensate_fills(fills, ack.broker_order_id)
                if compensated:
                    cancel_reservation()
                self.store.append_event("EXECUTION_ERROR", {**context, "outcome": "execution_error",
                    "phase": "fill_validation", "reason": "synthetic_leg_count_mismatch"},
                    f"execution_error:synthetic_legs:{order.client_order_id}", timestamp=timestamp)
                return False
            if order.vehicle == "synthetic" and any(item.quantity != order.quantity for item in fills):
                compensated = compensate_fills(fills, ack.broker_order_id)
                if compensated:
                    cancel_reservation()
                self.store.append_event("EXECUTION_ERROR", {**context, "outcome": "execution_error",
                    "phase": "fill_validation", "reason": "synthetic_quantity_mismatch",
                    "fill_quantities": [item.quantity for item in fills],
                    "requested_quantity": order.quantity},
                    f"execution_error:synthetic_quantity:{order.client_order_id}", timestamp=timestamp)
                return False
            if order.role is OrderRole.EXIT:
                assert position is not None
                try:
                    validate_exit_order(
                        order,
                        entry_instrument=position.instrument,
                        entry_vehicle=position.vehicle,
                        entry_side=position.side,
                        entry_quantity=position.quantity,
                        entry_leg_symbols=position.synthetic_legs,
                        filled_quantity=fill.quantity,
                        filled_instruments=tuple(item.instrument.symbol for item in fills),
                    )
                except ExitValidationError as exc:
                    compensated = compensate_fills(fills, ack.broker_order_id)
                    if compensated:
                        cancel_reservation()
                    else:
                        fail_closed(f"uncompensated_exit_fill:{exc.reason}")
                    self.store.append_event(
                        "EXECUTION_ERROR",
                        {**context, "outcome": "execution_error", "phase": "fill_validation",
                         "reason": exc.reason, "entry_order_id": entry_order_id},
                        f"execution_error:exit_fill_contract:{order.client_order_id}", timestamp=timestamp,
                    )
                    return False
            entry_cost = None
            if order.role is OrderRole.ENTRY:
                entry_cost = futures_cost(fill.quantity)
            execution_price = fill.price
            self._last_execution_fill = {
                "price": execution_price,
                "fill_timestamp": fill.timestamp,
                "symbol": fill.instrument.symbol,
                "quantity": fill.quantity,
            }
            cost = 0.0
            position = None
            if order.role is OrderRole.EXIT:
                cost = futures_cost(fill.quantity)
            register_entry = getattr(self.engine, "register_entry", None)
            if callable(register_entry) and order.role is OrderRole.ENTRY:
                try:
                    register_entry(order, fill_price=execution_price,
                                   entry_fill_time=timestamp,
                                   reference_price=reference_price)
                except TypeError as exc:
                    if "entry_fill_time" not in str(exc):
                        raise
                    try:
                        register_entry(order, fill_price=execution_price)
                    except Exception as exc:
                        compensated = compensate_fills(fills, ack.broker_order_id)
                        if compensated:
                            cancel_reservation()
                        self.store.append_event("EXECUTION_ERROR", {**context, "outcome": "execution_error",
                            "phase": "register_entry", "error_type": type(exc).__name__, "reason": str(exc)},
                        f"execution_error:register_entry:{order.client_order_id}", timestamp=timestamp)
                        return False
                except Exception as exc:
                    compensated = compensate_fills(fills, ack.broker_order_id)
                    if compensated:
                        cancel_reservation()
                    self.store.append_event("EXECUTION_ERROR", {**context, "outcome": "execution_error",
                        "phase": "register_entry", "error_type": type(exc).__name__, "reason": str(exc)},
                        f"execution_error:register_entry:{order.client_order_id}", timestamp=timestamp)
                    return False
            if order.role is OrderRole.ENTRY and self.coordinator is not None:
                try:
                    position = self.coordinator.fill(
                        order, price=execution_price, timestamp=timestamp,
                        synthetic_entry_prices=None,
                    )
                except Exception as exc:
                    compensated = compensate_fills(fills, ack.broker_order_id)
                    if compensated:
                        cancel_reservation()
                    else:
                        fail_closed(f"uncompensated_entry_fill:{type(exc).__name__}")
                    self.store.append_event(
                        "EXECUTION_ERROR", {**context, "outcome": "execution_error",
                                             "phase": "portfolio_fill",
                                             "error_type": type(exc).__name__, "reason": str(exc)},
                        f"execution_error:portfolio_fill:{order.client_order_id}", timestamp=timestamp,
                    )
                    return False
            if order.role is OrderRole.EXIT:
                assert entry_order_id is not None
            self.store.append_event("FILL", {**context,
                                              "broker_order_id": ack.broker_order_id,
                                              "symbol": fill.instrument.symbol,
                                              "quantity": fill.quantity,
                                              "price": execution_price,
                                              "legs": [{"symbol": item.instrument.symbol, "quantity": item.quantity,
                                                        "price": item.price, "side": (order.side if item.instrument.instrument_type.upper() != "PE" else (OrderSide.SELL if order.side is OrderSide.BUY else OrderSide.BUY)).value}
                                                       for item in fills] if order.vehicle == "synthetic" else None,
                                              "fill_timestamp": fill.timestamp,
                                              **({"cost_rs": entry_cost} if entry_cost is not None else {}),
                                              "outcome": "filled",
                                               **((
                                                   {"position": {
                                                       "symbol": position.instrument,
                                                       "vehicle": position.vehicle,
                                                       "quantity": position.quantity,
                                                       "average_price": position.entry_price,
                                                   }}
                                                   if order.role is OrderRole.ENTRY and position is not None
                                                   else {}
                                               ))}, f"fill:{fill.client_order_id}", timestamp=timestamp)
        else:
            self.store.append_event("ORDER_UNFILLED", {**context,
                                                        "broker_order_id": ack.broker_order_id,
                                                        "status": ack.status,
                                                        "outcome": "unfilled",
                                                        "reason": "no_fill_available"},
                                     f"order_unfilled:{order.client_order_id}", timestamp=timestamp)
            cancel_reservation()
            return False
        if order.role is OrderRole.EXIT and self.coordinator is not None:
            settlement = self.coordinator.fill(order, price=execution_price, timestamp=timestamp, cost=cost)
            if settlement is not None:
                self._last_execution_fill["realized_pnl"] = settlement["net_pnl"]
        self._persist_strategy_state()
        return bool(fill)

    def on_closed_bar(self, bar) -> None:
        with self._lock:
            if self._stopping or self.store.read_status().get("state") != "RUNNING":
                return
            if isinstance(self.broker, PaperBroker):
                self.broker.update_price(bar.instrument.symbol, bar.close)
            now = datetime.now(timezone.utc).isoformat()
            self.store.append_market_bars((bar,), source="live")
            self.store.patch_status({"last_bar_at": bar.timestamp.isoformat(), "last_heartbeat_at": now,
                                     "bars_seen": self.engine.bars_seen + 1})
            bar_key = f"{bar.instrument.exchange}:{bar.instrument.symbol}:{bar.timestamp.isoformat()}"
            self.store.append_event("BAR_CLOSED", {
                "exchange": bar.instrument.exchange,
                "symbol": bar.instrument.symbol,
                "instrument_type": bar.instrument.instrument_type,
                "timestamp": bar.timestamp.isoformat(),
                "close": bar.close,
            }, f"bar_closed:{bar_key}", timestamp=bar.timestamp.isoformat())
            if self._aggregator is None:
                return
            expired = self._aggregator.expire(now=time.monotonic())
            for incomplete in expired:
                self._record_incomplete(incomplete)
            bundle = self._aggregator.ingest(bar)
            # Settle positions carried into this bar before evaluating a new
            # bundle, matching the deterministic replay ordering.
            for action in self.engine.on_closed_bar(bar):
                filled = self._execute_paper_order(
                    action.intent,
                    session_date=bar.timestamp.astimezone(ZoneInfo("Asia/Kolkata")).date().isoformat(),
                    timestamp=bar.timestamp.isoformat(),
                )
                self.engine.settle_exit(action.intent.client_order_id, filled=filled)
                if filled and action.cell is not None:
                    self.engine.record_exit(
                        cell=action.cell,
                        reason=action.reason,
                        entry_bar=action.intent.entry_bar or 0,
                        exit_bar=self.engine.bars_seen,
                        date=bar.timestamp.astimezone(ZoneInfo("Asia/Kolkata")).date().isoformat(),
                        vehicle=action.intent.vehicle,
                        direction="long" if action.intent.side.value == "SELL" else "short",
                        quantity=action.intent.quantity,
                    )
                self.store.append_event("EXITDECISION", {
                    "decision_id": action.intent.client_order_id,
                    "vehicle": action.intent.vehicle,
                    "cell": action.cell,
                    "reason": action.reason,
                    "exit_mode": action.intent.exit_mode,
                    "bars_held": action.bars_held,
                    "mae_bp": action.mae_bp,
                    "mfe_bp": action.mfe_bp,
                    "exit_price": action.price,
                    "trigger_price": action.price,
                    "fill_price": (
                        self._last_execution_fill["price"]
                        if self._last_execution_fill is not None else None
                    ),
                    "market_time": bar.timestamp.isoformat(),
                    "processing_time": datetime.now(timezone.utc).isoformat(),
                    "trigger_source": "on_closed_bar",
                    "decision_at": bar.timestamp.isoformat(),
                    "decision_source": "live",
                    "execution_allowed": True,
                    "outcome": "exit_triggered",
                }, f"exit_decision:{action.intent.client_order_id}", timestamp=bar.timestamp.isoformat())
            self._refresh_pending_status()
            if bundle is not None:
                self.store.patch_status({"last_completed_bundle_minute": bundle.minute,
                                         "last_strategy_evaluation_minute": bundle.minute})
                self._process_bundle(bundle, source="live")

    def on_tick(self, bar: MarketBar) -> None:
        """Evaluate protective exits immediately on each live market tick."""
        with self._lock:
            if self._stopping or self.store.read_status().get("state") != "RUNNING":
                return
            if isinstance(self.broker, PaperBroker):
                self.broker.update_price(bar.instrument.symbol, bar.close)
            result = self.engine.on_tick(bar)
            for action in result:
                filled = self._execute_paper_order(
                    action.intent,
                    session_date=bar.timestamp.astimezone(ZoneInfo("Asia/Kolkata")).date().isoformat(),
                    timestamp=bar.timestamp.isoformat(),
                )
                self.engine.settle_exit(action.intent.client_order_id, filled=filled)
                if filled and action.cell is not None:
                    self.engine.record_exit(
                        cell=action.cell,
                        reason=action.reason,
                        entry_bar=action.intent.entry_bar or 0,
                        exit_bar=self.engine.bars_seen,
                        date=bar.timestamp.astimezone(ZoneInfo("Asia/Kolkata")).date().isoformat(),
                        vehicle=action.intent.vehicle,
                        direction="long" if action.intent.side.value == "SELL" else "short",
                        quantity=action.intent.quantity,
                    )
                self.store.append_event("EXITDECISION", {
                    "decision_id": action.intent.client_order_id,
                    "vehicle": action.intent.vehicle,
                    "cell": action.cell,
                    "reason": action.reason,
                    "exit_mode": action.intent.exit_mode,
                    "bars_held": action.bars_held,
                    "mae_bp": action.mae_bp,
                    "mfe_bp": action.mfe_bp,
                    "exit_price": action.price,
                    "trigger_price": action.price,
                    "fill_price": (
                        self._last_execution_fill["price"]
                        if self._last_execution_fill is not None else None
                    ),
                    "market_time": bar.timestamp.isoformat(),
                    "processing_time": datetime.now(timezone.utc).isoformat(),
                    "trigger_source": "on_tick",
                    "decision_at": bar.timestamp.isoformat(),
                    "decision_source": "live",
                    "execution_allowed": True,
                    "outcome": "exit_triggered",
                }, f"exit_decision:{action.intent.client_order_id}", timestamp=bar.timestamp.isoformat())

    def _record_incomplete(self, bundle) -> None:
        payload = {
            "minute": bundle.minute,
            "session_date": bundle.trading_date,
            "required_roles": [role_to_key(role) for role in bundle.required_roles],
            "roles_present": [role_to_key(role) for role in bundle.bars],
            "missing_roles": [role_to_key(role) for role in bundle.missing_roles],
            "last_bar_by_role": {role_to_key(role): bar.timestamp.isoformat() for role, bar in bundle.bars.items()},
        }
        self.store.append_event(
            "BUNDLE_INCOMPLETE", payload, f"bundle_incomplete:{bundle.bundle_id}",
            timestamp=serialize_datetime(_bundle_timestamp(bundle.trading_date, bundle.minute)),
        )

    def _refresh_pending_status(self) -> None:
        if self._aggregator is None:
            return
        pending = self._aggregator.pending_diagnostics()
        self.store.patch_status({
            "pending_bundle_minutes": [item["minute"] for item in pending],
            "pending_bundle_details": list(pending),
        })

    def on_feed_health(self, health: dict[str, object]) -> None:
        """Update feed health in status only; periodic samples are not audit events."""
        with self._lock:
            if self._stopping:
                return
            if self._aggregator is not None:
                self._refresh_pending_status()
            connected = bool(health.get("connected"))
            failed = str(health.get("state", "")).upper() == "FAILED"
            degraded = not connected or bool(health.get("transport_stale"))
            health_state = "FAILED" if failed else "DEGRADED" if degraded else "HEALTHY"
            self.store.patch_status({
                "feed_health": health,
                "feed_connected": connected,
                "health_state": health_state,
            })

    def stop(self) -> None:
        self._stopping = True
        with self._lock:
            startup = self._thread
            feed = self.feed
            broker = self.broker
            self.store.patch_status({"state": "STOPPING", "health_state": "STOPPING", "feed_connected": False})
        feed_stopped = True
        try:
            if feed:
                feed_result = feed.stop()
                feed_stopped = feed_result is not False
        except Exception:
            feed_stopped = False
            logger.exception("Runtime feed failed while stopping")
        finally:
            try:
                if broker:
                    broker.close()
            except Exception:
                feed_stopped = False
                logger.exception("Runtime broker failed while stopping")
            finally:
                with self._lock:
                    self._release_feed_lease()
                    final_state = "STOPPED" if feed_stopped else "ERROR"
                    self.store.patch_status({"state": final_state, "health_state": final_state,
                                             "feed_connected": False, "bars_seen": self.engine.bars_seen,
                                             "pending_bundle_minutes": [], "pending_bundle_details": []})
        if startup and startup is not threading.current_thread():
            startup.join()
        with self._lock:
            try:
                final_state = "STOPPED" if feed_stopped else "ERROR"
                self.store.patch_status({"state": final_state, "health_state": final_state,
                                         "feed_connected": False, "bars_seen": self.engine.bars_seen})
            except Exception as exc:
                logger.exception("Runtime worker failed while stopping")
                self.store.patch_status({"state": "ERROR", "error": str(exc)})

    def restart(self) -> None:
        self.stop()
        self.start()
