from __future__ import annotations

import threading
import time
import logging
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, NotRequired, TypedDict
from zoneinfo import ZoneInfo

from ftx_paper.broker import Broker, Fill, PaperBroker
from ftx_paper.capital_config import FtxCapitalConfig
from ftx_paper.contracts import (
    Instrument, MarketBar, MarketRole, OptionRole, OptionType, OrderRole, OrderSide, Role, SyntheticFutureQuote, normalize_exchange_timestamp, parse_role,
    synthetic_future_quote,
    role_to_key,
)
from ftx_paper.core import AggregatorConfig, CompletedBarAggregator, InstrumentKey, PaperEngine
from ftx_paper.core.cost import futures_cost, synthetic_futures_cost
from ftx_paper.core.settlement import ExitValidationError, validate_exit_order
from ftx_paper.config import NIFTY_LOT_SIZE
from ftx_paper.execution import PositionLedger
from ftx_paper.execution import PaperExecutionCoordinator
from .events import is_decision_event, serialize_datetime
from .store import RuntimeStore


logger = logging.getLogger("ftx-paper")


class LiveEntryTrade(TypedDict):
    instrument: str
    entry_price: float
    quantity: int
    side: OrderSide
    vehicle: str
    ce_symbol: str | None
    pe_symbol: str | None
    ce_entry_price: float | None
    pe_entry_price: float | None


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

    def __init__(self, store: RuntimeStore, auth: Any, specifications: list[dict[str, object]],
                 *, engine: PaperEngine | None = None, ledger: PositionLedger | None = None,
                 capital_config: FtxCapitalConfig | None = None,
                 feed_factory: Callable[..., Any] | None = None, broker_factory: Callable[[Any], Broker] | None = None,
                 client_factory: Callable[[], Any] | None = None, normalize_payload: Callable[..., Any] | None = None) -> None:
        if ledger is not None and capital_config is None:
            raise ValueError("capital_config is required when a ledger is configured")
        self.store, self.auth, self.specifications = store, auth, specifications
        self.capital_config = capital_config
        self.engine, self.ledger = engine or PaperEngine(), ledger
        strategy = getattr(self.engine, "strategy", None)
        portfolio = getattr(strategy, "portfolio", None)
        self.coordinator = PaperExecutionCoordinator(portfolio) if portfolio is not None else None
        self.feed_factory, self.broker_factory = feed_factory, broker_factory
        self.client_factory, self.normalize_payload = client_factory, normalize_payload
        self.feed = None
        self.broker = None
        self._thread = None
        self._lock = threading.RLock()
        self._stopping = False
        self._started = threading.Event()
        self._aggregator: CompletedBarAggregator | None = None
        self._last_execution_fill: LastExecutionFill | None = None
        self._latest_option_bars: dict[Role, MarketBar] = {}
        self._synthetic_symbols_by_order: dict[str, tuple[str, str]] = {}
        self._last_synthetic_quote_by_order: dict[str, SyntheticFutureQuote] = {}
        self._live_entry_trades: dict[str, LiveEntryTrade] = {}
        self._restore_ledger_state()

    def _restore_ledger_state(self) -> None:
        if self.ledger is None:
            return
        capital_config = self.capital_config
        if capital_config is None:
            raise ValueError("capital_config is required when a ledger is configured")
        status = self.store.read_status()
        raw_cash = status.get("capital")
        if raw_cash is None:
            if status:
                raise ValueError("runtime status has no persisted capital")
            return
        if isinstance(raw_cash, dict):
            raw_cash = raw_cash.get("capital")
        if raw_cash is None:
            raise ValueError("runtime status has no persisted capital")
        persisted_initial = status.get("initial_capital")
        if persisted_initial is None:
            raise ValueError("runtime status has no persisted initial_capital")
        if isinstance(persisted_initial, bool) or not isinstance(persisted_initial, (int, float)):
            raise ValueError("runtime status has an invalid initial_capital")
        if float(persisted_initial) != capital_config.initial_capital:
            raise ValueError(
                "capital config does not match runtime status: "
                f"configured={capital_config.initial_capital}, persisted={persisted_initial}"
            )
        raw_positions = status.get("open_positions", ())
        if not isinstance(raw_positions, (list, tuple)):
            raw_positions = ()
        self.ledger.restore_state(cash=float(raw_cash), positions=raw_positions)
        raw_entries = status.get("open_entry_trades", {})
        if not isinstance(raw_entries, dict):
            raise ValueError("runtime status has invalid open_entry_trades")
        for entry_order_id, raw_entry in raw_entries.items():
            if not isinstance(entry_order_id, str) or not isinstance(raw_entry, dict):
                raise ValueError("runtime status has invalid open entry trade")
            side = raw_entry.get("side")
            if side not in {OrderSide.BUY.value, OrderSide.SELL.value}:
                raise ValueError("runtime status has an invalid entry side")
            self._live_entry_trades[entry_order_id] = {
                "instrument": str(raw_entry["instrument"]),
                "entry_price": float(raw_entry["entry_price"]),
                "quantity": int(raw_entry["quantity"]),
                "side": OrderSide(side),
                "vehicle": str(raw_entry["vehicle"]),
                "ce_symbol": raw_entry.get("ce_symbol"),
                "pe_symbol": raw_entry.get("pe_symbol"),
                "ce_entry_price": raw_entry.get("ce_entry_price"),
                "pe_entry_price": raw_entry.get("pe_entry_price"),
            }

    def start(self) -> None:
        with self._lock:
            if self.store.read_status().get("state") in {"STARTING", "RUNNING"}:
                return
            self._stopping = False
            self._started.clear()
            self.store.patch_status({"state": "STARTING", "error": None, "started_at": datetime.now(timezone.utc).isoformat(),
                                     "initial_capital": self.capital_config.initial_capital if self.capital_config else None,
                                     "max_daily_loss": self.capital_config.max_daily_loss if self.capital_config else None,
                                     "max_net_directional_lots": self.capital_config.max_net_directional_lots if self.capital_config else None,
                                     "capital": self.ledger.cash if self.ledger else None,
                                     "pending_bundle_minutes": [], "pending_bundle_details": []})
            self._thread = threading.Thread(target=self._start_impl, daemon=True, name="ftx-paper-runtime")
            self._thread.start()

    def _start_impl(self) -> None:
        try:
            if self.auth is None or self.auth.access_token() is None:
                raise ValueError("Zerodha authentication required")
            from ftx_paper.broker.zerodha import ZerodhaFeed, classify_runtime_roles, create_kite_socket, discover_option_surface_contracts, load_startup_backfill, resolve_instruments
            client = self.client_factory() if self.client_factory else self.auth.authenticated_client()
            resolved: list[ZerodhaInstrument] = resolve_instruments(client, self.specifications)
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
            reset = getattr(self.engine, "reset", None)
            if callable(reset):
                reset()
            backfill = load_startup_backfill(client, resolved)
            self.store.append_market_bars(backfill, source="historical_backfill")
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
                    return
                self.feed.start()
                self.store.patch_status({"state": "RUNNING", "phase": "LIVE", "execution_enabled": True,
                                         "feed_connected": True})
                self._started.set()
        except Exception as exc:
            self._replaying = False
            if self.broker is not None:
                self.broker.close()
            self.store.patch_status({"state": "ERROR", "error": str(exc)})
            self._started.set()

    @staticmethod
    def _make_normalizer(resolved: list[ZerodhaInstrument]):
        from ftx_paper.broker.zerodha import instrument_expiry_iso

        by_token = {int(item["instrument_token"]): item for item in resolved}

        def normalize(payload):
            from datetime import datetime
            item = by_token[int(payload["instrument_token"])]
            timestamp = normalize_exchange_timestamp(
                payload.get("exchange_timestamp") or datetime.now(ZoneInfo("Asia/Kolkata"))
            )
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
            if source == "replay":
                self.store.append_event("ORDER_SUPPRESSED", {
                    "decision_id": order.client_order_id, "client_order_id": order.client_order_id,
                    "vehicle": order.vehicle,
                    "decision_source": "replay", "session_date": bundle.trading_date,
                    "execution_allowed": False, "reason": "startup_recovery",
                }, f"order_suppressed:{bundle.trading_date}:{order.client_order_id}", timestamp=timestamp)
            else:
                option_bars = (getattr(bundle, "supporting_inputs", None) or {}).get("bars", {})
                futures_bar = bundle.bars.get(MarketRole.FUTURES)
                synthetic = order.vehicle == "synthetic"
                quote = (
                    synthetic_future_quote(
                        futures_bar, option_bars,
                        symbols=(order.synthetic_legs[0].symbol, order.synthetic_legs[1].symbol)
                        if order.synthetic_legs else None,
                        same_minute=True,
                    )
                    if synthetic and isinstance(futures_bar, MarketBar) else None
                )
                if synthetic and isinstance(futures_bar, MarketBar) and quote is None:
                    self.store.append_event("EXECUTION_ERROR", {
                        "decision_id": order.client_order_id, "decision_source": source,
                        "vehicle": order.vehicle,
                        "session_date": bundle.trading_date, "execution_allowed": True,
                        "outcome": "execution_error", "reason": "missing_synthetic_future_quote",
                    }, f"execution_error:synthetic_quote:{order.client_order_id}", timestamp=timestamp)
                    continue
                self._execute_paper_order(order, session_date=bundle.trading_date, timestamp=timestamp,
                                           synthetic_quote=quote,
                                           reference_price=futures_bar.close if isinstance(futures_bar, MarketBar) else None)

    def _execute_paper_order(self, order, *, session_date: str | None = None,
                             timestamp: str | None = None,
                             synthetic_quote: SyntheticFutureQuote | None = None,
                             reference_price: float | None = None) -> bool:
        self._last_execution_fill = None
        def cancel_reservation() -> None:
            if self.coordinator is not None and order.role is OrderRole.ENTRY:
                self.coordinator.cancel(order)
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
        entry: LiveEntryTrade | None = None
        entry_order_id = order.entry_order_id
        if order.role is OrderRole.EXIT:
            if entry_order_id is None:
                self.store.append_event(
                    "EXECUTION_ERROR",
                    {**context, "outcome": "execution_error", "phase": "order_validation",
                     "reason": "exit_without_entry_order_id"},
                    f"execution_error:exit_identity:{order.client_order_id}", timestamp=timestamp,
                )
                return False
            entry = self._live_entry_trades.get(entry_order_id)
            if entry is None:
                self.store.append_event(
                    "EXECUTION_ERROR",
                    {**context, "outcome": "execution_error", "phase": "order_validation",
                     "reason": "exit_without_matching_entry", "entry_order_id": entry_order_id},
                    f"execution_error:exit_entry:{order.client_order_id}", timestamp=timestamp,
                )
                return False
            try:
                validate_exit_order(
                    order,
                    entry_instrument=entry["instrument"],
                    entry_vehicle=entry["vehicle"],
                    entry_side=entry["side"],
                    entry_quantity=entry["quantity"],
                    entry_leg_symbols=(entry["ce_symbol"], entry["pe_symbol"])
                    if entry["vehicle"] == "synthetic" and entry["ce_symbol"] and entry["pe_symbol"] else None,
                )
            except ExitValidationError as exc:
                self.store.append_event(
                    "EXECUTION_ERROR",
                    {**context, "outcome": "execution_error", "phase": "order_validation",
                     "reason": exc.reason, "entry_order_id": entry_order_id},
                    f"execution_error:exit_contract:{order.client_order_id}", timestamp=timestamp,
                )
                return False
        if synthetic_quote is not None:
            context.update({"vehicle": "synthetic", "ce_symbol": synthetic_quote.ce.instrument.symbol,
                            "pe_symbol": synthetic_quote.pe.instrument.symbol, "ce_strike": synthetic_quote.strike,
                            "ce_price": synthetic_quote.ce.close, "pe_price": synthetic_quote.pe.close,
                            "synthetic_price": synthetic_quote.price})
            context.update({
                ("ce_entry_price" if order.role is OrderRole.ENTRY else "ce_exit_price"): synthetic_quote.ce.close,
                ("pe_entry_price" if order.role is OrderRole.ENTRY else "pe_exit_price"): synthetic_quote.pe.close,
                ("synthetic_entry_price" if order.role is OrderRole.ENTRY else "synthetic_exit_price"): synthetic_quote.price,
            })
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
                assert entry is not None
                try:
                    validate_exit_order(
                        order,
                        entry_instrument=entry["instrument"],
                        entry_vehicle=entry["vehicle"],
                        entry_side=entry["side"],
                        entry_quantity=entry["quantity"],
                        entry_leg_symbols=(entry["ce_symbol"], entry["pe_symbol"])
                        if entry["vehicle"] == "synthetic" and entry["ce_symbol"] and entry["pe_symbol"] else None,
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
                if synthetic_quote is not None:
                    entry_cost = synthetic_futures_cost(
                        synthetic_quote.ce.close, synthetic_quote.pe.close,
                        fill.quantity,
                        is_short=order.side is OrderSide.SELL,
                    )
                else:
                    entry_cost = futures_cost(fill.quantity)
            execution_price = synthetic_quote.price if synthetic_quote is not None else fill.price
            self._last_execution_fill = {
                "price": execution_price,
                "fill_timestamp": fill.timestamp,
                "symbol": fill.instrument.symbol,
                "quantity": fill.quantity,
            }
            cost = 0.0
            # When a PaperPortfolio is attached, it is the sole P&L/cost
            # authority. Runtime computes legacy P&L only for the deprecated
            # ledger-only path.
            if order.role is OrderRole.EXIT and self.coordinator is None:
                assert entry is not None
                entry_side = entry["side"]
                signed = 1.0 if entry_side is OrderSide.BUY else -1.0
                gross_pnl = (
                    (execution_price - entry["entry_price"])
                    * NIFTY_LOT_SIZE * entry["quantity"] * signed
                )
                if entry["vehicle"] == "synthetic":
                    ce_entry_price = entry["ce_entry_price"]
                    pe_entry_price = entry["pe_entry_price"]
                    if ce_entry_price is None or pe_entry_price is None:
                        raise ValueError("synthetic entry is missing leg prices")
                    cost = synthetic_futures_cost(
                        ce_entry_price,
                        pe_entry_price,
                        entry["quantity"],
                        is_short=entry_side is OrderSide.SELL,
                    )
                else:
                    cost = futures_cost(entry["quantity"])
                self._last_execution_fill["realized_pnl"] = gross_pnl - cost
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
                            "phase": "ledger", "error_type": type(exc).__name__, "reason": str(exc)},
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
            if synthetic_quote is not None and order.role is OrderRole.ENTRY:
                self._synthetic_symbols_by_order[order.client_order_id] = (
                    synthetic_quote.ce.instrument.symbol, synthetic_quote.pe.instrument.symbol,
                )
                self._last_synthetic_quote_by_order[order.client_order_id] = synthetic_quote
            elif synthetic_quote is not None:
                if order.entry_order_id is not None:
                    self._last_synthetic_quote_by_order[order.entry_order_id] = synthetic_quote
            position = None
            if self.ledger and self.coordinator is None:
                try:
                    for item in fills:
                        leg_side = order.side
                        if order.vehicle == "synthetic" and item.instrument.instrument_type.upper() == "PE":
                            leg_side = OrderSide.SELL if order.side is OrderSide.BUY else OrderSide.BUY
                        position = self.ledger.apply_fill(item, leg_side, vehicle=order.vehicle)
                except Exception as exc:
                    compensated = compensate_fills(fills, ack.broker_order_id)
                    if compensated:
                        cancel_reservation()
                    self.store.append_event("EXECUTION_ERROR", {**context, "outcome": "execution_error",
                        "phase": "ledger", "error_type": type(exc).__name__, "reason": str(exc)},
                        f"execution_error:ledger:{order.client_order_id}", timestamp=timestamp)
                    return False
            if order.role is OrderRole.EXIT:
                assert entry_order_id is not None
                self._live_entry_trades.pop(entry_order_id, None)
            if order.role is OrderRole.ENTRY:
                self._live_entry_trades[order.client_order_id] = {
                    "instrument": order.instrument.symbol,
                    "entry_price": execution_price,
                    "quantity": fill.quantity,
                    "side": order.side,
                    "vehicle": order.vehicle,
                    "ce_symbol": synthetic_quote.ce.instrument.symbol if synthetic_quote is not None else None,
                    "pe_symbol": synthetic_quote.pe.instrument.symbol if synthetic_quote is not None else None,
                    "ce_entry_price": synthetic_quote.ce.close if synthetic_quote is not None else None,
                    "pe_entry_price": synthetic_quote.pe.close if synthetic_quote is not None else None,
                }
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
                                              **({"position": {
                                                  "symbol": position.symbol,
                                                  "vehicle": position.vehicle,
                                                  "quantity": position.quantity,
                                                  "average_price": position.average_price,
                                              }} if position else {})}, f"fill:{fill.client_order_id}", timestamp=timestamp)
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
        realized_pnl = (
            self._last_execution_fill.get("realized_pnl")
            if self._last_execution_fill is not None else None
        )
        if self.coordinator is None and isinstance(realized_pnl, (int, float)):
            strategy = getattr(self.engine, "strategy", None)
            update_portfolio_state = getattr(strategy, "update_portfolio_state", None)
            portfolio_state = getattr(strategy, "portfolio_state", {})
            if callable(update_portfolio_state) and isinstance(portfolio_state, dict):
                current_equity = float(portfolio_state.get("current_equity", 0.0)) + float(realized_pnl)
                peak_equity = max(float(portfolio_state.get("peak_equity", current_equity)), current_equity)
                update_portfolio_state(equity=current_equity, peak_equity=peak_equity)
        if fill and self.ledger and self.coordinator is None:
            capital_config = self.capital_config
            if capital_config is None:
                raise RuntimeError("capital_config is required when a ledger is configured")
            self.store.patch_status({"initial_capital": capital_config.initial_capital,
                                     "max_daily_loss": capital_config.max_daily_loss,
                                     "max_net_directional_lots": capital_config.max_net_directional_lots,
                                      "capital": self.ledger.cash, "open_positions": [
                 p.__dict__ if hasattr(p, "__dict__") else {"symbol": p.symbol, "quantity": p.quantity,
                 "average_price": p.average_price, "vehicle": p.vehicle} for p in self.ledger.positions()],
                                      "open_entry_trades": {
                                          entry_id: {**entry, "side": entry["side"].value}
                                          for entry_id, entry in self._live_entry_trades.items()
                                      }})
        return bool(fill)

    def _synthetic_symbols_for_exit(self, order) -> tuple[str, str] | None:
        if order.synthetic_legs:
            return tuple(leg.symbol for leg in order.synthetic_legs)
        return self._synthetic_symbols_by_order.get(order.entry_order_id)

    def _synthetic_exit_quote(self, order, bar: MarketBar,
                              option_bars: Mapping[Role, MarketBar] | None = None) -> SyntheticFutureQuote | None:
        quote = synthetic_future_quote(
            bar, self._latest_option_bars if option_bars is None else option_bars,
            symbols=self._synthetic_symbols_for_exit(order),
        )
        if quote is not None or option_bars is not None:
            return quote
        return self._last_synthetic_quote_by_order.get(order.entry_order_id)

    def _record_missing_synthetic_exit(self, order, timestamp: str) -> None:
        self.store.append_event("EXECUTION_ERROR", {
            "decision_id": order.client_order_id, "decision_source": "live",
            "execution_allowed": True, "outcome": "execution_error",
            "reason": "missing_synthetic_future_quote",
        }, f"execution_error:synthetic_quote:{order.client_order_id}", timestamp=timestamp)

    def on_closed_bar(self, bar) -> None:
        with self._lock:
            if self._stopping or self.store.read_status().get("state") != "RUNNING":
                return
            if bar.instrument.instrument_type.upper() in {"CE", "PE"}:
                self._latest_option_bars[OptionRole(bar.instrument.symbol)] = bar
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
            option_snapshot: dict[Role, MarketBar] | None = None
            if bundle is not None:
                supporting = getattr(bundle, "supporting_inputs", None) or {}
                raw_bars = supporting.get("bars", {})
                if isinstance(raw_bars, dict):
                    option_snapshot = {
                        role: value for role, value in raw_bars.items()
                        if isinstance(role, OptionRole) and isinstance(value, MarketBar)
                    }

            # Settle positions carried into this bar before evaluating a new
            # bundle, matching the deterministic replay ordering.
            for action in self.engine.on_closed_bar(bar):
                synthetic_quote = (
                    self._synthetic_exit_quote(action.intent, bar, option_snapshot)
                    if action.intent.vehicle == "synthetic" else None
                )
                if action.intent.vehicle == "synthetic" and synthetic_quote is None:
                    self._record_missing_synthetic_exit(action.intent, bar.timestamp.isoformat())
                    self.engine.settle_exit(action.intent.client_order_id, filled=False)
                    continue
                filled = self._execute_paper_order(
                    action.intent,
                    session_date=bar.timestamp.astimezone(ZoneInfo("Asia/Kolkata")).date().isoformat(),
                    timestamp=bar.timestamp.isoformat(),
                    synthetic_quote=synthetic_quote,
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
            if bar.instrument.instrument_type.upper() in {"CE", "PE"}:
                self._latest_option_bars[OptionRole(bar.instrument.symbol)] = bar
            if isinstance(self.broker, PaperBroker):
                self.broker.update_price(bar.instrument.symbol, bar.close)
            result = self.engine.on_tick(bar)
            for action in result:
                synthetic_quote = (self._synthetic_exit_quote(action.intent, bar)
                                   if action.intent.vehicle == "synthetic" else None)
                if action.intent.vehicle == "synthetic" and synthetic_quote is None:
                    self._record_missing_synthetic_exit(action.intent, bar.timestamp.isoformat())
                    self.engine.settle_exit(action.intent.client_order_id, filled=False)
                    continue
                filled = self._execute_paper_order(
                    action.intent,
                    session_date=bar.timestamp.astimezone(ZoneInfo("Asia/Kolkata")).date().isoformat(),
                    timestamp=bar.timestamp.isoformat(),
                    synthetic_quote=synthetic_quote,
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
            self.store.patch_status({"feed_health": health, "feed_connected": bool(health.get("connected"))})

    def stop(self) -> None:
        with self._lock:
            startup = self._thread
            self._stopping = True
            self.store.patch_status({"state": "STOPPING", "feed_connected": False})
            if self.feed:
                self.feed.stop()
            if self.broker:
                self.broker.close()
            self.store.patch_status({"state": "STOPPED", "bars_seen": self.engine.bars_seen,
                                     "pending_bundle_minutes": [], "pending_bundle_details": []})
        if startup and startup is not threading.current_thread():
            startup.join()
        with self._lock:
            try:
                self.store.patch_status({"state": "STOPPED", "bars_seen": self.engine.bars_seen})
            except Exception as exc:
                self.store.patch_status({"state": "ERROR", "error": str(exc)})

    def restart(self) -> None:
        self.stop()
        self.start()
