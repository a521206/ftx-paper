from __future__ import annotations

import threading
import time
import logging
from copy import deepcopy
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable
from zoneinfo import ZoneInfo

from ftx_paper.broker import Broker, PaperBroker
from ftx_paper.contracts import (
    Instrument, MarketBar, MarketRole, OptionRole, OptionType, OrderRole, OrderSide, SyntheticFutureQuote, normalize_exchange_timestamp, parse_role,
    synthetic_future_quote,
    role_to_key,
)
from ftx_paper.core import AggregatorConfig, CompletedBarAggregator, InstrumentKey, PaperEngine
from ftx_paper.execution import PositionLedger
from .events import is_decision_event, serialize_datetime
from .store import RuntimeStore


logger = logging.getLogger("ftx-paper")

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
                 feed_factory: Callable[..., Any] | None = None, broker_factory: Callable[[Any], Broker] | None = None,
                 client_factory: Callable[[], Any] | None = None, normalize_payload: Callable[..., Any] | None = None) -> None:
        self.store, self.auth, self.specifications = store, auth, specifications
        self.engine, self.ledger = engine or PaperEngine(), ledger
        self._restore_ledger_state()
        self.feed_factory, self.broker_factory = feed_factory, broker_factory
        self.client_factory, self.normalize_payload = client_factory, normalize_payload
        self.feed = None
        self.broker = None
        self._thread = None
        self._lock = threading.RLock()
        self._stopping = False
        self._started = threading.Event()
        self._aggregator: CompletedBarAggregator | None = None
        self._replaying = False
        self._replay_date: str | None = None
        self._last_execution_fill: dict[str, object] | None = None
        self._latest_option_bars: dict[OptionRole, MarketBar] = {}
        self._synthetic_symbols_by_order: dict[str, tuple[str, str]] = {}
        self._last_synthetic_quote_by_order: dict[str, SyntheticFutureQuote] = {}

    def _restore_ledger_state(self) -> None:
        if self.ledger is None:
            return
        status = self.store.read_status()
        raw_cash = status.get("capital", self.ledger.cash)
        if isinstance(raw_cash, dict):
            raw_cash = raw_cash.get("capital", raw_cash.get("current_equity", self.ledger.cash))
        raw_positions = status.get("open_positions", ())
        if not isinstance(raw_positions, (list, tuple)):
            raw_positions = ()
        self.ledger.restore_state(cash=float(raw_cash), positions=raw_positions)

    def start(self) -> None:
        with self._lock:
            if self.store.read_status().get("state") in {"STARTING", "RUNNING"}:
                return
            self._stopping = False
            self._started.clear()
            self.store.patch_status({"state": "STARTING", "error": None, "started_at": datetime.now(timezone.utc).isoformat(),
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
            self.broker = (self.broker_factory or (lambda _client: PaperBroker()))(client)
            self._replaying = True
            reset = getattr(self.engine, "reset", None)
            if callable(reset):
                reset()
            self.store.patch_status({"phase": "REPLAYING", "execution_enabled": False,
                                     "replay_date": None, "replay_bars_seen": 0})
            # Replay every resolved instrument so supporting inputs (notably
            # option volumes used for PCR) are present in decision bundles.
            backfill = load_startup_backfill(client, resolved)
            self.store.append_market_bars(backfill, source="historical_backfill")
            self._replay_dates(backfill)
            self._replaying = False
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
            if str(item["symbol"]).upper() in {"INDIA VIX", "INDIAVIX"}:
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

    def _replay_dates(self, bars: tuple[MarketBar, ...]) -> None:
        """Rebuild every stored trading date without executing orders."""
        if self._aggregator is None:
            return
        dates = sorted({
            bar.timestamp.astimezone(ZoneInfo("Asia/Kolkata")).date().isoformat()
            for bar in bars
        })
        replayed_bars = 0
        # Clear any recovered live engine state before taking the isolated
        # replay copy. The live engine is not used to evaluate replay bars.
        live_reset = getattr(self.engine, "reset", None)
        if callable(live_reset):
            live_reset()
        replay_engine = deepcopy(self.engine)
        reset = getattr(replay_engine, "reset", None)
        if callable(reset):
            reset()
        for index, session_date in enumerate(dates):
            if index:
                reset = getattr(replay_engine, "reset", None)
                if callable(reset):
                    reset()
            self._replay_date = session_date
            self.store.patch_status({"replay_date": session_date})
            self.store.clear_replay_events(session_date)
            for bar in bars:
                bar_date = bar.timestamp.astimezone(ZoneInfo("Asia/Kolkata")).date().isoformat()
                if bar_date != session_date:
                    continue
                replayed_bars += 1
                bundle = self._aggregator.ingest(bar)
                if bundle is not None:
                    self._process_replay_bundle(bundle, replay_engine)
                    self.store.patch_status({"replay_last_bar": bundle.minute, "replay_bars_seen": replayed_bars})
            for bundle in self._aggregator.flush(incomplete=False):
                self._process_replay_bundle(bundle, replay_engine)

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

    def _process_replay_bundle(self, bundle, replay_engine: PaperEngine) -> None:
        """Process startup replay without entering the live bundle path."""
        self._process_bundle_for_source(bundle, source="replay", engine=replay_engine)

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
                    "decision_source": "replay", "session_date": bundle.trading_date,
                    "execution_allowed": False, "reason": "startup_recovery",
                }, f"order_suppressed:{bundle.trading_date}:{order.client_order_id}", timestamp=timestamp)
            else:
                option_bars = (getattr(bundle, "supporting_inputs", None) or {}).get("bars", {})
                futures_bar = bundle.bars.get(MarketRole.FUTURES)
                quote = synthetic_future_quote(futures_bar, option_bars) if isinstance(futures_bar, MarketBar) else None
                if isinstance(futures_bar, MarketBar) and quote is None:
                    self.store.append_event("EXECUTION_ERROR", {
                        "decision_id": order.client_order_id, "decision_source": source,
                        "session_date": bundle.trading_date, "execution_allowed": True,
                        "outcome": "execution_error", "reason": "missing_synthetic_future_quote",
                    }, f"execution_error:synthetic_quote:{order.client_order_id}", timestamp=timestamp)
                    continue
                self._execute_paper_order(order, session_date=bundle.trading_date, timestamp=timestamp, synthetic_quote=quote)

    def _execute_paper_order(self, order, *, session_date: str | None = None,
                             timestamp: str | None = None,
                             synthetic_quote: SyntheticFutureQuote | None = None) -> bool:
        self._last_execution_fill = None
        context = {
            "decision_id": order.client_order_id,
            "client_order_id": order.client_order_id,
            "decision_source": "live",
            "session_date": session_date,
            "execution_allowed": True,
            "reason": order.reason,
        }
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
            if order.role is OrderRole.ENTRY:
                self._synthetic_symbols_by_order[order.client_order_id] = (
                    synthetic_quote.ce.instrument.symbol, synthetic_quote.pe.instrument.symbol,
                )
                self._last_synthetic_quote_by_order[order.client_order_id] = synthetic_quote
            else:
                entry_id = self._synthetic_entry_id(order)
                self._last_synthetic_quote_by_order[entry_id] = synthetic_quote
            if isinstance(self.broker, PaperBroker):
                # One paper fill represents both option legs at the current quote.
                self.broker.update_price(order.instrument.symbol, synthetic_quote.price)
        if self.broker is None:
            self.store.append_event("EXECUTION_ERROR", {**context,
                                                         "outcome": "execution_error",
                                                         "phase": "submit",
                                                         "reason": "broker_unavailable"},
                                    f"execution_error:submit:{order.client_order_id}", timestamp=timestamp)
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
            return False
        self.store.append_event("ORDER_ACK", {**context,
                                               "client_order_id": ack.client_order_id,
                                               "broker_order_id": ack.broker_order_id,
                                               "status": ack.status,
                                               "outcome": "acknowledged"}, f"order_ack:{ack.client_order_id}", timestamp=timestamp)
        try:
            fill = self.broker.poll_fill(order, ack.broker_order_id)
        except Exception as exc:
            self.store.append_event("EXECUTION_ERROR", {**context,
                                                         "broker_order_id": ack.broker_order_id,
                                                         "outcome": "execution_error",
                                                         "phase": "fill_poll",
                                                         "error_type": type(exc).__name__,
                                                         "reason": str(exc)},
                                    f"execution_error:fill_poll:{order.client_order_id}", timestamp=timestamp)
            return False
        if fill:
            self._last_execution_fill = {
                "price": fill.price,
                "fill_timestamp": fill.timestamp,
                "symbol": fill.instrument.symbol,
                "quantity": fill.quantity,
            }
            register_entry = getattr(self.engine, "register_entry", None)
            if callable(register_entry):
                try:
                    register_entry(order, fill_price=fill.price, entry_fill_time=timestamp)
                except TypeError as exc:
                    if "entry_fill_time" not in str(exc):
                        raise
                    register_entry(order, fill_price=fill.price)
            position = None
            if self.ledger:
                position = self.ledger.apply_fill(fill, order.side if order.side in (OrderSide.BUY, OrderSide.SELL) else OrderSide.BUY)
            self.store.append_event("FILL", {**context,
                                              "broker_order_id": ack.broker_order_id,
                                              "symbol": fill.instrument.symbol,
                                              "quantity": fill.quantity,
                                              "price": fill.price,
                                              "fill_timestamp": fill.timestamp,
                                              "outcome": "filled",
                                              **({"position": {
                                                  "symbol": position.symbol,
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
            return False
        if fill and self.ledger:
            self.store.patch_status({"capital": self.ledger.cash, "open_positions": [
                p.__dict__ if hasattr(p, "__dict__") else {"symbol": p.symbol, "quantity": p.quantity,
                "average_price": p.average_price} for p in self.ledger.positions()]})
        return bool(fill)

    def _synthetic_symbols_for_exit(self, order) -> tuple[str, str] | None:
        entry_id = self._synthetic_entry_id(order)
        return self._synthetic_symbols_by_order.get(entry_id)

    @staticmethod
    def _synthetic_entry_id(order) -> str:
        return str(order.client_order_id).removeprefix("exit-").split("-", 1)[0]

    def _synthetic_exit_quote(self, order, bar: MarketBar) -> SyntheticFutureQuote | None:
        quote = synthetic_future_quote(
            bar, self._latest_option_bars,
            symbols=self._synthetic_symbols_for_exit(order),
        )
        return quote or self._last_synthetic_quote_by_order.get(self._synthetic_entry_id(order))

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
                result = self.engine.on_bar(bar)
                for event in result.events:
                    self._persist_engine_event(event, source="live", bundle_id=bar.timestamp.isoformat(),
                                                session_date=bar.timestamp.astimezone(ZoneInfo("Asia/Kolkata")).date().isoformat(),
                                                decision_at=bar.timestamp, timestamp=bar.timestamp.isoformat())
                for order in result.orders:
                    self._execute_paper_order(
                        order,
                        session_date=bar.timestamp.astimezone(ZoneInfo("Asia/Kolkata")).date().isoformat(),
                        timestamp=bar.timestamp.isoformat(),
                    )
            else:
                # Settle positions carried into this bar before evaluating a
                # new bundle, matching the deterministic replay ordering.
                for action in self.engine.on_closed_bar(bar):
                    synthetic_quote = self._synthetic_exit_quote(action.intent, bar)
                    if synthetic_quote is None:
                        self._record_missing_synthetic_exit(action.intent, bar.timestamp.isoformat())
                        continue
                    filled = self._execute_paper_order(
                        action.intent,
                        session_date=bar.timestamp.astimezone(ZoneInfo("Asia/Kolkata")).date().isoformat(),
                        timestamp=bar.timestamp.isoformat(),
                        synthetic_quote=synthetic_quote,
                    )
                    self.engine.settle_exit(action.intent.client_order_id, filled=filled)
                    self.store.append_event("EXITDECISION", {
                        "decision_id": action.intent.client_order_id,
                        "cell": action.cell,
                        "reason": action.reason,
                        "exit_price": action.price,
                        "trigger_price": action.price,
                        "fill_price": (self._last_execution_fill or {}).get("price"),
                        "market_time": bar.timestamp.isoformat(),
                        "processing_time": datetime.now(timezone.utc).isoformat(),
                        "trigger_source": "on_closed_bar",
                        "decision_at": bar.timestamp.isoformat(),
                        "decision_source": "live",
                        "execution_allowed": True,
                        "outcome": "exit_triggered",
                    }, f"exit_decision:{action.intent.client_order_id}", timestamp=bar.timestamp.isoformat())
                expired = self._aggregator.expire(now=time.monotonic())
                for incomplete in expired:
                    self._record_incomplete(incomplete)
                bundle = self._aggregator.ingest(bar)
                self._refresh_pending_status()
                if bundle is not None:
                    self.store.patch_status({"last_completed_bundle_minute": bundle.minute,
                                             "last_strategy_evaluation_minute": bundle.minute})
                    self._process_bundle(bundle, source="live")

    def on_tick(self, bar: MarketBar) -> None:
        """Evaluate protective exits immediately on each live market tick."""
        with self._lock:
            if self._stopping or self._replaying or self.store.read_status().get("state") != "RUNNING":
                return
            if bar.instrument.instrument_type.upper() in {"CE", "PE"}:
                self._latest_option_bars[OptionRole(bar.instrument.symbol)] = bar
            if isinstance(self.broker, PaperBroker):
                self.broker.update_price(bar.instrument.symbol, bar.close)
            result = self.engine.on_tick(bar)
            for action in result:
                synthetic_quote = self._synthetic_exit_quote(action.intent, bar)
                if synthetic_quote is None:
                    self._record_missing_synthetic_exit(action.intent, bar.timestamp.isoformat())
                    continue
                filled = self._execute_paper_order(
                    action.intent,
                    session_date=bar.timestamp.astimezone(ZoneInfo("Asia/Kolkata")).date().isoformat(),
                    timestamp=bar.timestamp.isoformat(),
                    synthetic_quote=synthetic_quote,
                )
                self.engine.settle_exit(action.intent.client_order_id, filled=filled)
                self.store.append_event("EXITDECISION", {
                    "decision_id": action.intent.client_order_id,
                    "cell": action.cell,
                    "reason": action.reason,
                    "exit_price": action.price,
                    "trigger_price": action.price,
                    "fill_price": (self._last_execution_fill or {}).get("price"),
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
