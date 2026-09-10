from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any, Callable

from ftx_paper.broker import Broker
from ftx_paper.contracts import Instrument, MarketBar, OrderSide
from ftx_paper.core import PaperEngine
from ftx_paper.execution import PositionLedger
from .store import RuntimeStore


class RuntimeSession:
    """Single in-process owner of live market, execution, and runtime state."""

    def __init__(self, store: RuntimeStore, auth: Any, specifications: list[dict[str, object]],
                 *, engine: PaperEngine | None = None, ledger: PositionLedger | None = None,
                 feed_factory: Callable[..., Any] | None = None, broker_factory: Callable[[Any], Broker] | None = None,
                 client_factory: Callable[[], Any] | None = None, normalize_payload: Callable[..., Any] | None = None) -> None:
        self.store, self.auth, self.specifications = store, auth, specifications
        self.engine, self.ledger = engine or PaperEngine(), ledger
        self.feed_factory, self.broker_factory = feed_factory, broker_factory
        self.client_factory, self.normalize_payload = client_factory, normalize_payload
        self.feed = None
        self.broker = None
        self._thread = None
        self._lock = threading.RLock()
        self._stopping = False
        self._started = threading.Event()

    def start(self) -> None:
        with self._lock:
            if self.store.read_status().get("state") in {"STARTING", "RUNNING"}:
                return
            self._stopping = False
            self._started.clear()
            self.store.patch_status({"state": "STARTING", "error": None, "started_at": datetime.now(timezone.utc).isoformat()})
            self._thread = threading.Thread(target=self._start_impl, daemon=True, name="ftx-paper-runtime")
            self._thread.start()

    def _start_impl(self) -> None:
        try:
            if self.auth is None or self.auth.access_token() is None:
                raise ValueError("Zerodha authentication required")
            from ftx_paper.broker.zerodha import ZerodhaBroker, ZerodhaFeed, classify_runtime_roles, create_kite_socket, load_startup_backfill, resolve_instruments
            client = self.client_factory() if self.client_factory else self.auth.authenticated_client()
            resolved = resolve_instruments(client, self.specifications)
            roles = classify_runtime_roles(resolved)
            for bar in load_startup_backfill(client, [roles["futures"], roles["vix"]]):
                self.engine.on_bar(bar)
            normalize = self.normalize_payload or self._make_normalizer(resolved)
            socket = create_kite_socket(self.auth.api_key, self.auth.access_token())
            feed_type = self.feed_factory or ZerodhaFeed
            self.broker = (self.broker_factory or ZerodhaBroker)(client)
            self.feed = feed_type(socket, [int(item["instrument_token"]) for item in resolved], normalize, self.on_closed_bar)
            with self._lock:
                if self._stopping:
                    self.feed.stop()
                    self.broker.close()
                    return
                self.feed.start()
                self.store.patch_status({"state": "RUNNING", "feed_connected": True})
                self._started.set()
        except Exception as exc:
            self.store.patch_status({"state": "ERROR", "error": str(exc)})
            self._started.set()

    @staticmethod
    def _make_normalizer(resolved):
        by_token = {int(item["instrument_token"]): item for item in resolved}

        def normalize(payload):
            from datetime import datetime
            item = by_token[int(payload["instrument_token"])]
            timestamp = payload.get("exchange_timestamp") or datetime.now().astimezone()
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            price = float(payload["last_price"])
            instrument = Instrument(str(item["symbol"]), str(item["exchange"]), str(item.get("instrument_type", "INDEX")))
            return MarketBar(instrument, timestamp.replace(second=0, microsecond=0), price, price, price, price, payload.get("volume_traded"), payload.get("oi"))

        return normalize

    def on_closed_bar(self, bar) -> None:
        with self._lock:
            if self._stopping or self.store.read_status().get("state") != "RUNNING":
                return
            result = self.engine.on_bar(bar)
            now = datetime.now(timezone.utc).isoformat()
            self.store.patch_status({"last_bar_at": bar.timestamp.isoformat(), "last_heartbeat_at": now})
            metadata = self.engine.strategy_metadata
            for event in result.events:
                payload = dict(event)
                if metadata:
                    payload.update(strategy_version=metadata.version, config_hash=metadata.config_hash)
                self.store.append_event("ENGINE_EVENT", payload, f"decision:{bar.timestamp.isoformat()}:{self.engine.bars_seen}")
            for order in result.orders:
                ack = self.broker.submit(order)
                self.store.append_event("ORDER_ACK", {"client_order_id": ack.client_order_id, "broker_order_id": ack.broker_order_id, "status": ack.status}, f"order_ack:{ack.client_order_id}")
                fill = self.broker.poll_fill(order, ack.broker_order_id) if hasattr(self.broker, "poll_fill") else (self.broker.fills[-1] if getattr(self.broker, "fills", None) else None)
                if fill and self.ledger:
                    position = self.ledger.apply_fill(fill, order.side if order.side in (OrderSide.BUY, OrderSide.SELL) else OrderSide.BUY)
                    self.store.append_event("FILL", {"client_order_id": fill.client_order_id, "symbol": position.symbol, "quantity": position.quantity, "average_price": position.average_price}, f"fill:{fill.client_order_id}")
                    self.store.patch_status({"capital": self.ledger.cash, "open_positions": [p.__dict__ if hasattr(p, "__dict__") else {"symbol": p.symbol, "quantity": p.quantity, "average_price": p.average_price} for p in self.ledger.positions()]})

    def stop(self) -> None:
        with self._lock:
            self._stopping = True
            self.store.patch_status({"state": "STOPPING", "feed_connected": False})
            startup = self._thread
            if self.feed:
                self.feed.stop()
        if startup and startup is not threading.current_thread():
            startup.join()
        with self._lock:
            try:
                if self.feed:
                    self.feed.flush()
                if self.broker:
                    self.broker.close()
                self.store.patch_status({"state": "STOPPED", "bars_seen": self.engine.bars_seen})
            except Exception as exc:
                self.store.patch_status({"state": "ERROR", "error": str(exc)})

    def restart(self) -> None:
        self.stop()
        self.start()
