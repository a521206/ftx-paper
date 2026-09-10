from __future__ import annotations

from datetime import datetime, timezone

from ftx_paper.broker import Broker, MarketFeed
from ftx_paper.contracts import OrderSide
from ftx_paper.core import PaperEngine
from ftx_paper.execution import PositionLedger

from .store import RuntimeStore


def bootstrap_zerodha(auth, specifications, *, engine, store, normalize_payload, client_factory=None):
    """Compose broker adapters and warm up the pure strategy before live bars."""
    from ftx_paper.broker.zerodha import ZerodhaBroker, ZerodhaFeed, classify_runtime_roles, create_kite_socket, load_startup_backfill, resolve_instruments
    client = client_factory() if client_factory is not None else auth.authenticated_client()
    resolved = resolve_instruments(client, specifications)
    roles = classify_runtime_roles(resolved)
    warmup = load_startup_backfill(client, [roles["futures"], roles["vix"]])
    for bar in warmup:
        engine.on_bar(bar)
    socket = create_kite_socket(auth.api_key, auth.access_token())
    feed = ZerodhaFeed(socket, [int(item["instrument_token"]) for item in resolved], normalize_payload, engine.on_bar)
    return RuntimeWorker(store, engine, feed, ZerodhaBroker(client)), roles


class RuntimeWorker:
    def __init__(self, store: RuntimeStore, engine: PaperEngine, feed: MarketFeed, broker: Broker, ledger: PositionLedger | None = None) -> None:
        self.store, self.engine, self.feed, self.broker = store, engine, feed, broker
        self.ledger = ledger

    def run(self) -> None:
        self.store.recover_interrupted()
        initial = {"state": "RUNNING", "started_at": datetime.now(timezone.utc).isoformat()}
        metadata = self.engine.strategy_metadata
        if metadata is not None:
            initial["strategy"] = {
                "name": metadata.name,
                "version": metadata.version,
                "config_hash": metadata.config_hash,
            }
        if self.ledger is not None:
            initial.update({"capital": self.ledger.cash, "open_positions": []})
        self.store.patch_status(initial)
        try:
            self._consume_commands()
            for bar in self.feed.bars():
                self._consume_commands()
                result = self.engine.on_bar(bar)
                for event in result.events:
                    payload = dict(event)
                    if metadata is not None:
                        payload["strategy_version"] = metadata.version
                        payload["config_hash"] = metadata.config_hash
                    self.store.append_event(
                        "ENGINE_EVENT", payload,
                        f"decision:{bar.timestamp.isoformat()}:{self.engine.bars_seen}",
                    )
                for order in result.orders:
                    ack = self.broker.submit(order)
                    self.store.append_event("ORDER_ACK", {
                        "client_order_id": ack.client_order_id,
                        "broker_order_id": ack.broker_order_id,
                        "status": ack.status,
                    }, f"order_ack:{ack.client_order_id}")
                    if self.ledger is not None and hasattr(self.broker, "fills"):
                        fills = getattr(self.broker, "fills")
                        if fills:
                            fill = fills[-1]
                            side = order.side if order.side in (OrderSide.BUY, OrderSide.SELL) else OrderSide.BUY
                            position = self.ledger.apply_fill(fill, side)
                            self.store.append_event("FILL", {
                                "client_order_id": fill.client_order_id,
                                "symbol": position.symbol,
                                "quantity": position.quantity,
                                "average_price": position.average_price,
                            }, f"fill:{fill.client_order_id}")
                            self.store.patch_status({
                                "capital": self.ledger.cash,
                                "open_positions": [
                                    {"symbol": p.symbol, "quantity": p.quantity, "average_price": p.average_price}
                                    for p in self.ledger.positions()
                                ],
                            })
        finally:
            self.broker.close()
            self.store.patch_status({"state": "STOPPED", "bars_seen": self.engine.bars_seen})

    def _consume_commands(self) -> None:
        for item in self.store.claim_commands():
            self.store.append_event("RUNTIME_COMMAND", item)
