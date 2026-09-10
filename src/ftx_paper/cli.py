from __future__ import annotations

import os
import json
from datetime import datetime

from ftx_paper.api import create_app
from ftx_paper.config import PaperConfig
from ftx_paper.runtime import RuntimeStore
from ftx_paper.strategy import ConfiguredLiveStrategy
from ftx_paper.core import PaperEngine
from ftx_paper.ui import create_ui_app
from ftx_paper.broker.zerodha import ZerodhaAuth
from ftx_paper.broker.zerodha import classify_runtime_roles, resolve_instruments
from ftx_paper.broker.zerodha import create_kite_socket, ZerodhaBroker, ZerodhaFeed
from ftx_paper.contracts import Instrument, MarketBar


def api_main() -> None:
    config = PaperConfig.from_env()
    store = RuntimeStore(config.runtime_dir)
    try:
        auth = ZerodhaAuth.from_config_path(config.zerodha_config)
    except ValueError:
        auth = None
    create_app(store, zerodha_auth=auth).run(host=config.host, port=config.port, debug=False, use_reloader=False)


def worker_main() -> None:
    config = PaperConfig.from_env()
    auth = ZerodhaAuth.from_config_path(config.zerodha_config)
    if not config.zerodha_config.exists():
        raise SystemExit(f"Zerodha config not found: {config.zerodha_config}")
    raw = json.loads(config.zerodha_config.read_text(encoding="utf-8"))
    specifications = list(raw.get("instruments", ()))
    if auth.access_token() is None:
        print(f"Connect Zerodha before starting the worker: {auth.login_url()}")
        raise SystemExit(2)
    client = auth.authenticated_client()
    resolved = resolve_instruments(client, specifications)
    classify_runtime_roles(resolved)
    engine = PaperEngine(ConfiguredLiveStrategy())
    socket = create_kite_socket(auth.api_key, auth.access_token() or "")
    by_token = {int(item["instrument_token"]): item for item in resolved}

    def normalize(payload):
        item = by_token[int(payload["instrument_token"])]
        timestamp = payload.get("exchange_timestamp") or datetime.now().astimezone()
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        price = float(payload["last_price"])
        instrument = Instrument(str(item["symbol"]), str(item["exchange"]), str(item.get("instrument_type", "INDEX")))
        return MarketBar(instrument, timestamp.replace(second=0, microsecond=0), price, price, price, price, payload.get("volume_traded"), payload.get("oi"))

    feed = ZerodhaFeed(socket, list(by_token), normalize, engine.on_bar)
    feed.start()
    print(f"ftx-paper Zerodha pipeline started with {len(resolved)} instruments")
    try:
        import signal
        signal.pause()
    except (AttributeError, KeyboardInterrupt):
        pass
    finally:
        feed.stop()
        feed.flush()
        ZerodhaBroker(client).close()


def ui_main() -> None:
    config = PaperConfig.from_env()
    ui_port = int(os.getenv("FTX_UI_PORT", "8502"))
    create_ui_app().run(host=config.host, port=ui_port, debug=False, use_reloader=False)
