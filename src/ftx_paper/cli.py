from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

from ftx_paper.api import create_app
from ftx_paper.application import RuntimeOperations
from ftx_paper.config import PaperConfig
from ftx_paper.domain.capital import CapitalRuntimeContext, RESEARCH_CAPITAL_PROFILE
from ftx_paper.domain.portfolio import PortfolioState
from ftx_paper.infrastructure.sqlite.runtime_store import ProcessAlreadyRunningError
from ftx_paper.runtime.session import RuntimeSession
from ftx_paper.runtime.store import RuntimeStore
from ftx_paper.runtime.replay_worker import ReplayWorker
from ftx_paper.strategy import ConfiguredStrategyFactory
from ftx_paper.runtime.engine import MultiStrategyEngine
from ftx_paper.ui import create_ui_app
from ftx_paper.broker.shoonya import ShoonyaAuth
from ftx_paper.broker.xts import XTSAuth
from ftx_paper.broker.zerodha import ZerodhaAuth


def configure_logging(runtime_dir: Path) -> None:
    """Send durable diagnostics to a rotating file; mirror only problems to stderr."""
    runtime_dir.mkdir(parents=True, exist_ok=True)
    log_path = runtime_dir / "runtime.log"
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    if not any(
        isinstance(handler, RotatingFileHandler)
        and Path(getattr(handler, "baseFilename", "")).resolve() == log_path.resolve()
        for handler in root.handlers
    ):
        file_handler = RotatingFileHandler(
            log_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
    if not any(
        isinstance(handler, logging.StreamHandler)
        and not isinstance(handler, RotatingFileHandler)
        for handler in root.handlers
    ):
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.WARNING)
        stream_handler.setFormatter(formatter)
        root.addHandler(stream_handler)


def api_main() -> None:
    config = PaperConfig.from_env()
    configure_logging(config.runtime_dir)
    capital_profile = RESEARCH_CAPITAL_PROFILE
    portfolio = PortfolioState(capital_profile.initial_capital)
    capital_context = CapitalRuntimeContext(capital_profile, environment="live")
    strategy_factory = ConfiguredStrategyFactory(capital_profile=capital_profile)
    store = RuntimeStore(config.runtime_dir)
    try:
        instance_id = store.acquire_process_lease("api")
    except ProcessAlreadyRunningError as exc:
        raise SystemExit(str(exc)) from exc
    session = None
    try:
        broker_name = config.broker.lower()
        if broker_name == "xts":
            try:
                auth = XTSAuth.from_config_path(config.xts_config, token_db=config.auth_db)
            except ValueError:
                auth = None
            import json
            raw_value = json.loads(config.xts_config.read_text(encoding="utf-8")) if config.xts_config.exists() else {}
            if not isinstance(raw_value, dict):
                raise ValueError("XTS configuration must be a JSON object")
            raw = raw_value
            instruments_value = raw.get("instruments", ())
            if not isinstance(instruments_value, list) or any(not isinstance(item, dict) for item in instruments_value):
                raise ValueError("XTS instruments must be a list of objects")
        elif broker_name == "shoonya":
            try:
                auth = ShoonyaAuth.from_config_path(config.shoonya_config, token_db=config.auth_db)
            except ValueError:
                auth = None
            import json
            raw_value = json.loads(config.shoonya_config.read_text(encoding="utf-8")) if config.shoonya_config.exists() else {}
            if not isinstance(raw_value, dict):
                raise ValueError("Shoonya configuration must be a JSON object")
            raw = raw_value
            instruments_value = raw.get("instruments", ())
            if not isinstance(instruments_value, list) or any(not isinstance(item, dict) for item in instruments_value):
                raise ValueError("Shoonya instruments must be a list of objects")
        else:
            try:
                auth = ZerodhaAuth.from_config_path(config.zerodha_config, token_db=config.auth_db)
            except ValueError:
                auth = None
            import json
            raw_value = json.loads(config.zerodha_config.read_text(encoding="utf-8")) if config.zerodha_config.exists() else {}
            if not isinstance(raw_value, dict):
                raise ValueError("Zerodha configuration must be a JSON object")
            raw = raw_value
            instruments_value = raw.get("instruments", ())
            if not isinstance(instruments_value, list) or any(not isinstance(item, dict) for item in instruments_value):
                raise ValueError("Zerodha instruments must be a list of objects")
        session = RuntimeSession(
            store,
            auth,
            instruments_value,
            capital_profile=capital_profile,
            portfolio=portfolio,
            capital_context=capital_context,
            engine=MultiStrategyEngine({
                "ftx-paper-research": strategy_factory.create(
                    expiry_dates=store.read_expiry_dates(),
                    portfolio=portfolio,
                    capital_context=capital_context,
                ),
                "orderflow-research": strategy_factory.create(
                    strategy_name="ftx-paper-orderflow",
                ),
            }),
        )
        replay_worker = ReplayWorker(store, capital_profile=capital_profile)
        operations = RuntimeOperations(session, replay_worker)
        if auth is not None and auth.access_token() is not None:
            try:
                operations.request_start()
            except Exception as start_exc:
                logging.getLogger(__name__).warning("Auto-start runtime failed: %s", start_exc)
        create_app(
            store,
            zerodha_auth=auth if broker_name == "zerodha" else None,
            broker_auth=auth,
            session=session,
            capital_profile=capital_profile,
            replay_worker=replay_worker,
            runtime_operations=operations,
        ).run(host=config.host, port=config.port, debug=False, use_reloader=False)
    finally:
        try:
            if session is not None:
                session.stop()
        finally:
            store.release_process_lease("api", instance_id)


def ui_main() -> None:
    config = PaperConfig.from_env()
    store = RuntimeStore(config.runtime_dir)
    try:
        instance_id = store.acquire_process_lease("ui")
    except ProcessAlreadyRunningError as exc:
        raise SystemExit(str(exc)) from exc
    ui_port = int(os.getenv("FTX_UI_PORT", "8502"))
    try:
        create_ui_app().run(host=config.host, port=ui_port, debug=False, use_reloader=False)
    finally:
        store.release_process_lease("ui", instance_id)


if __name__ == "__main__":
    api_main()
