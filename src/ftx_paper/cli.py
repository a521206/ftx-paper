from __future__ import annotations

import os
from ftx_paper.api import create_app
from ftx_paper.config import PaperConfig
from ftx_paper.runtime import RuntimeSession, RuntimeStore
from ftx_paper.strategy import ConfiguredLiveStrategy
from ftx_paper.core import PaperEngine
from ftx_paper.ui import create_ui_app
from ftx_paper.broker.zerodha import ZerodhaAuth


def api_main() -> None:
    config = PaperConfig.from_env()
    store = RuntimeStore(config.runtime_dir)
    try:
        auth = ZerodhaAuth.from_config_path(config.zerodha_config)
    except ValueError:
        auth = None
    import json
    raw = json.loads(config.zerodha_config.read_text(encoding="utf-8")) if config.zerodha_config.exists() else {}
    session = RuntimeSession(store, auth, list(raw.get("instruments", ())), engine=PaperEngine(ConfiguredLiveStrategy()))
    create_app(store, zerodha_auth=auth, session=session).run(host=config.host, port=config.port, debug=False, use_reloader=False)


def ui_main() -> None:
    config = PaperConfig.from_env()
    ui_port = int(os.getenv("FTX_UI_PORT", "8502"))
    create_ui_app().run(host=config.host, port=ui_port, debug=False, use_reloader=False)


if __name__ == "__main__":
    api_main()
