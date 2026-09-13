from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _path(name: str, default: Path) -> Path:
    return Path(os.getenv(name, str(default))).expanduser()


PACKAGE_ROOT = Path(__file__).resolve().parents[2]
NIFTY_LOT_SIZE = 65


@dataclass(frozen=True, slots=True)
class PaperConfig:
    home: Path
    runtime_dir: Path
    auth_db: Path
    zerodha_config: Path
    capital_config: Path
    host: str = "127.0.0.1"
    port: int = 8501

    @classmethod
    def from_env(cls) -> "PaperConfig":
        home = _path("FTX_PAPER_HOME", Path.home() / ".ftx-paper")
        runtime_dir = _path("FTX_PAPER_RUNTIME_DIR", home / "runtime")
        return cls(
            home=home,
            runtime_dir=runtime_dir,
            auth_db=_path("FTX_PAPER_AUTH_DB", home / "auth" / "auth.sqlite3"),
            # Keep the standalone config beside the ftx-paper package so local
            # runs do not depend on the canonical repository layout.
            zerodha_config=_path(
                "FTX_PAPER_CONFIG_PATH",
                PACKAGE_ROOT / "config" / "zerodha.json" if (PACKAGE_ROOT / "config" / "zerodha.json").exists() else home / "config" / "zerodha.json",
            ),
            capital_config=_path("FTX_PAPER_CAPITAL_CONFIG", PACKAGE_ROOT / "config" / "ftx.json"),
            host=os.getenv("FTX_WEB_HOST", "127.0.0.1"),
            port=int(os.getenv("FTX_WEB_PORT", "8501")),
        )
