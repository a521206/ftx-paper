from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _path(name: str, default: Path) -> Path:
    return Path(os.getenv(name, str(default))).expanduser()


PROJECT_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True, slots=True)
class PaperConfig:
    home: Path
    runtime_dir: Path
    auth_db: Path
    zerodha_config: Path
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
            # Keep the standalone default, but discover the repository config for
            # local runs so the API and UI work without extra environment setup.
            zerodha_config=_path(
                "FTX_PAPER_CONFIG_PATH",
                PROJECT_ROOT / "config" / "zerodha.json" if (PROJECT_ROOT / "config" / "zerodha.json").exists() else home / "config" / "zerodha.json",
            ),
            host=os.getenv("FTX_WEB_HOST", "127.0.0.1"),
            port=int(os.getenv("FTX_WEB_PORT", "8501")),
        )
