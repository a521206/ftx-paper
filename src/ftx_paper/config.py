from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import time
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv


def _path(name: str, default: Path) -> Path:
    return Path(os.getenv(name, str(default))).expanduser()


NIFTY_LOT_SIZE = 65

IST = ZoneInfo("Asia/Kolkata")

# Two distinct session boundaries (IST).  Do not conflate them: the exit
# deadline force-closes open positions, while the data end still admits market
# input used for levels, extrema, and PCR.
SESSION_OPEN_MINUTES = 9 * 60 + 15
SESSION_EXIT_DEADLINE = "15:10"
SESSION_DATA_END = "15:30"
SESSION_EXIT_DEADLINE_TIME = time.fromisoformat(SESSION_EXIT_DEADLINE)


def _minute_of_day(value: str) -> int:
    parsed = time.fromisoformat(value)
    return parsed.hour * 60 + parsed.minute


SESSION_DATA_END_MINUTES = _minute_of_day(SESSION_DATA_END)


@dataclass(frozen=True, slots=True)
class PaperConfig:
    home: Path
    runtime_dir: Path
    auth_db: Path
    zerodha_config: Path
    shoonya_config: Path
    xts_config: Path
    broker: str = "xts"
    host: str = "127.0.0.1"
    port: int = 8501

    @classmethod
    def from_env(cls) -> "PaperConfig":
        load_dotenv(override=False)
        local_app_data = Path(os.getenv("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
        home = _path("FTX_PAPER_HOME", local_app_data / "ftx-paper")
        runtime_dir = _path("FTX_PAPER_RUNTIME_DIR", home / "runtime")
        zerodha_cfg = _path("FTX_PAPER_CONFIG_PATH", home / "config" / "zerodha.json")
        shoonya_cfg = _path("FTX_PAPER_SHOONYA_CONFIG_PATH", home / "config" / "shoonya.json")
        xts_cfg = _path("FTX_PAPER_XTS_CONFIG_PATH", home / "config" / "xts.json")
        broker = os.getenv("FTX_BROKER", "xts").lower().strip()

        return cls(
            home=home,
            runtime_dir=runtime_dir,
            auth_db=_path("FTX_PAPER_AUTH_DB", home / "auth" / "auth.sqlite3"),
            zerodha_config=zerodha_cfg,
            shoonya_config=shoonya_cfg,
            xts_config=xts_cfg,
            broker=broker,
            host=os.getenv("FTX_WEB_HOST", "127.0.0.1"),
            port=int(os.getenv("FTX_WEB_PORT", "8501")),
        )

