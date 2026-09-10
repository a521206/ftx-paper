from __future__ import annotations

import sqlite3
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")


class ZerodhaAuth:
    """Token persistence and Kite session exchange, isolated from the engine."""

    def __init__(self, token_db: str | Path, api_key: str, api_secret: str) -> None:
        self.token_db = Path(token_db)
        self.api_key = api_key
        self.api_secret = api_secret

    def _client(self) -> Any:
        try:
            from kiteconnect import KiteConnect
        except ImportError as exc:
            raise RuntimeError("Install ftx-paper[zerodha] to use Zerodha") from exc
        return KiteConnect(api_key=self.api_key)

    def login_url(self) -> str:
        return str(self._client().login_url())

    def authenticated_client(self) -> Any:
        token = self.access_token()
        if not token:
            raise RuntimeError("No valid Zerodha access token for today")
        client = self._client()
        client.set_access_token(token)
        return client

    def exchange(self, request_token: str) -> None:
        if not request_token.strip():
            raise ValueError("request_token is required")
        session = self._client().generate_session(request_token.strip(), api_secret=self.api_secret)
        token = str(session.get("access_token", "")).strip()
        if not token:
            raise RuntimeError("Zerodha did not return an access token")
        self.token_db.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.token_db) as db:
            db.execute("CREATE TABLE IF NOT EXISTS auth_tokens (provider TEXT PRIMARY KEY, token_date TEXT NOT NULL, access_token TEXT NOT NULL)")
            db.execute("INSERT INTO auth_tokens VALUES ('zerodha', ?, ?) ON CONFLICT(provider) DO UPDATE SET token_date=excluded.token_date, access_token=excluded.access_token", (datetime.now(IST).date().isoformat(), token))

    def access_token(self) -> str | None:
        if not self.token_db.exists():
            return None
        with sqlite3.connect(self.token_db) as db:
            row = db.execute("SELECT token_date, access_token FROM auth_tokens WHERE provider='zerodha'").fetchone()
        return str(row[1]) if row and row[0] == datetime.now(IST).date().isoformat() and row[1] else None

    @classmethod
    def from_config_path(cls, path: str | Path) -> "ZerodhaAuth":
        """Build auth from standalone config without consulting NiftyZoning."""
        config_path = Path(path)
        values: dict[str, str] = {}
        if config_path.exists():
            try:
                raw = json.loads(config_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise ValueError("Invalid standalone Zerodha config") from exc
            values = {key: str(raw.get(key, "")).strip() for key in ("api_key", "api_secret", "auth_db")}
        api_key = os.getenv("KITE_API_KEY", values.get("api_key", "")).strip()
        api_secret = os.getenv("KITE_API_SECRET", values.get("api_secret", "")).strip()
        if not api_key or not api_secret:
            raise ValueError("Missing Zerodha configuration: api_key, api_secret")
        token_db = os.getenv("FTX_PAPER_AUTH_DB", values.get("auth_db", "auth.sqlite3"))
        if not Path(token_db).is_absolute():
            token_db = str(config_path.parent / token_db)
        return cls(token_db, api_key, api_secret)
