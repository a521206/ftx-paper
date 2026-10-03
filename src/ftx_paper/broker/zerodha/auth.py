from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ftx_paper.ports.records import AuthToken


IST = ZoneInfo("Asia/Kolkata")


class ZerodhaAuth:
    """Token persistence and Kite session exchange, isolated from the engine."""

    def __init__(self, token_db: str | Path, api_key: str, api_secret: str, token_repository=None) -> None:
        self.token_db = Path(token_db)
        self.api_key = api_key
        self.api_secret = api_secret
        if token_repository is None:
            from ftx_paper.infrastructure.sqlite.auth_token_repository import SqliteTokenRepository
            token_repository = SqliteTokenRepository(self.token_db)
        self.token_repository = token_repository

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
        self.token_repository.save(AuthToken("zerodha", datetime.now(IST).date(), token))

    def access_token(self) -> str | None:
        stored = self.token_repository.get_valid("zerodha", datetime.now(IST).date())
        return stored.access_token if stored is not None else None

    @classmethod
    def from_config_path(cls, path: str | Path, *, token_db: str | Path | None = None) -> "ZerodhaAuth":
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
        resolved_token_db = token_db or os.getenv("FTX_PAPER_AUTH_DB", values.get("auth_db", "auth.sqlite3"))
        if not Path(resolved_token_db).is_absolute():
            resolved_token_db = str(config_path.parent / resolved_token_db)
        return cls(resolved_token_db, api_key, api_secret)
