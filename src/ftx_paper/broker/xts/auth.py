"""XTS authentication and token management with automated seamless headless login."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ftx_paper.ports.records import AuthToken

from .client import DEFAULT_ROOT_URL, XTSClient

IST = ZoneInfo("Asia/Kolkata")
logger = logging.getLogger(__name__)


class XTSAuth:
    """Token persistence and automated session login for Symphony / IIFL XTS."""

    provider: str = "xts"

    def __init__(
        self,
        token_db: str | Path,
        app_key: str,
        secret_key: str,
        *,
        root_url: str = DEFAULT_ROOT_URL,
        source: str = "WEBAPI",
        token_repository: Any | None = None,
    ) -> None:
        self.token_db = Path(token_db)
        self.app_key = app_key.strip()
        self.secret_key = secret_key.strip()
        self.root_url = root_url.rstrip("/")
        self.source = source.strip()
        self.user_id = ""
        self._last_auto_login_attempt: float = 0.0

        if token_repository is None:
            from ftx_paper.infrastructure.sqlite.auth_token_repository import (
                SqliteTokenRepository,
            )

            token_repository = SqliteTokenRepository(self.token_db)
        self.token_repository = token_repository

    @classmethod
    def from_config_path(
        cls,
        path: Path | str,
        token_db: Path | str | None = None,
    ) -> XTSAuth:
        """Instantiate XTSAuth from a JSON configuration file."""
        config_path = Path(path)
        if not config_path.exists():
            raise ValueError(f"XTS configuration file not found: {config_path}")
        raw = json.loads(config_path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("XTS configuration must be a JSON object")

        app_key = str(raw.get("app_key") or raw.get("appKey") or "").strip()
        secret_key = str(raw.get("secret_key") or raw.get("secretKey") or "").strip()
        root_url = str(raw.get("root_url") or raw.get("rootUrl") or DEFAULT_ROOT_URL).strip()
        source = str(raw.get("source") or "WEBAPI").strip()

        db_path = token_db or raw.get("auth_db") or (config_path.parent.parent / "data" / "auth.sqlite3")

        return cls(
            token_db=db_path,
            app_key=app_key,
            secret_key=secret_key,
            root_url=root_url,
            source=source,
        )

    def access_token(self) -> str | None:
        """Return the current valid access token for today, auto-logging in if missing."""
        today = datetime.now(IST).date()
        record = self.token_repository.get_valid("xts", today)
        if record is not None:
            if not self.user_id:
                try:
                    import base64
                    parts = record.access_token.split(".")
                    if len(parts) >= 2:
                        padding = 4 - len(parts[1]) % 4
                        payload_raw = base64.b64decode(parts[1] + "=" * padding).decode("utf-8", "ignore")
                        jwt_data = json.loads(payload_raw)
                        self.user_id = str(jwt_data.get("userID", "")).split("_")[0]
                except Exception:
                    pass
            return record.access_token

        # Seamless headless login without user interaction
        if self.app_key and self.secret_key:
            try:
                logger.info("Attempting automated headless login for XTS...")
                return self.login()
            except Exception as exc:
                logger.error("Automated XTS login failed: %s", exc)
                return None
        return None

    def login(self) -> str:
        """Perform automated headless login to XTS and persist the JWT token."""
        if not self.app_key or not self.secret_key:
            raise ValueError("XTS app_key and secret_key are required for login")

        client = XTSClient(root_url=self.root_url)
        client.login(self.app_key, self.secret_key, self.source)
        token = client.token
        if not token:
            raise RuntimeError("XTS login failed to produce a valid token")

        self.user_id = client.user_id or ""
        today = datetime.now(IST).date()
        self.token_repository.save(AuthToken("xts", today, token))
        logger.info("XTS login succeeded on %s (user: %s)", today, self.user_id)
        return token

    def authenticated_client(self) -> XTSClient:
        """Return a client configured with the current access token."""
        token = self.access_token()
        if not token:
            raise RuntimeError("No valid XTS access token available. Please configure valid app_key and secret_key.")
        return XTSClient(
            token=token,
            user_id=self.user_id,
            root_url=self.root_url,
        )
