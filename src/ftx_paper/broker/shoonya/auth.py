"""Shoonya authentication and token management with automated headless TOTP login."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
from datetime import datetime
from pathlib import Path
import struct
import time
from typing import Any
from zoneinfo import ZoneInfo

from ftx_paper.ports.records import AuthToken
from .client import ShoonyaClient

IST = ZoneInfo("Asia/Kolkata")
logger = logging.getLogger(__name__)


def generate_totp(secret: str, interval: int = 30, digits: int = 6) -> str:
    """Generate RFC 6238 time-based one-time password from base32 secret."""
    cleaned = secret.strip().replace(" ", "").upper()
    # Add padding if missing
    missing_padding = len(cleaned) % 8
    if missing_padding:
        cleaned += "=" * (8 - missing_padding)
    key = base64.b32decode(cleaned, casefold=True)
    counter = int(time.time() // interval)
    msg = struct.pack(">Q", counter)
    digest = hmac.new(key, msg, hashlib.sha1).digest()
    offset = digest[19] & 0x0F
    code = struct.unpack(">I", digest[offset : offset + 4])[0] & 0x7FFFFFFF
    return str(code % (10**digits)).zfill(digits)


class ShoonyaAuth:
    """Token persistence and automated session exchange for Shoonya (Finvasia)."""

    provider: str = "shoonya"

    def __init__(
        self,
        token_db: str | Path,
        uid: str,
        pwd: str,
        api_key: str,
        vc: str,
        totp_secret: str,
        imei: str = "ftx-paper-api",
        token_repository: Any | None = None,
        base_url: str | None = None,
    ) -> None:
        self.token_db = Path(token_db)
        self.uid = uid.strip()
        self.pwd = pwd.strip()
        self.api_key = api_key.strip()
        self.vc = vc.strip()
        self.totp_secret = totp_secret.strip()
        self.imei = imei.strip()
        self.base_url = base_url
        self._last_auto_login_attempt: float = 0.0

        if token_repository is None:
            from ftx_paper.infrastructure.sqlite.auth_token_repository import SqliteTokenRepository

            token_repository = SqliteTokenRepository(self.token_db)
        self.token_repository = token_repository

    def login_url(self) -> str:
        """Return the Shoonya OAuth authorization URL."""
        client_id = self.vc or f"{self.uid}_U"
        return f"https://api.shoonya.com/OAuthlogin/authorize/oauth?client_id={client_id}"

    def exchange(self, auth_code: str) -> str:
        """Exchange an OAuth authorization code for a session token."""
        clean_code = auth_code.strip()
        if not clean_code:
            raise ValueError("auth_code is required")
        client = ShoonyaClient(
            uid=self.uid,
            base_url=self.base_url or "https://api.shoonya.com/NorenWClientAPI",
        )
        client_id = self.vc or f"{self.uid}_U"
        res = client.generate_access_token(
            auth_code=clean_code,
            secret_code=self.api_key,
            client_id=client_id,
            uid=self.uid,
        )
        token = str(res.get("susertoken") or res.get("access_token", "")).strip()
        if not token:
            raise RuntimeError("Shoonya did not return an access token")
        today = datetime.now(IST).date()
        self.token_repository.save(AuthToken("shoonya", today, token))
        logger.info("Shoonya OAuth token exchange succeeded for %s on %s", self.uid, today)
        return token

    def save_token(self, token: str) -> None:
        """Save a manually provided session token."""
        clean_token = token.strip()
        if not clean_token:
            raise ValueError("token cannot be empty")
        today = datetime.now(IST).date()
        self.token_repository.save(AuthToken("shoonya", today, clean_token))
        logger.info("Shoonya token saved for %s on %s", self.uid, today)

    def login(self) -> str:
        """Perform login using configured credentials."""
        if not self.uid or not self.pwd or not self.api_key:
            raise ValueError("Missing Shoonya credentials: uid, pwd, or api_key")

        pwd_sha256 = hashlib.sha256(self.pwd.encode("utf-8")).hexdigest()
        appkey_raw = f"{self.uid}|{self.api_key}"
        appkey_sha256 = hashlib.sha256(appkey_raw.encode("utf-8")).hexdigest()
        factor2 = generate_totp(self.totp_secret) if self.totp_secret else ""

        client = ShoonyaClient(
            uid=self.uid,
            base_url=self.base_url or "https://api.shoonya.com/NorenWClientAPI",
        )
        try:
            res = client.quick_auth(
                uid=self.uid,
                pwd_sha256=pwd_sha256,
                factor2=factor2,
                vc=self.vc,
                appkey_sha256=appkey_sha256,
                imei=self.imei,
            )
            token = str(res.get("susertoken", "")).strip()
            if not token:
                raise RuntimeError("Shoonya did not return a valid session token")
        except Exception as exc:
            if "Invalid Vendor code" in str(exc):
                raise RuntimeError(
                    f"Shoonya has migrated authentication to OAuth 2.0. Please log in via OAuth: {self.login_url()}"
                ) from exc
            raise

        today = datetime.now(IST).date()
        self.token_repository.save(AuthToken("shoonya", today, token))
        logger.info("Shoonya authenticated successfully for %s on %s", self.uid, today)
        return token

    def access_token(self) -> str | None:
        """Return the active access token for today, automatically logging in if needed."""
        today = datetime.now(IST).date()
        stored = self.token_repository.get_valid("shoonya", today)
        if stored is not None and stored.access_token:
            return stored.access_token

        # Attempt automated headless login if credentials are provided
        if self.uid and self.pwd and self.api_key:
            now = time.monotonic()
            if now - self._last_auto_login_attempt > 60.0:
                self._last_auto_login_attempt = now
                try:
                    return self.login()
                except Exception as exc:
                    logger.warning("Automated Shoonya login failed: %s", exc)
                    return None
        return None

    def authenticated_client(self) -> ShoonyaClient:
        """Return an initialized client with the active session token."""
        token = self.access_token()
        if not token:
            raise RuntimeError("No valid Shoonya access token for today")
        cache_dir = self.token_db.parent / "shoonya_cache"
        return ShoonyaClient(
            token=token,
            uid=self.uid,
            base_url=self.base_url or "https://api.shoonya.com/NorenWClientAPI",
            cache_dir=cache_dir,
        )

    @classmethod
    def from_config_path(
        cls,
        path: str | Path,
        *,
        token_db: str | Path | None = None,
    ) -> "ShoonyaAuth":
        """Build ShoonyaAuth from a standalone JSON config or environment variables."""
        config_path = Path(path)
        values: dict[str, str] = {}
        if config_path.exists():
            try:
                raw = json.loads(config_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise ValueError("Invalid standalone Shoonya config") from exc
            values = {
                key: str(raw.get(key, "")).strip()
                for key in ("uid", "pwd", "api_key", "vc", "totp_secret", "imei", "auth_db", "token")
            }

        uid = os.getenv("SHOONYA_UID", values.get("uid", "")).strip()
        pwd = os.getenv("SHOONYA_PWD", values.get("pwd", "")).strip()
        api_key = os.getenv("SHOONYA_API_KEY", values.get("api_key", "")).strip()
        vc = os.getenv("SHOONYA_VC", values.get("vc", "")).strip()
        totp_secret = os.getenv("SHOONYA_TOTP_SECRET", values.get("totp_secret", "")).strip()
        imei = os.getenv("SHOONYA_IMEI", values.get("imei", "ftx-paper-api")).strip()
        manual_token = os.getenv("SHOONYA_TOKEN", values.get("token", "")).strip()

        if not uid or not api_key:
            raise ValueError("Missing Shoonya configuration: uid, api_key")

        resolved_token_db = token_db or os.getenv(
            "FTX_PAPER_AUTH_DB", values.get("auth_db", "auth.sqlite3")
        )
        if not Path(resolved_token_db).is_absolute():
            resolved_token_db = str(config_path.parent / resolved_token_db)

        instance = cls(
            resolved_token_db,
            uid=uid,
            pwd=pwd,
            api_key=api_key,
            vc=vc,
            totp_secret=totp_secret,
            imei=imei,
        )
        if manual_token:
            instance.save_token(manual_token)
        return instance
