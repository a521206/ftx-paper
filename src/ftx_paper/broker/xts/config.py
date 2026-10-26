"""Configuration for an XTS market-data deployment."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass(frozen=True, slots=True)
class XTSMarketDataConfig:
    """Explicit REST and WebSocket paths for one XTS market-data deployment."""

    base_url: str
    market_data_path: str
    websocket_path: str

    def __post_init__(self) -> None:
        parsed = urlparse(self.base_url.rstrip("/"))
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("XTS base_url must be an absolute HTTP(S) URL")
        if parsed.path not in {"", "/"}:
            raise ValueError("XTS base_url must not include a market-data path")
        for name, path in (("market_data_path", self.market_data_path), ("websocket_path", self.websocket_path)):
            if not path.startswith("/") or "?" in path or "#" in path:
                raise ValueError(f"XTS {name} must be an absolute path without query parameters")

    @property
    def market_data_url(self) -> str:
        return f"{self.base_url.rstrip('/')}/{self.market_data_path.lstrip('/')}"
