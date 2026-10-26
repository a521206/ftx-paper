"""Concurrency-safe XTS market-data token state."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock, RLock

from .exceptions import XTSAuthenticationError


@dataclass(frozen=True, slots=True)
class XTSTokenState:
    token: str
    user_id: str
    generation: int


class XTSMarketDataSession:
    """Own the token used by XTS market-data REST and WebSocket clients."""

    def __init__(
        self,
        token: str,
        user_id: str = "",
        *,
        refresh: Callable[[], str],
        user_id_provider: Callable[[], str] | None = None,
    ) -> None:
        self._state = XTSTokenState(token, user_id, 0)
        self._refresh = refresh
        self._user_id_provider = user_id_provider
        self._lock = RLock()
        self._refresh_lock = Lock()

    def snapshot(self) -> XTSTokenState:
        with self._lock:
            return self._state

    def current_token(self) -> str:
        return self.snapshot().token

    def refresh_if_stale(self, generation: int) -> XTSTokenState:
        """Refresh once for a failed generation, reusing concurrent refreshes."""
        with self._refresh_lock:
            with self._lock:
                if self._state.generation != generation:
                    return self._state
                previous_user_id = self._state.user_id
            try:
                token = str(self._refresh() or "").strip()
            except Exception as exc:
                raise XTSAuthenticationError("XTS session refresh failed") from exc
            if not token:
                raise XTSAuthenticationError("XTS session refresh returned no token")
            user_id = self._user_id_provider() if self._user_id_provider else previous_user_id
            with self._lock:
                if self._state.generation != generation:
                    return self._state
                self._state = XTSTokenState(token, user_id or "", generation + 1)
                return self._state
