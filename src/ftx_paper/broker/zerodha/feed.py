from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from time import sleep
from typing import Any, Protocol

from ftx_paper.contracts import MarketBar


class TickerSocket(Protocol):
    def connect(self, on_message: Callable[[Mapping[str, Any]], None], on_close: Callable[[], None]) -> None: ...
    def subscribe(self, tokens: list[int]) -> None: ...
    def close(self) -> None: ...


def create_kite_socket(api_key: str, access_token: str) -> TickerSocket:
    try:
        from kiteconnect import KiteTicker
    except ImportError as exc:
        raise RuntimeError("Install ftx-paper[zerodha] to use the live feed") from exc

    ticker = KiteTicker(api_key, access_token)

    class Socket:
        def connect(self, on_message, on_close):
            ticker.on_ticks = lambda _ws, payload: [on_message(item) for item in payload]
            ticker.on_close = lambda *_args: on_close()
            ticker.connect(threaded=True)

        def subscribe(self, tokens):
            ticker.subscribe(tokens)
            ticker.set_mode(ticker.MODE_QUOTE, tokens)

        def close(self):
            ticker.close()

    return Socket()


@dataclass(frozen=True, slots=True)
class ReconnectPolicy:
    max_attempts: int = 5
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 30.0

    def delay(self, attempt: int) -> float:
        return min(self.max_delay_seconds, self.initial_delay_seconds * (2 ** max(0, attempt - 1)))


class ZerodhaFeed:
    """Reconnectable Kite socket boundary; only normalized bars leave it."""

    def __init__(self, socket: TickerSocket, tokens: list[int], normalize: Callable[[Mapping[str, Any]], MarketBar], on_bar: Callable[[MarketBar], None], policy: ReconnectPolicy = ReconnectPolicy(), pause: Callable[[float], None] = sleep) -> None:
        if not tokens:
            raise ValueError("at least one Zerodha token is required")
        self.socket, self.tokens, self.normalize, self.on_bar = socket, tokens, normalize, on_bar
        self.policy, self.pause = policy, pause
        self._running = False
        self._attempts = 0
        self._current: dict[str, MarketBar] = {}

    def start(self) -> None:
        self._running = True
        self._attempts = 0
        self._connect()

    def stop(self) -> None:
        self._running = False
        self.socket.close()

    def _connect(self) -> None:
        self.socket.connect(self._on_message, self._on_close)
        self.socket.subscribe(self.tokens)

    def _on_message(self, payload: Mapping[str, Any]) -> None:
        if self._running:
            bar = self.normalize(payload)
            key = f"{bar.instrument.exchange}:{bar.instrument.symbol}"
            current = self._current.get(key)
            if current is None:
                self._current[key] = bar
            elif bar.timestamp == current.timestamp:
                self._current[key] = MarketBar(current.instrument, current.timestamp, current.open, max(current.high, bar.high), min(current.low, bar.low), bar.close, bar.volume, bar.open_interest)
            elif bar.timestamp > current.timestamp:
                self.on_bar(current)
                self._current[key] = bar

    def flush(self) -> None:
        for bar in tuple(self._current.values()):
            self.on_bar(bar)
        self._current.clear()

    def _on_close(self) -> None:
        if not self._running or self._attempts >= self.policy.max_attempts:
            return
        self._attempts += 1
        self.pause(self.policy.delay(self._attempts))
        self._connect()
