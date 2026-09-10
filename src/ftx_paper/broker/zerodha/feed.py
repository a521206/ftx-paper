from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Event, Thread, current_thread
from threading import RLock
from time import monotonic, sleep
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

    class Socket:
        def __init__(self):
            self._tokens: list[int] = []
            self._connected = False
            self._ticker = None

        def connect(self, on_message, on_close):
            self._connected = False
            # KiteTicker instances are single-use after close.  Recreate the
            # underlying websocket for every connection generation.
            ticker = KiteTicker(api_key, access_token)
            self._ticker = ticker
            ticker.on_ticks = lambda _ws, payload: [on_message(item) for item in payload]
            ticker.on_close = lambda *_args: on_close()
            ticker.on_connect = lambda *_args: self._on_connect()
            ticker.connect(threaded=True)

        def subscribe(self, tokens):
            self._tokens = [int(token) for token in tokens]
            if self._connected:
                self._send_subscription()

        def _on_connect(self):
            self._connected = True
            self._send_subscription()

        def _send_subscription(self):
            if self._tokens and self._ticker is not None:
                self._ticker.subscribe(self._tokens)
                # QUOTE is the established payload contract for the normalizer
                # and bar builder; keep it explicit across reconnects.
                self._ticker.set_mode(self._ticker.MODE_QUOTE, self._tokens)

        def close(self):
            self._connected = False
            if self._ticker is not None:
                self._ticker.close()

        @property
        def is_connected(self) -> bool:
            return self._connected

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

    def __init__(self, socket: TickerSocket, tokens: list[int], normalize: Callable[[Mapping[str, Any]], MarketBar], on_bar: Callable[[MarketBar], None], policy: ReconnectPolicy = ReconnectPolicy(), pause: Callable[[float], None] = sleep, *, on_health: Callable[[dict[str, Any]], None] | None = None, expected_instruments: Mapping[str, tuple[str, str]] | None = None, stale_after_seconds: float = 30.0, watchdog_interval_seconds: float = 5.0, clock: Callable[[], float] = monotonic) -> None:
        if not tokens:
            raise ValueError("at least one Zerodha token is required")
        self.socket, self.tokens, self.normalize, self.on_bar = socket, tokens, normalize, on_bar
        self.policy, self.pause = policy, pause
        self.on_health = on_health
        self.stale_after_seconds = stale_after_seconds
        self.watchdog_interval_seconds = watchdog_interval_seconds
        self.clock = clock
        self._running = False
        self._attempts = 0
        self._current: dict[str, MarketBar] = {}
        self._health: dict[str, dict[str, Any]] = {}
        self._watchdog_stop = Event()
        self._watchdog: Thread | None = None
        self._next_watchdog_reconnect = 0.0
        self._started_at = 0.0
        self._connected = False
        self._reconnect_lock = RLock()
        self._reconnecting = False
        self._health = {
            key: {"symbol": symbol, "exchange": exchange, "tick_count": 0, "bar_count": 0}
            for key, (exchange, symbol) in (expected_instruments or {}).items()
        }

    def start(self) -> None:
        self._running = True
        self._attempts = 0
        self._started_at = self.clock()
        self._connect()
        self._watchdog_stop.clear()
        self._watchdog = Thread(target=self._watchdog_loop, daemon=True, name="ftx-paper-feed-watchdog")
        self._watchdog.start()

    def stop(self) -> None:
        self._running = False
        self._watchdog_stop.set()
        self._connected = False
        self.socket.close()
        if self._watchdog and self._watchdog is not current_thread():
            self._watchdog.join(timeout=1)

    def _connect(self) -> None:
        self.socket.connect(self._on_message, self._on_close)
        self.socket.subscribe(self.tokens)
        self._connected = True

    def _on_message(self, payload: Mapping[str, Any]) -> None:
        if self._running:
            bar = self.normalize(payload)
            key = f"{bar.instrument.exchange}:{bar.instrument.symbol}"
            health = self._health.setdefault(key, {"symbol": bar.instrument.symbol, "exchange": bar.instrument.exchange, "tick_count": 0, "bar_count": 0})
            health["tick_count"] += 1
            health["last_tick_at"] = datetime.now(timezone.utc).isoformat()
            health["last_market_bar_at"] = bar.timestamp.isoformat()
            health["last_tick_seen_monotonic"] = self.clock()
            self._attempts = 0
            current = self._current.get(key)
            if current is None:
                self._current[key] = bar
            elif bar.timestamp == current.timestamp:
                self._current[key] = MarketBar(current.instrument, current.timestamp, current.open, max(current.high, bar.high), min(current.low, bar.low), bar.close, bar.volume, bar.open_interest)
            elif bar.timestamp > current.timestamp:
                self.on_bar(current)
                health["bar_count"] += 1
                health["last_closed_bar_at"] = current.timestamp.isoformat()
                self._current[key] = bar
            self._emit_health()

    def flush(self) -> None:
        for bar in tuple(self._current.values()):
            self.on_bar(bar)
            key = f"{bar.instrument.exchange}:{bar.instrument.symbol}"
            self._health.setdefault(key, {"symbol": bar.instrument.symbol, "exchange": bar.instrument.exchange, "tick_count": 0, "bar_count": 0})["last_closed_bar_at"] = bar.timestamp.isoformat()
        self._current.clear()

    def health_snapshot(self) -> dict[str, Any]:
        now = self.clock()
        instruments = {}
        stale = []
        for key, item in self._health.items():
            snapshot = {name: value for name, value in item.items() if name != "last_tick_seen_monotonic"}
            last_seen = item.get("last_tick_seen_monotonic")
            snapshot["stale_seconds"] = None if last_seen is None else max(0.0, now - last_seen)
            age = now - (last_seen if last_seen is not None else self._started_at)
            if age > self.stale_after_seconds:
                snapshot["stale_seconds"] = age
                stale.append(key)
            instruments[key] = snapshot
        socket_connected = getattr(self.socket, "is_connected", None)
        connected = bool(socket_connected) if socket_connected is not None else self._connected
        return {"connected": connected, "stale_instruments": stale, "instruments": instruments}

    def _emit_health(self) -> None:
        if self.on_health:
            self.on_health(self.health_snapshot())

    def _watchdog_loop(self) -> None:
        while not self._watchdog_stop.wait(self.watchdog_interval_seconds):
            snapshot = self.health_snapshot()
            if self.on_health:
                self.on_health(snapshot)
            if snapshot["stale_instruments"] and self._running and self.clock() >= self._next_watchdog_reconnect:
                self._reconnect()

    def _reconnect(self) -> None:
        # Drop partial bars: a bar crossing a socket generation is not causal.
        with self._reconnect_lock:
            if self._reconnecting or not self._running:
                return
            self._reconnecting = True
            try:
                self._current.clear()
                self._attempts += 1
                if self._attempts > self.policy.max_attempts:
                    return
                self._connected = False
                self._next_watchdog_reconnect = self.clock() + self.policy.max_delay_seconds
                self.socket.close()
                self._connect()
            finally:
                self._reconnecting = False

    def _on_close(self) -> None:
        with self._reconnect_lock:
            if not self._running or self._reconnecting or self._attempts >= self.policy.max_attempts:
                self._connected = False
                self._emit_health()
                return
            self._reconnecting = True
            try:
                self._connected = False
                self._attempts += 1
                self._current.clear()
                self.pause(self.policy.delay(self._attempts))
                self._connect()
            finally:
                self._reconnecting = False
        self._emit_health()
