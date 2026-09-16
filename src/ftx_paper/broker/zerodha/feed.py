from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, time as datetime_time, timezone
from threading import Event, Thread, current_thread
from threading import RLock
from time import monotonic, sleep
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from ftx_paper.contracts import MarketBar


class TickerSocket(Protocol):
    def connect(self, on_message: Callable[[Mapping[str, Any]], None], on_close: Callable[..., None]) -> None: ...
    def subscribe(self, tokens: list[int]) -> None: ...
    def close(self) -> None: ...


IST = ZoneInfo("Asia/Kolkata")


def is_nse_market_open(now: datetime | None = None) -> bool:
    """Return whether the regular NSE/NFO session is open in IST."""
    current = now or datetime.now(IST)
    if current.tzinfo is None:
        current = current.replace(tzinfo=IST)
    else:
        current = current.astimezone(IST)
    if current.weekday() >= 5:
        return False
    market_time = current.time()
    return datetime_time(9, 15) <= market_time <= datetime_time(15, 30)


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
            self._ready = Event()
            self._last_activity = 0.0
            self._close_notified = False

        def connect(self, on_message, on_close):
            self._connected = False
            self._ready.clear()
            self._close_notified = False
            # The wrapper owns reconnects. Running KiteTicker's reconnect loop
            # as well would create duplicate connection attempts during 429s.
            def handle_close(*args):
                if self._ticker is not ticker:
                    return
                if self._close_notified:
                    return
                self._close_notified = True
                code = args[1] if len(args) > 1 else None
                reason = args[2] if len(args) > 2 else ""
                on_close(code, reason)

            # Keep the wrapper's timeout aligned with the feed's readiness
            # contract. The outer feed owns reconnects.
            ticker = KiteTicker(api_key, access_token, reconnect=False, connect_timeout=10)
            self._ticker = ticker
            ticker.on_message = lambda _ws, _payload, _is_binary: self._mark_activity()
            ticker.on_ticks = lambda _ws, payload: [on_message(item) for item in payload]
            ticker.on_close = handle_close
            ticker.on_error = handle_close
            ticker.on_connect = lambda *_args: self._on_connect()
            ticker.connect(threaded=True)

        def _mark_activity(self):
            self._last_activity = monotonic()

        def subscribe(self, tokens):
            self._tokens = [int(token) for token in tokens]
            if self._connected:
                self._send_subscription()

        def _on_connect(self):
            self._connected = True
            self._mark_activity()
            self._ready.set()
            self._send_subscription()

        def _send_subscription(self):
            if self._tokens and self._ticker is not None:
                self._ticker.subscribe(self._tokens)
                # QUOTE is the established payload contract for the normalizer
                # and bar builder; keep it explicit across reconnects.
                self._ticker.set_mode(self._ticker.MODE_QUOTE, self._tokens)

        def close(self):
            self._connected = False
            self._ready.clear()
            if self._ticker is not None:
                self._ticker.close()

        def wait_until_connected(self, timeout: float = 15.0) -> bool:
            return self._ready.wait(timeout)

        @property
        def is_connected(self) -> bool:
            return self._connected

        @property
        def activity_age_seconds(self) -> float | None:
            if not self._last_activity:
                return None
            return max(0.0, monotonic() - self._last_activity)

    return Socket()


@dataclass(frozen=True, slots=True)
class ReconnectPolicy:
    max_attempts: int = 5
    initial_delay_seconds: float = 2.0
    max_delay_seconds: float = 60.0
    rate_limit_delay_seconds: float = 60.0

    def delay(self, attempt: int) -> float:
        return min(self.max_delay_seconds, self.initial_delay_seconds * (2 ** max(0, attempt - 1)))


class ZerodhaFeed:
    """Reconnectable Kite socket boundary; only normalized bars leave it."""

    def __init__(self, socket: TickerSocket, tokens: list[int], normalize: Callable[[Mapping[str, Any]], MarketBar], on_bar: Callable[[MarketBar], None], policy: ReconnectPolicy = ReconnectPolicy(), pause: Callable[[float], None] = sleep, *, on_tick: Callable[[MarketBar], None] | None = None, on_health: Callable[[dict[str, Any]], None] | None = None, expected_instruments: Mapping[str, tuple[str, str]] | None = None, stale_after_seconds: float = 30.0, watchdog_interval_seconds: float = 5.0, clock: Callable[[], float] = monotonic) -> None:
        if not tokens:
            raise ValueError("at least one Zerodha token is required")
        self.socket, self.tokens, self.normalize, self.on_bar = socket, tokens, normalize, on_bar
        self.policy, self.pause, self.on_tick = policy, pause, on_tick
        self.on_health = on_health
        self.stale_after_seconds = stale_after_seconds
        self.watchdog_interval_seconds = watchdog_interval_seconds
        self.clock = clock
        self._running = False
        self._attempts = 0
        self._current: dict[str, MarketBar] = {}
        self._health: dict[str, dict[str, Any]] = {}
        self._watchdog_stop = Event()
        self._stop_event = Event()
        self._reconnect_requested = Event()
        self._watchdog: Thread | None = None
        self._reconnect_worker: Thread | None = None
        self._started_at = 0.0
        self._connected = False
        self._last_error: str | None = None
        self._rate_limited_until: float | None = None
        self._reconnect_lock = RLock()
        self._reconnecting = False
        self._connection_generation = 0
        self._close_handled_generation: int | None = None
        self._failed = False
        self._health = {
            key: {"symbol": symbol, "exchange": exchange, "tick_count": 0, "bar_count": 0}
            for key, (exchange, symbol) in (expected_instruments or {}).items()
        }

    def start(self) -> None:
        self._running = True
        self._attempts = 0
        self._last_error = None
        self._rate_limited_until = None
        self._failed = False
        self._started_at = self.clock()
        self._stop_event.clear()
        self._watchdog_stop.clear()
        self._reconnect_requested.clear()
        self._reconnect_worker = Thread(
            target=self._reconnect_loop, daemon=True, name="ftx-paper-feed-reconnector"
        )
        self._reconnect_worker.start()
        self._connect()
        self._watchdog = Thread(target=self._watchdog_loop, daemon=True, name="ftx-paper-feed-watchdog")
        self._watchdog.start()

    def stop(self) -> None:
        self._running = False
        self._stop_event.set()
        self._watchdog_stop.set()
        self._reconnect_requested.set()
        self._connected = False
        self.socket.close()
        if self._watchdog and self._watchdog is not current_thread():
            self._watchdog.join(timeout=1)
        if self._reconnect_worker and self._reconnect_worker is not current_thread():
            self._reconnect_worker.join(timeout=1)

    def _connect(self) -> None:
        self._connection_generation += 1
        generation = self._connection_generation
        self._close_handled_generation = None

        def on_close(code: int | None = None, reason: str = "") -> None:
            self._on_close(code, reason, generation=generation)

        self.socket.connect(self._on_message, on_close)
        self.socket.subscribe(self.tokens)
        wait_until_connected = getattr(self.socket, "wait_until_connected", None)
        if callable(wait_until_connected):
            self._connected = bool(wait_until_connected(12.0))
        else:
            self._connected = True

    def _on_message(self, payload: Mapping[str, Any]) -> None:
        if self._running:
            bar = self.normalize(payload)
            if self.on_tick is not None:
                self.on_tick(bar)
            key = f"{bar.instrument.exchange}:{bar.instrument.symbol}"
            health = self._health.setdefault(key, {"symbol": bar.instrument.symbol, "exchange": bar.instrument.exchange, "tick_count": 0, "bar_count": 0})
            health["tick_count"] += 1
            health["last_tick_at"] = datetime.now(timezone.utc).isoformat()
            health["last_market_bar_at"] = bar.timestamp.isoformat()
            health["last_tick_seen_monotonic"] = self.clock()
            self._attempts = 0
            self._last_error = None
            self._rate_limited_until = None
            current = self._current.get(key)
            if current is None:
                self._current[key] = self._minute_bar(bar)
            elif bar.timestamp.replace(second=0, microsecond=0) == current.timestamp:
                self._current[key] = MarketBar(current.instrument, current.timestamp, current.open, max(current.high, bar.high), min(current.low, bar.low), bar.close, bar.volume, bar.open_interest)
            elif bar.timestamp.replace(second=0, microsecond=0) > current.timestamp:
                self.on_bar(current)
                health["bar_count"] += 1
                health["last_closed_bar_at"] = current.timestamp.isoformat()
                self._current[key] = self._minute_bar(bar)
            self._emit_health()

    def flush(self) -> None:
        for bar in tuple(self._current.values()):
            self.on_bar(bar)
            key = f"{bar.instrument.exchange}:{bar.instrument.symbol}"
            self._health.setdefault(key, {"symbol": bar.instrument.symbol, "exchange": bar.instrument.exchange, "tick_count": 0, "bar_count": 0})["last_closed_bar_at"] = bar.timestamp.isoformat()
        self._current.clear()

    @staticmethod
    def _minute_bar(bar: MarketBar) -> MarketBar:
        return MarketBar(bar.instrument, bar.timestamp.replace(second=0, microsecond=0), bar.open,
                          bar.high, bar.low, bar.close, bar.volume, bar.open_interest)

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
        activity_age = getattr(self.socket, "activity_age_seconds", None)
        transport_stale = bool(
            connected and activity_age is not None and activity_age > self.stale_after_seconds
        )
        return {
            "connected": connected,
            "state": "FAILED" if self._failed else ("CONNECTED" if connected else "DISCONNECTED"),
            "reconnect_attempts": self._attempts,
            "last_error": self._last_error,
            "transport_stale": transport_stale,
            "rate_limited": self._rate_limited_until is not None and now < self._rate_limited_until,
            "rate_limited_until_monotonic": self._rate_limited_until,
            "stale_instruments": stale,
            "instruments": instruments,
        }

    def _emit_health(self) -> None:
        if self.on_health:
            self.on_health(self.health_snapshot())

    def _watchdog_loop(self) -> None:
        while not self._watchdog_stop.wait(self.watchdog_interval_seconds):
            snapshot = self.health_snapshot()
            if self.on_health:
                self.on_health(snapshot)
            if self._should_reconnect(snapshot) and self._running:
                self._reconnect_requested.set()

    @staticmethod
    def _should_reconnect(snapshot: Mapping[str, Any]) -> bool:
        """Reconnect for transport failure, never merely for a quiet symbol."""
        return not bool(snapshot.get("connected")) or bool(snapshot.get("transport_stale"))

    def _reconnect_loop(self) -> None:
        while self._running:
            self._reconnect_requested.wait()
            self._reconnect_requested.clear()
            if self._running:
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
                    self._failed = True
                    self._connected = False
                    return
                self._connected = False
                delay = self.policy.delay(self._attempts)
                if self._rate_limited_until is not None:
                    delay = max(delay, self._rate_limited_until - self.clock())
                if self.pause is sleep:
                    if self._stop_event.wait(delay):
                        return
                else:
                    self.pause(delay)
                if not self._running:
                    return
                self.socket.close()
                self._connect()
            finally:
                self._reconnecting = False

    def _on_close(self, code: int | None = None, reason: str = "", *, generation: int | None = None) -> None:
        with self._reconnect_lock:
            if generation is not None and generation != self._connection_generation:
                return
            if generation is not None and generation == self._close_handled_generation:
                return
            self._close_handled_generation = generation
            if not self._running or self._attempts >= self.policy.max_attempts:
                self._connected = False
                if self._attempts >= self.policy.max_attempts:
                    self._failed = True
                self._last_error = f"{code}: {reason}" if code is not None else reason or None
                self._emit_health()
                return
            if self._reconnecting:
                self._connected = False
                self._last_error = f"{code}: {reason}" if code is not None else reason or None
                self._reconnect_requested.set()
                return
            self._connected = False
            self._last_error = f"{code}: {reason}" if code is not None else reason or None
            error_text = f"{code}: {reason}" if code is not None else reason
            if (
                "429" in error_text
                or "too many requests" in error_text.lower()
                or "toomanyrequests" in error_text.lower()
            ):
                self._rate_limited_until = self.clock() + self.policy.rate_limit_delay_seconds
            self._reconnect_requested.set()
        self._emit_health()
