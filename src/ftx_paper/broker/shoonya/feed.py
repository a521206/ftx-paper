"""Shoonya market feed managing socket lifecycle, reconnection, and bar aggregation."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, time as datetime_time, timedelta, timezone
import logging
from threading import Event, RLock, Thread, current_thread
from time import monotonic, sleep
from typing import Any
from zoneinfo import ZoneInfo

from ftx_paper.contracts import MarketBar
from .socket import AsyncShoonyaSocket

IST = ZoneInfo("Asia/Kolkata")
logger = logging.getLogger(__name__)


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


def create_shoonya_socket(uid: str, access_token: str, **kwargs: Any) -> AsyncShoonyaSocket:
    """Return an asynchronous Shoonya WebSocket transport."""
    return AsyncShoonyaSocket(uid, access_token, **kwargs)


@dataclass(frozen=True, slots=True)
class ReconnectPolicy:
    max_attempts: int = 5
    initial_delay_seconds: float = 5.0
    max_delay_seconds: float = 60.0
    rate_limit_delay_seconds: float = 60.0

    def delay(self, attempt: int) -> float:
        return min(self.max_delay_seconds, self.initial_delay_seconds * (2 ** max(0, attempt - 1)))


class ShoonyaFeed:
    """Reconnectable Shoonya feed boundary; aggregates ticks into minute bars."""

    def __init__(
        self,
        socket: AsyncShoonyaSocket,
        instruments: Sequence[tuple[str, str | int]],
        normalize: Callable[[Mapping[str, Any]], MarketBar],
        on_bar: Callable[[MarketBar], None],
        policy: ReconnectPolicy = ReconnectPolicy(),
        pause: Callable[[float], None] = sleep,
        *,
        on_tick: Callable[[MarketBar], None] | None = None,
        on_health: Callable[[dict[str, Any]], None] | None = None,
        expected_instruments: Mapping[str, tuple[str, str]] | None = None,
        stale_after_seconds: float = 30.0,
        watchdog_interval_seconds: float = 5.0,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        if not instruments:
            raise ValueError("at least one Shoonya instrument is required")
        self.socket = socket
        self.instruments = instruments
        self.normalize = normalize
        self.on_bar = on_bar
        self.policy = policy
        self.pause = pause
        self.on_tick = on_tick
        self.on_health = on_health
        self.stale_after_seconds = stale_after_seconds
        self.watchdog_interval_seconds = watchdog_interval_seconds
        self.clock = clock

        self._running = False
        self._attempts = 0
        self._current: dict[str, MarketBar] = {}
        self._stop_event = Event()
        self._supervisor: Thread | None = None
        self._started_at = 0.0
        self._connected = False
        self._last_error: str | None = None
        self._rate_limited_until: float | None = None
        self._next_retry_at: datetime | None = None
        self._connection_lock = RLock()
        self._reconnecting = False
        self._failed = False
        self._health: dict[str, dict[str, Any]] = {
            key: {"symbol": symbol, "exchange": exchange, "tick_count": 0, "bar_count": 0}
            for key, (exchange, symbol) in (expected_instruments or {}).items()
        }

    def start(self) -> None:
        if self._running:
            return
        if self._supervisor is not None and self._supervisor.is_alive():
            raise RuntimeError("previous Shoonya feed supervisor did not stop")
        self._running = True
        self._attempts = 0
        self._last_error = None
        self._rate_limited_until = None
        self._next_retry_at = None
        self._failed = False
        self._started_at = self.clock()
        self._stop_event.clear()
        self._supervisor = Thread(
            target=self._run_supervisor, daemon=True, name="ftx-paper-shoonya-feed"
        )
        self._supervisor.start()
        self._emit_health()

    def stop(self) -> bool:
        self._running = False
        self._stop_event.set()
        self._connected = False
        self._next_retry_at = None
        with self._connection_lock:
            shutdown = self.socket.close()
        shutdown_ok = getattr(shutdown, "stopped", True) is not False
        if not shutdown_ok:
            self._failed = True
            self._last_error = "WebSocket worker did not stop cleanly"
        if self._supervisor and self._supervisor is not current_thread():
            self._supervisor.join(timeout=5)
        supervisor_stopped = self._supervisor is None or not self._supervisor.is_alive()
        if not supervisor_stopped:
            logger.error("Shoonya feed supervisor did not stop within 5 seconds")
        return shutdown_ok and supervisor_stopped

    def _connect(self) -> None:
        with self._connection_lock:
            if not self._running:
                return

            def on_close(code: int | None = None, reason: str = "") -> None:
                self._connected = False
                self._last_error = f"{code}: {reason}" if code is not None else reason or None

            self.socket.connect(self._on_message, on_close)
            self.socket.subscribe(self.instruments)

        wait_until_connected = getattr(self.socket, "wait_until_connected", None)
        connected = bool(wait_until_connected(12.0)) if callable(wait_until_connected) else True
        with self._connection_lock:
            if not self._running:
                return
            self._connected = connected
            if connected:
                self._next_retry_at = None
            elif not self._last_error:
                self._last_error = "Timed out waiting for Shoonya WebSocket connection"

    def _on_message(self, payload: Mapping[str, Any]) -> None:
        if self._running:
            try:
                bar = self.normalize(payload)
            except Exception as exc:
                logger.debug("Normalize skipped payload: %s", exc)
                return

            if self.on_tick is not None:
                self.on_tick(bar)

            key = f"{bar.instrument.exchange}:{bar.instrument.symbol}"
            health = self._health.setdefault(
                key,
                {
                    "symbol": bar.instrument.symbol,
                    "exchange": bar.instrument.exchange,
                    "tick_count": 0,
                    "bar_count": 0,
                },
            )
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
                self._current[key] = MarketBar(
                    current.instrument,
                    current.timestamp,
                    current.open,
                    max(current.high, bar.high),
                    min(current.low, bar.low),
                    bar.close,
                    bar.volume,
                    bar.open_interest,
                )
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
            self._health.setdefault(
                key,
                {
                    "symbol": bar.instrument.symbol,
                    "exchange": bar.instrument.exchange,
                    "tick_count": 0,
                    "bar_count": 0,
                },
            )["last_closed_bar_at"] = bar.timestamp.isoformat()
        self._current.clear()

    @staticmethod
    def _minute_bar(bar: MarketBar) -> MarketBar:
        return MarketBar(
            bar.instrument,
            bar.timestamp.replace(second=0, microsecond=0),
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.volume,
            bar.open_interest,
        )

    def health_snapshot(self) -> dict[str, Any]:
        now = self.clock()
        instruments = {}
        stale = []
        for key, item in self._health.items():
            snapshot = {
                name: value
                for name, value in item.items()
                if name != "last_tick_seen_monotonic"
            }
            last_seen = item.get("last_tick_seen_monotonic")
            snapshot["stale_seconds"] = (
                None if last_seen is None else max(0.0, now - last_seen)
            )
            age = now - (last_seen if last_seen is not None else self._started_at)
            if age > self.stale_after_seconds:
                snapshot["stale_seconds"] = age
                stale.append(key)
            instruments[key] = snapshot

        socket_connected = getattr(self.socket, "is_connected", None)
        connected = (
            bool(socket_connected) if socket_connected is not None else self._connected
        )
        activity_age = getattr(self.socket, "activity_age_seconds", None)
        transport_stale = bool(
            connected
            and activity_age is not None
            and activity_age > self.stale_after_seconds
        )

        if self._failed:
            state = "FAILED"
        elif connected:
            state = "CONNECTED"
        elif self._reconnecting:
            state = "RECONNECTING"
        elif self._running:
            state = "DISCONNECTED"
        else:
            state = "STOPPED"

        latest_ticks = [
            item.get("last_tick_at")
            for item in instruments.values()
            if item.get("last_tick_at")
        ]

        return {
            "connected": connected,
            "state": state,
            "reconnect_attempts": self._attempts,
            "last_error": self._last_error,
            "next_retry_at": self._next_retry_at.isoformat()
            if self._next_retry_at
            else None,
            "health_updated_at": datetime.now(timezone.utc).isoformat(),
            "last_tick_at": max(latest_ticks) if latest_ticks else None,
            "transport_stale": transport_stale,
            "rate_limited": self._rate_limited_until is not None
            and now < self._rate_limited_until,
            "rate_limited_until_monotonic": self._rate_limited_until,
            "stale_instruments": stale,
            "instruments": instruments,
        }

    def _emit_health(self) -> None:
        if self.on_health:
            try:
                self.on_health(self.health_snapshot())
            except Exception:
                logger.exception("Unable to publish Shoonya feed health")

    def _run_supervisor(self) -> None:
        while self._running and not self._failed:
            self._reconnecting = self._attempts > 0
            self._current.clear()
            try:
                self._connect()
                self._emit_health()
                if self._connected:
                    self._wait_for_disconnect()
            except Exception as exc:
                self._connected = False
                self._last_error = f"Connection failed: {exc}"
                logger.exception("Shoonya feed connection failed")
            finally:
                if self._running:
                    try:
                        with self._connection_lock:
                            result = self.socket.close()
                    except Exception:
                        logger.exception("Unable to close Shoonya WebSocket")
                        self._last_error = "WebSocket worker did not stop cleanly"
                        self._failed = True
                    else:
                        if getattr(result, "stopped", True) is False:
                            self._last_error = "WebSocket worker did not stop cleanly"
                            self._failed = True
                self._connected = False
                self._reconnecting = False

            if not self._running or self._failed:
                if self._failed:
                    self._connected = False
                    self._next_retry_at = None
                    self._emit_health()
                break
            if self._attempts >= self.policy.max_attempts:
                self._failed = True
                self._next_retry_at = None
                self._emit_health()
                break

            self._attempts += 1
            delay = self.policy.delay(self._attempts)
            if self._rate_limited_until is not None:
                delay = max(delay, self._rate_limited_until - self.clock())
            self._next_retry_at = datetime.now(timezone.utc) + timedelta(
                seconds=max(0.0, delay)
            )
            self._emit_health()
            if self.pause is sleep:
                self._stop_event.wait(delay)
            elif self._running:
                self.pause(delay)
            self._next_retry_at = None

    def _wait_for_disconnect(self) -> None:
        while self._running and self._connected:
            self._stop_event.wait(self.watchdog_interval_seconds)
            if not self._running:
                return
            snapshot = self.health_snapshot()
            self._emit_health()
            if snapshot["transport_stale"]:
                self._last_error = "WebSocket transport is stale"
                return
