"""Single-owner asynchronous Shoonya (Finvasia) WebSocket transport."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
import json
import logging
from threading import Event, RLock, Thread, current_thread
from time import monotonic
from typing import Any

from .protocol import (
    connection_message,
    decode_message,
    heartbeat_message,
    subscription_messages,
)

logger = logging.getLogger(__name__)

DEFAULT_WS_URL = "wss://api.shoonya.com/NorenWSAPI/"


class SocketState(str, Enum):
    STOPPED = "STOPPED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    STOPPING = "STOPPING"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class ShutdownResult:
    stopped: bool
    worker_alive: bool


class AsyncShoonyaSocket:
    """Single-owner asynchronous transport for Shoonya WebSocket feed."""

    def __init__(
        self,
        uid: str,
        token: str,
        *,
        url: str = DEFAULT_WS_URL,
    ) -> None:
        self._uid = uid
        self._token = token
        self._url = url
        self._instruments: list[tuple[str, str | int]] = []
        self._connected = False
        self._authenticated = False
        self._websocket: Any = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: Thread | None = None
        self._task: asyncio.Task[None] | None = None
        self._ping_task: asyncio.Task[None] | None = None
        self._stop_event = Event()
        self._connection_done = Event()
        self._last_activity = 0.0
        self._close_notified = False
        self._state = SocketState.STOPPED
        self._on_message: Callable[[Mapping[str, Any]], None] | None = None
        self._on_close: Callable[..., None] | None = None
        self._lifecycle_lock = RLock()

    def connect(
        self,
        on_message: Callable[[Mapping[str, Any]], None],
        on_close: Callable[..., None],
    ) -> None:
        with self._lifecycle_lock:
            if self._thread is not None and self._thread.is_alive():
                raise RuntimeError("previous Shoonya WebSocket worker did not stop")
            self._connected = False
            self._authenticated = False
            self._connection_done.clear()
            self._close_notified = False
            self._last_activity = 0.0
            self._on_message = on_message
            self._on_close = on_close
            self._stop_event.clear()
            self._state = SocketState.CONNECTING
            self._thread = Thread(
                target=self._run_thread,
                daemon=True,
                name="ftx-paper-shoonya-ws",
            )
            self._thread.start()

    def subscribe(self, instruments: Sequence[tuple[str, str | int]]) -> None:
        """Subscribe to exchange-token pairs, e.g. [('NSE', 26000), ('NFO', 35000)]."""
        with self._lifecycle_lock:
            self._instruments = list(instruments)

            loop = self._loop
            ws = self._websocket
        if loop is not None and loop.is_running() and ws is not None and self._authenticated:
            asyncio.run_coroutine_threadsafe(self._send_subscription(), loop)

    def close(self) -> ShutdownResult:
        with self._lifecycle_lock:
            if self._state is SocketState.STOPPED:
                return ShutdownResult(stopped=True, worker_alive=False)
            self._state = SocketState.STOPPING
            self._stop_event.set()
            loop = self._loop
            task = self._task
            ping_task = self._ping_task
            ws = self._websocket
            thread = self._thread

        if loop is not None and loop.is_running():
            if ping_task is not None and not ping_task.done():
                loop.call_soon_threadsafe(ping_task.cancel)
            if task is not None and not task.done():
                loop.call_soon_threadsafe(task.cancel)
            if ws is not None:
                asyncio.run_coroutine_threadsafe(ws.close(), loop)

        if thread is not None and thread is not current_thread():
            thread.join(timeout=3.0)

        worker_alive = thread is not None and thread.is_alive()
        stopped = not worker_alive
        with self._lifecycle_lock:
            if stopped:
                self._state = SocketState.STOPPED
                self._connected = False
                self._authenticated = False
                self._websocket = None
                self._thread = None
        return ShutdownResult(stopped=stopped, worker_alive=worker_alive)

    def wait_until_connected(self, timeout: float = 15.0) -> bool:
        self._connection_done.wait(timeout)
        return self.is_connected

    @property
    def is_connected(self) -> bool:
        with self._lifecycle_lock:
            return self._connected and self._authenticated


    @property
    def activity_age_seconds(self) -> float | None:
        with self._lifecycle_lock:
            if self._last_activity <= 0.0:
                return None
            return max(0.0, monotonic() - self._last_activity)

    def _mark_activity(self) -> None:
        self._last_activity = monotonic()

    def _notify_close(self, code: int | None = None, reason: str = "") -> None:
        callback = None
        with self._lifecycle_lock:
            if not self._close_notified:
                self._close_notified = True
                callback = self._on_close
        if callback is not None:
            try:
                callback(code, reason)
            except Exception:
                logger.exception("Shoonya WebSocket on_close callback failed")

    def _run_thread(self) -> None:
        loop = asyncio.new_event_loop()
        loop.set_exception_handler(self._handle_loop_exception)
        with self._lifecycle_lock:
            self._loop = loop
            if self._stop_event.is_set():
                task = None
            else:
                task = loop.create_task(self._run_once())
                self._task = task
        asyncio.set_event_loop(loop)
        try:
            if task is not None:
                try:
                    loop.run_until_complete(task)
                except asyncio.CancelledError:
                    pass
        finally:
            self._connection_done.set()
            with self._lifecycle_lock:
                self._task = None
                self._ping_task = None
            pending = asyncio.all_tasks(loop)
            for t in pending:
                t.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()
            with self._lifecycle_lock:
                self._loop = None
                if self._stop_event.is_set():
                    self._state = SocketState.STOPPED

    @staticmethod
    def _handle_loop_exception(loop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
        exception = context.get("exception")
        if isinstance(exception, ConnectionResetError) and getattr(exception, "winerror", None) == 10054:
            return
        loop.default_exception_handler(context)

    async def _run_once(self) -> None:
        if self._stop_event.is_set():
            return
        try:
            from websockets.asyncio import client as websockets_client
        except ImportError as exc:
            with self._lifecycle_lock:
                self._state = SocketState.FAILED
            if not self._stop_event.is_set():
                self._notify_close(1006, "websockets is required for Shoonya live feed")
            logger.error("WebSockets client unavailable: %s", exc)
            return

        try:
            async with websockets_client.connect(
                self._url,
                open_timeout=10,
                close_timeout=2,
                ping_interval=None,
                compression=None,
                max_size=10 * 1024 * 1024,
                additional_headers={"User-Agent": "FTX-Paper-ShoonyaClient/1.0"},
            ) as websocket:
                with self._lifecycle_lock:
                    self._websocket = websocket
                    self._connected = True
                self._mark_activity()

                # Send connection/auth handshake
                auth_msg = connection_message(self._uid, self._token)
                await websocket.send(auth_msg)

                # Wait for connection ack
                authenticated = False
                while not authenticated and not self._stop_event.is_set():
                    msg = await websocket.recv()
                    self._mark_activity()
                    if isinstance(msg, bytes):
                        msg = msg.decode("utf-8", errors="replace")
                    try:
                        res = json.loads(msg)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(res, dict) and res.get("t") == "ck":
                        if str(res.get("s", "")).upper() == "OK":
                            authenticated = True
                            with self._lifecycle_lock:
                                self._authenticated = True
                                self._state = SocketState.CONNECTED
                            self._connection_done.set()
                            break
                        else:
                            raise RuntimeError(f"Shoonya WebSocket auth rejected: {res}")

                # Start periodic heartbeat ping task
                loop = asyncio.get_running_loop()
                self._ping_task = loop.create_task(self._run_heartbeat(websocket))

                # Send subscription
                await self._send_subscription()

                # Stream and dispatch incoming tick messages
                async for message in websocket:
                    self._mark_activity()
                    self._dispatch_message(message)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            with self._lifecycle_lock:
                self._state = SocketState.FAILED
            if not self._stop_event.is_set():
                self._notify_close(1006, str(exc))
        finally:
            with self._lifecycle_lock:
                self._connected = False
                self._authenticated = False
                self._websocket = None
            if not self._stop_event.is_set():
                self._notify_close(1006, "WebSocket connection closed")

    async def _run_heartbeat(self, websocket: Any) -> None:
        """Send a Shoonya heartbeat ping every 30 seconds."""
        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(30)
                if self._stop_event.is_set():
                    break
                await websocket.send(heartbeat_message())
        except (asyncio.CancelledError, Exception):
            pass

    async def _send_subscription(self) -> None:
        if self._websocket is None or not self._instruments:
            return
        msg = subscription_messages(self._instruments)
        await self._websocket.send(msg)

    def _dispatch_message(self, message: str | bytes) -> None:
        if self._on_message is None:
            return
        payloads = decode_message(message)
        for payload in payloads:
            try:
                self._on_message(payload)
            except Exception:
                logger.exception("Shoonya on_message callback raised an error")
