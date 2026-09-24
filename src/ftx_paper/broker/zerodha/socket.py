"""Single-owner asynchronous Zerodha WebSocket transport."""

from __future__ import annotations

import asyncio
import logging
import sys
import traceback
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from threading import Event, RLock, Thread, current_thread
from time import monotonic
from typing import Any

from .protocol import decode_binary_frame, decode_text_error, subscription_messages

logger = logging.getLogger(__name__)


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


class AsyncZerodhaSocket:
    """One connection attempt per worker; reconnect policy belongs to the feed."""

    def __init__(self, api_key: str, access_token: str) -> None:
        self._url = f"wss://ws.kite.trade?api_key={api_key}&access_token={access_token}"
        self._tokens: list[int] = []
        self._connected = False
        self._websocket: Any = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: Thread | None = None
        self._task: asyncio.Task[None] | None = None
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
                raise RuntimeError("previous Zerodha WebSocket worker did not stop")
            self._connected = False
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
                name="ftx-paper-zerodha-ws",
            )
            self._thread.start()

    def _run_thread(self) -> None:
        loop = asyncio.new_event_loop()
        loop.set_exception_handler(self._handle_loop_exception)
        with self._lifecycle_lock:
            self._loop = loop
            # close() sets the stop event before taking this lock. Publishing
            # the loop and task under one lock means close() either sees the
            # task and cancels it, or this worker sees shutdown and never
            # starts a connection attempt.
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
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True),
                )
            loop.close()
            with self._lifecycle_lock:
                self._loop = None
                if self._stop_event.is_set():
                    self._state = SocketState.STOPPED

    @staticmethod
    def _handle_loop_exception(
        loop: asyncio.AbstractEventLoop,
        context: dict[str, Any],
    ) -> None:
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
                self._notify_close(1006, "Install ftx-paper[zerodha] to use the live feed")
            logger.error("Zerodha WebSocket dependency is unavailable: %s", exc)
            return
        try:
            async with websockets_client.connect(
                self._url,
                open_timeout=10,
                close_timeout=2,
                ping_interval=None,
                compression=None,
                max_size=10 * 1024 * 1024,
                additional_headers={"User-Agent": "FTX-Paper-ZerodhaClient/1.0"},
            ) as websocket:
                with self._lifecycle_lock:
                    self._websocket = websocket
                    self._connected = True
                    self._state = SocketState.CONNECTED
                self._mark_activity()
                self._connection_done.set()
                await self._send_subscription()
                async for message in websocket:
                    self._mark_activity()
                    if isinstance(message, bytes):
                        self._dispatch_binary(message)
                    elif isinstance(message, str):
                        if await self._dispatch_text(message):
                            self._stop_event.set()
                            await websocket.close()
                            break
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
                self._websocket = None
            if not self._stop_event.is_set():
                self._notify_close(1006, "WebSocket connection closed")

    async def _send_subscription(self) -> None:
        if self._websocket is None or not self._tokens:
            return
        for message in subscription_messages(self._tokens):
            await self._websocket.send(message)

    def _dispatch_binary(self, message: bytes) -> None:
        try:
            payloads = decode_binary_frame(message)
        except Exception:
            logger.exception("Unable to decode Zerodha binary frame")
            return
        if self._on_message is None:
            return
        for payload in payloads:
            try:
                self._on_message(payload)
            except Exception:
                logger.exception("Zerodha tick consumer failed")

    @staticmethod
    def _parse_binary_message(message: bytes) -> tuple[dict[str, Any], ...]:
        """Compatibility shim for the former factory-local test boundary."""
        return decode_binary_frame(message)

    def _handle_text_message(self, message: str) -> None:
        """Compatibility shim for the former factory-local test boundary."""
        reason = decode_text_error(message)
        if reason is not None:
            self._notify_close(1008, reason)

    async def _dispatch_text(self, message: str) -> bool:
        reason = decode_text_error(message)
        if reason is not None:
            self._notify_close(1008, reason)
            return True
        return False

    def _notify_close(self, code: int, reason: str) -> None:
        with self._lifecycle_lock:
            if self._close_notified:
                return
            self._close_notified = True
            callback = self._on_close
        if callback is not None:
            callback(code, reason)

    def _mark_activity(self) -> None:
        self._last_activity = monotonic()

    def subscribe(self, tokens: list[int]) -> None:
        with self._lifecycle_lock:
            self._tokens = [int(token) for token in tokens]
            loop = self._loop
            connected = self._connected
        if connected and loop is not None:
            subscription = self._send_subscription()
            try:
                asyncio.run_coroutine_threadsafe(subscription, loop)
            except RuntimeError:
                subscription.close()
                logger.debug("Ignored subscription update while WebSocket loop was stopping")

    def close(self) -> ShutdownResult:
        # Set this before observing the loop/task. The worker publishes both
        # while holding _lifecycle_lock, so no new task can slip in behind the
        # shutdown snapshot.
        self._stop_event.set()
        with self._lifecycle_lock:
            self._state = SocketState.STOPPING
            loop = self._loop
            task = self._task
            thread = self._thread
        self._connected = False
        self._connection_done.set()
        if loop is not None and loop.is_running() and task is not None:
            loop.call_soon_threadsafe(task.cancel)
        if thread is not None and thread is not current_thread():
            thread.join(timeout=5)
        with self._lifecycle_lock:
            worker_alive = thread is not None and thread.is_alive()
            if not worker_alive:
                self._thread = None
                self._state = SocketState.STOPPED
            else:
                self._state = SocketState.FAILED
        if worker_alive:
            frames = sys._current_frames()
            frame = frames.get(thread.ident) if thread is not None else None
            stack = "".join(traceback.format_stack(frame)) if frame is not None else "<stack unavailable>"
            logger.error(
                "Zerodha WebSocket worker did not stop within 5 seconds; current stack:\n%s",
                stack,
            )
        return ShutdownResult(stopped=not worker_alive, worker_alive=worker_alive)

    def wait_until_connected(self, timeout: float = 15.0) -> bool:
        self._connection_done.wait(timeout)
        return self.is_connected

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def activity_age_seconds(self) -> float | None:
        if not self._last_activity:
            return None
        return max(0.0, monotonic() - self._last_activity)

    @property
    def state(self) -> SocketState:
        return self._state
