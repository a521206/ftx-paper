"""Single-owner asynchronous Symphony / IIFL XTS WebSocket transport."""

from __future__ import annotations

import asyncio
import logging
import urllib.parse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from threading import Event, RLock, Thread, current_thread
from time import monotonic
from typing import Any

from .client import DEFAULT_ROOT_URL, XTSClient
from .protocol import (
    build_subscription_payload,
    decode_binary_packets,
    decode_touchline_data,
    extract_socket_io_data,
)

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


class AsyncXTSSocket:
    """Single-owner asynchronous transport for Symphony / IIFL XTS WebSocket feed."""

    def __init__(
        self,
        token: str,
        user_id: str = "",
        *,
        root_url: str = DEFAULT_ROOT_URL,
        client: XTSClient | None = None,
    ) -> None:
        self._token = token
        self._user_id = user_id
        self._root_url = root_url.rstrip("/")
        self._client = client or XTSClient(token=token, user_id=user_id, root_url=self._root_url)
        self._instruments: list[tuple[str | int, int]] = []
        self._connected = False
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
                raise RuntimeError("previous XTS WebSocket worker did not stop")
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
                name="ftx-paper-xts-ws",
            )
            self._thread.start()

    def subscribe(self, instruments: Sequence[tuple[str | int, int]]) -> None:
        """Subscribe to exchange-token pairs."""
        with self._lifecycle_lock:
            self._instruments = list(instruments)
            # Send subscription via REST API
            if self._instruments and self._token:
                try:
                    payload = build_subscription_payload(self._instruments, message_code=1502)
                    self._client.subscription(payload["instruments"], message_code=1502)
                    logger.info("Subscribed %d instruments to XTS 1502 touchline feed", len(self._instruments))
                except Exception as exc:
                    logger.warning("XTS REST subscription failed: %s", exc)

    def is_connected(self) -> bool:
        with self._lifecycle_lock:
            return self._state is SocketState.CONNECTED and self._connected

    def wait_until_connected(self, timeout: float = 10.0) -> bool:
        return self._connection_done.wait(timeout=timeout)

    def is_worker_alive(self) -> bool:
        with self._lifecycle_lock:
            return self._thread is not None and self._thread.is_alive()

    def close(self, timeout: float = 5.0) -> ShutdownResult:
        with self._lifecycle_lock:
            if self._state is SocketState.STOPPED and (self._thread is None or not self._thread.is_alive()):
                return ShutdownResult(stopped=True, worker_alive=False)

            self._state = SocketState.STOPPING
            self._stop_event.set()
            loop = self._loop
            task = self._task
            ping_task = self._ping_task
            ws = self._websocket
            thread = self._thread

        if loop is not None and loop.is_running():
            async def _cleanup():
                if ping_task and not ping_task.done():
                    ping_task.cancel()
                if ws is not None:
                    try:
                        await ws.close()
                    except Exception:
                        pass
                if task and not task.done():
                    task.cancel()

            future = asyncio.run_coroutine_threadsafe(_cleanup(), loop)
            try:
                future.result(timeout=timeout)
            except Exception:
                pass

        if thread is not None and thread.is_alive() and current_thread() is not thread:
            thread.join(timeout=timeout)

        with self._lifecycle_lock:
            worker_alive = self._thread is not None and self._thread.is_alive()
            if not worker_alive:
                self._state = SocketState.STOPPED
                self._connected = False
                self._websocket = None
                self._thread = None
                self._loop = None
            return ShutdownResult(stopped=not worker_alive, worker_alive=worker_alive)

    def _build_ws_url(self) -> str:
        base_api = getattr(self._client, "root_url", self._root_url)
        parsed = urllib.parse.urlparse(base_api)
        scheme = "wss" if parsed.scheme in ("https", "wss") else "ws"
        netloc = parsed.netloc or parsed.path
        path = parsed.path.rstrip("/") if parsed.netloc else ""
        if not path:
            path = "/apibinarymarketdata"
        user_id = self._user_id or getattr(self._client, "user_id", "")
        if not user_id and self._token:
            try:
                import base64
                import json
                parts = self._token.split(".")
                if len(parts) >= 2:
                    padding = 4 - len(parts[1]) % 4
                    payload_raw = base64.b64decode(parts[1] + "=" * padding).decode("utf-8", "ignore")
                    jwt_data = json.loads(payload_raw)
                    user_id = str(jwt_data.get("userID", "")).split("_")[0]
            except Exception:
                pass
        query = urllib.parse.urlencode({
            "token": self._token,
            "userID": user_id,
            "publishFormat": "JSON",
            "broadcastMode": "Full",
            "EIO": "3",
            "transport": "websocket",
        })
        return f"{scheme}://{netloc}{path}/socket.io/?{query}"

    def _run_thread(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        with self._lifecycle_lock:
            self._loop = loop
        try:
            loop.run_until_complete(self._run_session())
        finally:
            try:
                pending = asyncio.all_tasks(loop)
                for t in pending:
                    t.cancel()
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                loop.close()
                with self._lifecycle_lock:
                    self._loop = None
                    self._connected = False
                    if self._state is not SocketState.STOPPING:
                        self._state = SocketState.STOPPED
                self._notify_close(1000, "Worker stopped")

    async def _run_session(self) -> None:
        import websockets

        ws_url = self._build_ws_url()
        reconnect_delay = 1.0

        while not self._stop_event.is_set():
            try:
                logger.info("Connecting to XTS WebSocket at %s", ws_url.split("?")[0])
                async with websockets.connect(
                    ws_url,
                    ping_interval=None,  # We manage Engine.IO heartbeats explicitly
                    close_timeout=5.0,
                    open_timeout=10.0,
                ) as ws:
                    with self._lifecycle_lock:
                        self._websocket = ws
                        self._last_activity = monotonic()

                    reconnect_delay = 1.0
                    self._ping_task = asyncio.create_task(self._ping_loop(ws))
                    try:
                        await self._receive_loop(ws)
                    finally:
                        if self._ping_task and not self._ping_task.done():
                            self._ping_task.cancel()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if self._stop_event.is_set():
                    break
                logger.warning("XTS WebSocket connection error: %s. Reconnecting in %.1fs...", exc, reconnect_delay)
                with self._lifecycle_lock:
                    self._connected = False
                    self._state = SocketState.CONNECTING
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 1.5, 30.0)

    async def _receive_loop(self, ws: Any) -> None:
        while not self._stop_event.is_set():
            try:
                msg = await ws.recv()
            except Exception:
                break

            with self._lifecycle_lock:
                self._last_activity = monotonic()

            if isinstance(msg, bytes):
                quotes = decode_binary_packets(msg)
                callback = self._on_message
                if callback:
                    for quote in quotes:
                        try:
                            callback(quote)
                        except Exception as cb_exc:
                            logger.error("Error in XTS message callback: %s", cb_exc)
                continue

            text = msg.strip()
            if not text:
                continue

            # Socket.IO binary packet announcement frame (e.g. 451-["xts-binary-packet", ...])
            if text.startswith("451-") or "xts-binary-packet" in text:
                continue

            # Engine.IO Handshake packet
            if text.startswith("0{"):
                # Send Socket.IO connect frame
                try:
                    await ws.send("40")
                except Exception:
                    break
                with self._lifecycle_lock:
                    self._connected = True
                    self._state = SocketState.CONNECTED
                    self._connection_done.set()
                logger.info("XTS WebSocket connected and handshake completed")

                # Resubscribe instruments
                if self._instruments:
                    try:
                        payload = build_subscription_payload(self._instruments, message_code=1502)
                        self._client.subscription(payload["instruments"], message_code=1502)
                    except Exception as sub_exc:
                        logger.warning("Failed resubscribing XTS instruments: %s", sub_exc)
                continue

            # Engine.IO Ping from server
            if text == "2":
                try:
                    await ws.send("3")  # Pong
                except Exception:
                    break
                continue

            # Engine.IO Pong
            if text == "3":
                continue

            # Socket.IO Event data
            if text.startswith("42"):
                result = extract_socket_io_data(text)
                if result is None:
                    continue
                event_name, data = result
                # 1502-json-full or 1501-json-full
                quotes = decode_touchline_data(data)
                callback = self._on_message
                if callback:
                    for quote in quotes:
                        try:
                            callback(quote)
                        except Exception as cb_exc:
                            logger.error("Error in XTS message callback: %s", cb_exc)

    async def _ping_loop(self, ws: Any) -> None:
        """Send periodic Engine.IO ping '2' every 25 seconds."""
        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(25.0)
                try:
                    await ws.send("2")
                except Exception:
                    break
        except asyncio.CancelledError:
            pass

    def _notify_close(self, code: int, reason: str) -> None:
        with self._lifecycle_lock:
            if self._close_notified:
                return
            self._close_notified = True
            callback = self._on_close
        if callback is not None:
            try:
                callback(code, reason)
            except Exception as exc:
                logger.error("Error in on_close callback: %s", exc)
