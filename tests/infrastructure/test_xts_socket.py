import asyncio
from typing import cast

from ftx_paper.broker.xts.client import XTSClient
from ftx_paper.broker.xts.socket import AsyncXTSSocket, ShutdownResult


def test_xts_socket_initial_state():
    socket = AsyncXTSSocket(token="test_token", user_id="U123")
    assert not socket.is_connected()
    assert not socket.is_worker_alive()


def test_xts_socket_close_when_stopped():
    socket = AsyncXTSSocket(token="test_token", user_id="U123")
    res = socket.close()
    assert res == ShutdownResult(stopped=True, worker_alive=False)


def test_websocket_url_does_not_duplicate_socket_io_path():
    socket = AsyncXTSSocket(
        token="test_token",
        user_id="U123",
        root_url="https://example.test/apimarketdata",
        websocket_path="/apibinarymarketdata/socket.io/",
    )

    assert socket._build_ws_url().startswith(
        "wss://example.test/apibinarymarketdata/socket.io/?"
    )


def test_xts_socket_does_not_resubscribe_after_shutdown_begins():
    class Client:
        def __init__(self):
            self.calls = 0

        def subscription(self, instruments, *, message_code):
            self.calls += 1

    class WebSocket:
        async def recv(self):
            return '0{}'

        async def send(self, message):
            socket._stop_event.set()

    client = Client()
    socket = AsyncXTSSocket(
        token="test_token", user_id="U123", client=cast(XTSClient, client)
    )
    socket._instruments = [(1, 123)]

    asyncio.run(socket._receive_loop(WebSocket()))

    assert client.calls == 0
