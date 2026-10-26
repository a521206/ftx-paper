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
