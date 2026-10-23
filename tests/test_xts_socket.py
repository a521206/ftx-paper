from ftx_paper.broker.xts.socket import AsyncXTSSocket, ShutdownResult


def test_xts_socket_initial_state():
    socket = AsyncXTSSocket(token="test_token", user_id="U123")
    assert not socket.is_connected()
    assert not socket.is_worker_alive()


def test_xts_socket_close_when_stopped():
    socket = AsyncXTSSocket(token="test_token", user_id="U123")
    res = socket.close()
    assert res == ShutdownResult(stopped=True, worker_alive=False)
