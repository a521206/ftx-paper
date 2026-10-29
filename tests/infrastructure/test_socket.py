from time import monotonic

from ftx_paper.broker.zerodha.socket import AsyncZerodhaSocket


def test_failed_connection_wakes_handshake_waiter(monkeypatch):
    async def fail_immediately(_self):
        return None

    monkeypatch.setattr(AsyncZerodhaSocket, "_run_once", fail_immediately)
    socket = AsyncZerodhaSocket("key", "token")
    socket.connect(lambda _payload: None, lambda *_args: None)

    started_at = monotonic()
    assert not socket.wait_until_connected(2)
    assert monotonic() - started_at < 1
    assert socket.close().stopped
