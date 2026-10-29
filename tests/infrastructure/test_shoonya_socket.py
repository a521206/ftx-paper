from time import monotonic

from ftx_paper.broker.shoonya.socket import AsyncShoonyaSocket


def test_shoonya_failed_connection_wakes_handshake_waiter(monkeypatch):
    async def fail_immediately(_self):
        return None

    monkeypatch.setattr(AsyncShoonyaSocket, "_run_once", fail_immediately)
    socket = AsyncShoonyaSocket("uid", "token")
    socket.connect(lambda _payload: None, lambda *_args: None)

    started_at = monotonic()
    assert not socket.wait_until_connected(2)
    assert monotonic() - started_at < 1
    assert socket.close().stopped
