from threading import Event
from time import monotonic

from ftx_paper.broker.zerodha.feed import ZerodhaFeed
from ftx_paper.broker.zerodha.socket import ShutdownResult


def test_initial_handshake_does_not_block_feed_start_or_stop():
    connecting = Event()
    released = Event()

    class Socket:
        def connect(self, _on_message, _on_close):
            connecting.set()

        def subscribe(self, _tokens):
            pass

        def wait_until_connected(self, _timeout):
            released.wait(5)
            return False

        def close(self):
            released.set()
            return ShutdownResult(stopped=True, worker_alive=False)

    feed = ZerodhaFeed(Socket(), [1], lambda payload: payload, lambda _bar: None)
    started_at = monotonic()
    feed.start()
    assert monotonic() - started_at < 1
    assert connecting.wait(1)

    stopped_at = monotonic()
    assert feed.stop()
    assert monotonic() - stopped_at < 1
    assert feed._initial_worker is not None and not feed._initial_worker.is_alive()
