from ftx_paper.application.process_market_bar import ProcessMarketBar


class _Events:
    def __init__(self):
        self.values = []

    def append(self, event_type, payload, idempotency_key=None):
        self.values.append((event_type, payload))
        return True


class _Uow:
    def __init__(self):
        self.events = _Events()
        self.committed = False
        self.rolled_back = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        if exc_type:
            self.rolled_back = True

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


def test_process_market_bar_uses_fresh_uow_and_explicit_commit():
    uow = _Uow()
    strategy = type("Strategy", (), {"on_bar": lambda self, bar: {"accepted": True}})()
    result = ProcessMarketBar(lambda: uow, strategy).execute(object())
    assert result == {"accepted": True}
    assert uow.committed
    assert not uow.rolled_back
    assert uow.events.values[0][0] == "MARKET_BAR_PROCESSED"
