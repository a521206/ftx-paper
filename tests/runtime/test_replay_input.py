from ftx_paper.runtime.replay_input import ReplayInput


class _Source:
    symbol = "NIFTY"
    expiry = ""

    def provenance(self):
        return {"source_bar_count": 0, "source_fingerprint": "source"}


class _Store:
    def __init__(self):
        self.read_dates_calls = 0
        self.read_expiry_dates_calls = 0

    def read_dates(self):
        self.read_dates_calls += 1
        return ["2026-01-02", "2026-01-05"]

    def read_expiry_dates(self):
        self.read_expiry_dates_calls += 1
        return ()


class _Resolver:
    def __init__(self):
        self.calls = []

    def resolve(self, session_date, *, rows=None):
        self.calls.append((session_date, rows))
        return _Source()


def test_manifest_reuses_loaded_rows_for_source_resolution():
    store = _Store()
    resolver = _Resolver()
    replay_input = ReplayInput(store, resolver)
    rows = [{
        "symbol": "NIFTY", "exchange": "NFO", "minute": "2026-01-05T03:45:00Z",
        "instrument_type": "FUT", "expiry": "2026-01-27",
    }]

    replay_input.manifest("2026-01-05", rows=rows, bars=(), futures_source=None)

    assert resolver.calls[0] == ("2026-01-05", rows)


def test_prior_source_provenance_is_cached_until_reset():
    store = _Store()
    resolver = _Resolver()
    replay_input = ReplayInput(store, resolver)

    first = replay_input.prior_source_provenance("2026-01-05")
    second = replay_input.prior_source_provenance("2026-01-05")

    assert first == second
    assert store.read_dates_calls == 1
    assert resolver.calls == [("2026-01-02", None)]

    replay_input.reset()
    replay_input.prior_source_provenance("2026-01-05")
    assert store.read_dates_calls == 2
