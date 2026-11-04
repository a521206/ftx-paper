from ftx_paper.runtime.orderflow_setup import validate_atr_artifact_source
from ftx_paper.runtime.futures_source import (
    FuturesSessionSource, SessionSourceIdentity,
)
from ftx_paper.runtime.replay_input import ReplayInput
import pytest


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


def test_bars_keeps_selected_futures_source_exchange():
    source = FuturesSessionSource(
        session_date="2026-01-05",
        identity=SessionSourceIdentity(
            symbol="NIFTY26JANFUT", exchange="NFO", instrument_type="FUT",
            expiry="2026-01-27", policy_version="test",
        ),
        bars=(), source_bar_count=0, source_fingerprint="source",
    )
    replay_input = ReplayInput(_Store(), _Resolver())
    rows = [{
        "symbol": "NIFTY26JANFUT", "exchange": "NFO",
        "minute": "2026-01-05T03:45:00Z", "instrument_type": "FUT",
        "expiry": "2026-01-27", "open": 100, "high": 101,
        "low": 99, "close": 100, "volume": 1,
    }]

    bars = replay_input.bars("2026-01-05", rows=rows, futures_source=source)

    assert len(bars) == 1
    assert bars[0].instrument.symbol == "NIFTY"
    assert bars[0].instrument.exchange == "NFO"


def test_atr_artifact_source_validation_rejects_invalid_lineage_and_late_source_bar():
    artifact = {
        "source": {
            "sessions": ["2026-01-02"],
            "source_fingerprints": ["history-fingerprint"],
            "source_bar_count": 376,
        },
        "payload": {
            "target_session": "2026-01-05",
            "source_session": "2026-01-02",
            "observed_at": "2026-01-02T10:00:00Z",
            "available_at": "2026-01-05T03:45:00Z",
        },
    }

    assert validate_atr_artifact_source(
        artifact, "2026-01-05",
    ) == artifact["payload"]

    invalid_lineage = {
        **artifact,
        "source": {**artifact["source"], "sessions": []},
    }
    with pytest.raises(ValueError, match="source lineage is invalid"):
        validate_atr_artifact_source(
            invalid_lineage, "2026-01-05",
        )

    late_source = {
        **artifact,
        "payload": {**artifact["payload"], "observed_at": "2026-01-02T10:09:00Z"},
    }
    with pytest.raises(ValueError, match="session bounds"):
        validate_atr_artifact_source(
            late_source, "2026-01-05",
        )
