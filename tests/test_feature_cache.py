from datetime import datetime

from ftx_paper.contracts import Instrument, MarketBar, MarketRole
from ftx_paper.market import DecisionBundle, FeatureMemoryCache, FeatureService, LiveFeatureCalculator
from ftx_paper.infrastructure.sqlite.feature_repository import SqliteFeatureRepository
from ftx_paper.runtime import RuntimeStore


def _bar(minute: int, close: float) -> MarketBar:
    return MarketBar(
        Instrument("NIFTYFUT", "NFO", "FUTURES"),
        datetime.fromisoformat(f"2026-01-01T09:{minute:02d}:00+05:30"),
        close - 1, close + 1, close - 2, close, 100,
    )


def _vix(minute: int, close: float) -> MarketBar:
    return MarketBar(
        Instrument("INDIA VIX", "NSE", "VIX"),
        datetime.fromisoformat(f"2026-01-01T09:{minute:02d}:00+05:30"),
        close, close, close, close, 10,
    )


def _bundle(bar: MarketBar) -> DecisionBundle:
    return DecisionBundle(
        f"2026-01-01:09:{bar.timestamp.minute:02d}", "2026-01-01",
        f"09:{bar.timestamp.minute:02d}", {MarketRole.FUTURES: bar}, (MarketRole.FUTURES,),
    )


class CountingCalculator(LiveFeatureCalculator):
    calls = 0

    def calculate_event(self, *args, **kwargs):
        self.calls += 1
        return super().calculate_event(*args, **kwargs)


def test_feature_service_uses_runtime_memory_cache():
    calculator = CountingCalculator()
    service = FeatureService(calculator=calculator, memory_cache=FeatureMemoryCache())
    bars = (_bar(15, 100), _bar(16, 101), _bar(17, 102))
    for bar in bars:
        service.ingest(bar)
    bundle = _bundle(bars[-1])

    first = service.enrich(bundle)
    second = service.enrich(bundle)

    assert first.feature_snapshot is second.feature_snapshot
    assert calculator.calls == 1


def test_feature_service_round_trips_sqlite_snapshot(tmp_path):
    store = RuntimeStore(tmp_path / "runtime")
    repository = SqliteFeatureRepository(store.database)
    service = FeatureService(repository=repository)
    bars = (_bar(15, 100), _bar(16, 101), _bar(17, 102))
    for bar in bars:
        service.ingest(bar)
    snapshot = service.enrich(_bundle(bars[-1])).feature_snapshot

    assert snapshot is not None
    restored = SqliteFeatureRepository(store.database).read_feature_snapshot(snapshot.key)
    assert restored is not None
    assert restored.values == snapshot.values
    assert restored.source_bar_ids == snapshot.source_bar_ids


def test_feature_service_excludes_vix_bars_after_event_cutoff():
    service = FeatureService()
    bars = (_bar(15, 100), _bar(16, 101), _bar(17, 102))
    for bar in (*bars[:2], _vix(15, 12), _vix(18, 99), bars[2]):
        service.ingest(bar)

    snapshot = service.enrich(_bundle(bars[-1])).feature_snapshot

    assert snapshot is not None
    assert snapshot.values["vix"] == 12.0
    assert snapshot.values["vix_open"] == 12.0
