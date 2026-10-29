from datetime import date, datetime, timedelta, timezone

from ftx_paper.contracts import Instrument, MarketBar, MarketRole
from ftx_paper.market import DecisionBundle, FeatureMemoryCache, FeatureService, LiveFeatureCalculator
from ftx_paper.market.feature_cache import (
    FeatureCacheKey, FeatureSnapshot, ORDERFLOW_DAILY_ATR_CALCULATOR_VERSION,
    ORDERFLOW_DAILY_ATR_FEATURE_SET,
    build_daily_atr_snapshot, build_orderflow_daily_atr_snapshot, daily_atr_from_snapshot,
)
from ftx_paper.infrastructure.sqlite.feature_repository import SqliteFeatureRepository
from ftx_paper.runtime import RuntimeStore
from ftx_paper.runtime.futures_source import materialize_orderflow_daily_atr
from ftx_paper.runtime.replay_worker import ReplayWorker


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


def test_daily_atr_artifact_requires_canonical_feature_set_and_availability():
    key = FeatureCacheKey(
        instrument="NIFTY", session_date="2026-01-02", timeframe="1d",
        cutoff_minute="00:00", feature_set=ORDERFLOW_DAILY_ATR_FEATURE_SET,
        calculator_version=ORDERFLOW_DAILY_ATR_CALCULATOR_VERSION,
        parameters=(
            ("series", "continuous"), ("window", "14"),
            ("zones", "09:15-11:00,11:00-13:00,13:00-15:00"),
        ),
        input_fingerprint="canonical-fingerprint",
    )
    snapshot = FeatureSnapshot(
        key=key, values={"atr_14": 12.5}, source_bar_ids=("daily-1",),
        computed_at=datetime.fromisoformat("2026-01-02T00:00:00+00:00"),
    )

    assert daily_atr_from_snapshot(snapshot) == 12.5
    assert daily_atr_from_snapshot(FeatureSnapshot(
        key=key, values={"atr_14": 12.5}, source_bar_ids=(),
        computed_at=snapshot.computed_at, availability="unavailable",
    )) is None


def test_sqlite_feature_repository_reads_daily_atr_artifact_by_session(tmp_path):
    store = RuntimeStore(tmp_path / "runtime")
    repository = SqliteFeatureRepository(store.database)
    key = FeatureCacheKey(
        instrument="NIFTY", session_date="2026-01-02", timeframe="1d",
        cutoff_minute="00:00", feature_set=ORDERFLOW_DAILY_ATR_FEATURE_SET,
        calculator_version=ORDERFLOW_DAILY_ATR_CALCULATOR_VERSION,
        parameters=(
            ("series", "continuous"), ("window", "14"),
            ("zones", "09:15-11:00,11:00-13:00,13:00-15:00"),
        ),
        input_fingerprint="canonical-fingerprint",
    )
    repository.write_feature_snapshot(FeatureSnapshot(
        key=key, values={"atr_14": 12.5}, source_bar_ids=("daily-1",),
        computed_at=datetime.fromisoformat("2026-01-02T00:00:00+00:00"),
    ))

    restored = repository.read_feature_snapshot_for_session(
        feature_set=ORDERFLOW_DAILY_ATR_FEATURE_SET,
        instrument="NIFTY", session_date="2026-01-02",
    )
    assert restored is not None
    assert daily_atr_from_snapshot(restored) == 12.5
    assert ReplayWorker(store)._prior_daily_atr("2026-01-02", []) == 12.5


def test_paper_materializes_daily_atr_from_its_own_market_history(tmp_path):
    store = RuntimeStore(tmp_path / "runtime")
    start = date(2026, 1, 1)
    bars = []
    dates = []
    for day_index in range(14):
        session_date = start + timedelta(days=day_index)
        date_value = session_date.isoformat()
        dates.append(date_value)
        for minute_index in range(375):
            timestamp = datetime.combine(
                session_date, datetime.strptime("09:15", "%H:%M").time(),
            ).replace(tzinfo=timezone(timedelta(hours=5, minutes=30))) + timedelta(minutes=minute_index)
            bars.append(MarketBar(
                Instrument("NIFTY", "NFO", "FUTURES", expiry="2026-12-31"),
                timestamp,
                100.0, 110.0, 100.0, 105.0, 100.0,
            ))
    store.append_market_bars(bars, source="paper-test")

    worker = ReplayWorker(store)
    assert worker._prior_daily_atr("2026-01-15", [*dates, "2026-01-15"]) == 10.0

    snapshot = SqliteFeatureRepository(store.database).read_feature_snapshot_for_session(
        feature_set=ORDERFLOW_DAILY_ATR_FEATURE_SET,
        instrument="NIFTY",
        session_date="2026-01-15",
    )
    assert snapshot is not None
    assert snapshot.key.parameters == (
        ("series", "continuous"), ("window", "14"),
        ("zones", "09:15-11:00,11:00-13:00,13:00-15:00"),
    )
    assert snapshot.source_bar_ids
    assert daily_atr_from_snapshot(snapshot) == 10.0


def test_live_atr_materializer_reuses_paper_history_and_artifact(tmp_path):
    store = RuntimeStore(tmp_path / "runtime")
    start = date(2026, 2, 1)
    bars = []
    for day_index in range(14):
        session_date = start + timedelta(days=day_index)
        for minute_index in range(375):
            timestamp = datetime.combine(
                session_date, datetime.strptime("09:15", "%H:%M").time(),
            ).replace(tzinfo=timezone(timedelta(hours=5, minutes=30))) + timedelta(minutes=minute_index)
            bars.append(MarketBar(
                Instrument("NIFTY", "NFO", "FUTURES", expiry="2026-12-31"),
                timestamp, 100.0, 110.0, 100.0, 105.0, 100.0,
            ))
    store.append_market_bars(bars, source="paper-test")

    assert materialize_orderflow_daily_atr(store, "2026-02-15") == 10.0
    assert materialize_orderflow_daily_atr(store, "2026-02-15") == 10.0


def test_daily_atr_snapshot_uses_true_range_across_session_closes():
    sessions = []
    for index in range(14):
        close = 100.0 + index * 5
        rows = []
        for minute_index in range(345):
            hour, minute = divmod(9 * 60 + 15 + minute_index, 60)
            rows.append({
                "minute": f"2026-01-{index + 1:02d}T{hour:02d}:{minute:02d}:00+05:30",
                "open": close,
                "high": close + 2,
                "low": close - 2,
                "close": close,
                "volume": 1,
            })
        sessions.append((
            f"2026-01-{index + 1:02d}",
            tuple(rows),
        ))

    snapshot = build_orderflow_daily_atr_snapshot("2026-01-15", sessions)

    assert snapshot.values["atr_14"] == (4.0 + 13.0 * 7.0) / 14.0
    assert snapshot.key.feature_set == ORDERFLOW_DAILY_ATR_FEATURE_SET


def test_daily_atr_snapshot_uses_strategy_zone_cutoff_for_complete_sessions():
    rows = []
    for minute_index in range(375):
        hour, minute = divmod(9 * 60 + 15 + minute_index, 60)
        rows.append({
            "minute": f"2026-01-01T{hour:02d}:{minute:02d}:00+05:30",
            "open": 100.0,
            "high": 110.0 if minute_index < 345 else 1_000.0,
            "low": 100.0,
            "close": 105.0,
            "volume": 1,
        })

    snapshot = build_orderflow_daily_atr_snapshot(
        "2026-01-15", tuple((f"2026-01-{index + 1:02d}", rows) for index in range(14)),
    )

    assert snapshot.values["atr_14"] == 10.0


def test_generic_daily_atr_snapshot_can_use_full_session_window():
    rows = []
    for minute_index in range(375):
        hour, minute = divmod(9 * 60 + 15 + minute_index, 60)
        rows.append({
            "minute": f"2026-01-01T{hour:02d}:{minute:02d}:00+05:30",
            "open": 100.0,
            "high": 1_000.0 if minute_index >= 345 else 110.0,
            "low": 100.0,
            "close": 105.0,
            "volume": 1,
        })

    snapshot = build_daily_atr_snapshot(
        "2026-01-15", tuple((f"2026-01-{index + 1:02d}", rows) for index in range(14)),
    )

    assert snapshot.values["atr_14"] == 900.0
