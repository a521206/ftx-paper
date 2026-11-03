from datetime import date, timedelta

from ftx_paper.contracts.setup import SetupRunRequest
from ftx_paper.infrastructure.artifacts import SetupArtifactStore, deserialize_atr_publication
from ftx_paper.strategies.orderflow.setup import build_daily_atr_task, build_prior_session_task


class _Store:
    def __init__(self, rows_by_date):
        self.rows_by_date = rows_by_date

    def read_dates(self):
        return list(self.rows_by_date)

    def read_market_bars(self, session_date):
        return list(self.rows_by_date[session_date])


def _store(session_count: int = 18) -> _Store:
    start = date(2026, 1, 1)
    rows = {}
    for offset in range(session_count):
        session = (start + timedelta(days=offset)).isoformat()
        rows[session] = [{
            "minute": f"{session}T15:30:00Z",
            "open": 100.0 + offset,
            "high": 110.0 + offset,
            "low": 90.0 + offset,
            "close": 100.0 + offset,
            "volume": 1,
            "open_interest": 1,
            "instrument_type": "FUTURES",
            "exchange": "NFO",
        }]
    return _Store(rows)


def test_daily_atr_materializes_typed_target_publication(tmp_path) -> None:
    store = _store()
    artifacts = SetupArtifactStore(tmp_path)
    task = build_daily_atr_task(store, artifacts)

    result = task.materialize(SetupRunRequest("ftx-paper-orderflow", "daily-atr", "catch_up"))

    assert result["materialized"] == 18
    identity = {
        "strategy_id": "ftx-paper-orderflow",
        "task_id": "daily-atr",
        "algorithm_version": "orderflow.daily-atr.wilder.v1",
        "config_fingerprint": "daily-atr-NIFTY-14",
        "instrument": "NIFTY",
    }
    artifact = artifacts.read(identity, "2026-01-15")
    assert artifact is not None
    assert artifact["status"] == "available"
    payload = artifact["payload"]
    assert payload["source_session"] == "2026-01-14"
    assert payload["available_at"] == "2026-01-15T03:45:00.000000Z"
    publication = deserialize_atr_publication(payload)
    assert publication.value is not None
    assert publication.lineage[-1].startswith("source:")


def test_daily_atr_rebuilds_later_values_after_source_correction(tmp_path) -> None:
    store = _store()
    artifacts = SetupArtifactStore(tmp_path)
    task = build_daily_atr_task(store, artifacts)
    request = SetupRunRequest("ftx-paper-orderflow", "daily-atr", "catch_up")
    task.materialize(request)

    identity = {
        "strategy_id": "ftx-paper-orderflow",
        "task_id": "daily-atr",
        "algorithm_version": "orderflow.daily-atr.wilder.v1",
        "config_fingerprint": "daily-atr-NIFTY-14",
        "instrument": "NIFTY",
    }
    before = artifacts.read(identity, "2026-01-18")
    assert before is not None
    store.rows_by_date["2026-01-05"][0]["high"] = 999.0
    task.materialize(request)
    after = artifacts.read(identity, "2026-01-18")
    assert after is not None
    assert after["manifest"]["artifact_sha256"] != before["manifest"]["artifact_sha256"]


def test_prior_value_from_session_keeps_preceding_source_history(tmp_path, monkeypatch) -> None:
    store = _store(session_count=3)
    artifacts = SetupArtifactStore(tmp_path)
    monkeypatch.setattr(
        "ftx_paper.strategies.orderflow.setup.publish_value_references",
        lambda *_args, **_kwargs: (),
    )

    task = build_prior_session_task(store, artifacts)
    result = task.materialize(
        SetupRunRequest(
            "ftx-paper-orderflow", "prior-session-value", "catch_up",
            from_session="2026-01-03",
        )
    )

    assert result["processed_sessions"] == 1
    identity = {
        "strategy_id": "ftx-paper-orderflow",
        "task_id": "prior-session-value",
        "algorithm_version": "orderflow.prior-session-value.v1",
        "config_fingerprint": "value-profile-default",
        "instrument": "NIFTY",
    }
    artifact = artifacts.read(identity, "2026-01-03")
    assert artifact is not None
    assert artifact["source"]["sessions"] == ["2026-01-02"]
