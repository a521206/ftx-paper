from datetime import date, datetime, timedelta

from ftx_paper.contracts.setup import SetupRunRequest
from ftx_paper.infrastructure.artifacts import SetupArtifactStore, deserialize_atr_publication
from ftx_paper.runtime.futures_source import FuturesSessionSourceResolver
from ftx_paper.strategies.orderflow.setup import build_daily_atr_task, build_prior_session_levels_task, build_prior_session_task


class _Store:
    def __init__(self, rows_by_date):
        self.rows_by_date = rows_by_date

    def read_dates(self):
        return list(self.rows_by_date)

    def read_market_bars(self, session_date):
        return list(self.rows_by_date[session_date])

    def read_market_futures_bars(self, session_date):
        return [
            row for row in self.read_market_bars(session_date)
            if str(row.get("instrument_type", "")).upper() in {"FUT", "FUTURES"}
        ]


def _store(session_count: int = 18) -> _Store:
    start = date(2026, 1, 1)
    rows = {}
    for offset in range(session_count):
        session = (start + timedelta(days=offset)).isoformat()
        rows[session] = [{
            "minute": f"{session}T03:45:00Z",
            "open": 100.0 + offset,
            "high": 110.0 + offset,
            "low": 90.0 + offset,
            "close": 100.0 + offset,
            "volume": 1,
            "open_interest": 1,
            "instrument_type": "FUTURES",
            "exchange": "NFO",
            "symbol": "NIFTY",
            "expiry": "2026-10-31",
        }]
    return _Store(rows)


def _complete_session_store(session_count: int) -> _Store:
    store = _store(session_count)
    for session, seed_rows in store.rows_by_date.items():
        seed = seed_rows[0]
        start = datetime.fromisoformat(f"{session}T03:45:00+00:00")
        store.rows_by_date[session] = [
            {
                **seed,
                "minute": (start + timedelta(minutes=index)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            for index in range(376)
        ]
    return store


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
    assert artifact["source"]["sessions"] == [
        f"2026-01-{day:02d}" for day in range(1, 15)
    ]
    assert artifact["source"]["completed_session_count"] == 14
    publication = deserialize_atr_publication(payload)
    assert publication.value is not None
    assert publication.lineage[-1].startswith("source:")


def test_daily_atr_prerequisite_does_not_read_sessions_after_target(tmp_path, monkeypatch) -> None:
    store = _store(session_count=4)
    read_sessions = []
    read_market_bars = store.read_market_bars

    def record_read(session_date):
        read_sessions.append(session_date)
        return read_market_bars(session_date)

    monkeypatch.setattr(store, "read_market_bars", record_read)

    result = build_daily_atr_task(store, SetupArtifactStore(tmp_path)).materialize(
        SetupRunRequest(
            "ftx-paper-orderflow", "daily-atr", "catch_up",
            from_session="2026-01-02", through_session="2026-01-02",
        )
    )

    assert result["processed_sessions"] == 1
    assert read_sessions == ["2026-01-01", "2026-01-02"]


def test_daily_atr_reuses_current_target_without_recomputing_values(tmp_path, monkeypatch) -> None:
    store = _store()
    artifacts = SetupArtifactStore(tmp_path)
    task = build_daily_atr_task(store, artifacts)
    request = SetupRunRequest(
        "ftx-paper-orderflow", "daily-atr", "catch_up",
        from_session="2026-01-18", through_session="2026-01-18",
    )
    task.materialize(request)

    def fail_calculation(_rows):
        raise AssertionError("current ATR should be reused")

    monkeypatch.setattr(
        "ftx_paper.strategies.orderflow.setup.daily_atr_cache",
        fail_calculation,
    )

    result = task.materialize(request)

    assert result["reused"] == 1
    assert result["materialized"] == 0


def test_daily_atr_uses_selected_front_contract_only(tmp_path) -> None:
    store = _store()
    for rows in store.rows_by_date.values():
        rows.append({**rows[0], "expiry": "2026-12-31", "high": 1000.0, "low": 1.0})
    artifacts = SetupArtifactStore(tmp_path)

    build_daily_atr_task(store, artifacts).materialize(
        SetupRunRequest("ftx-paper-orderflow", "daily-atr", "catch_up")
    )

    identity = {
        "strategy_id": "ftx-paper-orderflow", "task_id": "daily-atr",
        "algorithm_version": "orderflow.daily-atr.wilder.v1",
        "config_fingerprint": "daily-atr-NIFTY-14", "instrument": "NIFTY",
    }
    artifact = artifacts.read(identity, "2026-01-15")
    assert artifact is not None
    assert artifact["payload"]["value"] == 20.0


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
    repeated = task.materialize(
        SetupRunRequest(
            "ftx-paper-orderflow", "prior-session-value", "catch_up",
            from_session="2026-01-03",
        )
    )

    assert result["processed_sessions"] == 1
    assert repeated["reused"] == 1
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
    expected_fingerprint = FuturesSessionSourceResolver(store).resolve("2026-01-02")
    assert expected_fingerprint is not None
    assert artifact["source"]["source_fingerprints"] == [
        expected_fingerprint.provenance()["source_fingerprint"]
    ]
    assert artifact["source"]["source_fingerprint_recipe"] == "normalized-continuous-bars-v1"


def test_prior_session_levels_publish_extrema_with_bar_lineage(tmp_path) -> None:
    store = _complete_session_store(session_count=3)
    artifacts = SetupArtifactStore(tmp_path)
    task = build_prior_session_levels_task(store, artifacts)

    result = task.materialize(SetupRunRequest(
        "ftx-paper-orderflow", "prior-session-levels", "catch_up",
        from_session="2026-01-03", through_session="2026-01-03",
    ))

    assert result["materialized"] == 1
    identity = {
        "strategy_id": "ftx-paper-orderflow", "task_id": "prior-session-levels",
        "algorithm_version": "orderflow.prior-session-levels.v1",
        "config_fingerprint": "prior-session-levels-NIFTY", "instrument": "NIFTY",
    }
    artifact = artifacts.read(identity, "2026-01-03")
    assert artifact is not None
    payload = artifact["payload"]
    assert (payload["source_session"], payload["high"], payload["low"]) == ("2026-01-02", 111.0, 91.0)
    assert payload["observed_at"] == "2026-01-02T10:00:00.000000Z"
    assert payload["available_at"] == "2026-01-03T03:45:00.000000Z"
    assert payload["lineage"][0] == f"source:{artifact['source']['source_fingerprints'][0]}"
    assert len(payload["lineage"]) == 377
    assert payload["lineage"][-1] == "bar:2026-01-02T10:00:00Z"


def test_prior_session_levels_invalidate_only_targets_using_corrected_session(tmp_path) -> None:
    store = _complete_session_store(session_count=4)
    artifacts = SetupArtifactStore(tmp_path)
    task = build_prior_session_levels_task(store, artifacts)
    request = SetupRunRequest("ftx-paper-orderflow", "prior-session-levels", "catch_up")
    task.materialize(request)
    identity = {
        "strategy_id": "ftx-paper-orderflow", "task_id": "prior-session-levels",
        "algorithm_version": "orderflow.prior-session-levels.v1",
        "config_fingerprint": "prior-session-levels-NIFTY", "instrument": "NIFTY",
    }
    before = {day: artifacts.read(identity, f"2026-01-{day:02d}") for day in (2, 3, 4)}

    store.rows_by_date["2026-01-02"][0]["high"] = 999.0
    task.materialize(request)

    after = {day: artifacts.read(identity, f"2026-01-{day:02d}") for day in (2, 3, 4)}
    assert before[2] is not None and after[2] is not None
    assert before[2]["manifest"]["artifact_sha256"] == after[2]["manifest"]["artifact_sha256"]
    assert before[3] is not None and after[3] is not None
    assert before[3]["manifest"]["artifact_sha256"] != after[3]["manifest"]["artifact_sha256"]
    assert before[4] is not None and after[4] is not None
    assert before[4]["manifest"]["artifact_sha256"] == after[4]["manifest"]["artifact_sha256"]


def test_prior_session_levels_reject_incomplete_session(tmp_path) -> None:
    store = _store(session_count=2)
    artifacts = SetupArtifactStore(tmp_path)
    task = build_prior_session_levels_task(store, artifacts)

    result = task.materialize(SetupRunRequest(
        "ftx-paper-orderflow", "prior-session-levels", "catch_up",
        from_session="2026-01-02", through_session="2026-01-02",
    ))

    identity = {
        "strategy_id": "ftx-paper-orderflow", "task_id": "prior-session-levels",
        "algorithm_version": "orderflow.prior-session-levels.v1",
        "config_fingerprint": "prior-session-levels-NIFTY", "instrument": "NIFTY",
    }
    artifact = artifacts.read(identity, "2026-01-02")
    assert result["unavailable"] == 1
    assert artifact is not None
    assert artifact["status"] == "unavailable"
    assert artifact["unavailable_reason"] == "prior_session_source_incomplete"
