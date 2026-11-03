from datetime import datetime, timezone
import time

import pytest

from ftx_paper.infrastructure.artifacts import SetupArtifactStore, build_envelope, deserialize_value_publication
from ftx_paper.application.setup_worker import DailySetupTask, SetupTaskRegistry, SetupWorker
from ftx_paper.strategies.orderflow.value_publication import PriorSessionValueReference, ValuePublication


def _publication() -> ValuePublication:
    observed = datetime(2026, 11, 1, 10, tzinfo=timezone.utc)
    reference = PriorSessionValueReference("2026-11-01", 100.0, 105.0, 110.0, observed, observed, ("ref",))
    return ValuePublication(reference, observed, observed, ("ref",))


def test_value_publication_artifact_round_trip_and_digest(tmp_path):
    store = SetupArtifactStore(tmp_path)
    envelope = build_envelope(
        strategy_id="ftx-paper-orderflow", task_id="prior-session-value",
        algorithm_version="orderflow.prior-session-value.v1", target_session="2026-11-02",
        source={"selected_source": {"symbol": "NIFTY"}},
        configuration={"config_fingerprint": "cfg"}, publication=_publication(),
    )
    store.write(envelope, source_fingerprint="source")
    identity = {"strategy_id": "ftx-paper-orderflow", "task_id": "prior-session-value",
                "algorithm_version": "orderflow.prior-session-value.v1", "config_fingerprint": "cfg", "instrument": "NIFTY"}
    loaded = store.read(identity, "2026-11-02")
    assert loaded is not None
    assert deserialize_value_publication(loaded["payload"]) == _publication()


def test_corrupt_artifact_is_rejected_and_quarantined(tmp_path):
    store = SetupArtifactStore(tmp_path)
    envelope = build_envelope(
        strategy_id="ftx-paper-orderflow", task_id="prior-session-value",
        algorithm_version="orderflow.prior-session-value.v1", target_session="2026-11-02",
        source={"selected_source": {"symbol": "NIFTY"}}, configuration={"config_fingerprint": "cfg"},
        publication=_publication(),
    )
    path = store.write(envelope, source_fingerprint="source")
    path.write_text("{}", encoding="utf-8")
    identity = {"strategy_id": "ftx-paper-orderflow", "task_id": "prior-session-value",
                "algorithm_version": "orderflow.prior-session-value.v1", "config_fingerprint": "cfg", "instrument": "NIFTY"}
    with pytest.raises(ValueError, match="corrupt"):
        store.read(identity, "2026-11-02")
    assert not path.exists()
    assert list(path.parent.glob("quarantine-*"))


def test_setup_catalog_reads_updates_written_by_another_store_instance(tmp_path):
    writer = SetupArtifactStore(tmp_path)
    reader = SetupArtifactStore(tmp_path)
    identity = {
        "strategy_id": "ftx-paper-orderflow", "task_id": "prior-session-value",
        "algorithm_version": "orderflow.prior-session-value.v1",
        "config_fingerprint": "cfg", "instrument": "NIFTY",
    }

    def envelope(fingerprint):
        return build_envelope(
            strategy_id="ftx-paper-orderflow", task_id="prior-session-value",
            algorithm_version="orderflow.prior-session-value.v1", target_session="2026-11-02",
            source={"selected_source": {"symbol": "NIFTY"}, "source_fingerprints": [fingerprint]},
            configuration={"config_fingerprint": "cfg"}, publication=_publication(),
        )

    writer.write(envelope("old-source"), source_fingerprint="old-source")
    loaded = reader.read(identity, "2026-11-02")
    assert loaded is not None
    assert loaded["source"]["source_fingerprints"] == ["old-source"]

    writer.write(envelope("new-source"), source_fingerprint="new-source")

    loaded = reader.read(identity, "2026-11-02")
    assert loaded is not None
    assert loaded["source"]["source_fingerprints"] == ["new-source"]


def test_setup_worker_rejects_unknown_tasks_and_records_result():
    worker = SetupWorker(SetupTaskRegistry((DailySetupTask("s", "t", "v", lambda _request, _cancelled: {"materialized": 1}),)))
    with pytest.raises(ValueError, match="unknown setup task"):
        worker.submit({"strategy_id": "s", "task_id": "missing", "mode": "incremental"})
    job_id = worker.submit({"strategy_id": "s", "task_id": "t", "mode": "incremental"})
    deadline = time.monotonic() + 2
    result = worker.read(job_id)
    while result is not None and result["status"] not in {"completed", "failed", "cancelled"} and time.monotonic() < deadline:
        time.sleep(0.01)
        result = worker.read(job_id)
    assert result is not None
    assert result["status"] == "completed"
    assert result["tasks"] == {"t": {"materialized": 1}}


def test_setup_worker_ensures_all_strategy_tasks_before_replay():
    calls: list[tuple[str, str, str | None]] = []

    def materialize(request, _cancelled):
        calls.append((request.task_id, request.mode, request.through_session))
        return {"materialized": 1, "target_available": 1}

    worker = SetupWorker(SetupTaskRegistry((
        DailySetupTask("s", "first", "v1", materialize),
        DailySetupTask("s", "second", "v2", materialize),
        DailySetupTask("other", "ignored", "v3", materialize),
    )))
    try:
        results = worker.ensure("s", "2026-10-05")
    finally:
        worker.shutdown()

    assert [item["task_id"] for item in results] == ["first", "second"]
    assert calls == [
        ("first", "catch_up", "2026-10-05"),
        ("second", "catch_up", "2026-10-05"),
    ]


def test_setup_worker_allows_partial_setup_after_waiting():
    worker = SetupWorker(SetupTaskRegistry((
        DailySetupTask("s", "task", "v1", lambda _request, _cancelled: {
            "materialized": 1, "unavailable": 1, "target_available": 0,
        }),
    )))
    try:
        results = worker.ensure("s", "2026-10-05")
    finally:
        worker.shutdown()

    assert results[0]["status"] == "partial"
