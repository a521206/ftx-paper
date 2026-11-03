from ftx_paper.runtime import RuntimeStore
from ftx_paper.runtime.replay_worker import ReplayWorker


def _request(**overrides: object) -> dict[str, object]:
    request: dict[str, object] = {
        "session_date": "2026-01-05",
        "strategy_id": "ftx-paper-research",
        "vehicles": ["futures", "synthetic"],
    }
    request.update(overrides)
    return request


def test_submit_reuses_queued_equivalent_replay(tmp_path, monkeypatch):
    store = RuntimeStore(tmp_path)
    worker = ReplayWorker(store)
    monkeypatch.setattr(worker, "_ensure_started", lambda: None)

    first = worker.submit(_request())
    second = worker.submit(_request())

    assert second == first
    assert worker._queue.qsize() == 1
    assert store.read_replay_run(first)["status"] == "queued"


def test_submit_reuses_running_equivalent_replay_without_cancelling(tmp_path, monkeypatch):
    store = RuntimeStore(tmp_path)
    worker = ReplayWorker(store)
    monkeypatch.setattr(worker, "_ensure_started", lambda: None)

    first = worker.submit(_request())
    store.update_replay_run(first, status="running")
    second = worker.submit(_request())

    assert second == first
    run = store.read_replay_run(first)
    assert run is not None
    assert run["status"] == "running"
    assert worker._queue.qsize() == 1


def test_submit_new_parameters_creates_new_replay(tmp_path, monkeypatch):
    store = RuntimeStore(tmp_path)
    worker = ReplayWorker(store)
    monkeypatch.setattr(worker, "_ensure_started", lambda: None)

    first = worker.submit(_request())
    second = worker.submit(_request(include_parity_layers=True))

    assert second != first
    assert worker._queue.qsize() == 2
    assert store.read_replay_run(first)["status"] == "queued"
    assert store.read_replay_run(second)["status"] == "queued"
