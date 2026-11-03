from threading import Event

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


def test_orderflow_replay_ensures_setup_before_runner(tmp_path):
    calls: list[object] = []

    class SetupWorker:
        def ensure(self, strategy_id: str, through_session: str) -> None:
            calls.append(("setup", strategy_id, through_session))

    class Runner:
        def run(self, request: dict[str, object], _cancel: Event) -> dict[str, str]:
            calls.append(("replay", request["session_date"]))
            return {"status": "done"}

    worker = ReplayWorker(
        RuntimeStore(tmp_path), setup_worker=SetupWorker(),
    )
    worker.runners["ftx-paper-orderflow"] = Runner()

    result = worker._execute(_request(strategy_id="ftx-paper-orderflow"), Event())

    assert result == {"status": "done"}
    assert calls == [
        ("setup", "ftx-paper-orderflow", "2026-01-05"),
        ("replay", "2026-01-05"),
    ]
