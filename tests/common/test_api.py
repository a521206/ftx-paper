from types import SimpleNamespace

from ftx_paper.api.app import create_app
from ftx_paper.runtime import RuntimeStore


def test_api_cors_allows_replay_delete_from_ui_origin(tmp_path):
    session = SimpleNamespace(strategy_status=lambda: [])
    app = create_app(
        RuntimeStore(tmp_path),
        session=session,
        replay_worker=SimpleNamespace(),
    )

    response = app.test_client().options(
        "/api/v1/strategies/ftx-paper-orderflow/replays/run-1",
        headers={
            "Origin": "http://127.0.0.1:8502",
            "Access-Control-Request-Method": "DELETE",
            "Access-Control-Request-Headers": "content-type",
        },
    )

    assert response.status_code == 200
    assert "DELETE" in response.headers["Access-Control-Allow-Methods"]


def test_replay_status_view_does_not_return_result(tmp_path):
    store = RuntimeStore(tmp_path)
    store.create_replay_run(
        "status-run",
        {"session_date": "2026-01-05", "strategy_id": "ftx-paper-orderflow"},
    )
    store.update_replay_run("status-run", status="completed", result={"events": [1]})
    app = create_app(
        store,
        session=SimpleNamespace(strategy_status=lambda: []),
        replay_worker=SimpleNamespace(),
    )

    response = app.test_client().get(
        "/api/v1/strategies/ftx-paper-orderflow/replays/status-run?view=status",
    )

    assert response.status_code == 200
    assert response.get_json()["status"] == "completed"
    assert "result" not in response.get_json()


def test_replay_submission_does_not_block_on_setup(tmp_path):
    submitted: list[dict] = []
    run = {
        "run_id": "queued-run",
        "status": "queued",
        "request": {},
        "result": None,
        "error": None,
    }

    class Store:
        def recover_interrupted(self):
            pass

        def read_status(self):
            return {}

        def read_dates(self):
            return ("2026-10-05",)

        def read_replay_run(self, _run_id):
            return run

    class Operations:
        def submit_replay(self, payload):
            submitted.append(payload)
            return "queued-run"

    class SetupWorker:
        def ensure(self, *_args):
            raise AssertionError("setup must not run in the replay request")

    app = create_app(
        Store(),
        session=SimpleNamespace(
            strategy_status=lambda: [{"strategy_id": "ftx-paper-orderflow"}],
        ),
        replay_worker=SimpleNamespace(),
        runtime_operations=Operations(),
        setup_worker=SetupWorker(),
    )

    response = app.test_client().post(
        "/api/v1/strategies/ftx-paper-orderflow/replays",
        json={
            "session_date": "2026-10-05",
            "vehicles": ["futures"],
            "mode": "replay",
            "include_parity_layers": True,
        },
    )

    assert response.status_code == 202
    assert submitted[0]["session_date"] == "2026-10-05"
