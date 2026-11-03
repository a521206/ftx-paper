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
