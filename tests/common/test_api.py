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
