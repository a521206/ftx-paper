from __future__ import annotations

from types import SimpleNamespace

from ftx_paper.api.app import create_app
from ftx_paper.runtime.parity_artifacts import build_parity_artifact
from ftx_paper.runtime.store import RuntimeStore


def _stored_replay(tmp_path):
    store = RuntimeStore(tmp_path)
    run_id = "parity-api-fixture"
    request = {"session_date": "2026-01-05", "strategy_id": "ftx-paper-orderflow"}
    artifact = build_parity_artifact(
        {"strategy": {}, "bars_seen": 1, "metadata": {"normalized_fingerprint": "fixture"}},
        request=request,
        layer_records={
            "decision": [{
                "event_type": "POLICY_DECISION",
                "decision_at": "2026-01-05T14:30:00Z",
                "stage": "trigger",
                "outcome": "observed",
                "reason": "trigger_observation",
            }],
        },
    )
    store.create_replay_run(run_id, request)
    store.update_replay_run(
        run_id,
        status="completed",
        result={"parity_artifact": artifact},
    )
    return store, run_id, artifact


def test_parity_manifest_and_layer_endpoints_are_bounded(tmp_path) -> None:
    store, run_id, artifact = _stored_replay(tmp_path)
    client = create_app(store).test_client()

    manifest = client.get(f"/api/v1/replay/{run_id}/parity/manifest")
    assert manifest.status_code == 200
    assert manifest.get_json() == artifact["manifest"]

    page = client.get(f"/api/v1/replay/{run_id}/parity/decision?offset=0&limit=1")
    payload = page.get_json()
    assert page.status_code == 200
    assert payload["records"] == artifact["layers"]["decision"]
    assert payload["next_offset"] is None


def test_parity_layer_endpoint_rejects_invalid_pagination(tmp_path) -> None:
    store, run_id, _ = _stored_replay(tmp_path)
    client = create_app(store).test_client()

    response = client.get(f"/api/v1/replay/{run_id}/parity/decision?limit=1001")

    assert response.status_code == 400
    assert response.get_json()["error"]["code"] == "invalid_pagination"


def test_legacy_event_routes_are_not_exposed(tmp_path) -> None:
    client = create_app(RuntimeStore(tmp_path)).test_client()

    for path in ("/api/v1/events", "/api/v1/logs", "/api/v1/audit"):
        assert client.get(path).status_code == 404

    openapi = client.get("/api/v1/openapi.json")
    assert openapi.status_code == 200
    paths = openapi.get_json()["paths"]
    assert "/api/v1/events" not in paths


def test_historical_date_cannot_start_live_runtime(tmp_path) -> None:
    class Operations:
        started = False

        def request_start(self):
            self.started = True

        def request_stop(self):
            pass

        def request_restart(self):
            self.started = True

    operations = Operations()
    app = create_app(
        RuntimeStore(tmp_path),
        session=SimpleNamespace(strategy_status=lambda: ()),
        replay_worker=object(),
        runtime_operations=operations,
    )

    response = app.test_client().post(
        "/api/v1/runtime/start", json={"session_date": "2026-09-29"},
    )

    assert response.status_code == 400
    assert response.get_json()["error"]["code"] == "historical_session_requires_replay"
    assert not operations.started
