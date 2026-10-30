from __future__ import annotations

import json

from ftx_paper import replay_cli


def test_run_submits_canonical_request_and_polls_until_complete(monkeypatch):
    calls: list[tuple[str, str, object]] = []
    responses = iter([
        {"run_id": "replay-1", "status": "queued"},
        {"run_id": "replay-1", "status": "running"},
        {"run_id": "replay-1", "status": "completed", "result": {"summary": {}}},
    ])

    def fake_request(url, *, method="GET", payload=None):
        calls.append((url, method, payload))
        return next(responses)

    monkeypatch.setattr(replay_cli, "_request", fake_request)
    monkeypatch.setattr(replay_cli.time, "sleep", lambda _interval: None)

    result = replay_cli.run(
        "2026-09-29",
        base_url="http://127.0.0.1:8501/",
        strategy_id="ftx-paper-orderflow",
        vehicles=["futures"],
        include_parity_layers=True,
        timeout=10,
        interval=0,
    )

    assert result["status"] == "completed"
    assert calls == [
        (
                "http://127.0.0.1:8501/api/v1/strategies/ftx-paper-orderflow/replays",
            "POST",
            {
                "session_date": "2026-09-29",
                "vehicles": ["futures"],
                "mode": "replay",
                "include_parity_layers": True,
            },
        ),
            ("http://127.0.0.1:8501/api/v1/strategies/ftx-paper-orderflow/replays/replay-1", "GET", None),
            ("http://127.0.0.1:8501/api/v1/strategies/ftx-paper-orderflow/replays/replay-1", "GET", None),
    ]


def test_main_prints_json_result(monkeypatch, capsys):
    monkeypatch.setattr(
        replay_cli,
        "run",
        lambda *args, **kwargs: {"run_id": "replay-1", "status": "completed"},
    )

    assert replay_cli.main(["2026-09-29", "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == {"run_id": "replay-1", "status": "completed"}
