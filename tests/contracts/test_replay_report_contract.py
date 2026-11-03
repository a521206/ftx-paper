import pytest

from ftx_paper.contracts.replay import ReplayReport, ReplayResult, ReplaySummary


def _result(report: dict) -> ReplayResult:
    return ReplayResult(
        run_id="run-1",
        schema_version=1,
        execution_scope="replay",
        ledger_scope="replay",
        summary=ReplaySummary({
            "decisions_emitted": 0,
            "orders_filled": 0,
            "gross_pnl": 0,
            "risk_reward": 0,
        }),
        events=(),
        shadow_trades=(),
        shadow_ledger=(),
        parity_artifact=None,
        report=report,
    )


def test_replay_report_allows_runner_extensions() -> None:
    result = _result({
        "schema_version": 1,
        "report_type": "paper_replay",
        "strategy_id": "example-runner",
        "session_date": "2026-10-05",
        "summary": {},
        "metrics": {},
        "extensions": {"example-runner": {"trades": [{"id": "trade-1"}]}},
    })

    assert result.to_dict()["report"]["extensions"]["example-runner"]["trades"]


def test_replay_report_is_a_typed_common_envelope() -> None:
    report = ReplayReport.from_mapping({
        "schema_version": 1,
        "report_type": "paper_replay",
        "strategy_id": "example-runner",
        "session_date": "2026-10-05",
        "summary": {},
        "metrics": {},
        "extensions": {"example-runner": {"diagnostics": {"count": 1}}},
    })

    assert report.strategy_id == "example-runner"
    assert report.to_dict()["extensions"]["example-runner"]["diagnostics"]["count"] == 1


def test_replay_report_rejects_runner_fields_at_common_level() -> None:
    with pytest.raises(ValueError, match="unsupported replay report fields: trades"):
        _result({
            "schema_version": 1,
            "report_type": "paper_replay",
            "strategy_id": "example-runner",
            "session_date": "2026-10-05",
            "summary": {},
            "metrics": {},
            "trades": [],
        })


def test_replay_report_allows_runner_legacy_named_fields_inside_extensions() -> None:
    result = _result({
        "schema_version": 1,
        "report_type": "paper_replay",
        "strategy_id": "example-runner",
        "session_date": "2026-10-05",
        "summary": {},
        "metrics": {},
        "extensions": {"example-runner": {"ledger": []}},
    })

    assert result.to_dict()["report"]["extensions"]["example-runner"]["ledger"] == []
