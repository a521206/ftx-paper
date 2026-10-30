from __future__ import annotations

from datetime import datetime, timezone

from ftx_paper.contracts import Instrument, MarketBar
from ftx_paper.contracts.replay import LEGACY_REPLAY_FIELDS
from ftx_paper.runtime.replay_worker import ReplayWorker
from ftx_paper.application import build_strategy_catalog
from ftx_paper.runtime.store import RuntimeStore


def _request(include_parity_layers: bool = False) -> dict[str, object]:
    return {
        "session_date": "2026-09-29",
        "strategy_id": "ftx-paper-orderflow",
        "vehicles": ["futures"],
        "mode": "replay",
        "include_parity_layers": include_parity_layers,
    }


def _worker(tmp_path):
    worker = ReplayWorker(RuntimeStore(tmp_path), strategy_selector=build_strategy_catalog())
    worker._bars = lambda session_date, **kwargs: (
        MarketBar(
            Instrument("NIFTY26SEP", "NFO", "FUTURES", "2026-09-29"),
            datetime(2026, 9, 29, 3, 45, tzinfo=timezone.utc),
            22600, 22610, 22590, 22600,
        ),
    )
    return worker


def _legacy_keys(value):
    if isinstance(value, dict):
        return {key for key in value if key in LEGACY_REPLAY_FIELDS} | {
            key for child in value.values() for key in _legacy_keys(child)
        }
    if isinstance(value, list):
        return {key for child in value for key in _legacy_keys(child)}
    return set()


def test_replay_preserves_live_store_and_resets_replay_outputs(tmp_path) -> None:
    worker = _worker(tmp_path)
    before_status = worker.store.read_status()
    before_events = worker.store.read_events(1000)

    first = worker.execute_detached(_request())
    second = worker.execute_detached(_request())

    assert worker.store.read_status() == before_status
    assert worker.store.read_events(1000) == before_events
    assert first["authoritative_trades"] == []
    assert first["authoritative_ledger"] == []
    assert second["shadow_ledger"] == first["shadow_ledger"]
    assert _legacy_keys(first) == set()
    assert not {"trace", "candidate_trace", "trace_schema_version", "trace_bar_index_base"} & set(first)
    assert first["parity_artifact"] is None


def test_replay_parity_is_opt_in(tmp_path) -> None:
    result = _worker(tmp_path).execute_detached(_request(include_parity_layers=True))
    assert isinstance(result["parity_artifact"], dict)
    assert "replay_records" not in result
    assert result["report"]["summary"]
    assert "actions" not in result["report"]["summary"]
    for layer_records in result["parity_artifact"]["layers"].values():
        for record in layer_records:
            assert {"schema_version", "run_id", "execution_scope", "ledger_scope"} <= set(record)


def test_replay_scope_projects_authoritative_orderflow_artifact(tmp_path) -> None:
    request = _request(include_parity_layers=True)
    request.update({"mode": "replay"})
    result = _worker(tmp_path).execute_detached(request)

    artifact = result["parity_artifact"]
    assert result["execution_scope"] == "replay"
    assert artifact["scope"]["fingerprint_recipe"] == "normalized-bars-v1"
    assert all(record["execution_scope"] == "replay"
               for records in artifact["layers"].values() for record in records)
    assert any(record["kind"] == "daily_reset" for record in artifact["layers"]["portfolio_ledger"])
    assert result["authoritative_trades"] == []
    assert result["authoritative_ledger"] == []
