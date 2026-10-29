from datetime import datetime, timedelta, timezone
from types import MappingProxyType

from ftx_paper.contracts import Instrument, MarketBar
from ftx_paper.strategy.orderflow_capture import ReplayRecord, detached_asdict
from ftx_paper.strategy.orderflow_contract import OrderflowObservation, OrderflowObservationContext
from ftx_paper.runtime.replay_worker import ReplayWorker
from ftx_paper.runtime.store import RuntimeStore


UTC = timezone.utc


def test_replay_record_detaches_nested_mapping_proxies() -> None:
    timestamp = datetime(2026, 9, 29, 3, 45, tzinfo=UTC)
    record = ReplayRecord(
        1,
        timestamp,
        timestamp,
        {"nested": MappingProxyType({"value": 1})},
        {"nested": (MappingProxyType({"value": 2}),)},
    )

    assert record.to_dict()["before_state"] == {"nested": {"value": 1}}
    assert record.to_dict()["after_state"] == {"nested": ({"value": 2},)}


def test_orderflow_observation_projection_detaches_immutable_mappings() -> None:
    timestamp = datetime(2026, 9, 29, 3, 45, tzinfo=UTC)
    context = OrderflowObservationContext(
        "2026-09-29", "complete", "available", MappingProxyType({"source": "fixture"}),
    )
    observation = OrderflowObservation(
        "observation-1", None, "v1", "1m", "NIFTY", timestamp, timestamp,
        MappingProxyType({"close": 100.0}), context=context,
    )

    assert detached_asdict(observation)["features"] == {"close": 100.0}
    assert detached_asdict(observation)["context"]["replay_provenance"] == {"source": "fixture"}


def test_orderflow_replay_is_isolated_from_live_execution_state(tmp_path) -> None:
    bar = MarketBar(
        Instrument("NIFTY26SEP", "NFO", "FUTURES", "2026-09-29"),
        datetime(2026, 9, 29, 3, 45, tzinfo=UTC),
        22600, 22610, 22590, 22600,
    )
    next_bar = MarketBar(
        bar.instrument, bar.timestamp + timedelta(minutes=1),
        22600, 22610, 22590, 22605,
    )
    store = RuntimeStore(tmp_path)
    worker = ReplayWorker(store)
    worker._bars = lambda session_date, **_kwargs: (bar, next_bar)
    before_status = store.read_status()
    before_events = store.read_events(1000)

    result = worker.execute_detached({
        "session_date": "2026-09-29",
        "strategy_id": "ftx-paper-orderflow",
        "vehicles": ["futures", "synthetic"],
        "mode": "replay",
        "include_parity_layers": False,
    })

    assert result["execution_scope"] == "replay"
    assert result["shadow_trades"] == []
    assert any(item["event_type"] == "DAILY_RESET" for item in result["shadow_ledger"])
    assert result["execution_scope"] == "replay"
    assert result["ledger_scope"] == "replay"
    assert result["replay_records"]
    assert result["replay_records"][0]["checkpoints"]
    assert result["replay_records"][0]["before_state"]["engine"]["bars_seen"] == 0
    assert result["replay_records"][0]["after_state"]["engine"]["bars_seen"] == 1
    assert result["replay_records"][0]["available_at"] == next_bar.timestamp.isoformat()
    assert [item["phase"] for item in result["replay_records"][0]["checkpoints"]] == [
        "reference_publication", "existing_position_management", "permission_and_gap",
        "interaction_advancement", "opportunity_timeout", "opportunity_formation",
        "proposal_arbitration", "execution_settlement", "event_recording",
    ]
    assert not any(str(event.get("values", {}).get("event_type", "")).upper() in {"FILL", "RESERVATION", "SETTLEMENT"}
                   for event in result["events"])
    assert store.read_status() == before_status
    assert store.read_events(1000) == before_events


def test_paired_replay_compares_decisions_and_keeps_execution_projection_separate() -> None:
    decisions = [{
        "decision_id": "decision-1", "decision_stage": "admission",
        "outcome": "unavailable", "reason": "future_evidence",
        "source_observation_ids": ["obs-1"], "available_at": "2026-09-29T03:50:00+00:00",
    }]
    comparison = ReplayWorker.compare_paired_replay(
        {"events": decisions, "shadow_trades": [], "shadow_ledger": []},
        {"events": decisions, "shadow_trades": [{"status": "filled"}], "shadow_ledger": [{"net_pnl": 0}]},
    )

    assert comparison == {"matched": True, "mismatches": [], "diagnostic_mutations": False}
