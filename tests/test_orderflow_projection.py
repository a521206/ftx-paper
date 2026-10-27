from ftx_paper.api.orderflow import build_orderflow_projection


def _view(**changes):
    return {
        "at": "2026-10-27T10:00:00+05:30",
        "event_type": "CANDIDATEDECISION",
        "decision_id": "d-1",
        "stage": "interaction_response",
        "context": "vwap_zone",
        "direction": "long",
        "subject": {"setup": "VWAP rejection", "location": "vwap_zone"},
        "policy": {"outcome": "rejected", "reason_code": "response_pending"},
        "evidence": {"metrics": {"entry_price": 100, "stop": 98}, "state": {"response_state": "PENDING_RESPONSE"}, "provenance": {"reference_level": 100}},
        **changes,
    }


def test_orderflow_projection_uses_canonical_view_and_hides_raw_in_action():
    result = build_orderflow_projection(
        [{"decision_view": _view()}],
        [{"event_type": "CANDIDATEDECISION", "decision_view": _view(), "payload": {}}],
        [],
        [],
    )
    assert result["bias"] == "LONG"
    assert result["action"]["name"] == "Wait for confirmation"
    assert result["locations"][0]["response_status"] == "PENDING_RESPONSE"
    assert result["sequence"][0]["implication"] == "response_pending"


def test_orderflow_projection_calculates_risk_reward_for_trade_action():
    view = _view(
        direction="short",
        policy={"outcome": "accepted", "reason_code": "diagnostic_vwap_interaction"},
        evidence={"metrics": {"entry_price": 100, "stop": 102, "target_price": 96}, "state": {"response_state": "CONFIRMED"}, "provenance": {}},
    )
    result = build_orderflow_projection([{"decision_view": view}], [], [], [])
    assert result["bias"] == "SHORT"
    assert result["action"]["name"] == "Trade now"
    assert result["action"]["risk_reward"] == 2.0
