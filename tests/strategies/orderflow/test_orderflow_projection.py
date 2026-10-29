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
    assert result["locations"][0]["reference_only"] is True
    assert result["sequence"][0]["implication"] == "response_pending"


def test_orderflow_projection_distinguishes_confirmed_diagnostic_response() -> None:
    view = _view(
        evidence={
            "metrics": {"entry_price": 100},
            "state": {"response_state": "CONFIRMED"},
            "provenance": {},
        },
    )

    result = build_orderflow_projection([{"decision_view": view}], [], [], [])

    assert result["action"]["name"] == "Response confirmed (diagnostic only)"


def test_orderflow_projection_calculates_risk_reward_for_trade_action():
    view = _view(
        direction="short",
        policy={"outcome": "accepted", "reason_code": "diagnostic_vwap_interaction"},
        evidence={"metrics": {"entry_price": 100, "stop": 102, "target_price": 96}, "state": {"response_state": "CONFIRMED"}, "provenance": {}},
    )
    result = build_orderflow_projection([{"decision_view": view}], [], [], [])
    assert result["bias"] == "SHORT"
    assert result["action"]["name"] == "Eligible diagnostic candidate"
    assert result["action"]["risk_reward"] == 2.0


def test_orderflow_projection_does_not_misclassify_lifecycle_as_market_state():
    result = build_orderflow_projection([{"decision_view": _view()}], [], [], [])

    assert result["market_state"] is None


def test_orderflow_projection_surfaces_unavailable_diagnostics():
    unavailable = _view(
        decision_id="d-unavailable",
        stage="admission",
        policy={"outcome": "unavailable", "reason_code": "setup_scores_unavailable"},
    )

    result = build_orderflow_projection([{"decision_view": unavailable}], [], [], [])

    assert result["diagnostics"] == [{
        "at": "2026-10-27T10:00:00+05:30",
        "stage": "admission",
        "outcome": "unavailable",
        "reason": "setup_scores_unavailable",
        "decision_id": "d-unavailable",
        "evidence": {"metrics": {"entry_price": 100, "stop": 98}, "state": {"response_state": "PENDING_RESPONSE"}, "provenance": {"reference_level": 100}},
    }]


def test_orderflow_projection_uses_explicit_market_state_and_labels_positions():
    view = _view(
        market_state="TRENDING",
        evidence={
            "metrics": {},
            "state": {"market_state": "RANGE"},
            "provenance": {},
        },
    )
    result = build_orderflow_projection([{"decision_view": view}], [], [{"symbol": "NIFTY"}], [])

    assert result["market_state"] == "TRENDING"
    assert result["position_scope"] == "shared_portfolio"


def test_orderflow_projection_collapses_transitions_to_five_minute_bars():
    first = _view(at="2026-10-27T10:01:00+05:30", policy={"outcome": "rejected", "reason_code": "first"})
    second = _view(at="2026-10-27T10:04:00+05:30", policy={"outcome": "rejected", "reason_code": "latest"})
    third = _view(at="2026-10-27T10:05:00+05:30", policy={"outcome": "rejected", "reason_code": "next_bar"})
    events = [
        {"created_at": "2026-10-27T10:01:02+05:30", "event_type": "CANDIDATEDECISION", "decision_view": first, "payload": {}},
        {"created_at": "2026-10-27T10:04:02+05:30", "event_type": "CANDIDATEDECISION", "decision_view": second, "payload": {}},
        {"created_at": "2026-10-27T10:05:02+05:30", "event_type": "CANDIDATEDECISION", "decision_view": third, "payload": {}},
    ]

    sequence = build_orderflow_projection([{"decision_view": third}], events, [], [])['sequence']

    assert [item["time"] for item in sequence] == [
        "2026-10-27T10:00:00+05:30",
        "2026-10-27T10:05:00+05:30",
    ]
    assert [item["timeframe"] for item in sequence] == ["5m", "5m"]
    assert sequence[0]["implication"] == "latest"
