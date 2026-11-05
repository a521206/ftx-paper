from ftx_paper.strategies.orderflow.boundary.market_context import build_market_context


def _decision(*, at: str, stage: str, location: str, detail: str, outcome: str = "recorded") -> dict:
    return {
        "decision_view": {
            "at": at,
            "decision_id": f"decision-{at}",
            "event_type": "CANDIDATEDECISION",
            "stage": stage,
            "outcome": outcome,
            "subject": {"location": location, "direction": "LONG", "instrument": "NIFTY"},
            "policy": {"outcome": outcome, "message": detail},
            "evidence": {"provenance": {"timeframe": "5m", "reference_level": 22000}},
        },
    }


def test_market_context_projects_latest_read_and_location_state() -> None:
    context = build_market_context(
        [
            _decision(at="2026-11-05T10:20:00+05:30", stage="opportunity", location="PDH", detail="Acceptance developing"),
            _decision(at="2026-11-05T10:15:00+05:30", stage="interaction", location="PDH", detail="Location contacted"),
            _decision(at="2026-11-05T10:10:00+05:30", stage="interaction", location="VWAP", detail="Responsive rejection"),
        ],
        "2026-11-05",
    )

    assert context["interpretation"] == "Acceptance developing"
    assert context["state"] == "opportunity"
    assert [item["location"] for item in context["locations"]] == ["PDH", "VWAP"]
    assert context["locations"][0]["detail"] == "Acceptance developing"
    assert context["lifecycle"]["stage_counts"] == {
        "interaction": 2, "opportunity": 1, "candidate": 0, "admission": 0,
        "trigger": 0, "execution": 0, "exit": 0,
    }
