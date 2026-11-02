from datetime import datetime, timedelta, timezone

import pytest

from ftx_paper.strategies.orderflow.host_adapter import (
    HOST_ADAPTER_CONTRACT_ID,
    CanonicalInputAdapter,
    CanonicalOutputAdapter,
    LiveHostInputAdapter,
    MarketEvidence,
    ReplayHostInputAdapter,
)


UTC = timezone.utc


def _evidence() -> tuple[MarketEvidence, ...]:
    start = datetime(2026, 10, 1, 3, 45, tzinfo=UTC)
    return tuple(
        MarketEvidence(
            source_id=f"bar-{index}",
            observed_at=start + timedelta(minutes=index),
            available_at=start + timedelta(minutes=index),
            values={"close": 100 + index},
            lineage=(f"parent-{index}",),
        )
        for index in range(2)
    )


def test_live_and_bounded_replay_use_identical_canonical_input_envelopes() -> None:
    evidence = _evidence()
    live = LiveHostInputAdapter()
    for item in evidence:
        live.publish(item)
    replay = ReplayHostInputAdapter()
    replay.process(evidence)

    assert live.boundary.state.snapshot() == replay.boundary.state.snapshot()
    assert CanonicalInputAdapter.observation(evidence[0]).identity == evidence[0].source_id


def test_input_adapter_preserves_unavailable_evidence_without_repair() -> None:
    evidence = MarketEvidence(
        source_id="bar-incomplete",
        observed_at=datetime(2026, 10, 1, 3, 45, tzinfo=UTC),
        available_at=datetime(2026, 10, 1, 3, 46, tzinfo=UTC),
        unavailable=("source_gap",),
    )

    observation = CanonicalInputAdapter.observation(evidence)

    assert observation.unavailable == ("source_gap",)
    assert observation.values == {}


def test_input_adapter_rejects_naive_or_out_of_order_evidence() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        MarketEvidence(
            source_id="naive",
            observed_at=datetime(2026, 10, 1, 3, 45),
            available_at=datetime(2026, 10, 1, 3, 45, tzinfo=UTC),
        )

    evidence = _evidence()
    with pytest.raises(ValueError, match="timestamp ordered"):
        ReplayHostInputAdapter().process((evidence[1], evidence[0]))


def test_output_envelope_is_versioned_and_contains_only_supplied_facts() -> None:
    event = LiveHostInputAdapter().publish(_evidence()[0]).events[0]
    envelope = CanonicalOutputAdapter.envelope(
        events=(event,),
        proposals=({"identity": "proposal-1"},),
        snapshot={"schema_version": 1},
        artifact={"status": "UNAVAILABLE"},
    )

    payload = envelope.to_dict()

    assert payload["contract"] == {"id": HOST_ADAPTER_CONTRACT_ID, "version": 1}
    assert payload["events"][0]["identity"] == "bar-0"
    assert payload["artifact"] == {"status": "UNAVAILABLE"}


def test_output_adapter_rejects_non_json_host_metadata() -> None:
    with pytest.raises(ValueError, match="snapshot must be a JSON object"):
        CanonicalOutputAdapter.envelope(snapshot={"timestamp": object()})
