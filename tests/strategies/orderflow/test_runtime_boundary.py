from datetime import datetime, timezone
import math
import pytest

from ftx_paper.strategies.orderflow import (
    BoundaryState,
    CompletedObservation,
    LiveInputAdapter,
    ReplayInputAdapter,
    build_runtime_boundary_artifact,
)


def _observations() -> tuple[CompletedObservation, ...]:
    return tuple(
        CompletedObservation(
            identity=f"source:{index}",
            observed_at=datetime(2026, 9, 29, 9, 15 + index, tzinfo=timezone.utc),
            available_at=datetime(2026, 9, 29, 9, 16 + index, tzinfo=timezone.utc),
            values={"close": 100 + index},
        )
        for index in range(3)
    )


def test_live_incremental_and_replay_prefix_share_boundary_output() -> None:
    observations = _observations()
    live = LiveInputAdapter()
    for observation in observations:
        live.on_completed_observation(observation)
    replay = ReplayInputAdapter()
    replay.process(observations)
    assert live.state.snapshot() == replay.state.snapshot()
    assert live.state.snapshot_sha256 == replay.state.snapshot_sha256


def test_recovery_and_duplicate_observation_preserve_prefix() -> None:
    observations = _observations()
    first = ReplayInputAdapter()
    first.process(observations[:2])
    recovered = LiveInputAdapter(BoundaryState.from_snapshot(first.state.snapshot()))
    recovered.on_completed_observation(observations[1])
    recovered.on_completed_observation(observations[2])
    expected = ReplayInputAdapter()
    expected.process(observations)
    assert recovered.state.snapshot() == expected.state.snapshot()


def test_conflicting_duplicate_observation_is_rejected() -> None:
    observation = _observations()[0]
    adapter = LiveInputAdapter()
    adapter.on_completed_observation(observation)
    conflicting = CompletedObservation(
        identity=observation.identity,
        observed_at=observation.observed_at,
        available_at=observation.available_at,
        values={"close": 999},
    )
    with pytest.raises(ValueError, match="conflicting evidence"):
        adapter.on_completed_observation(conflicting)


def test_runtime_boundary_artifact_matches_all_execution_modes() -> None:
    artifact = build_runtime_boundary_artifact(_observations(), run_id="fixture")
    assert artifact["artifact_type"] == "orderflow_runtime_boundary"
    assert artifact["schema_version"] == 1
    assert artifact["coverage"] == {"complete": True, "observation_count": 3}
    assert artifact["replay"] == artifact["incremental"] == artifact["recovery"]
    assert artifact["replay"]["status"] == "MATCHED"


def test_observation_values_must_be_finite_json_values() -> None:
    with pytest.raises(ValueError, match="finite JSON numbers"):
        CompletedObservation(
            identity="source:nan",
            observed_at=datetime(2026, 9, 29, 9, 15, tzinfo=timezone.utc),
            available_at=datetime(2026, 9, 29, 9, 16, tzinfo=timezone.utc),
            values={"close": math.nan},
        )

    with pytest.raises(TypeError, match="JSON-compatible"):
        CompletedObservation(
            identity="source:bytes",
            observed_at=datetime(2026, 9, 29, 9, 15, tzinfo=timezone.utc),
            available_at=datetime(2026, 9, 29, 9, 16, tzinfo=timezone.utc),
            values={"close": bytearray(b"100")},
        )


def test_snapshot_parser_rejects_coerced_event_fields() -> None:
    snapshot = ReplayInputAdapter()
    snapshot.process(_observations()[:1])
    raw = snapshot.state.snapshot()
    raw["events"][0]["sequence"] = "0"

    with pytest.raises(ValueError, match="sequence"):
        BoundaryState.from_snapshot(raw)
