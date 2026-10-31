from datetime import datetime, timezone

import pytest

from ftx_paper.strategies.orderflow.orderflow_common import Aggregate
from ftx_paper.strategies.orderflow.runtime_boundary import CompletedObservation
from ftx_paper.strategies.orderflow.timestamps import (
    parse_causal_timestamp,
    parse_nifty_label,
    parse_session_timestamp,
    serialize_utc,
    session_minute,
)


def test_nifty_label_is_normalized_to_utc() -> None:
    instant = parse_nifty_label("2026-09-29T09:15")
    assert instant == datetime(2026, 9, 29, 3, 45, tzinfo=timezone.utc)
    assert serialize_utc(instant).endswith("Z")


def test_session_boundary_accepts_legacy_and_artifact_labels() -> None:
    assert parse_session_timestamp("2026-09-29", "09:15") == parse_nifty_label("2026-09-29T09:15")
    assert session_minute("2026-09-29", "2026-09-29T09:15") == "2026-09-29T09:15"


def test_causal_boundary_rejects_unzoned_local_labels() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        parse_causal_timestamp("2026-09-29T09:15")


def test_session_boundary_rejects_a_different_local_date() -> None:
    with pytest.raises(ValueError, match="does not belong to session"):
        parse_session_timestamp("2026-09-29", "2026-09-30T09:15")

    with pytest.raises(ValueError, match="does not belong to session"):
        parse_session_timestamp(
            "2026-09-29", datetime(2026, 9, 30, 3, 45, tzinfo=timezone.utc)
        )


def test_mixed_aware_inputs_compare_as_one_utc_clock() -> None:
    aggregate = Aggregate(
        "2026-09-29T09:15", "2026-09-29T09:19", "2026-09-29T09:20",
        "5m", 5, 100, 101, 99, 100, 10, "NIFTY", "", "FUT", "2026-09-29", "fp",
    )
    observation = CompletedObservation(
        "bar", datetime.fromisoformat("2026-09-29T03:49:00+00:00"),
        aggregate.available_at, {"close": aggregate.close},
    )
    assert observation.available_at == parse_nifty_label("2026-09-29T09:20")


def test_naive_internal_datetime_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        Aggregate(
            datetime(2026, 9, 29, 3, 45), datetime(2026, 9, 29, 3, 49),
            datetime(2026, 9, 29, 3, 50), "5m", 5, 100, 101, 99, 100, 10,
            "NIFTY", "", "FUT", "2026-09-29", "fp",
        )


def test_boundary_rejects_availability_before_observation() -> None:
    with pytest.raises(ValueError, match="availability"):
        CompletedObservation(
            "bar", datetime(2026, 9, 29, 3, 50, tzinfo=timezone.utc),
            datetime(2026, 9, 29, 3, 49, tzinfo=timezone.utc), {},
        )
