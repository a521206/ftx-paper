from datetime import datetime, timezone

from ftx_paper.strategies.orderflow import (
    PriorSessionValueReference,
    publish_prior_session_value,
)


def test_publication_preserves_adapter_evidence_and_causality() -> None:
    reference = PriorSessionValueReference(
        "2026-09-29", 99.0, 100.0, 101.0,
        datetime(2026, 9, 29, 15, 30, tzinfo=timezone.utc),
        datetime(2026, 9, 30, 9, 15, tzinfo=timezone.utc),
        ("profile:2026-09-29",),
    )

    class Adapter:
        def prior_session_value(self, session_date: str):
            assert session_date == "2026-09-30"
            return reference

    published = publish_prior_session_value(Adapter(), session_date="2026-09-30")
    assert published.reference is reference
    assert published.lineage == ("profile:2026-09-29",)
    assert published.unavailable_reason is None


def test_missing_adapter_evidence_fails_closed() -> None:
    class Adapter:
        def prior_session_value(self, session_date: str):
            return None

    published = publish_prior_session_value(Adapter(), session_date="2026-09-30")
    assert published.reference is None
    assert published.unavailable_reason == "missing_prior_session_value_evidence"
