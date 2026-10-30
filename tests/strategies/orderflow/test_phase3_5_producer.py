from __future__ import annotations

from typing import Sequence

from ftx_paper.strategies.orderflow import AtrPublication, observe_market
from ftx_paper.strategies.orderflow.evidence import (
    Phase35Evidence,
    SerializedEvidenceAdapter,
    evidence_payload,
    produce_evidence,
    serialize_evidence,
)
from ftx_paper.strategies.orderflow.hierarchy import MarketContext
from ftx_paper.strategies.orderflow.state_machine_types import SessionLevels
from ftx_paper.strategies.orderflow.value_publication import ValuePublication


def _rows(count: int = 10) -> list[dict[str, object]]:
    return [{
        "time": f"09:{15 + index:02d}", "open": 100 + index,
        "high": 101 + index, "low": 99 + index, "close": 100.5 + index,
        "volume": 10, "symbol": "NIFTY", "expiry": "",
        "contract_type": "CONTINUOUS", "session_date": "2026-09-29",
    } for index in range(count)]


def _artifact() -> dict[str, object]:
    observation = observe_market(_rows())
    return {
        "schema_version": 1,
        "artifact_type": "canonical_p2_market_observation",
        "source_fingerprint": observation.source_fingerprint,
        "aggregates": {
            timeframe: [item.to_dict() for item in values]
            for timeframe, values in observation.aggregates.items()
        },
    }


class _Adapter:
    def atr(self, session_date: str, symbol: str) -> AtrPublication:
        assert (session_date, symbol) == ("2026-09-29", "NIFTY")
        return AtrPublication(4.0, "2026-09-28T09:15", "2026-09-29T09:15", "atr:1", ("atr:1",))

    def session_levels(self, session_date: str) -> SessionLevels:
        assert session_date == "2026-09-29"
        return SessionLevels(99.0, 105.0, 98.0, 102.0, "2026-09-29T09:20", "2026-09-29T09:15")

    def prior_session(self, session_date: str) -> str:
        assert session_date == "2026-09-29"
        return "2026-09-28"

    def prior_value(self, session_date: str) -> ValuePublication:
        assert session_date == "2026-09-29"
        return ValuePublication(None, None, None, (), "missing_prior_session_value_evidence")

    def contexts(self, session_date: str) -> Sequence[MarketContext]:
        assert session_date == "2026-09-29"
        return (MarketContext("context", "1", "2026-09-29T09:20", "2026-09-29T09:20", "OR_HIGH", "BALANCE"),)


def test_producer_consumes_phase3_artifact_and_preserves_canonical_publications() -> None:
    result = produce_evidence(_artifact(), session_date="2026-09-29", adapter=_Adapter())

    assert isinstance(result, Phase35Evidence)
    aliases = {item.alias for item in result.publication.locations}
    assert {"OR_LOW", "OR_HIGH", "PD_LOW", "PD_HIGH", "SESSION_HIGH", "SESSION_LOW", "VWAP"} <= aliases
    assert result.publication.vwap_measurements
    assert result.publication.contexts[0].parent_context_id is None
    assert result.publication.prior_value.unavailable_reason == "missing_prior_session_value_evidence"


def test_producer_keeps_atr_dependent_evidence_unavailable() -> None:
    class MissingAdapter(_Adapter):
        def atr(self, session_date: str, symbol: str) -> AtrPublication:
            return AtrPublication.unavailable()

    result = produce_evidence(_artifact(), session_date="2026-09-29", adapter=MissingAdapter())

    assert result.publication.locations == ()
    assert result.publication.vwap_measurements == ()
    assert "missing_atr" in result.publication.unavailable


def test_producer_serialization_is_deterministic() -> None:
    result = produce_evidence(_artifact(), session_date="2026-09-29", adapter=_Adapter())

    assert serialize_evidence(result) == serialize_evidence(result)
    assert '"artifact_type":"canonical_phase3_5_evidence"' in serialize_evidence(result)


def test_serialized_publication_can_supply_a_second_producer_run() -> None:
    original = produce_evidence(_artifact(), session_date="2026-09-29", adapter=_Adapter())
    adapter = SerializedEvidenceAdapter(evidence_payload(original))

    replayed = produce_evidence(_artifact(), session_date="2026-09-29", adapter=adapter)

    assert serialize_evidence(replayed) == serialize_evidence(original)
