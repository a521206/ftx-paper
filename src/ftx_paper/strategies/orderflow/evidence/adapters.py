"""Adapters for replaying serialized canonical evidence through the producer."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Sequence

from ..evidence_publication import AtrPublication
from ..hierarchy import MarketContext
from ..location_geometry import LocationBand
from ..state_machine_types import LocationRecord, LocationStatus, SessionLevels
from ..value_publication import PriorSessionValueReference, ValuePublication
from .producer import EvidenceAdapter


def _datetime(value: Any) -> datetime | None:
    return datetime.fromisoformat(str(value)) if value is not None else None


def _location(value: Any) -> LocationRecord:
    if not isinstance(value, dict):
        raise ValueError("serialized location must be an object")
    band = value.get("band")
    if not isinstance(band, dict):
        raise ValueError("serialized location band is required")
    return LocationRecord(
        location_id=str(value["location_id"]), alias=str(value["alias"]),
        price=float(value["price"]), source_session=str(value["source_session"]),
        created_at=str(value["created_at"]), available_at=str(value["available_at"]),
        stream_id=str(value["stream_id"]),
        band=LocationBand(float(band["low"]), float(band["high"])),
        generation=int(value.get("generation", 1)),
        status=LocationStatus(value.get("status", "active")),
        frozen_atr=float(value["frozen_atr"]) if value.get("frozen_atr") is not None else None,
        parent_extreme_generation=(
            int(value["parent_extreme_generation"])
            if value.get("parent_extreme_generation") is not None else None
        ),
        source_aggregate_id=value.get("source_aggregate_id"),
        source_observation_id=value.get("source_observation_id"),
        timeframe=str(value.get("timeframe", "")),
        source_method=value.get("source_method"),
    )


def _context(value: Any) -> MarketContext:
    if not isinstance(value, dict):
        raise ValueError("serialized context must be an object")
    return MarketContext(
        context_id=str(value["context_id"]), version=str(value["version"]),
        timestamp=str(value["timestamp"]), available_at=str(value["available_at"]),
        location=value.get("location"), market_state=str(value["market_state"]),
        long_score=value.get("long_score"), short_score=value.get("short_score"),
        evidence=tuple(value.get("evidence", ())),
        source_timeframe=str(value.get("source_timeframe", "5m")),
        parent_context_id=value.get("parent_context_id"),
        definition_version=str(value.get("definition_version", "1")),
        source_provenance=tuple(value.get("source_provenance", ())),
    )


def _value_publication(value: Any) -> ValuePublication:
    if not isinstance(value, dict):
        raise ValueError("serialized prior value must be an object")
    reference_value = value.get("reference")
    if reference_value is None:
        return ValuePublication(
            None, _datetime(value.get("observed_at")), _datetime(value.get("available_at")),
            tuple(value.get("lineage", ())), value.get("unavailable_reason"),
        )
    if not isinstance(reference_value, dict):
        raise ValueError("serialized prior value reference must be an object")
    reference = PriorSessionValueReference(
        session_date=str(reference_value["session_date"]),
        value_area_low=float(reference_value["value_area_low"]),
        point_of_control=float(reference_value["point_of_control"]),
        value_area_high=float(reference_value["value_area_high"]),
        observed_at=_datetime(reference_value["observed_at"]) or datetime.min,
        available_at=_datetime(reference_value["available_at"]) or datetime.min,
        lineage=tuple(reference_value["lineage"]),
    )
    return ValuePublication(
        reference, reference.observed_at, reference.available_at, reference.lineage,
    )


class SerializedEvidenceAdapter(EvidenceAdapter):
    """Expose one serialized evidence publication as producer adapter inputs."""

    def __init__(self, publication: dict[str, Any]) -> None:
        value = publication.get("evidence", publication)
        if not isinstance(value, dict):
            raise ValueError("serialized evidence publication must be an object")
        atr_value = value.get("atr")
        if not isinstance(atr_value, dict):
            raise ValueError("serialized ATR publication is required")
        self._atr = AtrPublication(
            float(atr_value["value"]) if atr_value.get("value") is not None else None,
            atr_value.get("observed_at"), atr_value.get("available_at"),
            atr_value.get("source_observation_id"), tuple(atr_value.get("lineage", ())),
            atr_value.get("unavailable_reason"),
        )
        self._locations = tuple(_location(item) for item in value.get("locations", ()))
        self.context_stream_id = value.get("context_stream_id")
        self.location_stream_id = value.get("location_stream_id")
        if self.context_stream_id is not None and not isinstance(self.context_stream_id, str):
            raise ValueError("serialized context stream ID must be a string or null")
        if self.location_stream_id is not None and not isinstance(self.location_stream_id, str):
            raise ValueError("serialized location stream ID must be a string or null")
        self._prior_value = _value_publication(value.get("prior_value", {
            "reference": None, "lineage": (), "unavailable_reason": "missing_prior_session_value_evidence",
        }))
        self._contexts = tuple(_context(item) for item in value.get("contexts", ()))

    def atr(self, session_date: str, symbol: str) -> AtrPublication:
        del session_date, symbol
        return self._atr

    def session_levels(self, session_date: str) -> SessionLevels | None:
        opening = {item.alias: item for item in self._locations if item.alias in {"OR_LOW", "OR_HIGH"}}
        prior = {item.alias: item for item in self._locations if item.alias in {"PD_LOW", "PD_HIGH"}}
        if "OR_LOW" not in opening or "OR_HIGH" not in opening:
            return None
        return SessionLevels(
            opening["OR_LOW"].price, opening["OR_HIGH"].price,
            prior["PD_LOW"].price if "PD_LOW" in prior else None,
            prior["PD_HIGH"].price if "PD_HIGH" in prior else None,
            opening["OR_LOW"].available_at,
            prior.get("PD_LOW", prior.get("PD_HIGH", opening["OR_LOW"])).available_at,
        )

    def prior_session(self, session_date: str) -> str | None:
        del session_date
        for item in self._locations:
            if item.alias == "PD_LOW":
                return item.source_session
        return None

    def prior_value(self, session_date: str) -> ValuePublication:
        del session_date
        return self._prior_value

    def contexts(self, session_date: str) -> Sequence[MarketContext]:
        del session_date
        return self._contexts
