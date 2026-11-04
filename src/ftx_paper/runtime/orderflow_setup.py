"""Validated setup publications shared by live orderflow and bounded replay."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from ftx_paper.infrastructure.artifacts import (
    SetupArtifactStore,
    deserialize_atr_publication,
    deserialize_value_publication,
)
from ftx_paper.config import IST, SESSION_DATA_END_MINUTES, SESSION_OPEN_MINUTES
from ftx_paper.runtime.futures_source import FuturesSessionSourceResolver
from ftx_paper.strategies.orderflow.evidence_publication import AtrPublication
from ftx_paper.strategies.orderflow.value_publication import ValuePublication
from ftx_paper.strategies.orderflow.timestamps import parse_nifty_label, parse_utc


PRIOR_VALUE_IDENTITY = {
    "strategy_id": "ftx-paper-orderflow",
    "task_id": "prior-session-value",
    "algorithm_version": "orderflow.prior-session-value.v1",
    "config_fingerprint": "value-profile-default",
    "instrument": "NIFTY",
}
PRIOR_LEVELS_IDENTITY = {
    "strategy_id": "ftx-paper-orderflow",
    "task_id": "prior-session-levels",
    "algorithm_version": "orderflow.prior-session-levels.v1",
    "config_fingerprint": "prior-session-levels-NIFTY",
    "instrument": "NIFTY",
}
ATR_IDENTITY = {
    "strategy_id": "ftx-paper-orderflow",
    "task_id": "daily-atr",
    "algorithm_version": "orderflow.daily-atr.wilder.v1",
    "config_fingerprint": "daily-atr-NIFTY-14",
    "instrument": "NIFTY",
}


@dataclass(frozen=True, slots=True)
class OrderflowSetup:
    prior_session: str | None
    prior_value: ValuePublication
    atr: AtrPublication
    prior_session_levels: Mapping[str, Any]


def load_orderflow_setup(
    store: Any,
    artifact_store: SetupArtifactStore,
    source_resolver: FuturesSessionSourceResolver,
    session_date: str,
) -> OrderflowSetup:
    """Load setup artifacts only when their source lineage matches stored bars."""
    dates = sorted(str(value) for value in store.read_dates() if str(value) < session_date)
    prior_session = None
    for candidate in reversed(dates):
        candidate_source = source_resolver.resolve(candidate)
        candidate_provenance = (
            candidate_source.provenance() if candidate_source is not None else None
        )
        if (
            candidate_provenance
            and candidate_provenance.get("ordered_unique_minutes")
            and candidate_provenance.get("missing_minute_count") == 0
        ):
            prior_session = candidate
            break
    def source_provenance(session: str | None) -> Mapping[str, Any] | None:
        source = source_resolver.resolve(session) if session is not None else None
        value = source.provenance() if source is not None else None
        return value if (
            value
            and value.get("ordered_unique_minutes")
            and value.get("missing_minute_count") == 0
        ) else None

    prior_value = ValuePublication(None, None, None, (), "derived_artifact_unavailable")
    try:
        artifact = artifact_store.read(PRIOR_VALUE_IDENTITY, session_date)
        source = artifact.get("source", {}) if artifact else {}
        source_sessions = source.get("sessions", [])
        fingerprints = source.get("source_fingerprints", [])
        artifact_source_session = source_sessions[-1] if source_sessions else None
        artifact_provenance = source_provenance(artifact_source_session)
        if (
            artifact is None
            or artifact_provenance is None
            or not source_sessions
            or not fingerprints
            or fingerprints[-1] != artifact_provenance["source_fingerprint"]
        ):
            raise ValueError("prior-session-value source coverage is unavailable")
        prior_value = deserialize_value_publication(artifact["payload"])
    except (KeyError, OSError, TypeError, ValueError):
        pass

    prior_levels: Mapping[str, Any] = {}
    try:
        artifact = artifact_store.read(PRIOR_LEVELS_IDENTITY, session_date)
        source = artifact.get("source", {}) if artifact else {}
        fingerprints = source.get("source_fingerprints", [])
        payload = artifact.get("payload", {}) if artifact else {}
        artifact_source_session = payload.get("source_session")
        artifact_provenance = source_provenance(artifact_source_session)
        if (
            artifact is None
            or artifact.get("status") != "available"
            or artifact_provenance is None
            or payload.get("target_session") != session_date
            or source.get("sessions") != [artifact_source_session]
            or not fingerprints
            or fingerprints[-1] != artifact_provenance["source_fingerprint"]
        ):
            raise ValueError("prior-session-levels source coverage is unavailable")
        prior_levels = payload
    except (KeyError, OSError, TypeError, ValueError):
        pass

    atr = AtrPublication.unavailable("derived_artifact_unavailable")
    try:
        artifact = artifact_store.read(ATR_IDENTITY, session_date)
        if artifact is None or artifact.get("status") != "available":
            raise ValueError("daily ATR artifact is unavailable")
        payload = validate_atr_artifact_source(artifact, session_date)
        source_session = str(payload["source_session"])
        source = source_resolver.resolve(source_session)
        source_provenance_value = source_provenance(source_session)
        source_fingerprints = artifact["source"].get("source_fingerprints", [])
        if (
            source is None
            or source_provenance_value is None
            or not source_fingerprints
            or source_fingerprints[-1] != source_provenance_value["source_fingerprint"]
        ):
            raise ValueError("daily ATR source fingerprint is stale")
        atr = deserialize_atr_publication(payload)
    except (KeyError, OSError, TypeError, ValueError):
        pass

    return OrderflowSetup(prior_session, prior_value, atr, prior_levels)


def validate_atr_artifact_source(
    artifact: Mapping[str, Any], session_date: str,
) -> Mapping[str, Any]:
    """Validate a materialized ATR's source lineage and causal session bounds."""
    source = artifact.get("source")
    payload = artifact.get("payload")
    if not isinstance(source, Mapping) or not isinstance(payload, Mapping):
        raise ValueError("daily ATR setup artifact is incomplete")
    source_sessions = source.get("sessions")
    source_fingerprints = source.get("source_fingerprints")
    source_session = payload.get("source_session")
    if (
        not isinstance(source_sessions, list)
        or not source_sessions
        or not all(isinstance(item, str) for item in source_sessions)
        or source_sessions != sorted(set(source_sessions))
        or not isinstance(source_fingerprints, list)
        or not source_fingerprints
        or not all(isinstance(item, str) and item for item in source_fingerprints)
        or payload.get("target_session") != session_date
        or not isinstance(source_session, str)
        or source_sessions[-1] != source_session
        or source_session >= session_date
        or not isinstance(source.get("source_bar_count"), int)
        or source.get("source_bar_count", 0) <= 0
    ):
        raise ValueError("daily ATR setup artifact source lineage is invalid")
    observed_value = payload.get("observed_at")
    available_value = payload.get("available_at")
    if not isinstance(observed_value, str) or not isinstance(available_value, str):
        raise ValueError("daily ATR setup artifact timestamps are invalid")
    try:
        observed_at = parse_utc(observed_value)
        available_at = parse_utc(available_value)
        expected_available_at = parse_nifty_label(f"{session_date}T09:15")
    except (TypeError, ValueError) as exc:
        raise ValueError("daily ATR setup artifact timestamps are invalid") from exc
    local_observed_at = observed_at.astimezone(IST)
    observed_minute = local_observed_at.hour * 60 + local_observed_at.minute
    if (
        local_observed_at.date().isoformat() != source_session
        or not SESSION_OPEN_MINUTES <= observed_minute <= SESSION_DATA_END_MINUTES
        or available_at != expected_available_at
    ):
        raise ValueError("daily ATR setup artifact timestamps violate source session bounds")
    return payload
