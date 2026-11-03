"""Immutable daily setup artifact storage and validation."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import os
import re
from typing import Any, Mapping
from uuid import uuid4

from ftx_paper.strategies.orderflow.value_publication import PriorSessionValueReference, ValuePublication
from ftx_paper.strategies.orderflow.evidence_publication import AtrPublication


ARTIFACT_TYPE = "strategy_daily_setup"
CATALOG_TYPE = "strategy_daily_setup_catalog"
SCHEMA_VERSION = 1


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    normalized = value.astimezone(timezone.utc)
    return normalized.isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def serialize_value_publication(publication: ValuePublication) -> dict[str, Any]:
    reference = publication.reference
    if reference is None:
        if publication.observed_at is not None or publication.available_at is not None or publication.lineage:
            raise ValueError("unavailable publication has inconsistent evidence")
        return {
            "reference": None,
            "observed_at": None,
            "available_at": None,
            "lineage": [],
            "unavailable_reason": publication.unavailable_reason,
        }
    return {
        "reference": {
            "session_date": reference.session_date,
            "value_area_low": reference.value_area_low,
            "point_of_control": reference.point_of_control,
            "value_area_high": reference.value_area_high,
            "observed_at": _timestamp(reference.observed_at),
            "available_at": _timestamp(reference.available_at),
            "lineage": list(reference.lineage),
        },
        "observed_at": _timestamp(publication.observed_at),
        "available_at": _timestamp(publication.available_at),
        "lineage": list(publication.lineage),
        "unavailable_reason": None,
    }


def build_envelope(
    *,
    strategy_id: str,
    task_id: str,
    algorithm_version: str,
    target_session: str,
    source: Mapping[str, Any],
    configuration: Mapping[str, Any],
    publication: ValuePublication,
    contract_version: int = 1,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    payload = serialize_value_publication(publication)
    envelope: dict[str, Any] = {
        "artifact_type": ARTIFACT_TYPE,
        "schema_version": SCHEMA_VERSION,
        "contract": {"id": f"{strategy_id}.{task_id}", "version": contract_version},
        "strategy_id": strategy_id,
        "task_id": task_id,
        "algorithm_version": algorithm_version,
        "target_session": target_session,
        "status": "available" if publication.reference is not None else "unavailable",
        "created_at": _timestamp(created_at or datetime.now(timezone.utc)),
        "source": dict(source),
        "configuration": dict(configuration),
        "payload": payload,
    }
    envelope["manifest"] = {
        "payload_sha256": hashlib.sha256(canonical_json(payload)).hexdigest(),
        "artifact_sha256": hashlib.sha256(canonical_json(envelope)).hexdigest(),
    }
    return envelope


def build_payload_envelope(
    *, strategy_id: str, task_id: str, algorithm_version: str, target_session: str,
    source: Mapping[str, Any], configuration: Mapping[str, Any], payload: Any,
    status: str = "available", unavailable_reason: str | None = None,
    contract_version: int = 1, created_at: datetime | None = None,
) -> dict[str, Any]:
    envelope: dict[str, Any] = {
        "artifact_type": ARTIFACT_TYPE, "schema_version": SCHEMA_VERSION,
        "contract": {"id": f"{strategy_id}.{task_id}", "version": contract_version},
        "strategy_id": strategy_id, "task_id": task_id,
        "algorithm_version": algorithm_version, "target_session": target_session,
        "status": status, "created_at": _timestamp(created_at or datetime.now(timezone.utc)),
        "source": dict(source), "configuration": dict(configuration),
        "payload": payload,
    }
    if unavailable_reason is not None:
        envelope["unavailable_reason"] = unavailable_reason
    envelope["manifest"] = {
        "payload_sha256": hashlib.sha256(canonical_json(payload)).hexdigest(),
        "artifact_sha256": hashlib.sha256(canonical_json(envelope)).hexdigest(),
    }
    return envelope


def deserialize_value_publication(payload: Mapping[str, Any]) -> ValuePublication:
    reference = payload.get("reference")
    lineage = payload.get("lineage")
    reason = payload.get("unavailable_reason")
    if reference is None:
        if payload.get("observed_at") is not None or payload.get("available_at") is not None or lineage != [] or not reason:
            raise ValueError("invalid unavailable publication payload")
        return ValuePublication(None, None, None, (), str(reason))
    if not isinstance(reference, Mapping) or not isinstance(lineage, list) or not all(isinstance(item, str) for item in lineage):
        raise ValueError("invalid value publication reference payload")
    ref_lineage = reference.get("lineage")
    if not isinstance(ref_lineage, list) or tuple(ref_lineage) != tuple(lineage):
        raise ValueError("publication and reference lineage differ")
    observed = _parse_timestamp(reference.get("observed_at"), "reference.observed_at")
    available = _parse_timestamp(reference.get("available_at"), "reference.available_at")
    ref = PriorSessionValueReference(
        str(reference.get("session_date", "")),
        float(reference["value_area_low"]),
        float(reference["point_of_control"]),
        float(reference["value_area_high"]),
        observed,
        available,
        tuple(lineage),
    )
    if payload.get("observed_at") != _timestamp(observed) or payload.get("available_at") != _timestamp(available):
        raise ValueError("publication timestamps differ from reference")
    return ValuePublication(ref, observed, available, tuple(lineage))


def deserialize_atr_publication(payload: Mapping[str, Any]) -> AtrPublication:
    """Reconstruct the typed ATR publication after envelope validation."""
    validate_atr_payload(payload)
    observed = _parse_timestamp(payload["observed_at"], "payload.observed_at")
    available = _parse_timestamp(payload["available_at"], "payload.available_at")
    lineage = tuple(str(item) for item in payload["lineage"])
    return AtrPublication(
        float(payload["value"]), observed, available, lineage[0], lineage,
    )


def validate_atr_payload(payload: Mapping[str, Any]) -> None:
    required = {
        "target_session", "source_session", "symbol", "period", "method", "value",
        "observed_at", "available_at", "lineage",
    }
    if set(payload) != required:
        raise ValueError("invalid daily ATR payload fields")
    if not isinstance(payload["target_session"], str) or not isinstance(payload["source_session"], str):
        raise ValueError("daily ATR sessions must be strings")
    if not isinstance(payload["symbol"], str) or payload["method"] != "wilder":
        raise ValueError("invalid daily ATR identity")
    if payload["period"] != 14:
        raise ValueError("unsupported daily ATR period")
    value = payload["value"]
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)) or float(value) <= 0:
        raise ValueError("daily ATR value must be finite and positive")
    _parse_timestamp(payload["observed_at"], "payload.observed_at")
    _parse_timestamp(payload["available_at"], "payload.available_at")
    if not isinstance(payload["lineage"], list) or not payload["lineage"] or not all(isinstance(item, str) and item for item in payload["lineage"]):
        raise ValueError("daily ATR lineage is required")


def validate_prior_session_levels_payload(payload: Mapping[str, Any]) -> None:
    required = {"target_session", "source_session", "symbol", "high", "low", "observed_at", "available_at", "lineage"}
    if set(payload) != required:
        raise ValueError("invalid prior-session-levels payload fields")
    if not all(isinstance(payload[key], str) and payload[key] for key in ("target_session", "source_session", "symbol")):
        raise ValueError("invalid prior-session-levels identity")
    if payload["symbol"] != "NIFTY":
        raise ValueError("unsupported prior-session-levels symbol")
    high, low = payload["high"], payload["low"]
    if any(isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)) for value in (high, low)) or float(high) < float(low):
        raise ValueError("invalid prior-session extrema")
    observed = _parse_timestamp(payload["observed_at"], "payload.observed_at")
    available = _parse_timestamp(payload["available_at"], "payload.available_at")
    if available < observed:
        raise ValueError("prior-session-levels availability precedes observation")
    lineage = payload["lineage"]
    if not isinstance(lineage, list) or len(lineage) < 2 or not all(isinstance(item, str) and item for item in lineage):
        raise ValueError("prior-session-levels lineage is required")
    if not lineage[0].startswith("source:") or not any(item.startswith("bar:") for item in lineage[1:]):
        raise ValueError("prior-session-levels lineage must identify source bars")


class SetupArtifactStore:
    """Read/write immutable setup artifacts under a persistent home directory."""

    def __init__(self, home: str | Path):
        self.root = Path(home) / "derived" / "setup"

    def _base(self, identity: Mapping[str, str], *, output_root: str | Path | None = None) -> Path:
        for key in ("strategy_id", "task_id", "algorithm_version", "config_fingerprint", "instrument"):
            _component(identity[key], key)
        root = Path(output_root) if output_root is not None else self.root
        return root / identity["strategy_id"] / identity["task_id"] / identity["algorithm_version"] / identity["config_fingerprint"] / identity["instrument"]

    def artifact_path(self, identity: Mapping[str, str], target_session: str, source_fingerprint: str, *, output_root: str | Path | None = None) -> Path:
        _component(target_session, "target_session", date_value=True)
        _component(source_fingerprint, "source_fingerprint")
        return self._base(identity, output_root=output_root) / target_session / f"source-{source_fingerprint}" / "artifact.json"

    def catalog_path(self, identity: Mapping[str, str], *, output_root: str | Path | None = None) -> Path:
        return self._base(identity, output_root=output_root) / "catalog.json"

    def write(self, envelope: Mapping[str, Any], *, source_fingerprint: str, output_root: str | Path | None = None) -> Path:
        self.validate(envelope)
        identity = {key: str(envelope[key]) for key in ("strategy_id", "task_id", "algorithm_version")}
        identity["config_fingerprint"] = str(envelope["configuration"]["config_fingerprint"])
        identity["instrument"] = str(envelope["source"]["selected_source"]["symbol"])
        target = self.artifact_path(identity, str(envelope["target_session"]), source_fingerprint, output_root=output_root)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
        temporary.write_bytes(json.dumps(envelope, ensure_ascii=True, sort_keys=True, indent=2).encode("utf-8"))
        with temporary.open("r+b") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        self._publish_catalog(identity, str(envelope["target_session"]), target, envelope, output_root=output_root)
        return target

    def _publish_catalog(self, identity: Mapping[str, str], target_session: str, path: Path, envelope: Mapping[str, Any], *, output_root: str | Path | None = None) -> None:
        catalog_path = self.catalog_path(identity, output_root=output_root)
        catalog_path.parent.mkdir(parents=True, exist_ok=True)
        catalog = self.read_catalog(identity, output_root=output_root)
        catalog = catalog or {
            "schema_version": 1, "artifact_type": CATALOG_TYPE,
            **{key: identity[key] for key in ("strategy_id", "task_id", "algorithm_version", "config_fingerprint", "instrument")},
            "entries": {},
        }
        catalog["entries"][target_session] = {
            "status": envelope["status"],
            "path": str(path.relative_to(catalog_path.parent)).replace("\\", "/"),
            "artifact_sha256": str(envelope["manifest"]["artifact_sha256"]),
        }
        temporary = catalog_path.with_name(f".{catalog_path.name}.{uuid4().hex}.tmp")
        temporary.write_bytes(json.dumps(catalog, ensure_ascii=True, sort_keys=True, indent=2).encode("utf-8"))
        with temporary.open("r+b") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, catalog_path)

    def read_catalog(self, identity: Mapping[str, str], *, output_root: str | Path | None = None) -> dict[str, Any] | None:
        path = self.catalog_path(identity, output_root=output_root)
        if not path.is_file():
            return None
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict) or value.get("artifact_type") != CATALOG_TYPE:
            raise ValueError("invalid setup artifact catalog")
        return value

    def read(self, identity: Mapping[str, str], target_session: str, *, output_root: str | Path | None = None) -> dict[str, Any] | None:
        catalog = self.read_catalog(identity, output_root=output_root)
        entry = catalog.get("entries", {}).get(target_session) if catalog else None
        if not isinstance(entry, Mapping) or entry.get("status") not in {"available", "unavailable"}:
            return None
        path = self.catalog_path(identity, output_root=output_root).parent / str(entry["path"])
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            self.validate(value)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            self.quarantine(path)
            raise ValueError("corrupt setup artifact") from exc
        if value["manifest"]["artifact_sha256"] != entry.get("artifact_sha256"):
            self.quarantine(path)
            raise ValueError("setup catalog digest mismatch")
        return value

    def quarantine(self, path: Path) -> None:
        if path.is_file():
            os.replace(path, path.with_name(f"quarantine-{uuid4().hex}-{path.name}"))

    @staticmethod
    def validate(envelope: Mapping[str, Any]) -> None:
        if not isinstance(envelope, Mapping):
            raise ValueError("setup artifact must be an object")
        required = {"artifact_type", "schema_version", "contract", "strategy_id", "task_id", "algorithm_version", "target_session", "status", "source", "configuration", "payload", "manifest"}
        if set(envelope) < required or envelope.get("artifact_type") != ARTIFACT_TYPE or envelope.get("schema_version") != SCHEMA_VERSION:
            raise ValueError("unsupported setup artifact envelope")
        if not isinstance(envelope.get("contract"), Mapping) or not isinstance(envelope.get("source"), Mapping) or not isinstance(envelope.get("configuration"), Mapping):
            raise ValueError("invalid setup artifact identity sections")
        manifest = envelope["manifest"]
        if not isinstance(manifest, Mapping) or not isinstance(manifest.get("payload_sha256"), str) or not isinstance(manifest.get("artifact_sha256"), str):
            raise ValueError("invalid setup artifact manifest")
        if hashlib.sha256(canonical_json(envelope["payload"])).hexdigest() != manifest["payload_sha256"]:
            raise ValueError("setup payload digest mismatch")
        unsigned = dict(envelope)
        unsigned.pop("manifest", None)
        if hashlib.sha256(canonical_json(unsigned)).hexdigest() != manifest["artifact_sha256"]:
            raise ValueError("setup artifact digest mismatch")
        if envelope["status"] == "available":
            payload = envelope["payload"]
            if not isinstance(payload, Mapping):
                raise ValueError("available setup payload must be an object")
            contract_id = envelope["contract"].get("id")
            if contract_id == "ftx-paper-orderflow.prior-session-value":
                deserialize_value_publication(payload)
            elif contract_id == "ftx-paper-orderflow.daily-atr":
                validate_atr_payload(payload)
            elif contract_id == "ftx-paper-orderflow.prior-session-levels":
                validate_prior_session_levels_payload(payload)
            else:
                raise ValueError("unsupported setup artifact contract")
        elif envelope["status"] == "unavailable":
            if not envelope.get("unavailable_reason") and not (
                isinstance(envelope["payload"], Mapping)
                and envelope["payload"].get("unavailable_reason")
            ):
                raise ValueError("unavailable setup artifact requires a reason")
        else:
            raise ValueError("invalid setup artifact status")


def _component(value: Any, field: str, *, date_value: bool = False) -> None:
    if not isinstance(value, str) or not value or not re.fullmatch(r"[A-Za-z0-9_.-]+", value) or ".." in value:
        raise ValueError(f"invalid artifact path component: {field}")
    if date_value:
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError(f"{field} must be YYYY-MM-DD") from exc
