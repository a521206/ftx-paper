"""Common strategy snapshot envelope and validation."""

from __future__ import annotations

from collections.abc import Mapping


ENVELOPE_SCHEMA_VERSION = 1


def wrap_snapshot(
    *,
    strategy_id: str,
    metadata: Mapping[str, str],
    payload: Mapping[str, object],
    enabled: bool = True,
) -> dict[str, object]:
    if not strategy_id.strip():
        raise ValueError("strategy snapshot strategy_id is required")
    return {
        "envelope_schema_version": ENVELOPE_SCHEMA_VERSION,
        "strategy_id": strategy_id,
        "enabled": enabled,
        "metadata": dict(metadata),
        "payload": dict(payload),
    }


def unwrap_snapshot(raw: Mapping[str, object]) -> tuple[str | None, Mapping[str, object]]:
    if "envelope_schema_version" not in raw:
        return None, raw
    if raw.get("envelope_schema_version") != ENVELOPE_SCHEMA_VERSION:
        raise ValueError("unsupported strategy snapshot envelope schema")
    strategy_id = raw.get("strategy_id")
    payload = raw.get("payload")
    if not isinstance(strategy_id, str) or not strategy_id.strip():
        raise ValueError("strategy snapshot strategy_id is invalid")
    if not isinstance(raw.get("enabled"), bool):
        raise ValueError("strategy snapshot enabled flag is invalid")
    if not isinstance(raw.get("metadata"), Mapping):
        raise ValueError("strategy snapshot metadata is invalid")
    if not isinstance(payload, Mapping):
        raise ValueError("strategy snapshot payload is invalid")
    return strategy_id, payload


__all__ = ["ENVELOPE_SCHEMA_VERSION", "unwrap_snapshot", "wrap_snapshot"]
