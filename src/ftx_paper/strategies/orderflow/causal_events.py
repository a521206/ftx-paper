"""Canonical causal source-event identity shared by replay and live paths."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any
from datetime import datetime

from .timestamps import coerce_timestamp, serialize_utc


@dataclass(slots=True)
class CausalEventRegistry:
    """Publish source observations and resolve their neutral parent keys."""

    _keys: dict[str, str] = field(default_factory=dict)
    _ordinals: dict[tuple[str, str, str], int] = field(default_factory=dict)

    def publish_source_observation(
        self,
        *,
        source_id: str,
        session_date: str,
        observed_at: datetime | str,
        available_at: datetime | str,
        source_fingerprint: str | None = None,
    ) -> dict[str, Any]:
        observed_text = serialize_utc(coerce_timestamp(observed_at))
        available_text = serialize_utc(coerce_timestamp(available_at))
        existing = self._keys.get(source_id)
        if existing is not None:
            return {
                "kind": "source_observation",
                "identity": source_id,
                "neutral_event_key": existing,
                "causal_parent_id": None,
                "observed_at": observed_text,
                "available_at": available_text,
                "session_date": session_date,
                "source_fingerprint": source_fingerprint,
            }
        ordinal_key = (source_id, observed_text, available_text)
        ordinal = self._ordinals.get(ordinal_key, 0)
        self._ordinals[ordinal_key] = ordinal + 1
        basis = {
            "kind": "source_observation",
            "session_date": session_date,
            "observed_at": observed_text,
            "available_at": available_text,
            "causal_ids": (),
            "level": None,
            "reason": None,
            "source_id": source_id,
            "source_fingerprint": source_fingerprint,
            "same_clock_ordinal": ordinal,
        }
        key = "niftyzoning:derived:" + hashlib.sha256(
            json.dumps(basis, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        ).hexdigest()
        self._keys[source_id] = key
        return {
            "kind": "source_observation",
            "identity": source_id,
            "neutral_event_key": key,
            "causal_parent_id": None,
            "observed_at": observed_text,
            "available_at": available_text,
            "session_date": session_date,
            "source_fingerprint": source_fingerprint,
        }

    def parent_key(self, source_id: str) -> str | None:
        return self._keys.get(source_id)


__all__ = ["CausalEventRegistry"]
