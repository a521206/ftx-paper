from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any


_MODULE_PATH = Path(__file__).parents[1] / "scripts" / "orderflow" / "phase3_parity_harness.py"
_SPEC = importlib.util.spec_from_file_location("phase3_parity_harness", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_HARNESS = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_HARNESS)


def _payload(**updates: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "source_fingerprint": "source-1",
        "aggregates": {"1m": [{"time": "2026-09-29T09:15"}]},
        "source_gaps": [],
        "issues": [],
        "session_boundaries": {},
    }
    payload.update(updates)
    return payload


def test_matching_payloads_return_hash_manifest() -> None:
    result = _HARNESS.compare_payloads(_payload(), _payload())

    assert result["verdict"] == "MATCHED"
    assert set(result["manifest"]) == set(_HARNESS.SECTIONS)


def test_mismatch_reports_section_and_first_field() -> None:
    result = _HARNESS.compare_payloads(
        _payload(),
        _payload(aggregates={"1m": [{"time": "2026-09-29T09:16"}]}),
    )

    assert result["verdict"] == "MISMATCHED"
    assert result["first_mismatch"] == {
        "section": "aggregates",
        "path": ".1m[0].time",
        "canonical": "2026-09-29T09:15",
        "paper": "2026-09-29T09:16",
    }


def test_missing_section_is_reported_as_mismatch() -> None:
    result = _HARNESS.compare_payloads(_payload(), {})

    assert result["verdict"] == "INCOMPLETE"
    assert result["missing_sections"]["canonical"] == []
    assert result["missing_sections"]["paper"] == list(_HARNESS.SECTIONS)


def test_missing_sections_on_both_sides_cannot_match() -> None:
    result = _HARNESS.compare_payloads({}, {})

    assert result["verdict"] == "INCOMPLETE"
    assert result["missing_sections"]["canonical"] == list(_HARNESS.SECTIONS)
    assert result["missing_sections"]["paper"] == list(_HARNESS.SECTIONS)
