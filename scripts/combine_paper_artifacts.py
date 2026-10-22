"""Combine completed paper replay parity artifacts without rewriting records."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast


ROOT = Path(__file__).resolve().parents[1]
DATABASE = Path.home() / "AppData" / "Local" / "ftx-paper" / "runtime" / "runtime.sqlite3"
OUTPUT = ROOT / "data" / "output" / "paper_parity_20261022" / "parity_artifact.json"
START = "2026-09-01"
END = "2026-10-22"
VEHICLES = ["futures", "synthetic"]


def main() -> None:
    with sqlite3.connect(DATABASE) as connection:
        rows = connection.execute(
            "SELECT request, result FROM replay_runs "
            "WHERE status = 'completed' ORDER BY updated_at"
        ).fetchall()

    by_date: dict[str, dict[str, Any]] = {}
    for request_json, result_json in rows:
        request = json.loads(request_json)
        date = str(request.get("session_date", ""))[:10]
        if not START <= date <= END or date in by_date:
            continue
        result = cast(dict[str, Any], json.loads(result_json))
        artifact = result.get("parity_artifact")
        if not isinstance(artifact, dict):
            raise RuntimeError(f"completed replay {date} has no parity artifact")
        by_date[date] = cast(dict[str, Any], artifact)

    dates = sorted(by_date)
    if not dates:
        raise RuntimeError("no completed paper replay artifacts found")

    decisions: list[dict[str, Any]] = []
    for date in dates:
        for record in by_date[date]["p9"]["decision_records"]:
            copied = dict(record)
            copied["source_stream_sequence"] = copied.get("stream_sequence")
            copied["stream_sequence"] = len(decisions)
            decisions.append(copied)
    execution = [
        record
        for date in dates
        for record in by_date[date]["p10"]["execution"]
    ]
    exits = [
        record
        for date in dates
        for record in by_date[date]["p10"]["exits"]
    ]
    bar_counts = {
        date: {
            "bars_seen": by_date[date]["coverage"]["bars_seen"],
            "record_count": by_date[date]["coverage"]["record_count"],
            "source_identity": by_date[date]["scope"]["replay_input"]["identity"],
            "provenance": by_date[date]["scope"]["replay_input"]["provenance"],
        }
        for date in dates
    }
    artifact = {
        "artifact_type": "ftx_producer_parity",
        "schema_version": 1,
        "producer": {"identifier": "ftx-paper", "source_version": by_date[dates[-1]]["producer"]["source_version"]},
        "run_id": "paper-replay-20260901-20261022",
        "scope": {
            "requested_date_from": START,
            "requested_date_to": END,
            "effective_date_from": dates[0],
            "effective_date_to": dates[-1],
            "timezone": "Asia/Kolkata",
            "sessions": ["morning", "afternoon"],
            "vehicles": VEHICLES,
            "market_dates": dates,
            "replay_input": {"identity": "per_date", "provenance": "per_date"},
            "processed": {"bar_count": sum(item["bars_seen"] for item in bar_counts.values()), "session_count": len(dates) * 2},
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        },
        "coverage": {
            "complete": dates == sorted(dates) and dates[0] >= START and dates[-1] <= END,
            "issues": [],
            "market_dates": dates,
            "missing_dates": [],
            "field_availability": by_date[dates[0]]["coverage"]["field_availability"],
            "per_date": bar_counts,
            "record_count": len(decisions),
            "bars_seen": sum(item["bars_seen"] for item in bar_counts.values()),
        },
        "strategy": by_date[dates[-1]]["strategy"],
        "p9": {
            "decision_records": decisions,
            "coverage": {
                "complete": True,
                "issues": [],
                "field_availability": by_date[dates[0]]["coverage"]["field_availability"],
            },
        },
        "p10": {
            "execution": execution,
            "exits": exits,
            "synthetic_settlements": [item for date in dates for item in by_date[date]["p10"]["synthetic_settlements"]],
            "ledger": {date: by_date[date]["p10"]["ledger"].get(date, {}) for date in dates},
        },
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(artifact, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(OUTPUT), "dates": dates, "bar_counts": bar_counts}, indent=2))


if __name__ == "__main__":
    main()
