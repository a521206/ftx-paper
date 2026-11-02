"""CLI for the bounded orderflow replay pipeline."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
from typing import Any

from ftx_paper.config import PaperConfig
from ftx_paper.strategies.orderflow.replay_pipeline import OrderflowReplayEngine
from ftx_paper.strategies.orderflow.timestamps import session_minute

__all__ = ["OrderflowReplayEngine", "main"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="ISO trading date")
    parser.add_argument("--runtime-dir", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--include-interactions", action="store_true")
    parser.add_argument(
        "--source-artifact", type=Path,
        help="JSON mapping of session dates to normalized replay bars",
    )
    args = parser.parse_args(argv)
    try:
        engine = OrderflowReplayEngine()
        if args.source_artifact is None:
            result = engine.replay_session(
                args.date,
                runtime_dir=args.runtime_dir or PaperConfig.from_env().runtime_dir,
                include_interactions=args.include_interactions,
            )
        else:
            rows, rows_by_date = _load_source_artifact(args.source_artifact, args.date)
            result = engine.replay(
                rows,
                rows_by_date,
                session_date=args.date,
                include_interactions=args.include_interactions,
            )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        result = {"status": "INCOMPLETE", "reason": str(exc)}
    rendered = json.dumps(result, indent=2, sort_keys=True)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)
    return 0 if result.get("status") != "INCOMPLETE" else 1


def _load_source_artifact(
    path: Path, session_date: str,
) -> tuple[list[dict[str, Any]], dict[str, tuple[dict[str, Any], ...]]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("source artifact must be an object")
    sessions = payload.get("sessions", payload)
    if not isinstance(sessions, dict):
        raise ValueError("source artifact must map session dates to bar lists")

    rows: list[dict[str, Any]] = []
    rows_by_date: dict[str, tuple[dict[str, Any], ...]] = {}
    for raw_session_date, raw_rows in sessions.items():
        if not isinstance(raw_rows, list):
            raise ValueError("source artifact must map session dates to bar lists")
        normalized_date = str(raw_session_date)
        normalized_rows: list[dict[str, Any]] = []
        for row in raw_rows:
            if not isinstance(row, dict):
                continue
            normalized_rows.append(row)
            if normalized_date == session_date and _before_close(row.get("time"), session_date):
                rows.append(row)
        rows_by_date[normalized_date] = tuple(normalized_rows)

    if not rows:
        raise ValueError(f"source artifact has no rows for {session_date}")
    return rows, rows_by_date


def _before_close(value: object, session_date: str) -> bool:
    if not isinstance(value, (datetime, str)):
        raise ValueError("source artifact row time must be a timestamp")
    return session_minute(session_date, value)[-5:] <= "15:30"


if __name__ == "__main__":
    raise SystemExit(main())
