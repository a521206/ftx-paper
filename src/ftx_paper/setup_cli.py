"""Thin loopback API client for daily setup materialization."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Mapping
from urllib.request import Request, urlopen


def _request(url: str, *, method: str = "GET", payload: Mapping[str, object] | None = None) -> dict[str, object]:
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = Request(url, data=body, method=method, headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=30) as response:
        value = json.load(response)
    if not isinstance(value, dict):
        raise RuntimeError("setup API returned a non-object response")
    if value.get("error"):
        raise RuntimeError(str(value["error"]))
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run daily setup through the local API")
    parser.add_argument("--base-url", default=os.getenv("FTX_API_URL", "http://127.0.0.1:8501"))
    parser.add_argument("--strategy-id", default="ftx-paper-orderflow")
    parser.add_argument("--task-id", default="prior-session-value")
    parser.add_argument("--mode", choices=("catch_up", "incremental", "rebuild"), default="incremental")
    parser.add_argument("--from", dest="from_session")
    parser.add_argument("--through", dest="through_session")
    parser.add_argument("--output-root")
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--interval", type=float, default=0.25)
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args(argv)
    payload = {"schema_version": 1, "strategy_id": args.strategy_id, "task_id": args.task_id, "mode": args.mode,
               "from_session": args.from_session, "through_session": args.through_session,
               "output_root": args.output_root, "rebuild": args.mode == "rebuild", "requested_by": "cli"}
    try:
        root = args.base_url.rstrip("/")
        result = _request(f"{root}/api/v1/setup-runs", method="POST", payload=payload)
        job_id = str(result["job_id"])
        deadline = time.monotonic() + args.timeout
        while result.get("status") not in {"completed", "partial", "failed", "cancelled"}:
            if time.monotonic() >= deadline:
                raise RuntimeError(f"setup {job_id} did not complete within {args.timeout:g}s")
            time.sleep(max(0.05, args.interval))
            result = _request(f"{root}/api/v1/setup-runs/{job_id}")
    except Exception as exc:
        print(f"Setup error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True) if args.as_json else f"Setup {result.get('status')}: {result.get('job_id')}")
    return 0 if result.get("status") == "completed" else 1
