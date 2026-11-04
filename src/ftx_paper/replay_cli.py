"""Command-line client for the API-owned paper replay worker."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Mapping
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


TERMINAL_STATUSES = {"completed", "failed", "cancelled"}


def _request(url: str, *, method: str = "GET", payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = Request(url, data=body, method=method, headers={"Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=30) as response:
            value = json.load(response)
    except (HTTPError, URLError, TimeoutError) as exc:
        detail = getattr(exc, "reason", str(exc))
        raise RuntimeError(f"API request failed: {url}: {detail}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"API returned a non-object response: {url}")
    error = value.get("error")
    if error is not None:
        if isinstance(error, Mapping):
            raise RuntimeError(str(error.get("message", error)))
        raise RuntimeError(str(error))
    return value


def run(
    session_date: str,
    *,
    base_url: str,
    strategy_id: str,
    vehicles: list[str],
    include_parity_layers: bool,
    timeout: float,
    interval: float,
    fetch_result: bool = True,
) -> dict[str, Any]:
    root = base_url.rstrip("/")
    queued = _request(
        f"{root}/api/v1/strategies/{strategy_id}/replays",
        method="POST",
        payload={
            "session_date": session_date,
            "vehicles": vehicles,
            "mode": "replay",
            "include_parity_layers": include_parity_layers,
        },
    )
    run_id = queued.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        raise RuntimeError("API response did not include run_id")

    deadline = time.monotonic() + timeout
    poll_interval = max(0.05, interval)
    while True:
        result = _request(
            f"{root}/api/v1/strategies/{strategy_id}/replays/{run_id}?view=status",
        )
        if result.get("status") in TERMINAL_STATUSES:
            if not fetch_result:
                return result
            return _request(f"{root}/api/v1/strategies/{strategy_id}/replays/{run_id}")
        if time.monotonic() >= deadline:
            raise RuntimeError(f"replay {run_id} did not complete within {timeout:g}s")
        time.sleep(poll_interval)
        poll_interval = min(poll_interval * 1.5, 2.0)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a paper replay through the local API")
    parser.add_argument("session_date", help="Trading session date in YYYY-MM-DD format")
    parser.add_argument("--base-url", default=os.getenv("FTX_API_URL", "http://127.0.0.1:8501"))
    parser.add_argument("--strategy-id", default="ftx-paper-research")
    parser.add_argument("--vehicles", nargs="+", default=["futures"], choices=("futures", "synthetic"))
    parser.add_argument("--include-parity-layers", action="store_true")
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--interval", type=float, default=0.25)
    parser.add_argument("--json", action="store_true", dest="as_json", help="Print the complete API result as JSON")
    args = parser.parse_args(argv)
    try:
        result = run(
            args.session_date,
            base_url=args.base_url,
            strategy_id=args.strategy_id,
            vehicles=args.vehicles,
            include_parity_layers=args.include_parity_layers,
            timeout=args.timeout,
            interval=args.interval,
            fetch_result=args.as_json,
        )
    except RuntimeError as exc:
        print(f"Replay error: {exc}", file=sys.stderr)
        return 2

    if args.as_json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(f"Replay {result.get('status')}: {result.get('run_id', 'unknown')}")
        error = result.get("error")
        if error:
            print(f"Error: {error}", file=sys.stderr)
    return 0 if result.get("status") == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
