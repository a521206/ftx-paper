"""Compare the running Paper API's 2026-09-29 replay with canonical totals.

This is an opt-in integration regression, not a pytest unit test.  The API must
already be running and must have market data for the requested session.
"""

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


CANONICAL = {
    "setup_candidates": 17,
    "entries_filled": 4,
    "location_interactions": 74,
    "gross_reward_points": 155.2143,
    "achieved_risk_reward": 6.1443,
}
LOT_SIZE = 75.0


def _request(url: str, *, method: str = "GET", payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
    body = json.dumps(payload).encode() if payload is not None else None
    request = Request(url, data=body, method=method, headers={"Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=30) as response:
            value = json.load(response)
    except (HTTPError, URLError, TimeoutError) as exc:
        raise RuntimeError(f"API request failed: {url}: {exc}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"API returned a non-object response: {url}")
    return value


def _optional_summary(result: Mapping[str, Any], name: str) -> Any:
    for container_name in ("summary", "replay_summary", "canonical_summary"):
        container = result.get(container_name)
        if isinstance(container, Mapping) and name in container:
            return container[name]
    metadata = result.get("metadata")
    if isinstance(metadata, Mapping):
        summary = metadata.get("summary")
        if isinstance(summary, Mapping):
            return summary.get(name)
    return None


def _actual_metrics(result: Mapping[str, Any]) -> dict[str, Any]:
    trades = result.get("trades")
    if not isinstance(trades, list):
        raise RuntimeError("replay result does not contain a trades list")
    futures = [trade for trade in trades if isinstance(trade, Mapping) and trade.get("vehicle") == "futures"]
    gross_rs = sum(float(trade.get("gross_pnl_rs") or 0.0) for trade in futures)
    return {
        "setup_candidates": _optional_summary(result, "setup_candidates"),
        "entries_filled": len(futures),
        "location_interactions": _optional_summary(result, "location_interactions"),
        "gross_reward_points": gross_rs / LOT_SIZE,
        "achieved_risk_reward": _optional_summary(result, "achieved_risk_reward"),
    }


def run(base_url: str, *, timeout: float, interval: float) -> int:
    root = base_url.rstrip("/")
    queued = _request(
        f"{root}/api/v1/replay",
        method="POST",
        payload={
            "session_date": "2026-09-29",
            "strategy_id": "ftx-paper-orderflow",
            "vehicles": ["futures", "synthetic"],
        },
    )
    run_id = queued.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        raise RuntimeError("replay API response did not include run_id")

    deadline = time.monotonic() + timeout
    while True:
        status = _request(f"{root}/api/v1/replay/{run_id}")
        state = status.get("status")
        if state in {"completed", "failed", "cancelled"}:
            break
        if time.monotonic() >= deadline:
            raise RuntimeError(f"replay {run_id} did not complete within {timeout:g}s")
        time.sleep(interval)

    if state != "completed":
        raise RuntimeError(f"replay {run_id} ended with status {state!r}: {status.get('error')}")
    completed = _request(f"{root}/api/v1/replay/{run_id}?include=result")
    result = completed.get("result")
    if not isinstance(result, Mapping):
        raise RuntimeError("completed replay did not include result")

    actual = _actual_metrics(result)
    failures: list[str] = []
    for name, expected in CANONICAL.items():
        observed = actual[name]
        if observed is None:
            failures.append(f"{name}: unavailable (API did not publish this metric)")
        elif abs(float(observed) - expected) > 1e-4:
            failures.append(f"{name}: expected {expected}, got {observed}")

    print(json.dumps({"run_id": run_id, "expected": CANONICAL, "actual": actual}, indent=2, sort_keys=True))
    if failures:
        print("Regression FAILED:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    print("Regression PASSED")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=os.getenv("FTX_API_URL", "http://127.0.0.1:8501"))
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--interval", type=float, default=2.0)
    args = parser.parse_args(argv)
    try:
        return run(args.base_url, timeout=args.timeout, interval=args.interval)
    except RuntimeError as exc:
        print(f"Regression ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
