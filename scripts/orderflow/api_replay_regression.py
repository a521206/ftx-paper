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

from tabulate import tabulate


CANONICAL = {
    "setup_candidates": 17,
    "entries_filled": 4,
    "location_interactions": 74,
    "total_location_interactions": 122,
    "vwap_interactions": 48,
    "gross_reward_points": 155.2143,
    "achieved_risk_reward": 6.1443,
}
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


def _actual_metrics(result: Mapping[str, Any]) -> dict[str, Any]:
    summary = result.get("summary")
    if not isinstance(summary, Mapping):
        raise RuntimeError("replay result does not contain the versioned summary contract")
    required = {"run_id", "schema_version", "execution_scope", "ledger_scope", "summary",
                "events", "shadow_trades", "shadow_ledger", "authoritative_trades",
                "authoritative_ledger", "parity_artifact"}
    missing = required - set(result)
    if missing:
        raise RuntimeError(f"replay result is missing required fields: {sorted(missing)}")
    return {name: summary.get(name) for name in CANONICAL}


def _display_value(value: Any) -> str:
    if value is None:
        return "unavailable"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def run(base_url: str, *, timeout: float, interval: float) -> int:
    root = base_url.rstrip("/")
    queued = _request(
        f"{root}/api/v1/replay",
        method="POST",
        payload={
            "session_date": "2026-09-29",
            "strategy_id": "ftx-paper-orderflow",
            # The canonical strategy metrics are futures-owned.  Synthetic is a
            # reporting projection and must not participate in reconciliation.
            "vehicles": ["futures"],
            "mode": "replay",
            "include_parity_layers": False,
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
    summary = result.get("summary")
    failures: list[str] = []
    rows: list[list[str]] = []
    for name, expected in CANONICAL.items():
        observed = actual[name]
        if observed is None:
            failure = "unavailable (API did not publish this metric)"
            failures.append(f"{name}: {failure}")
            rows.append([name, _display_value(expected), "unavailable", "-", "FAIL"])
        elif abs(float(observed) - expected) > 1e-4:
            failures.append(f"{name}: expected {expected}, got {observed}")
            rows.append([
                name,
                _display_value(expected),
                _display_value(observed),
                _display_value(float(observed) - expected),
                "FAIL",
            ])
        else:
            rows.append([
                name,
                _display_value(expected),
                _display_value(observed),
                _display_value(float(observed) - expected),
                "PASS",
            ])

    outcome = "FAILED" if failures else "PASSED"
    print(f"Orderflow replay regression: {outcome}")
    print(f"Run ID: {run_id}")
    print(tabulate(rows, headers=["Metric", "Expected", "Actual", "Delta", "Result"], tablefmt="grid"))
    lineage = summary.get("metric_lineage") if isinstance(summary, Mapping) else None
    if isinstance(lineage, Mapping):
        print("Metric lineage:")
        for name, value in lineage.items():
            print(f"- {name}: {value}")
    print("Location interaction breakdown:")
    if isinstance(lineage, Mapping):
        print(f"- canonical_location_interaction_unique_count: {lineage.get('canonical_location_interaction_unique_count')}")
        print(f"- location_interaction_unique_count: {lineage.get('location_interaction_unique_count')}")
        print(f"- location_interaction_by_location: {lineage.get('location_interaction_by_location')}")
        print(f"- location_interaction_by_timeframe: {lineage.get('location_interaction_by_timeframe')}")
    if failures:
        print("Regression details:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=os.getenv("FTX_API_URL", "http://127.0.0.1:8501"))
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--interval", type=float, default=0.25)
    args = parser.parse_args(argv)
    try:
        return run(args.base_url, timeout=args.timeout, interval=args.interval)
    except RuntimeError as exc:
        print(f"Regression ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
