"""Standalone bounded replay through the shared Paper orderflow runtime."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from ftx_paper.contracts import Instrument, MarketBar, MarketRole
from ftx_paper.market.bundles import DecisionBundle

from .paper_runtime import PaperOrderflowRuntime
from .timestamps import coerce_timestamp, nifty_label, session_minute


def replay_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    rows_by_date: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Replay normalized futures rows through the single canonical owner."""
    if not rows:
        raise ValueError("replay session has no rows")
    session_date = str(rows[0]["session_date"])
    if any(str(row.get("session_date")) != session_date for row in rows):
        raise ValueError("replay rows contain a session mismatch")

    runtime = PaperOrderflowRuntime.from_rows(rows, rows_by_date=rows_by_date)
    results = runtime.replay(_bundles(rows))
    payload = dict(runtime.parity_payload([dict(row) for row in rows]))
    payload["records"] = [
        {"event_type": str(item.get("event_type", "ORDERFLOW_FACT")), **item}
        for result in results
        for item in result
        if isinstance(item, Mapping)
    ]
    payload["session_date"] = session_date
    payload["source_bar_count"] = len(rows)
    return payload


def _bundles(rows: Sequence[Mapping[str, Any]]) -> tuple[DecisionBundle, ...]:
    bundles: list[DecisionBundle] = []
    for row in rows:
        timestamp = coerce_timestamp(session_minute(str(row["session_date"]), row["time"]))
        bar = MarketBar(
            instrument=Instrument(
                symbol=str(row.get("symbol", "NIFTY")),
                exchange=str(row.get("exchange", "NFO")),
                instrument_type=str(row.get("contract_type", "FUTURES")),
                expiry=str(row.get("expiry", "")) or None,
            ),
            timestamp=timestamp,
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=int(row.get("volume", 0)),
            open_interest=int(row.get("oi", row.get("open_interest", 0)) or 0),
        )
        bundles.append(DecisionBundle(
            bundle_id=f"replay:{nifty_label(timestamp)}",
            trading_date=str(row["session_date"]),
            minute=str(row["time"])[:5],
            bars={MarketRole.FUTURES: bar},
            required_roles=(MarketRole.FUTURES,),
        ))
    return tuple(bundles)


__all__ = ["replay_rows"]
