from __future__ import annotations

from datetime import datetime
from typing import Any

from ftx_paper.api.schemas import error_payload
from ftx_paper.config import IST
from ftx_paper.contracts import validate_decision_view


def read_strategy_decisions(
    store: Any,
    *,
    strategy_id: str,
    selected_date: str | None,
    limit_text: str,
    before_text: str | None,
) -> tuple[dict[str, Any] | None, tuple[dict[str, Any], int] | None]:
    """Read one strategy's canonical decisions for strategy-owned APIs."""
    session_date = selected_date or datetime.now(IST).date().isoformat()
    try:
        parsed_date = datetime.strptime(session_date, "%Y-%m-%d").date()
        limit = int(limit_text)
        before_id = int(before_text) if before_text else None
    except ValueError:
        return None, (error_payload("invalid_request", "date, limit, and before are invalid"), 400)
    if parsed_date.isoformat() != session_date:
        return None, (error_payload("invalid_date", "date must be YYYY-MM-DD"), 400)
    if not 1 <= limit <= 100:
        return None, (error_payload("invalid_limit", "limit must be between 1 and 100"), 400)
    if before_id is not None and before_id < 1:
        return None, (error_payload("invalid_cursor", "before must be a positive event cursor"), 400)

    rows = store.read_decision_events(
        session_date=session_date,
        strategy_id=strategy_id,
        limit=limit + 1,
        before_id=before_id,
        canonical_only=True,
    )
    has_more = len(rows) > limit
    rows = rows[:limit]
    next_cursor = str(rows[-1]["_cursor_id"]) if has_more and rows else None
    decisions = []
    for row in rows:
        payload = dict(row.get("payload", {}))
        view = payload.get("decision_view")
        if not isinstance(view, dict):
            raise ValueError("canonical decision event is missing decision_view")
        event_id = row.pop("_cursor_id", None)
        decisions.append({
            **row,
            "category": "decision",
            "decision_view": validate_decision_view(view),
            "payload": payload,
            "event_id": event_id,
        })
    return {
        "date": session_date,
        "summary": store.read_decision_summary(session_date, strategy_id=strategy_id, canonical_only=True),
        "decisions": decisions,
        "has_more": has_more,
        "next_cursor": next_cursor,
    }, None
