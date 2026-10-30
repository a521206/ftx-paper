"""Neutral event, trade, and summary projections for replay results."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import hashlib
import json
from typing import Any

from ftx_paper.config import IST, NIFTY_LOT_SIZE, SESSION_OPEN_MINUTES
from ftx_paper.strategies.research.config import AFTERNOON_ENTRY_MINUTES, MORNING_ENTRY_MINUTES
from ftx_paper.contracts import TradePayload, build_decision_view, validate_decision_view
from ftx_paper.contracts.replay import (
    ExecutionScope, ReplayEvent, REPLAY_SCHEMA_VERSION, detach_replay_value,
)
from ftx_paper.runtime.events import is_parity_decision_event

__all__ = ["build_summary", "canonical_events", "is_parity_decision_event", "normalize_decision", "normalize_trade"]


def _timestamp_string(value: Any) -> str:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_events(
    events: Sequence[Mapping[str, Any]], *, run_id: str, execution_scope: ExecutionScope,
) -> list[ReplayEvent]:
    result = []
    for sequence, event in enumerate(events):
        raw_timestamp = (event.get("occurred_at") or event.get("timestamp") or event.get("decision_at")
                         or event.get("fill_timestamp") or f"{event.get('session_date', '1970-01-01')}T00:00:00Z")
        occurred_at = _timestamp_string(raw_timestamp)
        identity_source = json.dumps(
            {"sequence": sequence, "event_type": event.get("event_type"),
             "decision_id": event.get("decision_id"), "occurred_at": occurred_at},
            sort_keys=True, separators=(",", ":"), default=str,
        ).encode()
        result.append(ReplayEvent(
            schema_version=REPLAY_SCHEMA_VERSION, run_id=run_id,
            execution_scope=execution_scope, ledger_scope="replay", sequence=sequence,
            occurred_at=occurred_at, identity=hashlib.sha256(identity_source).hexdigest(),
            values=detach_replay_value(event),
        ))
    return result


def build_summary(events: Sequence[Mapping[str, Any]], trades: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
    decisions = [item for item in events if is_parity_decision_event(str(item.get("event_type", "")), item)]
    rewards = [float(item["gross_pnl_rs"]) / float(item["initial_risk_rs"])
               for item in trades if item.get("initial_risk_rs") not in (None, 0)]
    return {
        "decisions_emitted": len(decisions), "orders_filled": len(trades),
        "gross_pnl": round(sum(float(item.get("gross_pnl_rs") or 0.0) for item in trades), 4),
        "risk_reward": round(sum(rewards), 2),
        "metric_lineage": {
            "decision_event_count": len(decisions), "trade_count": len(trades),
            "trades_missing_initial_risk": sum(1 for item in trades if item.get("initial_risk_rs") is None),
            "risk_reward_component_count": len(rewards),
        },
    }


def normalize_decision(event: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = dict(event)
    event_type = str(payload.get("event_type", ""))
    raw_view = payload.get("decision_view")
    view = validate_decision_view(raw_view) if isinstance(raw_view, Mapping) else build_decision_view(
        payload, event_type=event_type,
    )
    subject = dict(view["subject"])
    policy = dict(view["policy"])
    evidence = dict(view["evidence"])
    metrics = dict(evidence.get("metrics") or {})
    return {
        "decision_id": view.get("decision_id"),
        "decision_stage": view.get("stage") or event_type.lower().removesuffix("decision"),
        "outcome": policy.get("outcome") or event_type.lower(),
        "rejection_reason": policy.get("reason_code"),
        "risk": {key: metrics.get(key) for key in ("stop_basis", "risk_per_trade", "risk_amount", "quantity", "score_multiplier")
                 if metrics.get(key) is not None},
        "sizing": {key: metrics.get(key) for key in ("quantity", "max_lots", "lot_size", "entry_price")
                   if metrics.get(key) is not None},
        "execution": {key: payload.get(key) for key in ("client_order_id", "execution_status", "execution_event_type")
                      if payload.get(key) is not None},
        "subject": subject, "evidence": evidence,
        "date": evidence.get("provenance", {}).get("session_date"),
        "timestamp": view.get("at"), "bar_index": metrics.get("sequence"),
    }


def normalize_trade(trade: Mapping[str, Any]) -> TradePayload:
    entry_at = datetime.fromisoformat(str(trade["entry_timestamp"]))
    exit_at = datetime.fromisoformat(str(trade["exit_timestamp"])) if trade.get("exit_timestamp") else None
    entry_ist = entry_at.astimezone(IST)
    exit_ist = exit_at.astimezone(IST) if exit_at else None
    stop = float(trade["stop_price"]) if trade.get("stop_price") is not None else None
    risk_points = (float(trade["initial_risk_points"]) if trade.get("initial_risk_points") is not None
                   else abs(float(trade["entry_price"]) - stop) if stop is not None else None)
    risk_rs = (float(trade["initial_risk_rs"]) if trade.get("initial_risk_rs") is not None
               else risk_points * NIFTY_LOT_SIZE * float(trade.get("quantity", 0.0))
               if risk_points is not None else None)
    entry_minutes = entry_ist.hour * 60 + entry_ist.minute - SESSION_OPEN_MINUTES
    session = (
        "morning" if MORNING_ENTRY_MINUTES[0] <= entry_minutes < MORNING_ENTRY_MINUTES[1]
        else "afternoon" if AFTERNOON_ENTRY_MINUTES[0] <= entry_minutes < AFTERNOON_ENTRY_MINUTES[1]
        else "unknown"
    )
    reason = str(trade.get("exit_reason")) if trade.get("exit_reason") is not None else None
    if reason == "trailing_stop":
        reason = "trail_stop"
    return {
        "status": str(trade.get("status", "open")), "date": entry_ist.date().isoformat(),
        "session": session,
        "vehicle": str(trade.get("vehicle", "synthetic")), "cell": str(trade.get("cell", "")),
        "direction": "long" if str(trade.get("side", "")).upper() == "BUY" else "short",
        "entry_time": entry_ist.strftime("%H:%M"), "exit_time": exit_ist.strftime("%H:%M") if exit_ist else "",
        "quantity": float(trade.get("quantity", 0.0)), "entry_price": float(trade["entry_price"]),
        "stop_price": stop, "initial_risk_points": risk_points, "initial_risk_rs": risk_rs,
        "exit_price": float(trade["exit_price"]) if trade.get("exit_price") is not None else None,
        "gross_pnl_rs": float(trade["gross_pnl_rs"]) if trade.get("gross_pnl_rs") is not None else None,
        "cost_rs": float(trade["cost_rs"]) if trade.get("cost_rs") is not None else None,
        "net_pnl_rs": float(trade["net_pnl_rs"]) if trade.get("net_pnl_rs") is not None else None,
        "exit_reason": reason, "ce_strike": trade.get("ce_strike"),
        "ce_entry_price": trade.get("ce_entry_price"), "pe_entry_price": trade.get("pe_entry_price"),
        "ce_exit_price": trade.get("ce_exit_price"), "pe_exit_price": trade.get("pe_exit_price"),
        "synthetic_entry_price": trade.get("synthetic_entry_price"),
        "synthetic_exit_price": trade.get("synthetic_exit_price"),
        "ce_symbol": trade.get("ce_symbol"), "pe_symbol": trade.get("pe_symbol"),
        "expiry": trade.get("ce_expiry") or trade.get("pe_expiry"),
        "quote_timestamp": trade.get("quote_timestamp"), "quote_source": trade.get("quote_source"),
        "ce_exit_symbol": trade.get("ce_exit_symbol"), "pe_exit_symbol": trade.get("pe_exit_symbol"),
        "exit_quote_timestamp": trade.get("exit_quote_timestamp"),
        "exit_quote_source": trade.get("exit_quote_source"),
    }


__all__ = ["build_summary", "canonical_events", "normalize_decision", "normalize_trade"]
