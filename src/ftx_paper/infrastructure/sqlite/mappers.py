"""Conversions between SQLite-shaped values and typed persistence records."""

import json
from collections.abc import Mapping
from datetime import date, datetime
from typing import Any

from ftx_paper.ports.records import AuthToken, ReplayRun, RuntimeContract, StoredEvent, StoredMarketBar


def _datetime(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("stored timestamp must be timezone-aware")
    return parsed


def event_from_row(row: Mapping[str, Any]) -> StoredEvent:
    return StoredEvent(int(row["id"]), str(row["event_type"]), json.loads(str(row["payload"])), _datetime(row["created_at"]))


def market_bar_from_row(row: Mapping[str, Any]) -> StoredMarketBar:
    return StoredMarketBar(
        symbol=str(row["symbol"]), exchange=str(row["exchange"]), minute=_datetime(row["minute"]),
        open=float(row["open"]), high=float(row["high"]), low=float(row["low"]), close=float(row["close"]),
        volume=float(row["volume"]) if row["volume"] is not None else None,
        open_interest=float(row["open_interest"]) if row["open_interest"] is not None else None,
        instrument_type=str(row["instrument_type"]) if row["instrument_type"] is not None else None,
        expiry=date.fromisoformat(str(row["expiry"])) if row["expiry"] else None,
        strike=float(row["strike"]) if row["strike"] is not None else None,
        option_type=str(row["option_type"]) if row["option_type"] is not None else None,
        source=str(row["source"]), ingested_at=_datetime(row["ingested_at"]),
    )


def replay_run_from_row(row: Mapping[str, Any]) -> ReplayRun:
    result_value = row["result"]
    error_value = row["error"]
    return ReplayRun(
        run_id=str(row["run_id"]), status=str(row["status"]),
        request=json.loads(str(row["request"])),
        result=json.loads(str(result_value)) if result_value else None,
        error=str(error_value) if error_value is not None else None,
        created_at=_datetime(row["created_at"]), updated_at=_datetime(row["updated_at"]),
    )


def contract_from_row(row: Mapping[str, Any]) -> RuntimeContract:
    return RuntimeContract(
        exchange=str(row["exchange"]), underlying=str(row["underlying"]), role=str(row["role"]),
        tradingsymbol=str(row["tradingsymbol"]), instrument_token=int(row["instrument_token"]),
        expiry=date.fromisoformat(str(row["expiry"])), selected_at=_datetime(row["selected_at"]),
    )


def auth_token_from_row(row: Mapping[str, Any]) -> AuthToken:
    return AuthToken(str(row["provider"]), date.fromisoformat(str(row["token_date"])), str(row["access_token"]))


def json_record(value: Mapping[str, object]) -> str:
    return json.dumps(dict(value), default=str)
