"""Deterministic, auditable replay-input contracts for orderflow.

This module owns only source normalization and identity.  It deliberately does
not aggregate bars or make strategy decisions; those are later causal layers.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
import hashlib
import json
import re
from collections.abc import Iterable, Sequence

from ftx_paper.config import IST
from ftx_paper.contracts import MarketBar, normalize_exchange_timestamp


@dataclass(frozen=True, slots=True)
class SourceIdentity:
    """Complete source identity used to partition causal replay state."""

    symbol: str
    exchange: str
    contract_type: str
    expiry: str | None = None
    strike: str | None = None
    option_type: str | None = None

    def key(self) -> str:
        return "|".join((
            self.symbol, self.exchange, self.contract_type,
            self.expiry or "", self.strike or "", self.option_type or "",
        ))


@dataclass(frozen=True, slots=True)
class OrderflowSourceMetadata:
    """Reproducibility metadata for one selected source/session."""

    source_fingerprint: str
    session_date: str
    timezone: str
    session_partition: str
    vehicle: str
    instrument_identity: str
    source_contract: str | None
    row_count: int
    ordered: bool
    rejected_rollover_candidates: tuple[str, ...] = ()
    ordered_unique_minute: bool = True
    missing_minute_count: int = 0
    selected_candidates: tuple[str, ...] = ()
    eligible_candidates: tuple[str, ...] = ()
    rejected_candidates: tuple[str, ...] = ()
    selection_rule: str = "dated_nearest_expiry"
    available_session_dates: tuple[str, ...] = ()
    discarded_session_dates: tuple[str, ...] = ()
    timestamp_convention: str = "1m label; completed at next minute; Asia/Kolkata"
    rollover_policy_version: str = "dated-nearest-expiry-v1"

    @property
    def identity(self) -> SourceIdentity:
        parts = self.instrument_identity.split("|")
        values = (parts + [""] * 6)[:6]
        return SourceIdentity(*values[:3], *(item or None for item in values[3:]))


@dataclass(frozen=True, slots=True)
class OrderflowReplayInput:
    """Normalized rows and source identity for one deterministic replay."""

    requested_date: str
    session_date: str
    rows: tuple[MarketBar, ...]
    metadata: OrderflowSourceMetadata

    replayed_session: bool = False
    selected_sessions: tuple[str, ...] = ()
    replayed_sessions: tuple[str, ...] = ()

    @property
    def replayed(self) -> bool:
        return self.replayed_session


def normalize_session_date(value: date | datetime | str) -> str:
    """Normalize an explicit session date in the configured market timezone."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("session datetime must include timezone information")
        return value.astimezone(IST).date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    raw = value.strip()
    if not raw:
        raise ValueError("session date is required")
    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError as exc:
        raise ValueError(f"invalid session date: {value!r}") from exc


def _session_minute(timestamp: datetime) -> tuple[str, str]:
    local = normalize_exchange_timestamp(timestamp).astimezone(IST)
    return local.date().isoformat(), local.strftime("%H:%M")


def _identity(bar: MarketBar) -> str:
    instrument = bar.instrument
    expiry = instrument.expiry or ""
    strike = "" if instrument.strike is None else str(instrument.strike)
    option_type = instrument.option_type.value if instrument.option_type is not None else ""
    return SourceIdentity(
        instrument.symbol, instrument.exchange, instrument.instrument_type,
        expiry or None, strike or None, option_type or None,
    ).key()


def _underlying(bar: MarketBar) -> str:
    return re.split(r"\d", bar.instrument.symbol, maxsplit=1)[0] or bar.instrument.symbol


def _row_digest(bar: MarketBar) -> dict[str, object]:
    return {
        "identity": _identity(bar),
        "timestamp": normalize_exchange_timestamp(bar.timestamp).isoformat(),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        "open_interest": bar.open_interest,
    }


def source_fingerprint(rows: Sequence[MarketBar]) -> str:
    """Hash ordered source rows, including identity and all price fields."""
    payload = json.dumps([_row_digest(row) for row in rows], separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def prepare_replay_input(
    rows: Iterable[MarketBar],
    requested_date: date | datetime | str,
    *,
    session_partition: str = "09:15-15:30",
    vehicle: str = "futures",
    source_contract: str | None = None,
    underlying: str | None = None,
    selection_rule: str = "dated_nearest_expiry",
) -> OrderflowReplayInput:
    """Select and validate one deterministic futures source for a session.

    Rows are never mixed across contracts.  When multiple dated contracts are
    supplied, callers must explicitly select the source contract.
    """
    requested = normalize_session_date(requested_date)
    if selection_rule not in {"dated_nearest_expiry", "continuous_identity"}:
        raise ValueError("selection_rule must be dated_nearest_expiry or continuous_identity")
    rollover_policy_version = (
        "continuous-source-v1"
        if selection_rule == "continuous_identity" else "dated-nearest-expiry-v1"
    )
    try:
        start_raw, end_raw = session_partition.split("-", maxsplit=1)
        start = time.fromisoformat(start_raw)
        end = time.fromisoformat(end_raw)
    except ValueError as exc:
        raise ValueError("session_partition must be HH:MM-HH:MM") from exc
    if start >= end:
        raise ValueError("session_partition must be a same-day increasing time range")
    all_rows = tuple(rows)
    all_dates = tuple(sorted({_session_minute(row.timestamp)[0] for row in all_rows}))
    normalized = tuple(row for row in all_rows if _session_minute(row.timestamp)[0] == requested)
    discarded_dates = tuple(item for item in all_dates if item != requested)
    if vehicle != "futures":
        raise ValueError("orderflow replay currently supports futures only")
    eligible_rows = [
        row for row in normalized
        if row.instrument.instrument_type.upper() in {"FUTURES", "FUT", "FUTURE"}
    ]
    underlyings = {_underlying(row) for row in eligible_rows}
    if underlying is None and len(underlyings) > 1:
        raise ValueError("multiple futures underlyings require explicit selection")
    selected_underlying = underlying or next(iter(underlyings), None)
    candidates = {
        _identity(row): row.instrument
        for row in eligible_rows
        if selected_underlying is None or _underlying(row) == selected_underlying
    }
    # Validate only the selected session window; out-of-session rows are not
    # part of the source stream used for aggregation or identity validation.
    for identity in candidates:
        candidate_rows = [
            row for row in normalized
            if _identity(row) == identity
            and start <= normalize_exchange_timestamp(row.timestamp).astimezone(IST).time().replace(tzinfo=None) < end
        ]
        timestamps = [normalize_exchange_timestamp(row.timestamp) for row in candidate_rows]
        if any(left >= right for left, right in zip(timestamps, timestamps[1:])):
            raise ValueError("orderflow source rows must be strictly chronological")
        minute_keys = [_session_minute(row.timestamp) for row in candidate_rows]
        if len(minute_keys) != len(set(minute_keys)):
            raise ValueError("orderflow source rows must have unique session minutes")
    if not candidates:
        if normalized:
            raise ValueError("requested replay contains no eligible futures source")
        metadata = OrderflowSourceMetadata(
            source_fingerprint=source_fingerprint(()), session_date=requested,
            timezone="Asia/Kolkata", session_partition=session_partition,
            vehicle=vehicle, instrument_identity="", source_contract=None,
            row_count=0, ordered=True,
            selected_candidates=(), eligible_candidates=(), rejected_candidates=(),
            selection_rule=selection_rule,
            rollover_policy_version=rollover_policy_version,
            available_session_dates=all_dates,
            discarded_session_dates=discarded_dates,
        )
        return OrderflowReplayInput(requested, requested, (), metadata, False, (), ())
    if source_contract is not None:
        matching = [identity for identity, instrument in candidates.items() if instrument.expiry == source_contract]
        if not matching:
            raise ValueError(f"requested source contract is unavailable: {source_contract!r}")
        if len(matching) != 1:
            raise ValueError(f"requested source contract is ambiguous: {source_contract!r}")
        selected_identity = matching[0]
    else:
        if len(candidates) > 1:
            raise ValueError(
                "mixed futures source identities require explicit source_contract"
            )
        selected_identity = next(iter(candidates))
    selected = tuple(
        row for row in normalized
        if _identity(row) == selected_identity
        and start <= normalize_exchange_timestamp(row.timestamp).astimezone(IST).time().replace(tzinfo=None) < end
    )
    rejected = tuple(sorted(identity for identity in candidates if identity != selected_identity))
    instrument = candidates[selected_identity]
    selected_minutes = [
        normalize_exchange_timestamp(row.timestamp).astimezone(IST).replace(second=0, microsecond=0)
        for row in selected
    ]
    expected_minutes = int((datetime.combine(date.min, end) - datetime.combine(date.min, start)).total_seconds() // 60)
    missing_minute_count = max(0, expected_minutes - len(set(selected_minutes)))
    metadata = OrderflowSourceMetadata(
        source_fingerprint=source_fingerprint(selected), session_date=requested,
        timezone="Asia/Kolkata", session_partition=session_partition,
        vehicle=vehicle, instrument_identity=selected_identity,
        source_contract=instrument.expiry, row_count=len(selected), ordered=True,
        rejected_rollover_candidates=rejected,
        ordered_unique_minute=len(selected_minutes) == len(set(selected_minutes)),
        missing_minute_count=missing_minute_count,
        selected_candidates=(selected_identity,),
        eligible_candidates=tuple(sorted(candidates)),
        rejected_candidates=rejected,
        selection_rule=selection_rule,
        rollover_policy_version=rollover_policy_version,
        available_session_dates=all_dates,
        discarded_session_dates=discarded_dates,
    )
    selected_sessions = (requested,) if candidates else ()
    replayed_sessions = (requested,) if selected else ()
    return OrderflowReplayInput(
        requested, requested, selected, metadata, bool(selected),
        selected_sessions, replayed_sessions,
    )


__all__ = [
    "OrderflowReplayInput", "OrderflowSourceMetadata", "SourceIdentity", "normalize_session_date",
    "prepare_replay_input", "source_fingerprint",
]
