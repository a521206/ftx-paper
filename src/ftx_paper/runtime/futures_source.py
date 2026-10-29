"""Deterministic dated-futures source selection for replay sessions."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date as date_type, datetime
import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any, cast

from ftx_paper.config import IST, SESSION_DATA_END_MINUTES, SESSION_OPEN_MINUTES
from ftx_paper.contracts import (
    FuturesContract, Instrument, MarketBar, SELECTION_POLICY_VERSION, parse_expiry,
    select_front_futures_contract,
)
from ftx_paper.market.feature_cache import (
    ORDERFLOW_DAILY_ATR_FEATURE_SET,
    FeatureSnapshot,
    build_orderflow_daily_atr_snapshot,
    daily_atr_from_snapshot,
)


@dataclass(frozen=True, slots=True)
class SessionSourceIdentity:
    """Identity of the storage contract selected for one session."""

    symbol: str
    exchange: str
    instrument_type: str
    expiry: str
    policy_version: str


@dataclass(frozen=True, slots=True)
class FuturesSessionSource:
    """One selected storage session projected into the strategy source layer."""

    session_date: str
    identity: SessionSourceIdentity
    bars: tuple[dict[str, Any], ...]
    source_bar_count: int
    source_fingerprint: str
    eligible_contracts: tuple[dict[str, str], ...] = ()
    rejected_contracts: tuple[dict[str, str], ...] = ()
    rollover_decision: dict[str, Any] | None = None

    @property
    def symbol(self) -> str:
        return self.identity.symbol

    @property
    def expiry(self) -> str:
        return self.identity.expiry

    @property
    def rollover_policy_version(self) -> str:
        return self.identity.policy_version

    def provenance(self) -> dict[str, Any]:
        minutes = [str(row.get("minute", ""))[11:16] for row in self.bars]
        expected_minutes = SESSION_DATA_END_MINUTES - SESSION_OPEN_MINUTES
        canonical_rows = [
            {
                "time": str(row["minute"])[11:16],
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": int(row.get("volume") or 0),
                "oi": int(row.get("open_interest") or 0),
                "symbol": "NIFTY",
                "expiry": "",
                "contract_type": "CONTINUOUS",
                "session_date": self.session_date,
            }
            for row in self.bars
        ]
        canonical_fingerprint = hashlib.sha256(
            json.dumps(canonical_rows, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return {
            "session_date": self.session_date,
            # Preserve storage identity while exposing the canonical strategy
            # fingerprint derived from the virtual continuous view.
            "instrument_type": "FUT_CONTINUOUS",
            "selected_symbol": "NIFTY",
            "selected_expiry": "",
            "normalized_series": "I",
            "storage_identity": {
                "symbol": self.symbol, "exchange": self.identity.exchange,
                "instrument_type": self.identity.instrument_type, "expiry": self.expiry,
            },
            "strategy_identity": {
                "symbol": "NIFTY", "series": "I", "contract_type": "CONTINUOUS",
                "expiry": "",
            },
            "rollover_policy_version": self.rollover_policy_version,
            "timestamp_convention": "1m label; completed at next minute; Asia/Kolkata",
            "source_bar_count": self.source_bar_count,
            "ordered_unique_minutes": len(minutes) == len(set(minutes)) and minutes == sorted(minutes),
            "missing_minute_count": max(0, expected_minutes - len(set(minutes))),
            "source_fingerprint": canonical_fingerprint,
            "source_contract_symbol": self.symbol,
            "source_contract_expiry": self.expiry,
            "eligible_contracts": list(self.eligible_contracts),
            "rejected_contracts": list(self.rejected_contracts),
            "rollover_decision": self.rollover_decision,
        }


class FuturesSessionSourceResolver:
    """Resolve one deterministic storage contract for the virtual source."""

    def __init__(self, store: Any) -> None:
        self.store = store

    def resolve(self, session_date: str) -> FuturesSessionSource | None:
        session_day = date_type.fromisoformat(session_date)
        rows = self.store.read_market_bars(session_date)
        futures = [row for row in rows if str(row.get("instrument_type", "")).upper() in {"FUT", "FUTURES"}]
        rejected: list[dict[str, Any]] = []
        contracts: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in futures:
            raw_expiry = row.get("expiry")
            if not raw_expiry:
                rejected.append({"symbol": str(row.get("symbol", "")), "reason": "missing_expiry"})
                continue
            expiry_day = parse_expiry(raw_expiry)
            expiry = expiry_day.isoformat() if expiry_day is not None else str(raw_expiry)[:10]
            if expiry_day is None:
                rejected.append({"symbol": str(row.get("symbol", "")), "expiry": expiry, "reason": "invalid_expiry"})
                continue
            symbol = str(row.get("symbol", ""))
            if expiry_day < session_day:
                rejected.append({"symbol": symbol, "expiry": expiry, "reason": "expired_before_session"})
                continue
            contracts.setdefault((expiry, symbol), []).append(row)
        if not contracts:
            return None

        selection = select_front_futures_contract(
            (FuturesContract(symbol=symbol, expiry=date_type.fromisoformat(expiry))
             for expiry, symbol in contracts),
            session_date=session_day,
        )
        if selection is None:
            return None
        selected_expiry = selection.expiry
        decision = selection.decision()
        selected_symbol = selection.symbol
        selected_rows = tuple(
            row for row in contracts[(selected_expiry.isoformat(), selected_symbol)]
            if self._in_session(row)
        )
        if not selected_rows:
            return None
        timestamps = [str(row["minute"]) for row in selected_rows]
        if len(set(timestamps)) != len(timestamps) or timestamps != sorted(timestamps):
            return None
        ordered = selected_rows
        if any(
            str(row.get("symbol")) != selected_symbol
            or str(row.get("expiry", ""))[:10] != selected_expiry.isoformat()
            or str(row.get("instrument_type", "")).upper() not in {"FUT", "FUTURES"}
            for row in ordered
        ):
            return None
        identity = SessionSourceIdentity(
            symbol=selected_symbol,
            exchange=str(ordered[0].get("exchange", "")),
            instrument_type="FUT",
            expiry=selected_expiry.isoformat(),
            policy_version=SELECTION_POLICY_VERSION,
        )
        fingerprint_rows = [
            {key: row.get(key) for key in (
                "symbol", "exchange", "minute", "open", "high", "low", "close",
                "volume", "open_interest", "instrument_type", "expiry",
            )}
            for row in ordered
        ]
        fingerprint = hashlib.sha256(
            json.dumps(
                {"identity": asdict(identity), "bars": fingerprint_rows},
                sort_keys=True, separators=(",", ":"), default=str,
            ).encode(),
        ).hexdigest()
        eligible = tuple(
            {"symbol": symbol, "expiry": expiry}
            for expiry, symbol in sorted(contracts)
        )
        rejected.extend(
            {"symbol": symbol, "expiry": expiry, "reason": "rollover_not_selected"}
            for expiry, symbol in sorted(contracts)
            if date_type.fromisoformat(expiry) != selected_expiry
            or symbol != selected_symbol
        )
        return FuturesSessionSource(
            session_date=session_date,
            identity=identity,
            bars=ordered,
            source_bar_count=len(ordered),
            source_fingerprint=fingerprint,
            eligible_contracts=eligible,
            rejected_contracts=tuple(rejected),
            rollover_decision=decision,
        )

    @staticmethod
    def _in_session(row: dict[str, Any]) -> bool:
        try:
            timestamp = datetime.fromisoformat(str(row["minute"])).astimezone(IST)
            minute = timestamp.hour * 60 + timestamp.minute
        except (KeyError, ValueError):
            return False
        # The canonical replay treats the data-end minute as the exclusive
        # completion boundary.
        return SESSION_OPEN_MINUTES <= minute < SESSION_DATA_END_MINUTES


def virtual_continuous_bar(bar: MarketBar) -> MarketBar:
    """Expose a dated futures bar through the canonical strategy identity."""
    if bar.instrument.instrument_type.upper() not in {"FUT", "FUTURE", "FUTURES"}:
        return bar
    if bar.instrument.symbol == "NIFTY" and bar.instrument.expiry is None:
        return bar
    return MarketBar(
        instrument=Instrument(
            symbol="NIFTY", exchange=bar.instrument.exchange,
            instrument_type="FUT", expiry=None,
        ),
        timestamp=bar.timestamp, open=bar.open, high=bar.high,
        low=bar.low, close=bar.close, volume=bar.volume,
        open_interest=bar.open_interest,
    )


def materialize_orderflow_daily_atr(store: Any, session_date: str) -> float | None:
    """Materialize and return the causal ATR needed by a live session.

    The current session is excluded. Historical bars remain Paper-owned and
    are resolved with the same source policy used by replay.
    """
    repository_factory = getattr(store, "feature_repository", None)
    dates_reader = getattr(store, "read_market_dates", None)
    if not callable(repository_factory) or not callable(dates_reader):
        return None
    repository = repository_factory()
    reader = getattr(repository, "read_feature_snapshot_for_session", None)
    writer = getattr(repository, "write_feature_snapshot", None)
    if not callable(reader) or not callable(writer):
        return None

    existing = reader(
        feature_set=ORDERFLOW_DAILY_ATR_FEATURE_SET,
        instrument="NIFTY",
        session_date=session_date,
    )
    existing_value = (
        daily_atr_from_snapshot(existing)
        if isinstance(existing, FeatureSnapshot) else None
    )
    resolver = FuturesSessionSourceResolver(store)
    completed: list[tuple[str, Sequence[Mapping[str, object]]]] = []
    available_dates = cast(Sequence[object], dates_reader())
    for prior_date in sorted(str(value) for value in available_dates if str(value) < session_date):
        source = resolver.resolve(prior_date)
        if source is None or not _complete_daily_session(source.bars):
            continue
        completed.append((prior_date, source.bars))
    if len(completed) < 14:
        return existing_value

    sessions = completed[-14:]
    prior_rows = completed[-15][1] if len(completed) >= 15 else ()
    prior_close = None
    if prior_rows:
        prior_day_rows = tuple(
            row for row in sorted(prior_rows, key=lambda item: str(item["minute"]))
            if str(row["minute"])[11:16] < "15:00"
        )
        if prior_day_rows:
            close_value = prior_day_rows[-1].get("close")
            if isinstance(close_value, (int, float)) and not isinstance(close_value, bool):
                prior_close = float(close_value)
    artifact = build_orderflow_daily_atr_snapshot(
        session_date, sessions, prior_close=prior_close,
    )
    if (
        isinstance(existing, FeatureSnapshot)
        and existing_value is not None
        and existing.key.input_fingerprint == artifact.key.input_fingerprint
    ):
        return existing_value
    writer(artifact)
    return daily_atr_from_snapshot(artifact)


def _complete_daily_session(rows: Sequence[Mapping[str, object]]) -> bool:
    if not rows:
        return False
    minutes = [str(row.get("minute", ""))[11:16] for row in rows]
    expected = []
    for minute in range(SESSION_OPEN_MINUTES, SESSION_DATA_END_MINUTES):
        expected.append(f"{minute // 60:02d}:{minute % 60:02d}")
    return minutes == expected


__all__ = [
    "FuturesSessionSource", "FuturesSessionSourceResolver", "SessionSourceIdentity",
    "materialize_orderflow_daily_atr", "virtual_continuous_bar",
]
