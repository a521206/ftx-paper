"""Deterministic dated-futures source selection for replay sessions."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date as date_type, datetime
import hashlib
import json
from typing import Any, Protocol, Sequence
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")
SESSION_OPEN_MINUTES = 9 * 60 + 15
SESSION_CLOSE_MINUTES = 15 * 60 + 10


@dataclass(frozen=True, slots=True)
class SessionSourceIdentity:
    """Identity of the dated monthly futures contract selected for a session."""

    symbol: str
    instrument_type: str
    expiry: str
    policy_version: str


@dataclass(frozen=True, slots=True)
class FuturesSessionSource:
    """One dated futures contract materialized for one replay session."""

    session_date: str
    identity: SessionSourceIdentity
    bars: tuple[dict[str, Any], ...]
    source_bar_count: int
    source_fingerprint: str

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
        return {
            "session_date": self.session_date,
            "instrument_type": self.identity.instrument_type,
            "selected_symbol": self.symbol,
            "selected_expiry": self.expiry,
            "rollover_policy_version": self.rollover_policy_version,
            "source_bar_count": self.source_bar_count,
            "source_fingerprint": self.source_fingerprint,
        }


class MonthlyFuturesRolloverPolicy(Protocol):
    version: str

    def select_contract(
        self, session_day: date_type, expiry_days: Sequence[date_type],
    ) -> date_type: ...


@dataclass(frozen=True, slots=True)
class PreExpiryMonthlyRolloverPolicy:
    """Select the next monthly contract near the front expiry."""

    rollover_days_before_expiry: int = 2
    version: str = "v1-preexpiry-2d"

    def __post_init__(self) -> None:
        if self.rollover_days_before_expiry < 0:
            raise ValueError("rollover_days_before_expiry must be non-negative")

    def select_contract(self, session_day: date_type, expiry_days: Sequence[date_type]) -> date_type:
        selected = expiry_days[0]
        if (
            len(expiry_days) > 1
            and (selected - session_day).days <= self.rollover_days_before_expiry
        ):
            selected = expiry_days[1]
        return selected


class FuturesSessionSourceResolver:
    """Resolve one unexpired dated contract per session without mixing rows."""

    def __init__(self, store: Any, *, policy: MonthlyFuturesRolloverPolicy | None = None,
                 rollover_days_before_expiry: int = 2) -> None:
        self.store = store
        self.policy = policy or PreExpiryMonthlyRolloverPolicy(rollover_days_before_expiry)

    def resolve(self, session_date: str) -> FuturesSessionSource | None:
        session_day = date_type.fromisoformat(session_date)
        rows = self.store.read_market_bars(session_date)
        futures = [
            row for row in rows
            if str(row.get("instrument_type", "")).upper() in {"FUT", "FUTURES"}
            and row.get("expiry")
        ]
        contracts: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in futures:
            expiry = str(row["expiry"])[:10]
            try:
                expiry_day = date_type.fromisoformat(expiry)
            except ValueError:
                continue
            if expiry_day >= session_day:
                contracts.setdefault((expiry, str(row["symbol"])), []).append(row)
        if not contracts:
            return None

        expiry_days = sorted({date_type.fromisoformat(expiry) for expiry, _ in contracts})
        selected_expiry = self.policy.select_contract(session_day, expiry_days)
        selected_symbol = min(
            symbol for expiry, symbol in contracts
            if date_type.fromisoformat(expiry) == selected_expiry
        )
        selected_rows = tuple(
            row for row in contracts[(selected_expiry.isoformat(), selected_symbol)]
            if self._in_session(row)
        )
        if not selected_rows:
            return None
        ordered = tuple(sorted(selected_rows, key=lambda row: str(row["minute"])))
        timestamps = [str(row["minute"]) for row in ordered]
        if len(set(timestamps)) != len(timestamps) or timestamps != sorted(timestamps):
            return None
        if any(
            str(row.get("symbol")) != selected_symbol
            or str(row.get("expiry", ""))[:10] != selected_expiry.isoformat()
            or str(row.get("instrument_type", "")).upper() not in {"FUT", "FUTURES"}
            for row in ordered
        ):
            return None
        identity = SessionSourceIdentity(
            symbol=selected_symbol,
            instrument_type="FUT",
            expiry=selected_expiry.isoformat(),
            policy_version=self.policy.version,
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
        return FuturesSessionSource(
            session_date=session_date,
            identity=identity,
            bars=ordered,
            source_bar_count=len(ordered),
            source_fingerprint=fingerprint,
        )

    @staticmethod
    def _in_session(row: dict[str, Any]) -> bool:
        try:
            timestamp = datetime.fromisoformat(str(row["minute"])).astimezone(IST)
            minute = timestamp.hour * 60 + timestamp.minute
        except (KeyError, ValueError):
            return False
        return SESSION_OPEN_MINUTES <= minute <= SESSION_CLOSE_MINUTES


__all__ = [
    "FuturesSessionSource", "FuturesSessionSourceResolver", "MonthlyFuturesRolloverPolicy",
    "PreExpiryMonthlyRolloverPolicy", "SessionSourceIdentity",
]
