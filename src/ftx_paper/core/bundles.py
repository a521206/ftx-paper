from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Mapping

from ftx_paper.contracts import MarketBar


def _utc(timestamp: datetime) -> datetime:
    """Normalize replay/live timestamps before doing deadline arithmetic."""
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class DecisionBundle:
    """Immutable, decision-clock input for exactly one completed minute."""

    bundle_id: str
    trading_date: str
    minute: str
    bars: Mapping[str, MarketBar]
    required_roles: tuple[str, ...]
    missing_roles: tuple[str, ...] = ()
    supporting_inputs: Mapping[str, object] | None = None

    @property
    def complete(self) -> bool:
        return not self.missing_roles


class CompletedBarAggregator:
    """Collect instrument bars and emit one bundle per date/minute.

    A minute is emitted immediately only when complete.  ``flush`` is the
    explicit close policy for incomplete minutes, so late instrument delivery
    cannot cause repeated strategy evaluations.
    """

    def __init__(self, role_by_instrument: Mapping[tuple[str, str], str], *, required_roles: Iterable[str] = ("futures", "vix"), deadline_seconds: float = 10.0) -> None:
        self._roles = dict(role_by_instrument)
        self.required_roles = tuple(required_roles)
        self.deadline_seconds = deadline_seconds
        if not self.required_roles or len(set(self.required_roles)) != len(self.required_roles):
            raise ValueError("required roles must be non-empty and unique")
        if not set(self.required_roles).issubset(self._roles.values()):
            raise ValueError("instrument roles omit a required input")
        self._pending: dict[tuple[str, str], dict[str, MarketBar]] = defaultdict(dict)
        self._emitted: set[tuple[str, str]] = set()
        self._first_seen: dict[tuple[str, str], datetime] = {}

    def ingest(self, bar: MarketBar) -> DecisionBundle | None:
        role = self._roles.get((bar.instrument.exchange, bar.instrument.symbol))
        if role is None:
            return None
        if role not in self.required_roles:
            return None
        key = (bar.timestamp.date().isoformat(), bar.timestamp.strftime("%H:%M"))
        if key in self._emitted:
            return None
        self._pending[key][role] = bar
        # Exchange minute timestamps provide a deterministic clock for expiry;
        # this also keeps replay and live behavior identical.
        self._first_seen.setdefault(key, _utc(bar.timestamp))
        if all(role in self._pending[key] for role in self.required_roles):
            return self._emit(key)
        return None

    def flush(self, *, incomplete: bool = True) -> tuple[DecisionBundle, ...]:
        result = []
        for key in sorted(tuple(self._pending)):
            if incomplete or all(role in self._pending[key] for role in self.required_roles):
                result.append(self._emit(key))
        return tuple(result)

    def pending(self) -> tuple[tuple[str, tuple[str, ...], tuple[str, ...]], ...]:
        """Inspect incomplete minutes without changing aggregation state."""
        result = []
        for (trading_date, minute), bars in sorted(self._pending.items()):
            missing = tuple(role for role in self.required_roles if role not in bars)
            present = tuple(role for role in self.required_roles if role in bars)
            result.append((minute, missing, present))
        return tuple(result)

    def expire(self, *, now: datetime | None = None) -> tuple[DecisionBundle, ...]:
        """Emit incomplete bundles whose grace period has elapsed."""
        now = _utc(now or datetime.now(timezone.utc))
        result = []
        for key in sorted(tuple(self._pending)):
            first_seen = _utc(self._first_seen.get(key, now))
            if (now - first_seen).total_seconds() >= self.deadline_seconds:
                result.append(self._emit(key))
        return tuple(result)

    def pending_diagnostics(self) -> tuple[dict[str, object], ...]:
        """Return bounded, UI-friendly diagnostics for incomplete minutes."""
        result = []
        for (trading_date, minute), bars in sorted(self._pending.items()):
            result.append({
                "trading_date": trading_date,
                "bundle_id": f"{trading_date}:{minute}",
                "minute": minute,
                "required_roles": list(self.required_roles),
                "roles_present": [role for role in self.required_roles if role in bars],
                "missing_roles": [role for role in self.required_roles if role not in bars],
                "last_bar_by_role": {role: bar.timestamp.isoformat() for role, bar in bars.items()},
            })
        return tuple(result)

    def _emit(self, key: tuple[str, str]) -> DecisionBundle:
        bars = self._pending.pop(key)
        self._first_seen.pop(key, None)
        missing = tuple(role for role in self.required_roles if role not in bars)
        trading_date, minute = key
        bundle = DecisionBundle(
            bundle_id=f"{trading_date}:{minute}", trading_date=trading_date, minute=minute,
            bars=dict(bars), required_roles=self.required_roles, missing_roles=missing,
        )
        self._emitted.add(key)
        return bundle


__all__ = ["CompletedBarAggregator", "DecisionBundle"]
