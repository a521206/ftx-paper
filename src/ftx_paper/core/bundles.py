from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import time
from typing import Iterable, Mapping

from ftx_paper.contracts import MarketBar, market_minute_key


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
    """Emit one futures-clocked bundle per date/minute.

    Same-minute supporting bars are preferred. Missing supporting bars carry
    forward their latest completed value, and late bars cannot mutate an
    emitted bundle.
    """

    def __init__(self, role_by_instrument: Mapping[tuple[str, str], str], *, required_roles: Iterable[str] = ("futures", "vix"), deadline_seconds: float = 10.0) -> None:
        self._roles = dict(role_by_instrument)
        self.required_roles = tuple(required_roles)
        self.deadline_seconds = deadline_seconds
        if not self.required_roles or len(set(self.required_roles)) != len(self.required_roles):
            raise ValueError("required roles must be non-empty and unique")
        if not set(self.required_roles).issubset(self._roles.values()):
            raise ValueError("instrument roles omit a required input")
        self._supporting_roles = frozenset(self._roles.values()) - frozenset(self.required_roles)
        self._pending: dict[tuple[str, str], dict[str, MarketBar]] = defaultdict(dict)
        self._emitted: set[tuple[str, str]] = set()
        self._first_seen_monotonic: dict[tuple[str, str], float] = {}
        self._latest: dict[str, MarketBar] = {}
        self._latest_trading_date: str | None = None

    def ingest(self, bar: MarketBar) -> DecisionBundle | None:
        role = self._roles.get((bar.instrument.exchange, bar.instrument.symbol))
        if role is None:
            return None
        if role not in self.required_roles and role not in self._supporting_roles:
            return None
        key = market_minute_key(bar.timestamp)
        trading_date, _ = key
        if self._latest_trading_date is not None and trading_date < self._latest_trading_date:
            return None
        if self._latest_trading_date != trading_date:
            self._latest.clear()
            self._latest_trading_date = trading_date
        self._latest[role] = bar
        if key in self._emitted:
            return None
        self._pending[key][role] = bar
        self._first_seen_monotonic.setdefault(key, time.monotonic())
        if self.required_roles == ("futures",) and role == "futures":
            return self._emit(key)
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

    def expire(self, *, now: float | None = None) -> tuple[DecisionBundle, ...]:
        """Emit incomplete bundles whose grace period has elapsed."""
        now = now if now is not None else time.monotonic()
        result = []
        for key in sorted(tuple(self._pending)):
            first_seen = self._first_seen_monotonic.get(key, now)
            if now - first_seen >= self.deadline_seconds:
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
        current = self._pending.pop(key)
        self._first_seen_monotonic.pop(key, None)
        bars = dict(current)
        sources = {role: "same_minute" for role in current}
        missing_same_minute: list[str] = []
        unavailable_inputs: list[str] = []
        for role in self._supporting_roles:
            if role in bars:
                continue
            missing_same_minute.append(role)
            previous = self._latest.get(role)
            if previous is None:
                unavailable_inputs.append(role)
                continue
            bars[role] = previous
            sources[role] = "carried_forward"
        missing = tuple(role for role in self.required_roles if role not in bars)
        trading_date, minute = key
        bundle = DecisionBundle(
            bundle_id=f"{trading_date}:{minute}", trading_date=trading_date, minute=minute,
            bars={role: bar for role, bar in bars.items() if role in self.required_roles},
            required_roles=self.required_roles, missing_roles=missing,
            supporting_inputs={
                "bars": {role: bar for role, bar in bars.items() if role in self._supporting_roles},
                "sources": sources,
                "missing": tuple(sorted(missing_same_minute)),
                "unavailable": tuple(sorted(unavailable_inputs)),
            },
        )
        self._emitted.add(key)
        return bundle


__all__ = ["CompletedBarAggregator", "DecisionBundle"]
