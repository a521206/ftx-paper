from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import time
from typing import Iterable, Mapping

from ftx_paper.contracts import MarketBar, market_minute_key


@dataclass(frozen=True, slots=True)
class DecisionBundle:
    """Frozen decision-clock input for exactly one completed minute."""

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


@dataclass(frozen=True, slots=True)
class InstrumentKey:
    """Stable identity used to map a market instrument to a bundle role."""

    exchange: str
    symbol: str


@dataclass(frozen=True, slots=True)
class AggregatorConfig:
    """Configuration for the completed-bar decision clock."""

    required_roles: tuple[str, ...] = ("futures", "vix")
    deadline_seconds: float = 10.0

    def __post_init__(self) -> None:
        if not self.required_roles or len(set(self.required_roles)) != len(self.required_roles):
            raise ValueError("required roles must be non-empty and unique")
        if self.deadline_seconds < 0:
            raise ValueError("deadline_seconds must be non-negative")


class CompletedBarAggregator:
    """Emit one futures-clocked bundle per date/minute.

    Same-minute supporting bars are preferred. Missing supporting bars carry
    forward their latest completed value, and late bars cannot mutate an
    emitted bundle.
    """

    def __init__(
        self,
        role_by_instrument: Mapping[InstrumentKey, str],
        config: AggregatorConfig | None = None,
        *,
        required_roles: Iterable[str] | None = None,
        deadline_seconds: float | None = None,
    ) -> None:
        # Accept tuple keys temporarily at this boundary for old callers;
        # internal state always uses the named identity type.
        self._roles = {
            key if isinstance(key, InstrumentKey) else InstrumentKey(*key): role
            for key, role in role_by_instrument.items()
        }
        if config is not None and (required_roles is not None or deadline_seconds is not None):
            raise TypeError("use config or legacy keyword options, not both")
        if config is None:
            config = AggregatorConfig(
                required_roles=("futures", "vix") if required_roles is None else tuple(required_roles),
                deadline_seconds=10.0 if deadline_seconds is None else deadline_seconds,
            )
        self.required_roles = config.required_roles
        self.deadline_seconds = config.deadline_seconds
        if not set(self.required_roles).issubset(self._roles.values()):
            raise ValueError("instrument roles omit a required input")
        self._supporting_roles = tuple(sorted(set(self._roles.values()) - set(self.required_roles)))
        self._pending: dict[tuple[str, str], dict[str, MarketBar]] = defaultdict(dict)
        self._last_seen_by_role: dict[str, tuple[str, str]] = {}
        self._first_seen_monotonic: dict[tuple[str, str], float] = {}
        self._latest: dict[str, MarketBar] = {}
        self._latest_trading_date: str | None = None
        self._last_futures_key: tuple[str, str] | None = None
        self._last_emitted_key: tuple[str, str] | None = None

    def ingest(self, bar: MarketBar) -> DecisionBundle | None:
        role = self._roles.get(InstrumentKey(bar.instrument.exchange, bar.instrument.symbol))
        if role is None:
            return None
        key = market_minute_key(bar.timestamp)
        trading_date, _ = key
        if (
            self._latest_trading_date is not None
            and trading_date < self._latest_trading_date
        ):
            return None

        # A new trading date starts a fresh causal session.  Incomplete
        # bundles from the prior date cannot be completed after the clock has
        # advanced, and supporting values must never carry across the boundary.
        if self._latest_trading_date != trading_date:
            self._pending.clear()
            self._first_seen_monotonic.clear()
            self._last_seen_by_role.clear()
            self._latest.clear()
            self._latest_trading_date = trading_date
            self._last_futures_key = None
            self._last_emitted_key = None

        if role == "futures":
            # Futures are the decision clock.  A duplicate or late futures
            # bar must not mutate the prefix or create a second decision.
            if self._last_futures_key is not None and key <= self._last_futures_key:
                return None
            for pending_key in tuple(self._pending):
                if pending_key < key:
                    self._pending.pop(pending_key)
                    self._first_seen_monotonic.pop(pending_key, None)
            self._last_futures_key = key
        else:
            last_seen = self._last_seen_by_role.get(role)
            # Supporting bars may arrive after their futures minute and be
            # carried into the next bundle, but an older bar must never roll
            # the carry-forward value backward.
            if last_seen is not None and key <= last_seen:
                return None
            # Once the futures clock has advanced, a late supporting bar can
            # no longer complete that older minute without reordering output.
            if self._last_futures_key is not None and key < self._last_futures_key:
                return None

        if role != "futures":
            self._last_seen_by_role[role] = key

        self._latest[role] = bar
        if self._last_emitted_key is not None and key <= self._last_emitted_key:
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
        if self._last_emitted_key is not None and key < self._last_emitted_key:
            raise RuntimeError("decision bundles cannot be emitted out of order")
        current = self._pending.pop(key)
        self._first_seen_monotonic.pop(key, None)
        bars = dict(current)
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
        ordered_roles = (*self.required_roles, *self._supporting_roles)
        sources = {
            role: "same_minute" if role in current else "carried_forward"
            for role in ordered_roles
            if role in bars
        }
        ordered_bars = {role: bars[role] for role in ordered_roles if role in bars}
        missing = tuple(role for role in self.required_roles if role not in bars)
        trading_date, minute = key
        bundle = DecisionBundle(
            bundle_id=f"{trading_date}:{minute}", trading_date=trading_date, minute=minute,
            bars={role: ordered_bars[role] for role in self.required_roles if role in ordered_bars},
            required_roles=self.required_roles, missing_roles=missing,
            supporting_inputs={
                "bars": {role: ordered_bars[role] for role in self._supporting_roles if role in ordered_bars},
                "sources": sources,
                "missing": tuple(sorted(missing_same_minute)),
                "unavailable": tuple(sorted(unavailable_inputs)),
            },
        )
        self._last_emitted_key = key
        return bundle


__all__ = ["AggregatorConfig", "CompletedBarAggregator", "DecisionBundle", "InstrumentKey"]
