"""Deterministic, replayable risk-gate state for the paper strategy.

The gate deliberately keeps exposure separate from thesis state.  Exposure is
the canonical net-directional concurrency budget and survives session-window
segments; thesis and cell cooldown state is date-scoped and is cleared at a
trading-date boundary.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Mapping

if TYPE_CHECKING:
    from ftx_paper.domain.capital import CapitalRuntimeContext


@dataclass
class CellGateState:
    consecutive_stops: int = 0
    total_entries: int = 0
    last_entry_bar: int | None = None
    locked_until_bar: int = 0
    cooldown_until_bar: int = 0
    failed_today: bool = False


@dataclass
class RiskGateState:
    """State machine for directional, thesis, and per-cell entry gates."""

    max_net_directional_lots: float = 8.0
    thesis_cooldown_bars: int = 0
    entry_cooldown_bars: int = 15
    max_consecutive_stops: int = 2
    max_daily_entries: int = 2
    cell_cooldown_bars: int = 30
    net_directional_lots: float = 0.0
    trading_date: str | None = None
    cells: dict[str, CellGateState] = field(default_factory=dict)

    @classmethod
    def from_context(cls, context: "CapitalRuntimeContext", **overrides: object) -> "RiskGateState":
        """Build mutable gate state from the immutable capital contract."""
        return cls(
            max_net_directional_lots=context.max_net_directional_lots,
            **overrides,
        )

    def _new_day(self, date: str | None) -> None:
        if date is not None and date != self.trading_date:
            self.net_directional_lots = 0.0
            self.cells.clear()
            self.trading_date = date

    def new_day(self, date: str | None) -> None:
        """Advance the gate to a trading date at an ownership boundary."""
        self._new_day(date)

    @staticmethod
    def _signed(direction: str, quantity: int) -> float:
        return float(quantity if direction.lower() in {"long", "buy"} else -quantity)

    def rejection_reason(self, *, cell: str, direction: str, bar: int,
                         quantity: int, date: str | None = None) -> str | None:
        self._new_day(date)
        thesis_reason = self.thesis_rejection_reason(
            cell=cell, bar=bar, date=date,
        )
        if thesis_reason is not None:
            return thesis_reason
        return self.directional_rejection_reason(
            direction=direction, quantity=quantity, date=date,
        )

    def thesis_rejection_reason(self, *, cell: str, bar: int,
                                date: str | None = None) -> str | None:
        self._new_day(date)
        state = self.cells.setdefault(cell, CellGateState())
        if state.failed_today or state.consecutive_stops >= self.max_consecutive_stops:
            return "thesis_failed"
        if (
            state.last_entry_bar is not None
            and bar < state.last_entry_bar + self.entry_cooldown_bars
        ):
            return "entry_cooldown"
        if bar < state.locked_until_bar:
            return "thesis_cooldown"
        if bar < state.cooldown_until_bar:
            return "cell_cooldown"
        if state.total_entries >= self.max_daily_entries:
            return "daily_cell_entry_limit"
        return None

    def directional_rejection_reason(self, *, direction: str, quantity: int,
                                     date: str | None = None) -> str | None:
        self._new_day(date)
        if quantity < 1:
            return "insufficient_directional_headroom"
        if abs(self.net_directional_lots + self._signed(direction, quantity)) > self.max_net_directional_lots:
            return "directional_exposure_limit"
        return None

    def can_enter(self, *, cell: str, direction: str, bar: int,
                  quantity: int = 1, date: str | None = None) -> bool:
        return self.rejection_reason(
            cell=cell, direction=direction, bar=bar, quantity=quantity, date=date,
        ) is None

    def remaining_directional_lots(self, direction: str) -> float:
        """Return directional headroom for the next executable entry."""
        signed = self._signed(direction, 1)
        return max(
            0.0,
            self.max_net_directional_lots - self.net_directional_lots
            if signed > 0
            else self.max_net_directional_lots + self.net_directional_lots,
        )

    def record_entry(self, *, cell: str, direction: str, quantity: int,
                     date: str | None = None, bar: int | None = None) -> None:
        self._new_day(date)
        # The canonical decision loop resolves the candidate's exit before
        # evaluating the next candidate. Its thesis counter therefore
        # behaves as an entry-time gate during replay, even though the
        # canonical tracker is updated through its result callback.
        state = self.cells.setdefault(cell, CellGateState())
        state.total_entries += 1
        if bar is not None:
            state.last_entry_bar = int(bar)
        self.net_directional_lots += self._signed(direction, quantity)

    def cancel_entry(self, *, cell: str, direction: str, quantity: int,
                     date: str | None = None) -> None:
        """Release an entry reservation that never reached the broker."""
        self._new_day(date)
        state = self.cells.get(cell)
        if state is not None:
            state.total_entries = max(0, state.total_entries - 1)
            if state.total_entries == 0 and state.consecutive_stops == 0:
                self.cells.pop(cell, None)
        self.net_directional_lots -= self._signed(direction, quantity)

    def record_exit(self, *, cell: str, reason: str, exit_bar: int,
                    entry_bar: int = 0, date: str | None = None,
                    direction: str | None = None, quantity: int = 0) -> None:
        self._new_day(date)
        if direction is not None and quantity > 0:
            self.net_directional_lots -= self._signed(direction, quantity)
        state = self.cells.setdefault(cell, CellGateState())
        state.cooldown_until_bar = exit_bar + self.cell_cooldown_bars
        if reason == "hard_stop":
            state.consecutive_stops += 1
            state.locked_until_bar = exit_bar + self.thesis_cooldown_bars
            if state.consecutive_stops >= self.max_consecutive_stops:
                state.failed_today = True
        else:
            state.consecutive_stops = 0
        # ``entry_bar`` remains part of the callback contract for replay
        # records; cooldown is anchored on the observed exit bar.

    def reset(self) -> None:
        self.net_directional_lots = 0.0
        self.trading_date = None
        self.cells.clear()

    def reset_segment(self) -> None:
        """Reset the per-segment thesis tracker while preserving exposure."""
        for state in self.cells.values():
            state.consecutive_stops = 0
            state.total_entries = 0
            state.locked_until_bar = 0
            state.cooldown_until_bar = 0
            state.failed_today = False

    def snapshot(self) -> dict[str, object]:
        return {
            "schema_version": 2,
            "max_net_directional_lots": self.max_net_directional_lots,
            "thesis_cooldown_bars": self.thesis_cooldown_bars,
            "entry_cooldown_bars": self.entry_cooldown_bars,
            "max_consecutive_stops": self.max_consecutive_stops,
            "max_daily_entries": self.max_daily_entries,
            "cell_cooldown_bars": self.cell_cooldown_bars,
            "net_directional_lots": self.net_directional_lots,
            "trading_date": self.trading_date,
            "cells": {cell: asdict(state) for cell, state in self.cells.items()},
        }

    def restore(self, snapshot: Mapping[str, object]) -> None:
        version = snapshot.get("schema_version")
        if version not in (1, 2):
            raise ValueError("unsupported risk-gate snapshot schema")
        fields = [
            "max_net_directional_lots", "max_consecutive_stops", "max_daily_entries",
        ]
        if version == 2:
            fields.extend(("thesis_cooldown_bars", "entry_cooldown_bars", "cell_cooldown_bars"))
        for name in fields:
            if name in snapshot:
                value = snapshot[name]
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    raise ValueError(f"risk-gate snapshot {name} is invalid")
                setattr(self, name, type(getattr(self, name))(value))
        raw_cells = snapshot.get("cells", {})
        if not isinstance(raw_cells, Mapping):
            raise ValueError("risk-gate snapshot cells must be an object")
        raw_net_directional_lots = snapshot.get("net_directional_lots", 0.0)
        if isinstance(raw_net_directional_lots, bool) or not isinstance(raw_net_directional_lots, (int, float)):
            raise ValueError("risk-gate snapshot net_directional_lots is invalid")
        self.net_directional_lots = float(raw_net_directional_lots)
        raw_date = snapshot.get("trading_date")
        self.trading_date = str(raw_date) if raw_date is not None else None
        self.cells = {}
        for cell, values in raw_cells.items():
            if not isinstance(values, Mapping):
                continue
            state = CellGateState(**dict(values))
            if version == 1:
                # Old snapshots held 30-minute segment timers. They are not
                # compatible with the current bar-based entry/exit cooldowns.
                state.last_entry_bar = None
                state.locked_until_bar = 0
                state.cooldown_until_bar = 0
            self.cells[str(cell)] = state


__all__ = ["CellGateState", "RiskGateState"]
