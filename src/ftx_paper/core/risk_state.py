"""Deterministic, replayable risk-gate state for the paper strategy.

The gate deliberately keeps exposure separate from thesis state.  Exposure is
the canonical net-directional concurrency budget and survives session-window
segments; thesis and cell cooldown state is date-scoped and is cleared at a
trading-date boundary.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Mapping


@dataclass
class CellGateState:
    consecutive_stops: int = 0
    total_entries: int = 0
    locked_until_bar: int = 0
    cooldown_until_bar: int = 0
    failed_today: bool = False


@dataclass
class RiskGateState:
    """State machine for directional, thesis, and per-cell entry gates."""

    max_net_directional_lots: float = 8.0
    thesis_cooldown_bars: int = 60
    max_consecutive_stops: int = 2
    max_daily_entries: int = 2
    cell_cooldown_bars: int = 20
    net_directional_lots: float = 0.0
    trading_date: str | None = None
    cells: dict[str, CellGateState] = field(default_factory=dict)

    def _new_day(self, date: str | None) -> None:
        if date is not None and date != self.trading_date:
            self.net_directional_lots = 0.0
            self.cells.clear()
            self.trading_date = date

    @staticmethod
    def _signed(direction: str, quantity: int) -> float:
        return float(quantity if direction.lower() in {"long", "buy"} else -quantity)

    def rejection_reason(self, *, cell: str, direction: str, bar: int,
                         quantity: int, date: str | None = None) -> str | None:
        self._new_day(date)
        state = self.cells.setdefault(cell, CellGateState())
        if abs(self.net_directional_lots + self._signed(direction, quantity)) > self.max_net_directional_lots:
            return "directional_exposure_limit"
        if state.failed_today or state.consecutive_stops >= self.max_consecutive_stops:
            return "thesis_failed"
        if bar < state.locked_until_bar:
            return "thesis_cooldown"
        if bar < state.cooldown_until_bar:
            return "cell_cooldown"
        if state.total_entries >= self.max_daily_entries:
            return "daily_cell_entry_limit"
        return None

    def can_enter(self, *, cell: str, direction: str, bar: int,
                  quantity: int = 1, date: str | None = None) -> bool:
        return self.rejection_reason(
            cell=cell, direction=direction, bar=bar, quantity=quantity, date=date,
        ) is None

    def record_entry(self, *, cell: str, direction: str, quantity: int,
                     date: str | None = None) -> None:
        self._new_day(date)
        self.cells.setdefault(cell, CellGateState()).total_entries += 1
        self.net_directional_lots += self._signed(direction, quantity)

    def record_exit(self, *, cell: str, reason: str, exit_bar: int,
                    entry_bar: int = 0, date: str | None = None) -> None:
        self._new_day(date)
        state = self.cells.setdefault(cell, CellGateState())
        state.cooldown_until_bar = exit_bar + self.cell_cooldown_bars + 1
        if reason in {"hard_stop", "stop", "thesis_failure"}:
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
        """Reset thesis/cell state at a session cooldown segment boundary."""
        self.cells.clear()

    def snapshot(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "max_net_directional_lots": self.max_net_directional_lots,
            "thesis_cooldown_bars": self.thesis_cooldown_bars,
            "max_consecutive_stops": self.max_consecutive_stops,
            "max_daily_entries": self.max_daily_entries,
            "cell_cooldown_bars": self.cell_cooldown_bars,
            "net_directional_lots": self.net_directional_lots,
            "trading_date": self.trading_date,
            "cells": {cell: asdict(state) for cell, state in self.cells.items()},
        }

    def restore(self, snapshot: Mapping[str, object]) -> None:
        if snapshot.get("schema_version") != 1:
            raise ValueError("unsupported risk-gate snapshot schema")
        for name in (
            "max_net_directional_lots", "thesis_cooldown_bars",
            "max_consecutive_stops", "max_daily_entries", "cell_cooldown_bars",
        ):
            if name in snapshot:
                value = snapshot[name]
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    raise ValueError(f"risk-gate snapshot {name} is invalid")
                setattr(self, name, type(getattr(self, name))(value))
        raw_cells = snapshot.get("cells", {})
        if not isinstance(raw_cells, Mapping):
            raise ValueError("risk-gate snapshot cells must be an object")
        self.net_directional_lots = float(snapshot.get("net_directional_lots", 0.0))
        raw_date = snapshot.get("trading_date")
        self.trading_date = str(raw_date) if raw_date is not None else None
        self.cells = {
            str(cell): CellGateState(**dict(values))
            for cell, values in raw_cells.items()
            if isinstance(values, Mapping)
        }


__all__ = ["CellGateState", "RiskGateState"]
