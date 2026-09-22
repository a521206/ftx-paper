"""Standalone Paper execution costs matching the canonical FTX model."""

from __future__ import annotations

FUTURES_RTD_COST_PER_LOT = 500.0
OPTION_BASE_COST_PER_LEG = 1.0 * 65.0 + 20.0 + 20.0
OPTION_LEGS = 2
STT_RATE = 0.0015


def futures_cost(lots: float) -> float:
    if lots < 0:
        raise ValueError("lots must be non-negative")
    return FUTURES_RTD_COST_PER_LOT * lots


def synthetic_futures_cost(ce_entry_premium: float, pe_entry_premium: float,
                           lots: float, *, is_short: bool) -> float:
    if ce_entry_premium <= 0 or pe_entry_premium <= 0 or lots < 0:
        raise ValueError("premiums must be positive and lots non-negative")
    sold_premium = ce_entry_premium if is_short else pe_entry_premium
    # The canonical pipeline scales the complete two-leg cost stack by the
    # traded lot count.  Keep the Paper implementation local, but preserve
    # the same economics for reporting parity.
    one_lot_cost = (OPTION_BASE_COST_PER_LEG * OPTION_LEGS
                    + STT_RATE * sold_premium * 65.0)
    return one_lot_cost * lots


__all__ = ["FUTURES_RTD_COST_PER_LOT", "OPTION_BASE_COST_PER_LEG", "OPTION_LEGS",
           "STT_RATE", "futures_cost", "synthetic_futures_cost"]
