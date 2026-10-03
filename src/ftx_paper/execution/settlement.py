from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
import math

from ftx_paper.contracts import OrderIntent, OrderSide
from ftx_paper.contracts.decision_artifacts import FuturesExecutionPlan, SyntheticSettlement
from ftx_paper.config import NIFTY_LOT_SIZE
from .cost import synthetic_futures_cost


def settle_synthetic_plan(
    plan: FuturesExecutionPlan,
    *,
    entry_premiums: Mapping[str, object] | None,
    exit_premiums: Mapping[str, object] | None,
    exit_at: datetime,
) -> SyntheticSettlement:
    """Derive one synthetic settlement from an accepted futures plan."""
    trade = plan.trade_plan
    if not isinstance(exit_at, datetime) or exit_at.tzinfo is None or exit_at.utcoffset() is None:
        raise ValueError("exit_at must be timezone-aware")
    contract = dict(plan.contract) if isinstance(plan.contract, Mapping) else None
    base = {"plan_id": trade.decision_id, "contract": contract,
            "entry_at": trade.decision_at, "exit_at": exit_at,
            "premium_provenance": {"entry": _provenance(entry_premiums),
                                   "exit": _provenance(exit_premiums)}}
    if not _valid_contract(contract):
        return SyntheticSettlement(status="invalid_contract", entry_premiums=None,
                                    exit_premiums=None, costs=None, pnl=None, **base)
    entry = _positive_pair(entry_premiums)
    if entry is None:
        return SyntheticSettlement(status="missing_entry_premium", entry_premiums=None,
                                    exit_premiums=None, costs=None, pnl=None, **base)
    exit_pair = _positive_pair(exit_premiums)
    if exit_pair is None:
        return SyntheticSettlement(status="missing_exit_premium", entry_premiums=entry,
                                    exit_premiums=None, costs=None, pnl=None, **base)
    direction = trade.direction.strip().lower()
    if direction not in {"long", "short"}:
        return SyntheticSettlement(status="invalid_contract", entry_premiums=entry,
                                    exit_premiums=exit_pair, costs=None, pnl=None, **base)
    lots = trade.final_quantity
    ce_entry, pe_entry = entry["ce"], entry["pe"]
    ce_exit, pe_exit = exit_pair["ce"], exit_pair["pe"]
    if direction == "long":
        gross = ((ce_exit - ce_entry) + (pe_entry - pe_exit)) * NIFTY_LOT_SIZE * lots
    else:
        gross = ((ce_entry - ce_exit) + (pe_exit - pe_entry)) * NIFTY_LOT_SIZE * lots
    costs = synthetic_futures_cost(ce_entry, pe_entry, lots, is_short=direction == "short")
    return SyntheticSettlement(status="settled", entry_premiums=entry, exit_premiums=exit_pair,
                                costs=costs, pnl=gross - costs, **base)


def _positive_pair(values: Mapping[str, object] | None) -> dict[str, float] | None:
    if values is None:
        return None
    ce_raw, pe_raw = values.get("ce"), values.get("pe")
    if isinstance(ce_raw, bool) or not isinstance(ce_raw, (int, float, str)):
        return None
    if isinstance(pe_raw, bool) or not isinstance(pe_raw, (int, float, str)):
        return None
    try:
        ce, pe = float(ce_raw), float(pe_raw)
    except ValueError:
        return None
    if not math.isfinite(ce) or not math.isfinite(pe) or ce <= 0 or pe <= 0:
        return None
    return {"ce": ce, "pe": pe}


def _provenance(values: Mapping[str, object] | None) -> object:
    return values.get("provenance") if isinstance(values, Mapping) else None


def _valid_contract(contract: Mapping[str, object] | None) -> bool:
    if contract is None or not contract.get("expiry"):
        return False
    strike = contract.get("strike")
    if isinstance(strike, bool) or not isinstance(strike, (int, float, str)):
        return False
    try:
        return float(strike) > 0
    except ValueError:
        return False


class ExitValidationError(ValueError):
    """An exit cannot be applied to the referenced entry position."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)

    def __str__(self) -> str:
        return self.reason


def validate_exit_order(
    order: OrderIntent,
    *,
    entry_instrument: str,
    entry_vehicle: str,
    entry_side: OrderSide,
    entry_quantity: int,
    entry_leg_symbols: tuple[str, str] | None = None,
    filled_quantity: int | None = None,
    filled_instruments: tuple[str, ...] | None = None,
) -> str:
    """Validate the canonical entry identity and execution contract for an exit."""
    entry_order_id = order.entry_order_id
    if entry_order_id is None:
        raise ExitValidationError("exit_without_entry_order_id")
    if order.instrument.symbol != entry_instrument:
        raise ExitValidationError("exit_instrument_mismatch")
    if order.vehicle != entry_vehicle:
        raise ExitValidationError("exit_vehicle_mismatch")
    expected_side = OrderSide.SELL if entry_side is OrderSide.BUY else OrderSide.BUY
    if order.side is not expected_side:
        raise ExitValidationError("exit_side_mismatch")
    if order.quantity != entry_quantity:
        raise ExitValidationError("exit_quantity_mismatch")
    if filled_quantity is not None and filled_quantity != entry_quantity:
        raise ExitValidationError("fill_quantity_mismatch")
    if order.vehicle == "synthetic":
        if entry_leg_symbols is None or order.synthetic_legs is None:
            raise ExitValidationError("exit_missing_synthetic_legs")
        order_leg_symbols = tuple(sorted(leg.symbol for leg in order.synthetic_legs))
        if order_leg_symbols != tuple(sorted(entry_leg_symbols)):
            raise ExitValidationError("exit_synthetic_legs_mismatch")
        if filled_instruments is not None and tuple(sorted(filled_instruments)) != tuple(sorted(entry_leg_symbols)):
            raise ExitValidationError("fill_synthetic_legs_mismatch")
    elif filled_instruments is not None and filled_instruments != (entry_instrument,):
        raise ExitValidationError("fill_instrument_mismatch")
    return entry_order_id
