from __future__ import annotations

from ftx_paper.contracts import OrderIntent, OrderSide


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
