"""Serialization of live strategy state."""

from collections.abc import Mapping


def snapshot_strategy(strategy) -> Mapping[str, object]:
    """Return the persisted state required to resume a live strategy."""
    decision_positions = {
        order_id: {
            "position": strategy._position_snapshot(position),
            "exit_state": exits.snapshot(),
        }
        for order_id, (position, exits) in strategy._decision_positions.items()
    }
    profile = strategy.capital_profile
    return {
        "schema_version": 1,
        "enabled_vehicles": list(strategy.enabled_vehicles),
        "capital": {
            "initial_capital": profile.initial_capital,
            "max_daily_loss": profile.max_daily_loss,
            "max_net_directional_lots": profile.max_net_directional_lots,
            "risk_per_trade": profile.risk_per_trade,
            "max_lots": profile.max_lots,
            "cell_session_risk_buffer_fraction": profile.cell_session_risk_buffer_fraction,
        },
        "config": strategy.config.as_dict(),
        "risk_gate": strategy._decision_engine.risk_snapshot(),
        "portfolio": strategy.portfolio.snapshot(),
        "decision_positions": decision_positions,
        "pending_exits": dict(strategy._pending_exits),
        "vehicle_risk_limits": {
            key: {"max_quantity": value.max_quantity, "margin_per_lot": value.margin_per_lot}
            for key, value in strategy.vehicle_risk_limits.items()
        },
    }
