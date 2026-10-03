"""Serialization and restoration of live strategy state."""

from collections.abc import Mapping, Sequence
from datetime import datetime
import math

from ftx_paper.contracts import Instrument, OptionType, OrderSide
from ftx_paper.domain.portfolio import PortfolioState
from .exits import ExitStateMachine, PositionState
from .risk import VehicleRiskLimits
from .config import (
    AFTERNOON_ENTRY_MINUTES,
    DEFAULT_CONFIG,
    MORNING_ENTRY_MINUTES,
    STRATEGY_NAME,
    STRATEGY_VERSION,
    StrategyConfig,
)


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


def restore_strategy(cls, snapshot: Mapping[str, object], *, capital_profile):
    """Restore a configured strategy from its versioned persisted state."""
    schema_version = snapshot.get("schema_version", 0)
    if not isinstance(schema_version, int) or isinstance(schema_version, bool) or schema_version != 1:
        raise ValueError("unsupported strategy snapshot schema")
    raw_vehicles = snapshot.get("enabled_vehicles", (snapshot.get("vehicle", "futures"),))
    if isinstance(raw_vehicles, str) or not isinstance(raw_vehicles, Sequence):
        raise ValueError("snapshot enabled_vehicles must be a sequence")
    if any(not isinstance(item, str) for item in raw_vehicles):
        raise ValueError("snapshot enabled_vehicles must contain strings")
    enabled_vehicles = tuple(item.lower() for item in raw_vehicles)
    if not enabled_vehicles or any(item not in {"futures", "synthetic"} for item in enabled_vehicles):
        raise ValueError("snapshot enabled_vehicles must contain 'futures' and/or 'synthetic'")

    raw_capital = snapshot.get("capital")
    if not isinstance(raw_capital, Mapping):
        raise ValueError("strategy snapshot capital must be an object")
    for name, configured in (
        ("initial_capital", capital_profile.initial_capital),
        ("max_daily_loss", capital_profile.max_daily_loss),
        ("max_net_directional_lots", capital_profile.max_net_directional_lots),
        ("risk_per_trade", capital_profile.risk_per_trade),
        ("max_lots", capital_profile.max_lots),
    ):
        persisted = raw_capital.get(name)
        if isinstance(persisted, bool) or not isinstance(persisted, (int, float)) or float(persisted) != configured:
            raise ValueError(
                f"capital config does not match strategy snapshot for {name}: "
                f"configured={configured}, snapshot={persisted}"
            )
    persisted_buffer = raw_capital.get("cell_session_risk_buffer_fraction")
    if (persisted_buffer is not None and (
            isinstance(persisted_buffer, bool) or not isinstance(persisted_buffer, (int, float))
            or not math.isfinite(persisted_buffer)
            or float(persisted_buffer) != capital_profile.cell_session_risk_buffer_fraction)):
        raise ValueError("capital config does not match strategy snapshot for cell_session_risk_buffer_fraction")

    raw_config = snapshot.get("config", {})
    if not isinstance(raw_config, Mapping):
        raise ValueError("strategy snapshot config must be an object")
    allowed_keys = {"name", "version", "morning_entry_minutes", "afternoon_entry_minutes",
                    "entry_cooldown_bars", "post_exit_cooldown_bars"}
    for key in raw_config:
        if not isinstance(key, str) or key not in allowed_keys:
            raise ValueError(f"unexpected strategy snapshot config key: {key!r}")
    name = raw_config.get("name", STRATEGY_NAME)
    version = raw_config.get("version", STRATEGY_VERSION)
    if not isinstance(name, str) or not isinstance(version, str):
        raise ValueError("strategy snapshot name and version must be strings")
    morning = _parse_minute_pair("morning_entry_minutes", raw_config.get("morning_entry_minutes", MORNING_ENTRY_MINUTES))
    afternoon = _parse_minute_pair("afternoon_entry_minutes", raw_config.get("afternoon_entry_minutes", AFTERNOON_ENTRY_MINUTES))
    config = StrategyConfig(
        name=name, version=version, morning_entry_minutes=morning,
        afternoon_entry_minutes=afternoon,
        entry_cooldown_bars=_parse_non_negative_int("entry_cooldown_bars", raw_config.get("entry_cooldown_bars", DEFAULT_CONFIG.entry_cooldown_bars)),
        post_exit_cooldown_bars=_parse_non_negative_int("post_exit_cooldown_bars", raw_config.get("post_exit_cooldown_bars", DEFAULT_CONFIG.post_exit_cooldown_bars)),
    )

    raw_limits = snapshot.get("vehicle_risk_limits", {})
    if not isinstance(raw_limits, Mapping):
        raise ValueError("strategy snapshot vehicle_risk_limits must be an object")
    limits = {}
    for key, value in raw_limits.items():
        if not isinstance(value, Mapping):
            raise ValueError(f"vehicle risk limits for {key!r} must be an object")
        try:
            limits[str(key).lower()] = VehicleRiskLimits(**dict(value))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid vehicle risk limits for {key!r}") from exc

    raw_portfolio = snapshot.get("portfolio")
    if raw_portfolio is not None and not isinstance(raw_portfolio, Mapping):
        raise ValueError("strategy snapshot portfolio must be an object")
    portfolio = PortfolioState.from_snapshot(raw_portfolio) if isinstance(raw_portfolio, Mapping) else None
    strategy = cls(config=config, capital_profile=capital_profile, enabled_vehicles=enabled_vehicles,
                   vehicle_risk_limits=limits, portfolio=portfolio)
    risk_snapshot = snapshot.get("risk_gate")
    if isinstance(risk_snapshot, Mapping):
        strategy._decision_engine.restore_risk_snapshot(dict(risk_snapshot))
    raw_positions = snapshot.get("decision_positions", {})
    if not isinstance(raw_positions, Mapping):
        raise ValueError("strategy snapshot decision_positions must be an object")
    if set(strategy.portfolio.positions) != set(raw_positions):
        raise ValueError("strategy snapshot portfolio and decision positions do not match")
    for order_id, raw_position in raw_positions.items():
        if not isinstance(order_id, str) or not isinstance(raw_position, Mapping):
            raise ValueError("strategy snapshot has invalid decision position")
        raw_position_data = raw_position.get("position")
        raw_exit_state = raw_position.get("exit_state")
        if not isinstance(raw_position_data, Mapping) or not isinstance(raw_exit_state, Mapping):
            raise ValueError("strategy snapshot decision position is incomplete")
        position = _position_from_snapshot(raw_position_data)
        if position.entry_order_id != order_id:
            raise ValueError("strategy snapshot position key does not match entry_order_id")
        strategy._decision_positions[order_id] = (position, ExitStateMachine.from_snapshot(raw_exit_state))
    raw_pending = snapshot.get("pending_exits", {})
    if not isinstance(raw_pending, Mapping):
        raise ValueError("strategy snapshot pending_exits must be an object")
    if any(not isinstance(key, str) or not isinstance(value, str) for key, value in raw_pending.items()):
        raise ValueError("strategy snapshot pending_exits must contain strings")
    strategy._pending_exits = dict(raw_pending)
    return strategy


def _instrument_from_snapshot(raw: Mapping[str, object]) -> Instrument:
    required = ("symbol", "exchange", "instrument_type")
    if any(not isinstance(raw.get(name), str) or not raw[name] for name in required):
        raise ValueError("strategy snapshot instrument identity is invalid")
    option_type = raw.get("option_type")
    if option_type is not None and not isinstance(option_type, str):
        raise ValueError("strategy snapshot option_type is invalid")
    expiry = raw.get("expiry")
    strike = raw.get("strike")
    if expiry is not None and not isinstance(expiry, str):
        raise ValueError("strategy snapshot expiry is invalid")
    if (strike is not None and (isinstance(strike, bool) or not isinstance(strike, (int, float))
            or not math.isfinite(strike))):
        raise ValueError("strategy snapshot strike is invalid")
    symbol = raw["symbol"]
    exchange = raw["exchange"]
    instrument_type = raw["instrument_type"]
    assert isinstance(symbol, str) and isinstance(exchange, str) and isinstance(instrument_type, str)
    return Instrument(symbol, exchange, instrument_type, expiry,
                      float(strike) if strike is not None else None,
                      OptionType(option_type) if option_type is not None else None)


def _position_from_snapshot(raw: Mapping[str, object]) -> PositionState:
    instrument_raw = raw.get("instrument")
    if not isinstance(instrument_raw, Mapping):
        raise ValueError("strategy snapshot position instrument must be an object")
    entry_fill_time = raw.get("entry_fill_time")
    if isinstance(entry_fill_time, str):
        entry_fill_time = datetime.fromisoformat(entry_fill_time)
    elif entry_fill_time is not None:
        raise ValueError("strategy snapshot entry_fill_time is invalid")
    legs_raw = raw.get("synthetic_legs")
    legs = None
    if legs_raw is not None:
        if not isinstance(legs_raw, Sequence) or len(legs_raw) != 2 or any(not isinstance(item, Mapping) for item in legs_raw):
            raise ValueError("strategy snapshot synthetic_legs must contain two instruments")
        legs = (_instrument_from_snapshot(legs_raw[0]), _instrument_from_snapshot(legs_raw[1]))
    for name in ("entry_price", "stop_price", "quantity", "side"):
        if name not in raw or raw[name] is None:
            raise ValueError(f"strategy snapshot position missing {name}")
    if not isinstance(raw["side"], str):
        raise ValueError("strategy snapshot position side is invalid")
    vehicle = raw.get("vehicle", "futures")
    if not isinstance(vehicle, str) or vehicle not in {"futures", "synthetic"}:
        raise ValueError("strategy snapshot position vehicle is invalid")
    if vehicle == "synthetic":
        if legs is None or {leg.instrument_type.upper() for leg in legs} != {"CE", "PE"}:
            raise ValueError("synthetic strategy snapshot position must contain CE and PE legs")
        if (legs[0].expiry, legs[0].strike) != (legs[1].expiry, legs[1].strike):
            raise ValueError("synthetic strategy snapshot legs must share expiry and strike")
    elif legs is not None:
        raise ValueError("futures strategy snapshot position cannot contain synthetic legs")
    exit_mode = raw.get("exit_mode", "signal")
    if not isinstance(exit_mode, str):
        raise ValueError("strategy snapshot exit_mode is invalid")
    entry_order_id = raw.get("entry_order_id")
    if entry_order_id is not None and not isinstance(entry_order_id, str):
        raise ValueError("strategy snapshot entry_order_id is invalid")
    cell = raw.get("cell")
    if cell is not None and not isinstance(cell, str):
        raise ValueError("strategy snapshot cell is invalid")
    entry_price = _finite_number("entry_price", raw["entry_price"])
    stop_price = _finite_number("stop_price", raw["stop_price"])
    quantity = _positive_int("quantity", raw["quantity"])
    return PositionState(
        _instrument_from_snapshot(instrument_raw), entry_price, stop_price,
        quantity, OrderSide(raw["side"]), cell=cell,
        exit_mode=exit_mode, entry_fill_time=entry_fill_time,
         exit_reference_price=_optional_number(raw.get("exit_reference_price")),
         target_price=_optional_number(raw.get("target_price")),
        vehicle=vehicle, synthetic_legs=legs,
         entry_bar=_optional_int(raw.get("entry_bar")),
        entry_order_id=entry_order_id,
    )


def _parse_minute_pair(key: str, value: object) -> tuple[int, int]:
    if isinstance(value, str) or not isinstance(value, Sequence) or isinstance(value, (bytes, bytearray)):
        raise ValueError(f"{key} must be a two-integer sequence")
    if len(value) != 2 or any(not isinstance(item, int) or isinstance(item, bool) for item in value):
        raise ValueError(f"{key} must contain exactly two integers")
    return int(value[0]), int(value[1])


def _parse_non_negative_int(key: str, value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{key} must be a non-negative integer")
    return value


def _finite_number(key: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"strategy snapshot {key} is invalid")
    return float(value)


def _positive_int(key: str, value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"strategy snapshot {key} is invalid")
    return value


def _optional_number(value: object) -> float | None:
    return _finite_number("optional numeric field", value) if value is not None else None


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError("strategy snapshot entry_bar is invalid")
    return value
