from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, time

from ftx_paper.contracts import MarketBar, OrderIntent
from ftx_paper.capital_config import FtxCapitalConfig

from .config import (
    AFTERNOON_ENTRY_MINUTES,
    COOLDOWN_MINUTES,
    DEFAULT_CONFIG,
    MORNING_ENTRY_MINUTES,
    TRAIL_ACTIVATE_BP,
    TRAIL_DISTANCE_BP,
    STRATEGY_NAME,
    STRATEGY_VERSION,
    StrategyConfig,
)
from ftx_paper.core.strategy import StrategyMetadata
from ftx_paper.core import ExitStateMachine, LiveFeatureCalculator, PositionState, RiskSizer, SetupPolicy
from ftx_paper.contracts import OrderSide
from ftx_paper.core import DecisionBundle, IndependentLiveDecisionEngine
from ftx_paper.core.scoring import calculate_setup_score, compute_selling_structure


class ConfiguredLiveStrategy:
    """Live strategy shell with explicit feature/decision injection.

    The rule implementation is intentionally supplied as a callable while the
    NiftyZoning behavior is being migrated. This keeps the engine boundary
    stable and prevents research modules from becoming dependencies.
    """

    name = STRATEGY_NAME
    version = STRATEGY_VERSION

    def __init__(
        self,
        capital_config: FtxCapitalConfig,
        decide: Callable[[MarketBar], tuple[OrderIntent, ...]] | None = None,
        config: StrategyConfig = DEFAULT_CONFIG,
        expiry_dates: frozenset[str] = frozenset(),
        vehicle: str = "futures",
    ) -> None:
        self._decide = decide
        self.config = config
        self._bars: list[MarketBar] = []
        self._position: PositionState | None = None
        self._features = LiveFeatureCalculator()
        self._policy = SetupPolicy()
        self._risk = RiskSizer()
        self._exits = ExitStateMachine(trail_distance=10.0)
        self._decision_positions: dict[str, tuple[PositionState, ExitStateMachine]] = {}
        self._pending_exits: dict[str, str] = {}
        self.capital_config = capital_config
        self._capital = capital_config.initial_capital
        self._daily_date: str | None = None
        self._daily_start_equity = self._capital
        self.vehicle = str(vehicle).lower()
        if self.vehicle not in {"futures", "synthetic"}:
            raise ValueError("vehicle must be 'futures' or 'synthetic'")
        self._equity = self._capital
        self._peak_equity = self._capital
        self._daily_date = None
        self._daily_start_equity = self._capital
        self._decision_engine = IndependentLiveDecisionEngine(
            version=self.version, config_hash=self.metadata.config_hash,
            cooldown_minutes=config.cooldown_minutes, capital=self._capital,
            max_daily_loss=capital_config.max_daily_loss,
            max_net_directional_lots=capital_config.max_net_directional_lots,
            morning_entry_minutes=config.morning_entry_minutes,
            afternoon_entry_minutes=config.afternoon_entry_minutes,
            expiry_dates=expiry_dates,
            vehicle=self.vehicle,
        )

    @property
    def portfolio_state(self) -> dict[str, float]:
        """Return the equity inputs used by the risk/sizing layer."""
        return {
            "initial_capital": float(self._capital),
            "current_equity": float(self._equity),
            "peak_equity": float(self._peak_equity),
        }

    def update_portfolio_state(self, *, equity: float, peak_equity: float | None = None) -> None:
        """Update replay/live sizing inputs after a settled portfolio event."""
        self._equity = float(equity)
        self._peak_equity = max(float(peak_equity if peak_equity is not None else self._peak_equity), self._equity)
        self._decision_engine.update_portfolio_state(equity=self._equity, peak_equity=self._peak_equity)

    @property
    def metadata(self) -> StrategyMetadata:
        config_hash = self.config.config_hash
        return StrategyMetadata(self.name, self.version, config_hash)

    def snapshot(self) -> Mapping[str, object]:
        return {
            "schema_version": 1,
            "vehicle": self.vehicle,
            "capital": {
                "initial_capital": self.capital_config.initial_capital,
                "max_daily_loss": self.capital_config.max_daily_loss,
                "max_net_directional_lots": self.capital_config.max_net_directional_lots,
            },
            "config": self.config.as_dict(),
            "risk_gate": self._decision_engine.risk_snapshot(),
        }

    def reset(self) -> None:
        """Clear all bar, position, feature, and decision-gate state."""
        self._bars.clear()
        self._position = None
        self._features = LiveFeatureCalculator()
        self._exits = ExitStateMachine(trail_distance=10.0)
        self._decision_positions.clear()
        self._pending_exits.clear()
        self._equity = self._capital
        self._peak_equity = self._capital
        self._decision_engine = IndependentLiveDecisionEngine(
            version=self.version, config_hash=self.metadata.config_hash,
            cooldown_minutes=self.config.cooldown_minutes, capital=self._capital,
            max_daily_loss=self.capital_config.max_daily_loss,
            max_net_directional_lots=self.capital_config.max_net_directional_lots,
            morning_entry_minutes=self.config.morning_entry_minutes,
            afternoon_entry_minutes=self.config.afternoon_entry_minutes,
            expiry_dates=self._decision_engine.expiry_dates,
            vehicle=self.vehicle,
        )

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, object], *, capital_config: FtxCapitalConfig) -> "ConfiguredLiveStrategy":
        schema_version = snapshot.get("schema_version", 0)
        if not isinstance(schema_version, int) or isinstance(schema_version, bool) or schema_version != 1:
            raise ValueError("unsupported strategy snapshot schema")
        vehicle = snapshot.get("vehicle", "futures")
        if not isinstance(vehicle, str) or vehicle.lower() not in {"futures", "synthetic"}:
            raise ValueError("snapshot vehicle must be 'futures' or 'synthetic'")
        raw_capital = snapshot.get("capital")
        if not isinstance(raw_capital, Mapping):
            raise ValueError("strategy snapshot capital must be an object")
        for name, configured in (
            ("initial_capital", capital_config.initial_capital),
            ("max_daily_loss", capital_config.max_daily_loss),
            ("max_net_directional_lots", capital_config.max_net_directional_lots),
        ):
            persisted = raw_capital.get(name)
            if isinstance(persisted, bool) or not isinstance(persisted, (int, float)) or float(persisted) != configured:
                raise ValueError(
                    f"capital config does not match strategy snapshot for {name}: "
                    f"configured={configured}, snapshot={persisted}"
                )
        raw_config = snapshot.get("config", {})
        if not isinstance(raw_config, Mapping):
            raise ValueError("strategy snapshot config must be an object")
        allowed_keys = {"name", "version", "morning_entry_minutes", "afternoon_entry_minutes", "cooldown_minutes"}
        for key in raw_config:
            if not isinstance(key, str) or key not in allowed_keys:
                raise ValueError(f"unexpected strategy snapshot config key: {key!r}")

        name = raw_config.get("name", STRATEGY_NAME)
        if not isinstance(name, str):
            raise ValueError("name must be a string")
        version = raw_config.get("version", STRATEGY_VERSION)
        if not isinstance(version, str):
            raise ValueError("version must be a string")
        morning_entry_minutes = MORNING_ENTRY_MINUTES
        if "morning_entry_minutes" in raw_config:
            morning_entry_minutes = cls._parse_minute_pair("morning_entry_minutes", raw_config["morning_entry_minutes"])
        afternoon_entry_minutes = AFTERNOON_ENTRY_MINUTES
        if "afternoon_entry_minutes" in raw_config:
            afternoon_entry_minutes = cls._parse_minute_pair("afternoon_entry_minutes", raw_config["afternoon_entry_minutes"])
        cooldown_minutes = COOLDOWN_MINUTES
        if "cooldown_minutes" in raw_config:
            cooldown = raw_config["cooldown_minutes"]
            if not isinstance(cooldown, int) or isinstance(cooldown, bool) or cooldown < 0:
                raise ValueError("cooldown_minutes must be a non-negative integer")
            cooldown_minutes = cooldown
        config = StrategyConfig(
            name=name,
            version=version,
            morning_entry_minutes=morning_entry_minutes,
            afternoon_entry_minutes=afternoon_entry_minutes,
            cooldown_minutes=cooldown_minutes,
        )
        strategy = cls(config=config, capital_config=capital_config, vehicle=vehicle.lower())
        risk_snapshot = snapshot.get("risk_gate")
        if isinstance(risk_snapshot, Mapping):
            strategy._decision_engine.restore_risk_snapshot(dict(risk_snapshot))
        return strategy

    @staticmethod
    def _parse_minute_pair(key: str, value: object) -> tuple[int, int]:
        if isinstance(value, str):
            raise ValueError(f"{key} must be a two-integer sequence, not a string")
        if not isinstance(value, Sequence) or isinstance(value, (bytes, bytearray)):
            raise ValueError(f"{key} must be a two-integer sequence")
        if len(value) != 2 or any(not isinstance(item, int) or isinstance(item, bool) for item in value):
            raise ValueError(f"{key} must contain exactly two integers")
        return (int(value[0]), int(value[1]))

    def on_bar(self, bar: MarketBar) -> tuple[OrderIntent, ...]:
        if self._decide is not None:
            return self._decide(bar)
        self._bars.append(bar)
        features = self._features.calculate(tuple(self._bars))
        if self._position is not None:
            action = self._exits.evaluate(self._position, timestamp=bar.timestamp, high=bar.high,
                                          low=bar.low, close=bar.close, open=bar.open,
                                          client_order_id=f"exit-{bar.timestamp.isoformat()}")
            if action is not None:
                self._position = None
                return (action.intent,)
        decision = self._policy.evaluate(bar, features)
        if decision is None:
            return ()
        if features.vwap is None:
            return ()
        trading_date = bar.timestamp.date().isoformat()
        if trading_date != self._daily_date:
            self._daily_date = trading_date
            self._daily_start_equity = self._equity
        if self._equity < self._daily_start_equity * (1 - self.capital_config.max_daily_loss):
            return ()
        stop = bar.close - features.atr if decision.side is OrderSide.BUY and features.atr else bar.close + features.atr if features.atr else bar.close - 5 if decision.side is OrderSide.BUY else bar.close + 5
        prior = tuple(self._bars[:-1])
        selling = compute_selling_structure(
            prior, bar,
            vix_open=float(bar.close), vix_at_event=float(bar.close),
        )
        score, _ = calculate_setup_score(
            selling,
            {"minutes_from_open": float(bar.timestamp.hour * 60 + bar.timestamp.minute - (9 * 60 + 15))},
            float(bar.close), float(bar.close), None,
            abs(bar.close - features.vwap) <= self._policy.proximity,
        )
        sized = self._risk.size(capital=self._capital, equity=self._equity, peak_equity=self._peak_equity, entry=bar.close, stop=stop, score=score)
        if not sized.approved:
            return ()
        quantity = min(sized.quantity, int(self.capital_config.max_net_directional_lots))
        if quantity < 1:
            return ()
        self._exits.reset()
        self._position = PositionState(bar.instrument, bar.close, stop, quantity, decision.side,
                                       exit_mode="trail", entry_fill_time=bar.timestamp)
        return (self._policy.to_order(decision, bar, quantity, f"entry-{bar.timestamp.isoformat()}"),)

    def on_bundle(self, bundle: DecisionBundle):
        if self._decide is not None:
            return ()
        return self._decision_engine.evaluate(bundle)

    def register_entry(self, order: OrderIntent, *, fill_price: float | None = None,
                       entry_fill_time: datetime | str | None = None,
                       reference_price: float | None = None) -> None:
        if order.stop_price is None or order.cell is None:
            return
        if isinstance(entry_fill_time, str):
            entry_fill_time = datetime.fromisoformat(entry_fill_time)
        position = PositionState(
            order.instrument, fill_price if fill_price is not None else order.limit_price or 0.0,
            order.stop_price, order.quantity, order.side, cell=order.cell,
            exit_mode=order.exit_mode or "signal",
            entry_fill_time=entry_fill_time,
            exit_reference_price=reference_price,
            target_price=getattr(order, "target_price", None),
        )
        trail_kwargs = (
            {"trail_activation_bp": TRAIL_ACTIVATE_BP, "trail_distance_bp": TRAIL_DISTANCE_BP}
            if order.exit_mode == "trail" else {}
        )
        self._decision_positions[order.client_order_id] = (
            position, ExitStateMachine(**trail_kwargs, close_time=time(15, 30)),
        )

    def on_tick(self, bar: MarketBar):
        actions = []
        for order_id, (position, exits) in tuple(self._decision_positions.items()):
            if order_id in self._pending_exits.values():
                continue
            if bar.instrument != position.instrument:
                continue
            action = exits.evaluate_tick(position, timestamp=bar.timestamp, price=bar.close, client_order_id=f"exit-{order_id}-{bar.timestamp.isoformat()}")
            if action is not None:
                actions.append(action)
                self._pending_exits[action.intent.client_order_id] = order_id
        return tuple(actions)

    def on_closed_bar(self, bar: MarketBar):
        """Evaluate signal exits once per completed futures bar."""
        actions = []
        for order_id, (position, exits) in tuple(self._decision_positions.items()):
            if bar.instrument != position.instrument or order_id in self._pending_exits.values():
                continue
            if position.entry_fill_time is not None and bar.timestamp < position.entry_fill_time:
                continue
            action = exits.evaluate(position, timestamp=bar.timestamp, high=bar.high, low=bar.low,
                                    close=bar.close, open=bar.open,
                                    client_order_id=f"exit-{order_id}-{bar.timestamp.isoformat()}")
            if action is not None:
                actions.append(action)
                self._pending_exits[action.intent.client_order_id] = order_id
        return tuple(actions)

    def settle_exit(self, exit_order_id: str, *, filled: bool) -> None:
        entry_order_id = self._pending_exits.pop(exit_order_id, None)
        if filled and entry_order_id is not None:
            self._decision_positions.pop(entry_order_id, None)

    def record_exit(self, *, cell: str, reason: str, entry_bar: int,
                    exit_bar: int, date: str) -> None:
        """Apply an execution-layer exit settlement to the decision gate."""
        self._decision_engine.record_exit(
            cell=cell, reason=reason, entry_bar=entry_bar, exit_bar=exit_bar,
            date=date,
        )
