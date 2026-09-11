from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import time

from ftx_paper.contracts import MarketBar, OrderIntent

from .config import (
    AFTERNOON_ENTRY_MINUTES,
    COOLDOWN_MINUTES,
    DEFAULT_CONFIG,
    MORNING_ENTRY_MINUTES,
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
        capital: float,
        decide: Callable[[MarketBar], tuple[OrderIntent, ...]] | None = None,
        config: StrategyConfig = DEFAULT_CONFIG,
        expiry_dates: frozenset[str] = frozenset(),
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
        self._capital = capital
        self._decision_engine = IndependentLiveDecisionEngine(
            version=self.version, config_hash=self.metadata.config_hash,
            cooldown_minutes=config.cooldown_minutes, capital=capital,
            morning_entry_minutes=config.morning_entry_minutes,
            afternoon_entry_minutes=config.afternoon_entry_minutes,
            expiry_dates=expiry_dates,
        )

    @property
    def metadata(self) -> StrategyMetadata:
        config_hash = self.config.config_hash
        return StrategyMetadata(self.name, self.version, config_hash)

    def snapshot(self) -> Mapping[str, object]:
        return {
            "schema_version": 1,
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
        self._decision_engine = IndependentLiveDecisionEngine(
            version=self.version, config_hash=self.metadata.config_hash,
            cooldown_minutes=self.config.cooldown_minutes, capital=self._capital,
            morning_entry_minutes=self.config.morning_entry_minutes,
            afternoon_entry_minutes=self.config.afternoon_entry_minutes,
            expiry_dates=self._decision_engine.expiry_dates,
        )

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, object], *, capital: float) -> "ConfiguredLiveStrategy":
        schema_version = snapshot.get("schema_version", 0)
        if not isinstance(schema_version, int) or isinstance(schema_version, bool) or schema_version != 1:
            raise ValueError("unsupported strategy snapshot schema")
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
        strategy = cls(config=config, capital=capital)
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
            action = self._exits.evaluate(self._position, timestamp=bar.timestamp, high=bar.high, low=bar.low, close=bar.close, client_order_id=f"exit-{bar.timestamp.isoformat()}")
            if action is not None:
                self._position = None
                return (action.intent,)
        decision = self._policy.evaluate(bar, features)
        if decision is None:
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
        sized = self._risk.size(capital=self._capital, equity=self._capital, peak_equity=self._capital, entry=bar.close, stop=stop, score=score)
        if not sized.approved:
            return ()
        self._position = PositionState(bar.instrument, bar.close, stop, sized.quantity, decision.side, exit_mode="trail")
        return (self._policy.to_order(decision, bar, sized.quantity, f"entry-{bar.timestamp.isoformat()}"),)

    def on_bundle(self, bundle: DecisionBundle):
        if self._decide is not None:
            return ()
        return self._decision_engine.evaluate(bundle)

    def register_entry(self, order: OrderIntent, *, fill_price: float | None = None) -> None:
        if order.stop_price is None or order.cell is None:
            return
        position = PositionState(
            order.instrument, fill_price if fill_price is not None else order.limit_price or 0.0,
            order.stop_price, order.quantity, order.side, cell=order.cell,
            exit_mode=order.exit_mode or "signal",
        )
        trail_distance = 10.0 if order.exit_mode == "trail" else None
        self._decision_positions[order.client_order_id] = (
            position, ExitStateMachine(trail_distance=trail_distance, close_time=time(15, 20)),
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
            action = exits.evaluate(position, timestamp=bar.timestamp, high=bar.high, low=bar.low,
                                    close=bar.close, client_order_id=f"exit-{order_id}-{bar.timestamp.isoformat()}")
            if action is not None:
                actions.append(action)
                self._pending_exits[action.intent.client_order_id] = order_id
        return tuple(actions)

    def settle_exit(self, exit_order_id: str, *, filled: bool) -> None:
        entry_order_id = self._pending_exits.pop(exit_order_id, None)
        if filled and entry_order_id is not None:
            self._decision_positions.pop(entry_order_id, None)

    def record_exit(self, **kwargs: object) -> None:
        """Apply an execution-layer exit settlement to the decision gate."""
        self._decision_engine.record_exit(**kwargs)
