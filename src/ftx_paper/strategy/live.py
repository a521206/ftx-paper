from __future__ import annotations

from collections.abc import Callable, Mapping

from ftx_paper.contracts import MarketBar, OrderIntent

from .config import DEFAULT_CONFIG, STRATEGY_NAME, STRATEGY_VERSION, StrategyConfig
from ftx_paper.core.strategy import StrategyMetadata
from ftx_paper.core import ExitStateMachine, LiveFeatureCalculator, PositionState, RiskSizer, SetupPolicy
from ftx_paper.contracts import OrderSide
from ftx_paper.core import DecisionBundle, IndependentLiveDecisionEngine


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
        decide: Callable[[MarketBar], tuple[OrderIntent, ...]] | None = None,
        config: StrategyConfig = DEFAULT_CONFIG,
        capital: float = 100_000.0,
    ) -> None:
        self._decide = decide
        self.config = config
        self._bars: list[MarketBar] = []
        self._position: PositionState | None = None
        self._features = LiveFeatureCalculator()
        self._policy = SetupPolicy()
        self._risk = RiskSizer()
        self._exits = ExitStateMachine(trail_distance=10.0)
        self._capital = capital
        self._decision_engine = IndependentLiveDecisionEngine(
            version=self.version, config_hash=self.metadata.config_hash,
            cooldown_minutes=config.cooldown_minutes, capital=capital,
        )

    @property
    def metadata(self) -> StrategyMetadata:
        config_hash = self.config.config_hash
        return StrategyMetadata(self.name, self.version, config_hash)

    def snapshot(self) -> Mapping[str, object]:
        return {"schema_version": 1, "config": self.config.as_dict()}

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, object]) -> "ConfiguredLiveStrategy":
        if int(snapshot.get("schema_version", 0)) != 1:
            raise ValueError("unsupported strategy snapshot schema")
        raw_config = snapshot.get("config", {})
        if not isinstance(raw_config, Mapping):
            raise ValueError("strategy snapshot config must be an object")
        config_values = dict(raw_config)
        for key in ("morning_entry_minutes", "afternoon_entry_minutes"):
            if key in config_values:
                config_values[key] = tuple(config_values[key])
        return cls(config=StrategyConfig(**config_values))

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
        sized = self._risk.size(capital=self._capital, equity=self._capital, peak_equity=self._capital, entry=bar.close, stop=stop)
        if not sized.approved:
            return ()
        self._position = PositionState(bar.instrument, bar.close, stop, sized.quantity, decision.side)
        return (self._policy.to_order(decision, bar, sized.quantity, f"entry-{bar.timestamp.isoformat()}"),)

    def on_bundle(self, bundle: DecisionBundle):
        if self._decide is not None:
            return ()
        return self._decision_engine.evaluate(bundle)
