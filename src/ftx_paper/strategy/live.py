from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, time

from ftx_paper.contracts import Instrument, MarketBar, OrderIntent
from ftx_paper.domain.capital import CapitalRuntimeContext, RESEARCH_CAPITAL_PROFILE, ResearchCapitalProfile
from ftx_paper.domain.portfolio import PortfolioState
from ftx_paper.domain.decision_context import DecisionContext
from ftx_paper.execution.events import ExecutionNotification

from .config import (
    DEFAULT_CONFIG,
    TRAIL_ACTIVATE_BP,
    TRAIL_DISTANCE_BP,
    STRATEGY_NAME,
    STRATEGY_VERSION,
    StrategyConfig,
)
from .protocol import StrategyMetadata
from .exits import ExitStateMachine, PositionState
from .risk import VehicleRiskLimits
from ftx_paper.market.bundles import DecisionBundle
from .decision import IndependentLiveDecisionEngine
from .snapshot import restore_strategy, snapshot_strategy


class ConfiguredLiveStrategy:
    """Live strategy shell with explicit feature/decision injection.

    The rule implementation is intentionally supplied as a callable while the
    NiftyZoning behavior is being migrated. This keeps the engine boundary
    stable and prevents research modules from becoming dependencies.
    """

    name = STRATEGY_NAME
    version = STRATEGY_VERSION

    @property
    def vehicle(self) -> str:
        """Legacy single-vehicle view; orders remain explicitly per vehicle."""
        return self.enabled_vehicles[0] if len(self.enabled_vehicles) == 1 else "mixed"

    def __init__(
        self,
        capital_profile: ResearchCapitalProfile = RESEARCH_CAPITAL_PROFILE,
        config: StrategyConfig = DEFAULT_CONFIG,
        expiry_dates: frozenset[str] = frozenset(),
        prior_day_high: float | None = None,
        prior_day_low: float | None = None,
        enabled_vehicles: Sequence[str] = ("futures", "synthetic"),
        vehicle: str | None = None,
        vehicle_risk_limits: Mapping[str, VehicleRiskLimits] | None = None,
        portfolio: PortfolioState | None = None,
        capital_context: CapitalRuntimeContext | None = None,
    ) -> None:
        self.config = config
        self._decision_positions: dict[str, tuple[PositionState, ExitStateMachine]] = {}
        self._pending_exits: dict[str, str] = {}
        self.capital_profile = capital_profile
        self.portfolio = portfolio or PortfolioState(capital_profile.initial_capital)
        self._capital = self.portfolio.initial_capital
        if vehicle is not None:
            enabled_vehicles = (vehicle,)
        self.enabled_vehicles = tuple(dict.fromkeys(str(item).lower() for item in enabled_vehicles))
        if "futures" not in self.enabled_vehicles:
            raise ValueError("synthetic is reporting-only; futures must be enabled")
        self.capital_context = capital_context or CapitalRuntimeContext(self.capital_profile, environment="live")
        if self.capital_context.profile != self.capital_profile:
            raise ValueError("capital_context profile must match capital_profile")
        self.vehicle_risk_limits = {str(k).lower(): v for k, v in (vehicle_risk_limits or {}).items()}
        if not self.enabled_vehicles or any(item not in {"futures", "synthetic"} for item in self.enabled_vehicles):
            raise ValueError("enabled_vehicles must contain 'futures' and/or 'synthetic'")
        self._decision_engine = IndependentLiveDecisionEngine(
            version=self.version, config_hash=self.metadata.config_hash,
            entry_cooldown_bars=config.entry_cooldown_bars,
            post_exit_cooldown_bars=config.post_exit_cooldown_bars,
            capital=self._capital,
            portfolio=self.portfolio,
            max_daily_loss=self.capital_context.profile.max_daily_loss,
            max_net_directional_lots=self.capital_context.max_net_directional_lots,
            risk_per_trade=self.capital_context.risk_per_trade,
            max_lots=self.capital_context.max_lots,
            morning_entry_minutes=config.morning_entry_minutes,
            afternoon_entry_minutes=config.afternoon_entry_minutes,
            prior_day_high=prior_day_high,
            prior_day_low=prior_day_low,
            expiry_dates=expiry_dates,
            enabled_vehicles=self.enabled_vehicles,
            vehicle_risk_limits=self.vehicle_risk_limits,
            capital_context=self.capital_context,
        )

    @property
    def portfolio_state(self) -> dict[str, float]:
        """Return the equity inputs used by the risk/sizing layer."""
        return {
            "initial_capital": float(self.portfolio.initial_capital),
            "current_equity": float(self.portfolio.equity),
            "peak_equity": float(self.portfolio.peak_equity),
        }

    @property
    def metadata(self) -> StrategyMetadata:
        config_hash = self.config.config_hash
        return StrategyMetadata(self.name, self.version, config_hash)

    def snapshot(self) -> Mapping[str, object]:
        return snapshot_strategy(self)

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, object], *, capital_profile: ResearchCapitalProfile) -> "ConfiguredLiveStrategy":
        return restore_strategy(cls, snapshot, capital_profile=capital_profile)

    @staticmethod
    def _instrument_snapshot(instrument: Instrument) -> dict[str, object]:
        return {
            "symbol": instrument.symbol, "exchange": instrument.exchange,
            "instrument_type": instrument.instrument_type, "expiry": instrument.expiry,
            "strike": instrument.strike,
            "option_type": instrument.option_type.value if instrument.option_type is not None else None,
        }

    @staticmethod
    def _position_snapshot(position: PositionState) -> dict[str, object]:
        return {
            "instrument": ConfiguredLiveStrategy._instrument_snapshot(position.instrument),
            "entry_price": position.entry_price, "stop_price": position.stop_price,
            "quantity": position.quantity, "side": position.side.value, "cell": position.cell,
            "exit_mode": position.exit_mode,
            "entry_fill_time": position.entry_fill_time.isoformat() if position.entry_fill_time else None,
            "exit_reference_price": position.exit_reference_price, "target_price": position.target_price,
            "vehicle": position.vehicle, "entry_bar": position.entry_bar,
            "entry_order_id": position.entry_order_id,
            "synthetic_legs": [ConfiguredLiveStrategy._instrument_snapshot(item) for item in position.synthetic_legs]
            if position.synthetic_legs else None,
        }

    def on_bundle(self, bundle: DecisionBundle):
        return self._decision_engine.evaluate(bundle)

    def evaluate(self, bundle: DecisionBundle, context: DecisionContext):
        """Evaluate with a common account snapshot while retaining signal state."""
        return self._decision_engine.evaluate(bundle)

    def on_execution_event(self, event: ExecutionNotification) -> None:
        """Receive common execution outcomes without owning account state."""
        if event.event_type in {"REJECTED", "CANCELLED", "ERROR"}:
            pending = self._pending_exits.pop(event.client_order_id, None)
            if pending is not None:
                self.settle_exit(event.client_order_id, filled=False)

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
            vehicle=order.vehicle,
            synthetic_legs=order.synthetic_legs,
            entry_bar=order.entry_bar,
            entry_order_id=order.client_order_id,
        )
        trail_kwargs = (
            {"trail_activation_bp": TRAIL_ACTIVATE_BP, "trail_distance_bp": TRAIL_DISTANCE_BP}
            if order.exit_mode == "trail" else {}
        )
        self._decision_positions[order.client_order_id] = (
                position,
                ExitStateMachine(
                    **trail_kwargs,
                    close_time=time(15, 10),
                    initial_close=position.exit_reference_price or position.entry_price,
                    counter_move_bars=1 if position.cell == "new_low" else 2,
                ),
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
                                    close=bar.close, open=bar.open, volume=bar.volume,
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
                    exit_bar: int, date: str, vehicle: str = "futures",
                    direction: str | None = None, quantity: int = 0) -> None:
        """Apply an execution-layer exit settlement to the decision gate."""
        self._decision_engine.record_exit(
            cell=cell, reason=reason, entry_bar=entry_bar, exit_bar=exit_bar,
            date=date, vehicle=vehicle, direction=direction, quantity=quantity,
        )

    def cancel_entry(self, *, cell: str, direction: str, quantity: int,
                     date: str, vehicle: str = "futures") -> None:
        self._decision_engine.cancel_entry(cell=cell, direction=direction, quantity=quantity,
                                           date=date, vehicle=vehicle)
