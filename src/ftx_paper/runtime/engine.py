from __future__ import annotations

from dataclasses import dataclass
from dataclasses import replace
from collections.abc import Mapping
from typing import Any
from typing import cast

from ftx_paper.contracts import MarketBar, OrderIntent
from ftx_paper.market.bundles import DecisionBundle
from ftx_paper.strategy.protocol import Strategy, StrategyMetadata
from ftx_paper.strategy.decision import DecisionEvent
from ftx_paper.domain.decision_context import DecisionContext
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.strategy.exits import ExitAction


@dataclass(frozen=True, slots=True)
class EngineResult:
    orders: tuple[OrderIntent, ...] = ()
    events: tuple[dict[str, object], ...] = ()


class StrategyDispatchError(RuntimeError):
    """A strategy failed while evaluating a shared market bundle."""

    def __init__(self, strategy_id: str, cause: Exception) -> None:
        self.strategy_id = strategy_id
        self.cause = cause
        super().__init__(f"strategy {strategy_id!r} failed: {cause}")


@dataclass(frozen=True, slots=True)
class StrategyInstance:
    """Runtime identity and lifecycle state for one configured strategy."""

    strategy_id: str
    strategy: Strategy
    account_id: str | None = None
    enabled: bool = True


class PaperEngine:
    """Runtime coordinator around a replaceable, broker-neutral strategy."""

    def __init__(self, strategy: Strategy | None = None, *,
                 emit_rejected_decisions: bool = False) -> None:
        self.bars_seen = 0
        self.strategy = strategy
        self.emit_rejected_decisions = emit_rejected_decisions

    @property
    def strategy_metadata(self) -> StrategyMetadata | None:
        return self.strategy.metadata if self.strategy is not None else None

    def reset(self) -> None:
        """Reset deterministic strategy state before a fresh replay."""
        self.bars_seen = 0
        reset = getattr(self.strategy, "reset", None)
        if callable(reset):
            reset()

    def on_bundle(self, bundle: DecisionBundle, context: DecisionContext | None = None) -> EngineResult:
        """Evaluate one synchronized minute, then hand orders to execution."""
        self.bars_seen += len(bundle.bars)
        if self.strategy is None or not hasattr(self.strategy, "on_bundle"):
            return EngineResult(events=({"bundle_id": bundle.bundle_id, "minute": bundle.minute,
                                         "bundle_complete": bundle.complete},))
        evaluate = getattr(self.strategy, "evaluate", None)
        decisions = cast(
            tuple[DecisionEvent, ...],
            evaluate(bundle, context) if callable(evaluate) and context is not None
            else self.strategy.on_bundle(bundle),
        )
        orders = tuple(order for item in decisions if (order := item.order) is not None)
        events = tuple(
            {"event_type": item.event_type, **dict(item.payload)}
            for item in decisions
            if self.emit_rejected_decisions
            or item.event_type not in {"REJECTEDDECISION", "SIZING_REJECTED"}
        )
        return EngineResult(orders=orders, events=events)

    def on_execution_event(self, event: ExecutionNotification) -> None:
        handler = getattr(self.strategy, "on_execution_event", None)
        if callable(handler):
            handler(event)

    def record_exit(self, **kwargs: object) -> None:
        """Forward a settled exit to strategies that maintain risk-gate state."""
        record_exit = getattr(self.strategy, "record_exit", None)
        if callable(record_exit):
            record_exit(**kwargs)

    def cancel_entry(self, **kwargs: object) -> None:
        handler = getattr(self.strategy, "cancel_entry", None)
        if callable(handler):
            handler(**kwargs)

    def on_tick(self, bar: MarketBar) -> tuple[ExitAction, ...]:
        """Forward a live tick to the strategy's protective exit state."""
        on_tick = getattr(self.strategy, "on_tick", None)
        result = on_tick(bar) if callable(on_tick) else ()
        return cast(tuple[ExitAction, ...], result) if result else ()

    def register_entry(self, order: OrderIntent, *, fill_price: float | None = None,
                       entry_fill_time=None, reference_price: float | None = None) -> None:
        register = getattr(self.strategy, "register_entry", None)
        if callable(register):
            try:
                register(order, fill_price=fill_price, entry_fill_time=entry_fill_time,
                         reference_price=reference_price)
            except TypeError as exc:
                if "entry_fill_time" not in str(exc) and "reference_price" not in str(exc):
                    raise
                try:
                    register(order, fill_price=fill_price, entry_fill_time=entry_fill_time)
                except TypeError as fallback_exc:
                    if "entry_fill_time" not in str(fallback_exc):
                        raise
                    register(order, fill_price=fill_price)

    def on_closed_bar(self, bar: MarketBar) -> tuple[ExitAction, ...]:
        handler = getattr(self.strategy, "on_closed_bar", None)
        result = handler(bar) if callable(handler) else ()
        return cast(tuple[ExitAction, ...], result) if result else ()

    def settle_exit(self, exit_order_id: str, *, filled: bool, strategy_id: str | None = None) -> None:
        handler = getattr(self.strategy, "settle_exit", None)
        if callable(handler):
            handler(exit_order_id, filled=filled)


class MultiStrategyEngine:
    """Dispatch market and execution events to independent strategy instances.

    The coordinator deliberately shares no strategy state.  Account ownership,
    authorization, execution, and persistence remain in ``RuntimeSession``;
    this class only coordinates strategy policy and adds attribution.
    """

    def __init__(self, strategies: Mapping[str, StrategyInstance | Strategy], *,
                 emit_rejected_decisions: bool = False) -> None:
        if not strategies:
            raise ValueError("at least one strategy is required")
        instances: list[StrategyInstance] = []
        for strategy_id, value in strategies.items():
            instance = value if isinstance(value, StrategyInstance) else StrategyInstance(strategy_id, value)
            if instance.strategy_id != strategy_id:
                raise ValueError("strategy mapping key must match strategy_id")
            if not instance.strategy_id.strip():
                raise ValueError("strategy_id must not be empty")
            instances.append(instance)
        self.instances = tuple(instances)
        self._engines = {
            item.strategy_id: PaperEngine(item.strategy, emit_rejected_decisions=emit_rejected_decisions)
            for item in self.instances
        }

    @property
    def strategy(self) -> Strategy | None:
        """Compatibility view for callers that only support one strategy."""
        return self.instances[0].strategy if len(self.instances) == 1 else None

    @property
    def strategy_metadata(self) -> StrategyMetadata | None:
        return self.instances[0].strategy.metadata if len(self.instances) == 1 else None

    @property
    def bars_seen(self) -> int:
        """Compatibility view for the single-strategy runtime metrics."""
        return self._engines[self.instances[0].strategy_id].bars_seen

    def strategy_status(self) -> tuple[dict[str, object], ...]:
        return tuple({
            "strategy_id": item.strategy_id,
            "name": item.strategy.metadata.name,
            "version": item.strategy.metadata.version,
            "enabled": item.enabled,
            "state": "enabled" if item.enabled else "disabled",
        } for item in self.instances)

    def set_account_context(self, portfolio: Any, capital_context: Any) -> None:
        """Bind all instances to the runtime-owned account context."""
        for item in self.instances:
            for name, value in (("portfolio", portfolio), ("capital_context", capital_context)):
                if hasattr(item.strategy, name):
                    setattr(item.strategy, name, value)

    def reset(self) -> None:
        for engine in self._engines.values():
            engine.reset()

    def snapshot(self) -> Mapping[str, object]:
        """Return independently keyed snapshots for runtime persistence."""
        snapshots: dict[str, object] = {}
        for item in self.instances:
            snapshot = getattr(item.strategy, "snapshot", None)
            if callable(snapshot):
                snapshots[item.strategy_id] = {
                    "account_id": item.account_id,
                    "enabled": item.enabled,
                    "strategy": dict(cast(Mapping[str, object], snapshot())),
                }
        return {
            "schema_version": 1,
            "instances": snapshots,
        }

    def restore_snapshots(self, raw: Mapping[str, object], *, capital_profile: Any) -> None:
        """Restore compatible instance state without creating another runtime."""
        schema_version = raw.get("schema_version")
        if schema_version is not None and schema_version != 1:
            raise ValueError(f"unsupported strategy snapshot schema version: {schema_version!r}")
        instances = raw.get("instances")
        if not isinstance(instances, Mapping):
            if len(self.instances) != 1:
                raise ValueError("multi-strategy snapshot instances must be an object")
            instances = {self.instances[0].strategy_id: {"strategy": raw}}
        restored: list[StrategyInstance] = []
        for item in self.instances:
            payload = instances.get(item.strategy_id)
            if not isinstance(payload, Mapping):
                restored.append(item)
                continue
            snapshot = payload.get("strategy")
            restore = getattr(type(item.strategy), "from_snapshot", None)
            if callable(restore) and isinstance(snapshot, Mapping):
                strategy = cast(Strategy, restore(snapshot, capital_profile=capital_profile))
            else:
                strategy = item.strategy
            restored.append(StrategyInstance(
                item.strategy_id, strategy,
                account_id=item.account_id,
                enabled=item.enabled,
            ))
        self.instances = tuple(restored)
        self._engines = {item.strategy_id: PaperEngine(item.strategy) for item in self.instances}

    def on_bundle(self, bundle: DecisionBundle, context: DecisionContext | None = None) -> EngineResult:
        orders: list[OrderIntent] = []
        events: list[dict[str, object]] = []
        for item in self.instances:
            if not item.enabled:
                continue
            engine = self._engines[item.strategy_id]
            try:
                result = engine.on_bundle(bundle, context=context)
            except Exception as exc:
                raise StrategyDispatchError(item.strategy_id, exc) from exc
            for event in result.events:
                events.append({**event, "strategy_id": item.strategy_id})
            for order in result.orders:
                client_order_id = order.client_order_id
                prefix = f"{item.strategy_id}:"
                if not client_order_id.startswith(prefix):
                    client_order_id = prefix + client_order_id
                orders.append(replace(
                    order, client_order_id=client_order_id,
                    strategy_id=order.strategy_id or item.strategy_id,
                    account_id=order.account_id or item.account_id,
                ))
        return EngineResult(tuple(orders), tuple(events))

    def on_execution_event(self, event: ExecutionNotification) -> None:
        targets = (self._engines[event.strategy_id],) if event.strategy_id in self._engines else self._engines.values()
        for engine in targets:
            engine.on_execution_event(event)

    def on_tick(self, bar: MarketBar) -> tuple[ExitAction, ...]:
        actions: list[ExitAction] = []
        for item in self.instances:
            if item.enabled:
                try:
                    result = self._engines[item.strategy_id].on_tick(bar)
                except Exception as exc:
                    raise StrategyDispatchError(item.strategy_id, exc) from exc
                actions.extend(self._attributed_exits(item.strategy_id, result))
        return tuple(actions)

    def on_closed_bar(self, bar: MarketBar) -> tuple[ExitAction, ...]:
        actions: list[ExitAction] = []
        for item in self.instances:
            if item.enabled:
                try:
                    result = self._engines[item.strategy_id].on_closed_bar(bar)
                except Exception as exc:
                    raise StrategyDispatchError(item.strategy_id, exc) from exc
                actions.extend(self._attributed_exits(item.strategy_id, result))
        return tuple(actions)

    @staticmethod
    def _attributed_exits(strategy_id: str, actions: tuple[ExitAction, ...]) -> tuple[ExitAction, ...]:
        return tuple(
            replace(action, intent=replace(
                action.intent,
                strategy_id=action.intent.strategy_id or strategy_id,
            ))
            for action in actions
        )

    def register_entry(self, order: OrderIntent, *, fill_price: float | None = None,
                       entry_fill_time: Any = None, reference_price: float | None = None) -> None:
        engine = self._engines.get(order.strategy_id or "")
        if engine is not None:
            engine.register_entry(order, fill_price=fill_price, entry_fill_time=entry_fill_time,
                                  reference_price=reference_price)

    def record_exit(self, *, strategy_id: str | None = None, **kwargs: object) -> None:
        engine = self._engines.get(strategy_id or "")
        if engine is not None:
            engine.record_exit(**kwargs)

    def cancel_entry(self, *, strategy_id: str | None = None, **kwargs: object) -> None:
        engine = self._engines.get(strategy_id or "")
        if engine is not None:
            engine.cancel_entry(**kwargs)

    def settle_exit(self, exit_order_id: str, *, filled: bool, strategy_id: str | None = None) -> None:
        engine = self._engines.get(strategy_id or "")
        if engine is not None:
            engine.settle_exit(exit_order_id, filled=filled)
