"""Paper host owner for one canonical orderflow transition machine."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any, cast

from ftx_paper.domain.decision_context import DecisionContext
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.execution.service import ExecutionService
from ftx_paper.contracts import MarketBar
from ftx_paper.market.bundles import DecisionBundle

from .boundary.orderflow import OrderflowStrategy
from .boundary.orderflow_config import OrderflowConfig
from .coordinator import OrderflowInput
from .execution import ExecutionCoordinator
from .execution_adapter import OrderflowExecutionAdapter
from .evidence.serialization import evidence_payload
from .evidence_publication import AtrPublication, publish_phase4a_evidence
from .opportunity_coordinator import LifecycleFormation
from .state_machine_types import LocationRegistry
from .transition import OrderflowEngine
from .feature_store import CanonicalInputRow
from .providers import OrderflowInputStream, CanonicalFormationProvider, PriorSessionLevels
from .state_machine_reporting import build_replay_payload
from .vwap_location import VwapMeasurement
from .value_publication import ValuePublication


def _published_vwap_measurements(locations: Iterable[Any]) -> tuple[VwapMeasurement, ...]:
    """Project VWAP measurements from the canonical registered locations."""
    return tuple(
        VwapMeasurement(
            timeframe=location.timeframe,
            session_date=location.source_session,
            source_aggregate_id=str(location.source_aggregate_id),
            observed_at=location.created_at,
            available_at=location.available_at,
            value=float(location.price),
            source_observation_id=str(location.source_observation_id),
            calculation_version=location.source_method or "vwap_typical_price_volume_v1",
        )
        for location in locations
        if location.alias == "VWAP"
        and location.timeframe in {"1m", "5m"}
        and location.source_aggregate_id
        and location.source_observation_id
    )


class PaperOrderflowRuntime:
    """Own one canonical orderflow machine for a Paper runtime instance.

    ``RuntimeSession`` remains responsible for transport, account state,
    lifecycle, persistence, and notifications. This class owns only the
    strategy-specific transition state and its Paper execution adapter.
    """

    def __init__(
        self,
        *,
        config: OrderflowConfig | None = None,
        strategy: OrderflowStrategy | None = None,
        engine: OrderflowEngine | None = None,
        execution_adapter: OrderflowExecutionAdapter | None = None,
        clock_input_provider: Callable[[object], OrderflowInput] | None = None,
        formation_provider: Callable[[OrderflowInput], Sequence[LifecycleFormation]] | None = None,
    ) -> None:
        if strategy is not None and any(
            value is not None
            for value in (config, engine, execution_adapter, clock_input_provider,
                          formation_provider)
        ):
            raise ValueError("strategy cannot be combined with construction overrides")
        self.execution_adapter = (
            strategy.execution_adapter if strategy is not None and strategy.execution_adapter is not None
            else execution_adapter or OrderflowExecutionAdapter()
        )
        if strategy is not None:
            if strategy.execution_adapter is None:
                strategy.bind_execution_adapter(self.execution_adapter)
            self.engine = strategy.engine
            self.strategy = strategy
            return
        self.engine = engine or OrderflowEngine(
            LocationRegistry("NIFTY::1m"),
            execution_owner=ExecutionCoordinator(),
            execution_adapter=self.execution_adapter,
        )
        self.strategy = OrderflowStrategy(
            config=config,
            engine=self.engine,
            execution_adapter=self.execution_adapter,
            clock_input_provider=clock_input_provider,
            formation_provider=formation_provider or CanonicalFormationProvider(),
        )

    @classmethod
    def from_rows(
        cls,
        rows: Sequence[CanonicalInputRow],
        *,
        rows_by_date: Mapping[str, Sequence[CanonicalInputRow]] | None = None,
        formations: Iterable[LifecycleFormation] = (),
        config: OrderflowConfig | None = None,
        execution_adapter: OrderflowExecutionAdapter | None = None,
    ) -> "PaperOrderflowRuntime":
        """Construct an isolated bounded runtime from canonical source rows."""
        input_provider = OrderflowInputStream.from_rows(rows, rows_by_date=rows_by_date)
        formation_provider = CanonicalFormationProvider(formations=formations)
        return cls(
            config=config,
            execution_adapter=execution_adapter,
            clock_input_provider=input_provider,
            formation_provider=formation_provider,
        )

    @property
    def metadata(self):
        return self.strategy.metadata

    def on_bundle(self, bundle: DecisionBundle) -> tuple[object, ...]:
        return self.strategy.on_bundle(bundle)

    def evaluate(self, bundle: DecisionBundle, context: DecisionContext) -> tuple[object, ...]:
        return self.strategy.evaluate(bundle, context)

    def on_execution_event(self, event: ExecutionNotification) -> None:
        self.strategy.on_execution_event(event)

    def bind_execution_service(self, service: ExecutionService, *, diagnostic: bool = False) -> None:
        self.strategy.bind_execution_service(service, diagnostic=diagnostic)

    def bind_replay_bars(
        self,
        bars: Sequence[MarketBar],
        *,
        feature_history_bars: Sequence[MarketBar] = (),
        prior_values: Mapping[str, ValuePublication] | None = None,
        atr_values: Mapping[str, AtrPublication] | None = None,
        prior_session_levels: Mapping[str, PriorSessionLevels] | None = None,
    ) -> None:
        """Install target-session input with validated setup publications."""
        self.strategy.bind_input_provider(
            OrderflowInputStream.from_continuous_bars(
                bars,
                feature_history_bars=feature_history_bars,
                prior_values=prior_values,
                atr_values=atr_values,
                prior_session_levels=prior_session_levels,
            )
        )

    def bind_live_bars(self, bars: Sequence[MarketBar]) -> None:
        """Seed the shared provider with startup historical bars."""
        self.strategy.bind_input_provider(OrderflowInputStream.from_continuous_bars(bars))

    def append_feature_bar(self, bar: MarketBar) -> None:
        """Append one selected completed live bar to the input stream."""
        self.strategy.append_feature_bar(bar)

    def publish_setup(
        self,
        session_date: str,
        *,
        atr: AtrPublication | None = None,
        prior_value: ValuePublication | None = None,
        prior_session_levels: PriorSessionLevels | None = None,
    ) -> None:
        """Publish validated setup artifacts to the same live/replay input stream."""
        self.strategy.publish_setup(
            session_date,
            atr=atr,
            prior_value=prior_value,
            prior_session_levels=prior_session_levels,
        )

    def replay(
        self,
        bundles: Iterable[DecisionBundle],
        *,
        cancelled: Callable[[], bool] | None = None,
    ) -> tuple[tuple[object, ...], ...]:
        """Process a bounded replay through the same strategy boundary as live.

        The caller supplies already loaded, chronologically ordered bundles.
        This method does not touch feeds, stores, brokers, or live callbacks;
        a retry must construct a new ``PaperOrderflowRuntime`` instance.
        """
        results: list[tuple[object, ...]] = []
        for bundle in bundles:
            if cancelled is not None and cancelled():
                break
            results.append(self.on_bundle(bundle))
        return tuple(results)

    def snapshot(self) -> Mapping[str, Any]:
        """Expose canonical facts without reconstructing policy state."""
        snapshot = dict(self.strategy.snapshot())
        snapshot["runtime_schema_version"] = 1
        return snapshot

    @classmethod
    def from_snapshot(
        cls, snapshot: Mapping[str, Any], **_: object,
    ) -> "PaperOrderflowRuntime":
        """Construct an isolated runtime restored from a canonical snapshot."""
        if snapshot.get("runtime_schema_version") != 1:
            raise ValueError("unsupported orderflow runtime snapshot")
        strategy_snapshot = dict(snapshot)
        strategy_snapshot.pop("runtime_schema_version", None)
        return cls(strategy=OrderflowStrategy.from_snapshot(strategy_snapshot))

    def parity_payload(self, rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
        """Project only producer-owned facts emitted by the canonical owner."""
        payload = build_replay_payload(
            self.engine, [], [], [dict(row) for row in rows], capture_mode="replay",
        )
        stream = self.strategy.input_stream
        envelopes = stream.envelopes if stream is not None else ()
        if envelopes:
            latest = envelopes[-1].input
            locations = tuple(self.engine.location_registry.records.values())
            publication = publish_phase4a_evidence(
                atr=cast(AtrPublication, latest.atr),
                locations=locations,
                vwap_measurements=_published_vwap_measurements(locations),
                prior_value=latest.prior_value,
                contexts=tuple(
                    context
                    for versions in self.engine.context.market_contexts.versions.values()
                    for context in versions
                ),
                unavailable=envelopes[-1].unavailable,
                location_stream_id=self.engine.location_registry.stream_id,
            )
            payload["evidence"] = evidence_payload({
                "session_date": rows[0].get("session_date") if rows else None,
                "source_fingerprint": None,
                "publication": publication,
            })["evidence"]
        return payload


__all__ = ["PaperOrderflowRuntime"]
