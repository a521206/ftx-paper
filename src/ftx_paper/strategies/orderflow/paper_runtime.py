"""Paper host owner for one canonical orderflow transition machine."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

from ftx_paper.domain.decision_context import DecisionContext
from ftx_paper.execution.events import ExecutionNotification
from ftx_paper.execution.service import ExecutionService

from .boundary.orderflow import OrderflowStrategy
from .boundary.orderflow_config import OrderflowConfig
from .coordinator import OrderflowClockInput
from .execution import ExecutionCoordinator
from .execution_adapter import OrderflowExecutionAdapter
from .evidence.serialization import evidence_payload
from .evidence_publication import publish_phase4a_evidence
from .opportunity_coordinator import LifecycleFormation
from .state_machine_types import LocationRegistry
from .transition import OrderflowTransitionOwner
from .providers import CanonicalInputProvider, CanonicalFormationProvider
from .state_machine_reporting import build_replay_payload
from .vwap_location import VwapMeasurement


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
        transition_owner: OrderflowTransitionOwner | None = None,
        execution_adapter: OrderflowExecutionAdapter | None = None,
        clock_input_provider: Callable[[object], OrderflowClockInput] | None = None,
        formation_provider: Callable[[OrderflowClockInput], Sequence[LifecycleFormation]] | None = None,
        formation_owner: object | None = None,
    ) -> None:
        self.execution_adapter = execution_adapter or OrderflowExecutionAdapter()
        self.transition_owner = transition_owner or OrderflowTransitionOwner(
            LocationRegistry("NIFTY::1m"),
            execution_owner=ExecutionCoordinator(),
            execution_adapter=self.execution_adapter,
        )
        self.strategy = OrderflowStrategy(
            config=config,
            transition_owner=self.transition_owner,
            execution_adapter=self.execution_adapter,
            clock_input_provider=clock_input_provider,
            formation_provider=formation_provider or CanonicalFormationProvider(owner=formation_owner),
        )

    @classmethod
    def from_rows(
        cls,
        rows: Sequence[Mapping[str, Any]],
        *,
        rows_by_date: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
        formations: Iterable[LifecycleFormation] = (),
        config: OrderflowConfig | None = None,
        execution_adapter: OrderflowExecutionAdapter | None = None,
    ) -> "PaperOrderflowRuntime":
        """Construct an isolated bounded runtime from canonical source rows."""
        input_provider = CanonicalInputProvider.from_rows(rows, rows_by_date=rows_by_date)
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

    def on_bundle(self, bundle: object) -> tuple[object, ...]:
        return self.strategy.on_bundle(bundle)

    def evaluate(self, bundle: object, context: DecisionContext) -> tuple[object, ...]:
        return self.strategy.evaluate(bundle, context)

    def on_execution_event(self, event: ExecutionNotification) -> None:
        self.strategy.on_execution_event(event)

    def bind_execution_service(self, service: ExecutionService, *, diagnostic: bool = False) -> None:
        self.strategy.bind_execution_service(service, diagnostic=diagnostic)

    def bind_replay_bars(
        self, bars: Sequence[Any], *, historical_bars: Sequence[Any] = (),
    ) -> None:
        """Install canonical input with explicit warmup and target bars."""
        self.strategy.bind_input_provider(
            CanonicalInputProvider.from_market_bars((*historical_bars, *bars))
        )

    def bind_live_bars(self, bars: Sequence[Any]) -> None:
        """Seed the shared provider with startup historical bars."""
        self.strategy.bind_input_provider(CanonicalInputProvider.from_market_bars(bars))

    def append_feature_bar(self, bar: Any) -> None:
        """Append one completed live bar to the shared canonical provider."""
        provider = self.strategy._clock_input_provider
        if isinstance(provider, CanonicalInputProvider):
            provider.append_market_bars((bar,))

    def replay(
        self,
        bundles: Iterable[object],
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
    def from_snapshot(cls, snapshot: Mapping[str, Any]) -> "PaperOrderflowRuntime":
        """Construct an isolated runtime restored from a canonical snapshot."""
        if snapshot.get("runtime_schema_version") != 1:
            raise ValueError("unsupported orderflow runtime snapshot")
        strategy_snapshot = dict(snapshot)
        strategy_snapshot.pop("runtime_schema_version", None)
        strategy = OrderflowStrategy.from_snapshot(strategy_snapshot)
        return cls(
            transition_owner=strategy._transition_owner,
            execution_adapter=strategy._execution_adapter,
        )

    def parity_payload(self, rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
        """Project only producer-owned facts emitted by the canonical owner."""
        payload = build_replay_payload(
            self.transition_owner, [], [], [dict(row) for row in rows], capture_mode="replay",
        )
        provider = self.strategy._clock_input_provider
        envelopes = getattr(provider, "envelopes", ())
        if envelopes:
            latest = envelopes[-1].input
            locations = tuple(self.transition_owner.location_registry.records.values())
            publication = publish_phase4a_evidence(
                atr=latest.atr,
                locations=locations,
                vwap_measurements=_published_vwap_measurements(locations),
                prior_value=latest.prior_value,
                contexts=tuple(
                    context
                    for versions in self.transition_owner.context.market_contexts.versions.values()
                    for context in versions
                ),
                unavailable=envelopes[-1].unavailable,
                location_stream_id=self.transition_owner.location_registry.stream_id,
            )
            payload["evidence"] = evidence_payload({
                "session_date": rows[0].get("session_date") if rows else None,
                "source_fingerprint": None,
                "publication": publication,
            })["evidence"]
        return payload


__all__ = ["PaperOrderflowRuntime"]
