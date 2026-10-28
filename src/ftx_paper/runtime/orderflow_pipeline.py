"""Shared market-input boundary used by live and replay runtimes."""

from __future__ import annotations

from typing import Any

from ftx_paper.contracts import MarketBar
from ftx_paper.market import CompletedBarAggregator, FeatureService
from ftx_paper.strategy.orderflow_producer import CausalOrderflowProducer


def ingest_orderflow_bar(
    bar: MarketBar,
    *,
    producer: CausalOrderflowProducer,
    feature_service: FeatureService,
    aggregator: CompletedBarAggregator,
    expire_at: float | None = None,
):
    """Feed one completed bar through the canonical causal input sequence.

    Live and replay may differ after a bundle is emitted (persistence and
    execution are environment-specific), but source observation, feature, and
    decision-bundle construction must be identical.
    """
    producer.ingest(bar)
    feature_service.ingest(bar)
    expired: tuple[Any, ...] = ()
    if expire_at is not None:
        expired = tuple(aggregator.expire(now=expire_at))
    return expired, aggregator.ingest(bar)


__all__ = ["ingest_orderflow_bar"]
