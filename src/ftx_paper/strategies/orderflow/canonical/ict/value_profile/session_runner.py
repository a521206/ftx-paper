"""Single-session value profile walk with optional injected prior geometry."""

from __future__ import annotations

from typing import Sequence

from ftx_paper.strategies.orderflow.canonical.ict.bars.model import CompletedBar
from ftx_paper.strategies.orderflow.canonical.ict.features.model import PriceMeasurement
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.builder import run_value_profile
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.config import ValueProfileConfig
from ftx_paper.strategies.orderflow.canonical.ict.value_profile.records import (
    ValueProfileResult,
    ValueProfileSnapshot,
)


def run_current_session_value_profile(
    bars: Sequence[CompletedBar],
    measurements: Sequence[PriceMeasurement],
    *,
    config: ValueProfileConfig,
    injected_prior: ValueProfileSnapshot | None = None,
) -> ValueProfileResult:
    """Build today's scopes with optional resolved prior-session geometry."""
    return run_value_profile(
        bars,
        measurements,
        config=config,
        injected_prior=injected_prior,
    )
