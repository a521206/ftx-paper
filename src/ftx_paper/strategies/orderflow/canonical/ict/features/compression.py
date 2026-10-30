"""Causal compression measurements inside established balance episodes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from ftx_paper.strategies.orderflow.canonical.ict.bars.model import CompletedBar
from ftx_paper.strategies.orderflow.canonical.ict.domain import CompressionObservation, EpisodeLifecycle, EvidenceStatus


@dataclass(frozen=True, slots=True)
class CompressionConfig:
    """Lookbacks for separate spatial, volatility, and containment measures."""

    active_band_lookback: int = 3
    volatility_recent_lookback: int = 3
    volatility_baseline_lookback: int = 3

    def __post_init__(self) -> None:
        for name, value in (
            ("active_band_lookback", self.active_band_lookback),
            ("volatility_recent_lookback", self.volatility_recent_lookback),
            ("volatility_baseline_lookback", self.volatility_baseline_lookback),
        ):
            if value < 1:
                raise ValueError(f"{name} must be positive")


def _unavailable(bar: CompletedBar, episode_id: str, reason: str) -> CompressionObservation:
    return CompressionObservation.unavailable(
        episode_id=episode_id,
        observed_at=bar.timestamp,
        available_at=bar.available_at,
        reason=reason,
    )


def build_compression_observations(
    bars: Sequence[CompletedBar],
    detections: Sequence[Any],
    config: CompressionConfig = CompressionConfig(),
) -> tuple[CompressionObservation | None, ...]:
    """Return one causal observation per bar, or ``None`` before balance exists."""
    if len(bars) != len(detections):
        raise ValueError("bars and balance detections must be aligned")
    observations: list[CompressionObservation | None] = []
    for index, (bar, detection) in enumerate(zip(bars, detections)):
        episode = detection.episode
        if episode is None or episode.lifecycle is not EpisodeLifecycle.ACTIVE:
            observations.append(None)
            continue
        episode_start = next(
            position
            for position, prior in enumerate(detections[: index + 1])
            if prior.episode is not None and prior.episode.episode_id == episode.episode_id
        )
        episode_bars = bars[episode_start : index + 1]
        contained = bar.high <= episode.upper_boundary and bar.low >= episode.lower_boundary
        containment_duration = 0
        for prior_bar in reversed(episode_bars):
            if prior_bar.high > episode.upper_boundary or prior_bar.low < episode.lower_boundary:
                break
            containment_duration += 1
        if len(episode_bars) < config.active_band_lookback:
            observations.append(_unavailable(bar, episode.episode_id, "insufficient active-band history"))
            continue
        active = episode_bars[-config.active_band_lookback :]
        outer_width = episode.upper_boundary - episode.lower_boundary
        active_width = max(item.high for item in active) - min(item.low for item in active)
        recent_count = config.volatility_recent_lookback
        baseline_count = config.volatility_baseline_lookback
        if len(episode_bars) < recent_count + baseline_count:
            observations.append(_unavailable(bar, episode.episode_id, "insufficient volatility history"))
            continue
        recent = episode_bars[-recent_count:]
        baseline = episode_bars[-recent_count - baseline_count : -recent_count]
        recent_volatility = sum(item.high - item.low for item in recent) / recent_count
        prior_volatility = sum(item.high - item.low for item in baseline) / baseline_count
        if prior_volatility == 0 or outer_width == 0:
            observations.append(_unavailable(bar, episode.episode_id, "zero compression denominator"))
            continue
        active_midpoint = (max(item.high for item in active) + min(item.low for item in active)) / 2
        observations.append(
            CompressionObservation(
                episode_id=episode.episode_id,
                observed_at=bar.timestamp,
                available_at=bar.available_at,
                active_band_width=active_width,
                active_band_fraction=active_width / outer_width,
                outer_range_width=outer_width,
                volatility=recent_volatility,
                prior_volatility=prior_volatility,
                volatility_ratio=recent_volatility / prior_volatility,
                position_in_range=(active_midpoint - episode.lower_boundary) / outer_width,
                containment_duration=containment_duration,
                contained=contained,
                status=EvidenceStatus.PASSED,
                reason="measurements available; no compression classification",
            )
        )
    return tuple(observations)


COMPRESSION_FEATURES = {
    "ict.active_band_fraction": lambda observations: tuple(
        None if item is None else item.active_band_fraction for item in observations
    ),
    "ict.volatility_ratio": lambda observations: tuple(
        None if item is None else item.volatility_ratio for item in observations
    ),
    "ict.containment_duration": lambda observations: tuple(
        None
        if item is None or item.containment_duration is None
        else float(item.containment_duration)
        for item in observations
    ),
    "ict.position_in_range": lambda observations: tuple(
        None if item is None else item.position_in_range for item in observations
    ),
}
