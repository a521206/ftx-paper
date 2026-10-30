"""Public ICT Feature API; consumers do not import measurement internals."""

from collections.abc import Sequence

from ftx_paper.strategies.orderflow.canonical.ict.bars.model import CompletedBar
from ftx_paper.strategies.orderflow.canonical.ict.features.compression import CompressionConfig, build_compression_observations
from ftx_paper.strategies.orderflow.canonical.ict.features.model import MeasurementConfig, PriceMeasurement, measure_bars, measure_latest
from ftx_paper.strategies.orderflow.canonical.ict.features.registry import get_feature, list_features


def build_measurements(
    bars: Sequence[CompletedBar], config: MeasurementConfig = MeasurementConfig()
) -> tuple[PriceMeasurement, ...]:
    return measure_bars(bars, config)


def build_measurement(
    bars: Sequence[CompletedBar], config: MeasurementConfig = MeasurementConfig()
) -> PriceMeasurement:
    return measure_latest(bars, config)


def build_feature(
    name: str, bars: Sequence[CompletedBar], config: MeasurementConfig = MeasurementConfig()
) -> tuple[float | None, ...]:
    return get_feature(name).builder(bars, config)


def feature_metadata(name: str) -> dict[str, str]:
    entry = get_feature(name)
    return {
        "name": entry.name,
        "description": entry.description,
        "minimum_history": entry.minimum_history,
        "zero_denominator": "unavailable (None), never inferred as balance or compression",
    }


def build_compression_feature(
    name: str,
    bars: Sequence[CompletedBar],
    detections: Sequence[object],
    config: CompressionConfig = CompressionConfig(),
) -> tuple[float | None, ...]:
    """Build one registered compression feature from causal balance detections."""
    observations = build_compression_observations(bars, detections, config)
    return get_feature(name).builder(observations)


__all__ = [
    "CompressionConfig",
    "MeasurementConfig",
    "PriceMeasurement",
    "build_compression_feature",
    "build_compression_observations",
    "build_feature",
    "build_measurements",
    "build_measurement",
    "feature_metadata",
    "list_features",
]
