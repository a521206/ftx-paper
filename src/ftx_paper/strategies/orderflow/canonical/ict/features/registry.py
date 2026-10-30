"""Registry for the standalone ICT measurement API."""

from dataclasses import dataclass
from typing import Any, Callable

from ftx_paper.strategies.orderflow.canonical.ict.features.model import MEASUREMENT_BUILDERS
from ftx_paper.strategies.orderflow.canonical.ict.features.compression import COMPRESSION_FEATURES


@dataclass(frozen=True, slots=True)
class FeatureDefinition:
    name: str
    description: str
    minimum_history: str
    builder: Callable[..., Any]


FEATURE_REGISTRY = {
    name: FeatureDefinition(
        name=name,
        description=description,
        minimum_history=minimum,
        builder=MEASUREMENT_BUILDERS[name],
    )
    for name, description, minimum in (
        ("ict.directional_efficiency", "Absolute net close change divided by total absolute close-to-close movement.", "directional_efficiency_lookback bars"),
        ("ict.consecutive_bar_overlap", "Mean intersection width divided by the smaller range across consecutive pairs.", "overlap_lookback + 1 bars"),
        ("ict.rolling_range_width", "Maximum high minus minimum low over the rolling window.", "rolling_range_lookback bars"),
        ("ict.volatility_contraction", "Recent mean bar range divided by the preceding non-overlapping baseline mean range.", "recent + baseline lookbacks"),
        ("ict.true_range", "Bar true range; first session bar uses high minus low only.", "1 bar"),
        ("ict.atr", "Wilder-smoothed average true range by default; resets each session_date.", "atr_lookback session bars"),
    )
}

for _name, _builder in COMPRESSION_FEATURES.items():
    FEATURE_REGISTRY[_name] = FeatureDefinition(
        name=_name,
        description={
            "ict.active_band_fraction": "Recent active-band width relative to frozen outer balance range.",
            "ict.volatility_ratio": "Recent mean bar range relative to its preceding baseline.",
            "ict.containment_duration": "Consecutive completed bars contained by frozen balance boundaries.",
            "ict.position_in_range": "Recent active-band midpoint within the frozen outer range; excursions are not clipped.",
        }[_name],
        minimum_history="configured active and volatility lookbacks after episode confirmation",
        builder=_builder,
    )

def get_feature(name: str) -> FeatureDefinition:
    try:
        return FEATURE_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(f"Unregistered ICT feature: {name}") from exc


def list_features() -> tuple[str, ...]:
    return tuple(FEATURE_REGISTRY)
