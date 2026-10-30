"""Versioned configuration for the causal value-profile track."""

from dataclasses import asdict, dataclass
import hashlib
import json
from typing import Literal

from ftx_paper.strategies.orderflow.canonical.ict.value_profile.enums import ProfileScope

_V1_SCOPES = frozenset(
    {
        ProfileScope.PRIOR_SESSION,
        ProfileScope.SESSION_DEVELOPING,
        ProfileScope.ZONE,
        ProfileScope.ROLLING_BARS,
    }
)


@dataclass(frozen=True, slots=True)
class ValueProfileConfig:
    """Profile rules kept separate from episode, macro, and narrative configuration."""

    profile_version: str = "value_profile.v1"
    value_area_fraction: float = 0.70
    bin_mode: Literal["tick", "atr_scaled"] = "tick"
    tick_size: float = 0.05
    atr_mult: float = 0.25
    price_assignment: Literal["typical", "close", "range_distribute"] = "typical"
    scopes: tuple[ProfileScope, ...] = (
        ProfileScope.PRIOR_SESSION,
        ProfileScope.SESSION_DEVELOPING,
    )
    developing_policy: Literal["emit", "omit"] = "emit"
    gap_policy: Literal["freeze_last", "invalidate_developing"] = "freeze_last"
    session_reset: bool = True
    rolling_window_bars: int = 30
    daily_history_lookback_sessions: int = 20
    prior_session_require_complete: bool = True
    cluster_overlap_ratio: float = 0.50
    cluster_acceptance_half_life: float = 5.0
    cluster_break_confirmation_bars: int = 2

    def __post_init__(self) -> None:
        if not self.profile_version:
            raise ValueError("value profile version cannot be empty")
        if not 0.0 < self.value_area_fraction <= 1.0:
            raise ValueError("value area fraction must be in (0, 1]")
        if self.bin_mode != "tick":
            raise ValueError("v1 histogram uses raw typical price; only tick bin mode is supported")
        if self.tick_size <= 0:
            raise ValueError("tick size must be positive")
        if self.atr_mult <= 0:
            raise ValueError("atr multiplier must be positive")
        if self.price_assignment not in {"typical", "close", "range_distribute"}:
            raise ValueError("price assignment is not supported")
        if self.price_assignment != "typical":
            raise ValueError("v1 supports typical price assignment only")
        if not self.scopes:
            raise ValueError("at least one profile scope is required")
        unsupported = [scope for scope in self.scopes if scope not in _V1_SCOPES]
        if unsupported:
            names = ", ".join(scope.value for scope in unsupported)
            raise ValueError(f"v1 does not implement profile scopes: {names}")
        if self.developing_policy not in {"emit", "omit"}:
            raise ValueError("developing policy must be emit or omit")
        if self.gap_policy not in {"freeze_last", "invalidate_developing"}:
            raise ValueError("gap policy must freeze_last or invalidate_developing")
        if self.rolling_window_bars < 2:
            raise ValueError("rolling window bars must be at least two")
        if ProfileScope.ROLLING_BARS in self.scopes and self.rolling_window_bars < 2:
            raise ValueError("rolling scope requires rolling_window_bars >= 2")
        if self.daily_history_lookback_sessions < 1:
            raise ValueError("daily history lookback must be at least one session")
        if not 0.0 < self.cluster_overlap_ratio <= 1.0:
            raise ValueError("cluster overlap ratio must be in (0, 1]")
        if self.cluster_acceptance_half_life <= 0:
            raise ValueError("cluster acceptance half-life must be positive")
        if self.cluster_break_confirmation_bars < 1:
            raise ValueError("cluster break confirmation bars must be positive")

    def fingerprint(self) -> str:
        """Return a stable digest for this exact configuration."""
        payload = {
            **asdict(self),
            "scopes": [scope.value for scope in self.scopes],
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
