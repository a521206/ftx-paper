"""Configuration and stable constants for the orderflow strategy."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json

from .orderflow_capture import CaptureMode
from .policy_contract import ExecutionMode


ORDERFLOW_NAME = "ftx-paper-orderflow"
ORDERFLOW_VERSION = "0.1.0"


@dataclass(frozen=True, slots=True)
class OrderflowConfig:
    name: str = ORDERFLOW_NAME
    version: str = ORDERFLOW_VERSION
    enabled_timeframes: tuple[str, ...] = ("1m", "5m")
    session_windows: tuple[tuple[str, str], ...] = (("09:15", "15:30"),)
    response_timeframe: str = "5m"
    session_reset: str = "trading_date"
    response_type: str = "confirmed_rejection"
    response_window_candles: int = 3
    confirmation_rule: str = "completed_response_candle"
    invalidation_rule: str = "close_outside_frozen_band"
    trigger_type: str = "breakout"
    admission_policy_version: str = "diagnostic_v1"
    stop_policy: str = "shared_adaptive_stop"
    exit_policy: str = "shared_exit_state_machine"
    feature_set: tuple[str, ...] = ("close", "volume")
    enabled_vehicles: tuple[str, ...] = ("futures",)
    fill_permission_margin: float | None = 0.05
    execution_mode: ExecutionMode = ExecutionMode.DIAGNOSTIC
    timezone: str = "Asia/Kolkata"
    market_session: tuple[str, str] = ("09:15", "15:30")
    vehicle: str = "futures"
    replay_mode: str = "causal"
    capture_mode: CaptureMode = CaptureMode.FULL
    vwap_enabled: bool = True

    def __post_init__(self) -> None:
        if self.response_timeframe not in self.enabled_timeframes:
            raise ValueError("response timeframe must be enabled")
        if not self.feature_set or len(self.feature_set) != len(set(self.feature_set)):
            raise ValueError("orderflow feature_set must be non-empty and unique")
        if "futures" not in self.enabled_vehicles:
            raise ValueError("orderflow requires futures as the enabled vehicle")
        if self.response_window_candles <= 0:
            raise ValueError("response window must be positive")
        if self.timezone != "Asia/Kolkata":
            raise ValueError("orderflow currently requires the Asia/Kolkata timezone")
        if len(self.market_session) != 2 or self.market_session[0] >= self.market_session[1]:
            raise ValueError("market_session must be an increasing start/end pair")
        if self.vehicle != "futures":
            raise ValueError("orderflow currently supports futures only")
        if self.replay_mode != "causal":
            raise ValueError("orderflow currently requires causal replay mode")

    @property
    def config_hash(self) -> str:
        payload = asdict(self)
        payload["execution_mode"] = self.execution_mode.value
        payload["capture_mode"] = self.capture_mode.value
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


__all__ = ["ORDERFLOW_NAME", "ORDERFLOW_VERSION", "OrderflowConfig"]
