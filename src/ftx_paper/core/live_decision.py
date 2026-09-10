from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from .bundles import DecisionBundle
from ftx_paper.contracts import OrderIntent, OrderSide
from .risk import RiskSizer


@dataclass(frozen=True, slots=True)
class LiveDecision:
    event_type: str
    payload: dict[str, object]
    order: OrderIntent | None = None


class IndependentLiveDecisionEngine:
    """Live-side causal decision implementation.

    This module intentionally owns its location, feature, setup, gate,
    direction, and cooldown logic.  It has no historical-pipeline imports.
    """

    def __init__(self, *, version: str, config_hash: str, cooldown_minutes: int = 30, capital: float = 100_000.0, prior_day_high: float | None = None, prior_day_low: float | None = None) -> None:
        self.version, self.config_hash = version, config_hash
        self.cooldown_minutes = cooldown_minutes
        self.capital = capital
        self.prior_day_high, self.prior_day_low = prior_day_high, prior_day_low
        self._futures: list = []
        self._last_decision: datetime | None = None
        self._trading_date: str | None = None

    def evaluate(self, bundle: DecisionBundle) -> tuple[LiveDecision, ...]:
        if self._trading_date != bundle.trading_date:
            self._futures.clear()
            self._last_decision = None
            self._trading_date = bundle.trading_date
        base = {"decision_minute": bundle.minute, "minute": bundle.minute, "bundle_id": bundle.bundle_id,
                "strategy_version": self.version, "config_hash": self.config_hash,
                "required_input_availability": {r: r not in bundle.missing_roles for r in bundle.required_roles}}
        if not bundle.complete:
            return (LiveDecision("BUNDLE_INCOMPLETE", {**base, "reason": "missing_" + "+".join(bundle.missing_roles)}),)
        vix_bar = bundle.bars.get("vix")
        if vix_bar is None:
            decision_id = sha256(f"{bundle.bundle_id}:missing_vix".encode()).hexdigest()[:24]
            return (LiveDecision("REJECTEDDECISION", {**base, "decision_id": decision_id, "cell": "NONE", "direction": "NONE", "setup_type": "Skip", "outcome": "policy rejection", "reason": "missing_vix", "required_input_availability": {**base["required_input_availability"], "vix": False}, "feature_values": {}}),)
        futures = bundle.bars["futures"]
        self._futures.append(futures)
        if len(self._futures) < 3:
            return (LiveDecision("CANDIDATE", {**base, "outcome": "no qualifying setup", "reason": "warmup", "feature_values": {}}),)
        prior = self._futures[:-1]
        vwap = sum(((b.high + b.low + b.close) / 3) * (b.volume or 0) for b in prior) / max(sum(b.volume or 0 for b in prior), 1e-12)
        session_high, session_low = max(b.high for b in prior), min(b.low for b in prior)
        opening = prior[:15]
        or_high, or_low = max(b.high for b in opening), min(b.low for b in opening)
        current = futures.close
        t = datetime.fromisoformat(bundle.minute).time()
        feature_values = {"vwap": vwap, "session_high": session_high, "session_low": session_low,
                          "opening_range_high": or_high, "opening_range_low": or_low,
                          "vix": vix_bar.close}
        location = "VWAP_ZONE" if abs(current - vwap) <= 15 else "SESSION_HIGH" if abs(current - session_high) <= 15 else "SESSION_LOW" if abs(current - session_low) <= 15 else None
        cell = location or "NONE"
        direction = "long" if current >= vwap else "short"
        candidate_id = sha256(f"{bundle.bundle_id}:{cell}".encode()).hexdigest()[:24]
        candidate = {**base, "decision_id": candidate_id, "cell": cell, "direction": direction,
                     "setup_type": "reversal_at_" + cell.lower() if location else "Skip",
                     "feature_values": feature_values, "entry_price": current, "outcome": "candidate"}
        events = [LiveDecision("CANDIDATE", candidate)]
        reason = None
        if location is None:
            reason = "no_qualifying_setup"
        elif not (datetime.strptime("10:15", "%H:%M").time() <= t <= datetime.strptime("14:15", "%H:%M").time()):
            reason = "outside_session_window"
        elif self._last_decision and (datetime.fromisoformat(bundle.minute) - self._last_decision).total_seconds() < self.cooldown_minutes * 60:
            reason = "cooldown"
        elif vix_bar.close <= 0:
            reason = "vix_gate"
        if reason:
            events.append(LiveDecision("REJECTEDDECISION", {**candidate, "outcome": "policy rejection", "reason": reason, "decision_id": sha256(f"{candidate_id}:{reason}".encode()).hexdigest()[:24]}))
            return tuple(events)
        self._last_decision = datetime.fromisoformat(bundle.minute)
        side = OrderSide.BUY if direction == "long" else OrderSide.SELL
        stop = current - max(futures.high - futures.low, 5.0) if side is OrderSide.BUY else current + max(futures.high - futures.low, 5.0)
        sizing = RiskSizer().size(capital=self.capital, equity=self.capital, peak_equity=self.capital, entry=current, stop=stop)
        events.append(LiveDecision("ACCEPTEDDECISION", {**candidate, "outcome": "accepted", "reason": "eligible"}))
        if not sizing.approved:
            events.append(LiveDecision("SIZING_REJECTED", {**candidate, "outcome": "sizing rejection", "reason": sizing.reason}))
            return tuple(events)
        order = OrderIntent(candidate_id, futures.instrument, side, sizing.quantity, reason="live_policy_accepted")
        events[-1] = LiveDecision("ACCEPTEDDECISION", {**candidate, "outcome": "accepted", "reason": "eligible"}, order=order)
        return tuple(events)


__all__ = ["IndependentLiveDecisionEngine", "LiveDecision"]
