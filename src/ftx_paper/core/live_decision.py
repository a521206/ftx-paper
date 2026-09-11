from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from zoneinfo import ZoneInfo
from .bundles import DecisionBundle
from ftx_paper.contracts import MarketRole, OrderIntent, OrderSide, Role
from .risk import RiskSizer
from .scoring import calculate_setup_score, compute_selling_structure, score_to_setup_type


def _option_pcr(bundle: DecisionBundle) -> float | None:
    """Read volume PCR from option bars carried as supporting bundle inputs."""
    supporting = bundle.supporting_inputs or {}
    bars = supporting.get("bars", {})
    if not isinstance(bars, dict):
        return None
    call_volume = sum(float(bar.volume or 0) for bar in bars.values()
                      if str(getattr(bar.instrument, "instrument_type", "")).upper() == "CE")
    put_volume = sum(float(bar.volume or 0) for bar in bars.values()
                     if str(getattr(bar.instrument, "instrument_type", "")).upper() == "PE")
    return put_volume / call_volume if call_volume > 0 else None


def _supporting_bar(bundle: DecisionBundle, role: Role):
    supporting = bundle.supporting_inputs or {}
    bars = supporting.get("bars", {})
    return bars.get(role) if isinstance(bars, dict) else None


def _decision_datetime(bundle: DecisionBundle) -> datetime:
    """Return the timezone-aware instant at which the bundle was evaluated."""
    if "T" in bundle.minute:
        parsed = datetime.fromisoformat(bundle.minute)
    else:
        parsed = datetime.fromisoformat(f"{bundle.trading_date}T{bundle.minute}")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
    return parsed


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

    def __init__(self, *, version: str, config_hash: str, cooldown_minutes: int = 30, capital: float = 100_000.0, prior_day_high: float | None = None, prior_day_low: float | None = None, morning_entry_minutes: tuple[int, int] = (60, 120), afternoon_entry_minutes: tuple[int, int] = (255, 300)) -> None:
        self.version, self.config_hash = version, config_hash
        self.cooldown_minutes = cooldown_minutes
        self.capital = capital
        self.prior_day_high, self.prior_day_low = prior_day_high, prior_day_low
        self.morning_entry_minutes = morning_entry_minutes
        self.afternoon_entry_minutes = afternoon_entry_minutes
        self._futures: list = []
        self._previous_pcr: float | None = None
        self._previous_vix = None
        self._last_decision: datetime | None = None
        self._trading_date: str | None = None
        self._decision_session: str | None = None
        self._vix_open: float | None = None

    def _session_for_time(self, decision_at: datetime) -> str | None:
        ist_time = decision_at.astimezone(ZoneInfo("Asia/Kolkata"))
        minutes_from_open = ist_time.hour * 60 + ist_time.minute - (9 * 60 + 15)
        if self.morning_entry_minutes[0] <= minutes_from_open < self.morning_entry_minutes[1]:
            return "morning"
        if self.afternoon_entry_minutes[0] <= minutes_from_open < self.afternoon_entry_minutes[1]:
            return "afternoon"
        return None

    def evaluate(self, bundle: DecisionBundle) -> tuple[LiveDecision, ...]:
        if self._trading_date != bundle.trading_date:
            self._futures.clear()
            self._previous_pcr = None
            self._previous_vix = None
            self._vix_open = None
            self._last_decision = None
            self._decision_session = None
            self._trading_date = bundle.trading_date
        decision_at = _decision_datetime(bundle)
        decision_session = self._session_for_time(decision_at)
        if decision_session is not None and decision_session != self._decision_session:
            self._last_decision = None
            self._decision_session = decision_session
        current_pcr = _option_pcr(bundle)
        if current_pcr is not None:
            self._previous_pcr = current_pcr
        pcr = current_pcr if current_pcr is not None else self._previous_pcr
        base = {"decision_at": decision_at.isoformat(), "bundle_id": bundle.bundle_id,
                "strategy_version": self.version, "config_hash": self.config_hash,
                "required_input_availability": {r: r not in bundle.missing_roles for r in bundle.required_roles}}
        if not bundle.complete:
            return ()
        current_vix = bundle.bars.get(MarketRole.VIX) or _supporting_bar(bundle, MarketRole.VIX)
        if current_vix is not None:
            self._previous_vix = current_vix
            if self._vix_open is None:
                self._vix_open = float(current_vix.close)
        vix_bar = current_vix or self._previous_vix
        if vix_bar is None:
            decision_id = sha256(f"{bundle.bundle_id}:missing_vix".encode()).hexdigest()[:24]
            return (LiveDecision("REJECTEDDECISION", {**base, "decision_id": decision_id, "cell": "NONE", "direction": "NONE", "setup_type": "Skip", "outcome": "policy rejection", "reason": "missing_vix", "required_input_availability": {**base["required_input_availability"], "vix": False}, "feature_values": {}}),)
        futures = bundle.bars[MarketRole.FUTURES]
        self._futures.append(futures)
        if len(self._futures) < 3:
            return (LiveDecision("WARMUP", {**base, "reason": "insufficient_history", "feature_values": {}}),)
        prior = self._futures[:-1]
        vwap = sum(((b.high + b.low + b.close) / 3) * (b.volume or 0) for b in prior) / max(sum(b.volume or 0 for b in prior), 1e-12)
        session_high, session_low = max(b.high for b in prior), min(b.low for b in prior)
        opening = prior[:15]
        or_high, or_low = max(b.high for b in opening), min(b.low for b in opening)
        current = futures.close
        decision_dt = decision_at
        stop_basis = max(futures.high - futures.low, 5.0)
        feature_values = {"vwap": vwap, "session_high": session_high, "session_low": session_low,
                          "opening_range_high": or_high, "opening_range_low": or_low,
                          "vix": vix_bar.close, "pcr": pcr}
        location = "VWAP_ZONE" if abs(current - vwap) <= 15 else "SESSION_HIGH" if abs(current - session_high) <= 15 else "SESSION_LOW" if abs(current - session_low) <= 15 else None
        cell = location or "NONE"
        if location is None:
            return ()
        direction = "long" if current >= vwap else "short"
        round_level = round(current / 50) * 50
        structural_proximity = (
            (self.prior_day_low is not None and abs(current - self.prior_day_low) <= 15)
            or (self.prior_day_high is not None and abs(current - self.prior_day_high) <= 15)
            or abs(current - round_level) <= 15
        )
        selling = compute_selling_structure(
            prior, futures, vix_open=float(self._vix_open or vix_bar.close),
            vix_at_event=float(vix_bar.close),
        )
        ist_decision_at = decision_at.astimezone(ZoneInfo("Asia/Kolkata"))
        minutes_from_open = ist_decision_at.hour * 60 + ist_decision_at.minute - (9 * 60 + 15)
        score, score_factors = calculate_setup_score(
            selling, {"minutes_from_open": float(minutes_from_open)}, float(vix_bar.close),
            float(self._vix_open or vix_bar.close), pcr, structural_proximity,
        )
        score_setup_type, score_multiplier = score_to_setup_type(score)
        setup_family = "reversal_at_" + cell.lower() if location else "Skip"
        sequence = len(self._futures)
        candidate_id = sha256(f"{bundle.bundle_id}:{cell}".encode()).hexdigest()[:24]
        candidate = {**base, "decision_id": candidate_id, "cell": cell, "direction": direction,
                     "setup_type": setup_family,
                     "score_setup_type": score_setup_type,
                     "score": score, "score_factors": score_factors,
                     "score_multiplier": score_multiplier, "sequence": sequence,
                     "feature_values": feature_values, "entry_price": current, "outcome": "candidate"}
        candidate.update(vix_at_event=vix_bar.close, pcr_at_event=pcr, stop_basis=stop_basis)
        events = []
        reason = None
        if location is None:
            reason = "no_qualifying_setup"
        elif decision_session is None:
            reason = "outside_session_window"
        elif score_setup_type == "Skip":
            reason = "setup_score_skip"
        elif self._last_decision and (decision_dt - self._last_decision).total_seconds() < self.cooldown_minutes * 60:
            reason = "cooldown"
        elif vix_bar.close <= 0:
            reason = "vix_gate"
        if reason:
            events.append(LiveDecision("REJECTEDDECISION", {**candidate, "outcome": "policy rejection", "reason": reason, "decision_id": sha256(f"{candidate_id}:{reason}".encode()).hexdigest()[:24]}))
            return tuple(events)
        self._last_decision = decision_dt
        side = OrderSide.BUY if direction == "long" else OrderSide.SELL
        stop = current - max(futures.high - futures.low, 5.0) if side is OrderSide.BUY else current + max(futures.high - futures.low, 5.0)
        sizing = RiskSizer().size(
            capital=self.capital, equity=self.capital, peak_equity=self.capital,
            entry=current, stop=stop, score=score,
        )
        candidate.update(
            requested_quantity=sizing.quantity,
            score_multiplier=sizing.score_multiplier,
            risk_amount=sizing.risk_amount,
        )
        events.append(LiveDecision("ACCEPTEDDECISION", {**candidate, "outcome": "accepted", "reason": "eligible"}))
        if not sizing.approved:
            events.append(LiveDecision("SIZING_REJECTED", {**candidate, "outcome": "sizing rejection", "reason": sizing.reason}))
            return tuple(events)
        order = OrderIntent(candidate_id, futures.instrument, side, sizing.quantity, reason="live_policy_accepted")
        events[-1] = LiveDecision("ACCEPTEDDECISION", {**candidate, "outcome": "accepted", "reason": "eligible"}, order=order)
        return tuple(events)


__all__ = ["IndependentLiveDecisionEngine", "LiveDecision"]
