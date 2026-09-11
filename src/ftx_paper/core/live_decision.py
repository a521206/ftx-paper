from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from zoneinfo import ZoneInfo
from .bundles import DecisionBundle
from ftx_paper.contracts import MarketRole, OptionRole, OrderIntent, Role, role_to_key
from .features import option_pcr_at_event, vix_open_and_event
from .location_engine import Cell, LocationDetector, TransitionPattern, transition_patterns_allow
from .risk import RiskSizer
from .risk_state import RiskGateState
from .adaptive_stop import adaptive_stop_bp, stop_price
from .scoring import calculate_setup_score, compute_selling_structure, score_to_setup_type
from ftx_paper.strategy.config import (
    AFTERNOON_CELL_POLICIES,
    AFTERNOON_ENTRY_MINUTES,
    MORNING_CELL_POLICIES,
    MORNING_ENTRY_MINUTES,
    Session,
)


def _option_pcr(bundle: DecisionBundle) -> float | None:
    """Read exact-minute volume PCR, with the canonical 10:00 cutoff."""
    supporting = bundle.supporting_inputs or {}
    bars = supporting.get("bars", {})
    if not isinstance(bars, dict):
        return None
    futures = bundle.bars.get(MarketRole.FUTURES)
    if futures is None:
        return None
    sources = supporting.get("sources", {})
    if isinstance(sources, dict):
        bars = {
            key: bar for key, bar in bars.items()
            if sources.get(
                role_to_key(key) if isinstance(key, (MarketRole, OptionRole)) else str(key),
                "same_minute",
            ) == "same_minute"
        }
    return option_pcr_at_event(bars, futures)


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

    def __init__(self, *, version: str, config_hash: str, capital: float, cooldown_minutes: int = 30, prior_day_high: float | None = None, prior_day_low: float | None = None, morning_entry_minutes: tuple[int, int] = MORNING_ENTRY_MINUTES, afternoon_entry_minutes: tuple[int, int] = AFTERNOON_ENTRY_MINUTES, transition_patterns: tuple[TransitionPattern, ...] = (), expiry_dates: frozenset[str] = frozenset(), risk_gate: RiskGateState | None = None) -> None:
        self.version, self.config_hash = version, config_hash
        self.cooldown_minutes = cooldown_minutes
        self.capital = capital
        self.prior_day_high, self.prior_day_low = prior_day_high, prior_day_low
        self.morning_entry_minutes = morning_entry_minutes
        self.afternoon_entry_minutes = afternoon_entry_minutes
        self.transition_patterns = transition_patterns
        self.expiry_dates = expiry_dates
        self._cell_policies = {
            (Session.MORNING, item.cell): item for item in MORNING_CELL_POLICIES
        }
        self._cell_policies.update({
            (Session.AFTERNOON, item.cell): item for item in AFTERNOON_CELL_POLICIES
        })
        self._futures: list = []
        self._previous_vix = None
        self._vix_history: list = []
        self._location_detector = LocationDetector()
        self._last_decision: datetime | None = None
        self._trading_date: str | None = None
        self._decision_session: Session | None = None
        self._vix_open: float | None = None
        self.risk_gate = risk_gate or RiskGateState()

    def _session_for_time(self, decision_at: datetime) -> Session:
        ist_time = decision_at.astimezone(ZoneInfo("Asia/Kolkata"))
        minutes_from_open = ist_time.hour * 60 + ist_time.minute - (9 * 60 + 15)
        if self.morning_entry_minutes[0] <= minutes_from_open < self.morning_entry_minutes[1]:
            return Session.MORNING
        if self.afternoon_entry_minutes[0] <= minutes_from_open < self.afternoon_entry_minutes[1]:
            return Session.AFTERNOON
        return Session.OUTSIDE

    def evaluate(self, bundle: DecisionBundle) -> tuple[LiveDecision, ...]:
        if self._trading_date != bundle.trading_date:
            if self._futures:
                self._location_detector.close_day()
                self.prior_day_high, self.prior_day_low = self._location_detector.prior_day_levels
            self._futures.clear()
            self._previous_vix = None
            self._vix_history.clear()
            self._vix_open = None
            self._location_detector.reset(prior_day_high=self.prior_day_high, prior_day_low=self.prior_day_low)
            self._last_decision = None
            self._decision_session = None
            self._trading_date = bundle.trading_date
        self.risk_gate._new_day(bundle.trading_date)
        decision_at = _decision_datetime(bundle)
        decision_session = self._session_for_time(decision_at)
        if decision_session is not self._decision_session:
            self._last_decision = None
            self._decision_session = decision_session
        current_pcr = _option_pcr(bundle)
        pcr = current_pcr
        base = {"decision_at": decision_at.isoformat(), "bundle_id": bundle.bundle_id,
                "strategy_version": self.version, "config_hash": self.config_hash,
                "required_input_availability": {r: r not in bundle.missing_roles for r in bundle.required_roles}}
        if not bundle.complete:
            return ()
        current_vix = bundle.bars.get(MarketRole.VIX) or _supporting_bar(bundle, MarketRole.VIX)
        if current_vix is not None:
            self._previous_vix = current_vix
            self._vix_history.append(current_vix)
            opening, _ = vix_open_and_event(tuple(self._vix_history), current_vix)
            if opening is not None:
                self._vix_open = opening
        vix_bar = current_vix or self._previous_vix
        if vix_bar is None:
            decision_id = sha256(f"{bundle.bundle_id}:missing_vix".encode()).hexdigest()[:24]
            return (LiveDecision("REJECTEDDECISION", {**base, "decision_id": decision_id, "cell": "NONE", "direction": "NONE", "setup_type": "Skip", "outcome": "policy rejection", "reason": "missing_vix", "required_input_availability": {**base["required_input_availability"], "vix": False}, "feature_values": {}}),)
        futures = bundle.bars[MarketRole.FUTURES]
        self._futures.append(futures)
        if len(self._futures) < 3:
            self._location_detector.observe(futures)
            return (LiveDecision("WARMUP", {**base, "reason": "insufficient_history", "feature_values": {}}),)
        prior = tuple(self._futures[:-1])
        location_snapshot = self._location_detector.observe(futures)
        if location_snapshot is None:
            return (LiveDecision("WARMUP", {**base, "reason": "insufficient_history", "feature_values": {}}),)
        features = location_snapshot.features
        vwap = features.vwap
        session_high, session_low = features.session_high, features.session_low
        or_high, or_low = features.opening_range_high, features.opening_range_low
        current = futures.close
        decision_dt = decision_at
        stop_basis = adaptive_stop_bp(
            prior, float(vix_bar.close),
            is_expiry_day=bundle.trading_date in self.expiry_dates,
        )
        feature_values = {"vwap": vwap, "session_high": session_high, "session_low": session_low,
                          "opening_range_high": or_high, "opening_range_low": or_low,
                          "atr": features.atr, "prior_day_high": features.prior_day_high,
                          "prior_day_low": features.prior_day_low,
                          "vix": vix_bar.close, "vix_open": self._vix_open,
                          "pcr": pcr}
        if location_snapshot.cell is None:
            return ()
        configured = [
            (Cell.parse(name), policy) for (session, name), policy in self._cell_policies.items()
            if session is decision_session and Cell.parse(name).locations.issubset(set(location_snapshot.locations))
        ]
        if not configured:
            return ()
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
        sequence = len(self._futures)
        events = []
        for cell, cell_policy in configured:
            direction = cell_policy.direction.value.lower()
            candidate_id = sha256(f"{decision_at.isoformat()}:{sequence}:{cell.name}".encode()).hexdigest()[:24]
            candidate = {**base, "decision_id": candidate_id, "cell": cell.name, "locations": [item.value for item in cell.ordered_locations], "direction": direction,
                         "setup_type": score_setup_type, "score": score, "score_factors": score_factors,
                         "score_multiplier": score_multiplier, "sequence": sequence,
                         "feature_values": feature_values, "entry_price": current, "outcome": "candidate",
                         "exit_mode": cell_policy.exit_mode.value}
            candidate.update(vix_at_event=vix_bar.close, pcr_at_event=pcr, stop_basis=stop_basis,
                             transitions=[{"reference": item.reference.value, "kind": item.kind.value,
                                           "from": item.from_side.value, "to": item.to_side.value}
                                          for item in location_snapshot.transitions])
            reason = None
            if score_setup_type == "Skip":
                reason = "setup_score_skip"
            elif not transition_patterns_allow(location_snapshot, cell_policy.transition_patterns or self.transition_patterns):
                reason = "transition_policy_mismatch"
            elif self._last_decision and (decision_dt - self._last_decision).total_seconds() < self.cooldown_minutes * 60:
                reason = "cooldown"
            elif vix_bar.close <= 0:
                reason = "vix_gate"
            if reason:
                events.append(LiveDecision(
                    "REJECTEDDECISION",
                    {**candidate, "outcome": "policy rejection", "reason": reason,
                     "decision_id": sha256(f"{candidate_id}:{reason}".encode()).hexdigest()[:24]},
                ))
                continue
            self._last_decision = decision_dt
            side = cell_policy.direction
            stop = stop_price(current, side.value, stop_basis)
            sizing = RiskSizer().size(capital=self.capital, equity=self.capital, peak_equity=self.capital, entry=current, stop=stop, score=score, vix=float(vix_bar.close), is_expiry_day=bundle.trading_date in self.expiry_dates)
            candidate.update(requested_quantity=sizing.quantity, score_multiplier=sizing.score_multiplier, risk_amount=sizing.risk_amount)
            events.append(LiveDecision("ACCEPTEDDECISION", {**candidate, "outcome": "accepted", "reason": "eligible"}))
            if not sizing.approved:
                events.append(LiveDecision("SIZING_REJECTED", {
                    **candidate, "outcome": "sizing rejection", "reason": sizing.reason,
                }))
                continue
            gate_reason = self.risk_gate.rejection_reason(cell=cell.name, direction=direction, bar=sequence, quantity=sizing.quantity, date=bundle.trading_date)
            if gate_reason is not None:
                events.append(LiveDecision(
                    "REJECTEDDECISION",
                    {**candidate, "outcome": "risk rejection", "reason": gate_reason,
                     "decision_id": sha256(f"{candidate_id}:{gate_reason}".encode()).hexdigest()[:24]},
                ))
                continue
            self.risk_gate.record_entry(cell=cell.name, direction=direction, quantity=sizing.quantity, date=bundle.trading_date)
            order = OrderIntent(candidate_id, futures.instrument, side, sizing.quantity, reason="live_policy_accepted", cell=cell.name, stop_price=stop, exit_mode=cell_policy.exit_mode.value, entry_bar=sequence)
            events[-1] = LiveDecision("ACCEPTEDDECISION", {**candidate, "outcome": "accepted", "reason": "eligible"}, order=order)
        return tuple(events)

    def record_exit(self, *, cell: str, reason: str, entry_bar: int,
                    exit_bar: int, date: str) -> None:
        """Apply a completed exit to the replay/live risk state."""
        self.risk_gate.record_exit(
            cell=cell, reason=reason, entry_bar=entry_bar, exit_bar=exit_bar,
            date=date,
        )

    def risk_snapshot(self) -> dict[str, object]:
        """Return JSON-safe gate state for persistence across segments."""
        return self.risk_gate.snapshot()

    def restore_risk_snapshot(self, snapshot: dict[str, object]) -> None:
        self.risk_gate.restore(snapshot)


__all__ = ["IndependentLiveDecisionEngine", "LiveDecision"]
