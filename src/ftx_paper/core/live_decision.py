from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from zoneinfo import ZoneInfo
from .bundles import DecisionBundle
from ftx_paper.config import NIFTY_LOT_SIZE
from ftx_paper.contracts import MarketBar, MarketRole, OptionRole, OrderIntent, OrderRole, Role, synthetic_future_quote, role_to_key
from .cost import FUTURES_RTD_COST_PER_LOT
from .features import option_pcr_at_event, vix_open_and_event
from .location_engine import Cell, LocationDetector, TransitionPattern, transition_patterns_allow
from .risk import RiskConfig, RiskEngine, VehicleRiskLimits
from .sizing import SizingPipeline, SizingPipelineInput
from .risk_state import RiskGateState
from .portfolio import PortfolioState
from .adaptive_stop import adaptive_stop_bp, stop_price
from .scoring import SCORE_WEAK, calculate_setup_score, compute_selling_structure, score_to_setup_type
from ftx_paper.capital_context import CapitalRuntimeContext
from ftx_paper.strategy.config import (
    AFTERNOON_CELL_POLICIES,
    AFTERNOON_ENTRY_MINUTES,
    MORNING_CELL_POLICIES,
    MORNING_ENTRY_MINUTES,
    CellPolicyConfig,
    Session,
)


def _option_pcr(bundle: DecisionBundle, expiry_dates: frozenset[str] = frozenset()) -> float | None:
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
    return option_pcr_at_event(bars, futures, expiry_dates=expiry_dates)


def _supporting_bar(bundle: DecisionBundle, role: Role):
    supporting = bundle.supporting_inputs or {}
    bars = supporting.get("bars", {})
    return bars.get(role) if isinstance(bars, dict) else None


def _same_minute_option_bars(bundle: DecisionBundle) -> dict[Role, MarketBar]:
    """Return typed, same-minute supporting option bars only."""
    supporting = bundle.supporting_inputs or {}
    bars = supporting.get("bars", {})
    if not isinstance(bars, Mapping):
        return {}
    sources = supporting.get("sources", {})
    return {
        role: bar
        for role, bar in bars.items()
        if isinstance(role, (MarketRole, OptionRole))
        and (
            not isinstance(sources, Mapping)
            or sources.get(role_to_key(role), "same_minute") == "same_minute"
        )
    }


def _synthetic_settlement_metadata(bundle: DecisionBundle) -> dict[str, object]:
    """Capture optional synthetic lookup data without affecting futures decisions."""
    futures = bundle.bars.get(MarketRole.FUTURES)
    if futures is None:
        return {"synthetic_premium_status": "unavailable", "synthetic_contract": None}
    supporting = bundle.supporting_inputs or {}
    bars = supporting.get("bars", {})
    selection_bar = bars.get(MarketRole.SPOT, futures) if isinstance(bars, Mapping) else futures
    quote = synthetic_future_quote(
        selection_bar, _same_minute_option_bars(bundle), same_minute=True,
    )
    if quote is None:
        return {"synthetic_premium_status": "unavailable", "synthetic_contract": None}
    return {
        "synthetic_premium_status": "available",
        "synthetic_contract": {
            "expiry": quote.ce.instrument.expiry,
            "strike": quote.strike,
            "ce_symbol": quote.ce.instrument.symbol,
            "pe_symbol": quote.pe.instrument.symbol,
            "entry_minute": quote.ce.timestamp.isoformat(),
        },
        "synthetic_premium_lookup": {
            "ce": quote.ce.close,
            "pe": quote.pe.close,
        },
    }


def _decision_datetime(bundle: DecisionBundle) -> datetime:
    """Return the timezone-aware instant at which the bundle was evaluated."""
    if "T" in bundle.minute:
        parsed = datetime.fromisoformat(bundle.minute)
    else:
        parsed = datetime.fromisoformat(f"{bundle.trading_date}T{bundle.minute}")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
    return parsed


def _configured_policies_for_cell(
    cell_policies: dict[tuple[Session, Cell], CellPolicyConfig],
    session: Session,
    detected_cell: Cell,
) -> list[tuple[Cell, CellPolicyConfig]]:
    """Return policies whose canonical composite exactly matches the event."""
    return [
        (cell, policy) for (policy_session, cell), policy in cell_policies.items()
        if policy_session is session and cell == detected_cell
    ]


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

    def __init__(self, *, version: str, config_hash: str, capital: float, portfolio: PortfolioState | None = None, max_daily_loss: float = 0.05, max_net_directional_lots: float = 8.0, risk_per_trade: float = 0.01, max_lots: int = 3, entry_cooldown_bars: int = 15, post_exit_cooldown_bars: int = 30, prior_day_high: float | None = None, prior_day_low: float | None = None, morning_entry_minutes: tuple[int, int] = MORNING_ENTRY_MINUTES, afternoon_entry_minutes: tuple[int, int] = AFTERNOON_ENTRY_MINUTES, transition_patterns: tuple[TransitionPattern, ...] = (), expiry_dates: frozenset[str] = frozenset(), risk_gate: RiskGateState | None = None, enabled_vehicles: tuple[str, ...] = ("futures", "synthetic"), vehicle: str | None = None, vehicle_risk_limits: Mapping[str, VehicleRiskLimits] | None = None, capital_context: CapitalRuntimeContext | None = None, setup_score_skip_filter: bool = True) -> None:
        self.version, self.config_hash = version, config_hash
        self.portfolio = portfolio or PortfolioState(capital)
        self.capital = self.portfolio.initial_capital
        self.capital_context = capital_context
        self.setup_score_skip_filter = setup_score_skip_filter
        if vehicle is not None:
            enabled_vehicles = (str(vehicle).lower(),)
        self.enabled_vehicles = tuple(dict.fromkeys(str(item).lower() for item in enabled_vehicles))
        if not self.enabled_vehicles or any(item not in {"futures", "synthetic"} for item in self.enabled_vehicles):
            raise ValueError("enabled_vehicles must contain 'futures' and/or 'synthetic'")
        if "futures" not in self.enabled_vehicles:
            raise ValueError("synthetic is reporting-only; futures must be enabled")
        self.max_daily_loss = max_daily_loss
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
        self._trading_date: str | None = None
        self._decision_session: Session | None = None
        self._decision_segment: tuple[Session, int] | None = None
        self._vix_open: float | None = None
        # Futures is the only decision and execution vehicle. Synthetic data is
        # retained as optional settlement metadata, never as a second gate or
        # sizing path.
        self.risk_gate = risk_gate or (
            RiskGateState.from_context(
                capital_context,
                entry_cooldown_bars=entry_cooldown_bars,
                thesis_cooldown_bars=0,
                cell_cooldown_bars=post_exit_cooldown_bars,
            )
            if capital_context is not None
            else RiskGateState(
                max_net_directional_lots=max_net_directional_lots,
                entry_cooldown_bars=entry_cooldown_bars,
                thesis_cooldown_bars=0,
                cell_cooldown_bars=post_exit_cooldown_bars,
            )
        )
        effective_vehicle_limits = dict(vehicle_risk_limits or {})
        if capital_context is not None:
            effective_vehicle_limits.update({
                name: VehicleRiskLimits(limit.max_lots, limit.margin_per_lot)
                for name, limit in capital_context.profile.vehicle_limits
            })
        self._vehicle_sizers = {
            "futures": RiskEngine(
                config=RiskConfig(
                    risk_fraction=risk_per_trade,
                    max_quantity=max_lots,
                    margin_utilization_cap=0.80,
                ),
                vehicle_limits=effective_vehicle_limits,
                context=capital_context,
            )
        }
        self._sizing_pipeline = SizingPipeline(capital_context)
        self._open_margin_used = 0.0

    @property
    def open_margin_used(self) -> float:
        return self.portfolio.open_margin

    def _session_for_time(self, decision_at: datetime) -> Session:
        ist_time = decision_at.astimezone(ZoneInfo("Asia/Kolkata"))
        minutes_from_open = ist_time.hour * 60 + ist_time.minute - (9 * 60 + 15)
        if self.morning_entry_minutes[0] <= minutes_from_open < self.morning_entry_minutes[1]:
            return Session.MORNING
        if self.afternoon_entry_minutes[0] <= minutes_from_open < self.afternoon_entry_minutes[1]:
            return Session.AFTERNOON
        return Session.OUTSIDE

    def _session_selected(self, session: Session) -> bool:
        return (
            session is not Session.OUTSIDE
            and (
                self.capital_context is None
                or session.value in self.capital_context.selected_sessions
            )
        )

    def _session_segment(self, decision_at: datetime) -> tuple[Session, int] | None:
        """Return the session state scope; cooldowns advance on market bars."""
        session = self._session_for_time(decision_at)
        if session is Session.OUTSIDE:
            return None
        return session, 0

    def evaluate(self, bundle: DecisionBundle) -> tuple[LiveDecision, ...]:
        if self._trading_date != bundle.trading_date:
            self.portfolio.start_day()
            if self._futures:
                self._location_detector.close_day()
                self.prior_day_high, self.prior_day_low = self._location_detector.prior_day_levels
            self._futures.clear()
            self._previous_vix = None
            self._vix_history.clear()
            self._vix_open = None
            self._location_detector.reset(prior_day_high=self.prior_day_high, prior_day_low=self.prior_day_low)
            self._decision_session = None
            self._decision_segment = None
            self._trading_date = bundle.trading_date
        self.risk_gate._new_day(bundle.trading_date)
        decision_at = _decision_datetime(bundle)
        decision_session = self._session_for_time(decision_at)
        decision_segment = self._session_segment(decision_at)
        if decision_segment != self._decision_segment:
            if self._decision_segment is not None:
                self.risk_gate.reset_segment()
            self._decision_segment = decision_segment
            self._decision_session = decision_session
        current_pcr = _option_pcr(bundle, self.expiry_dates)
        pcr = current_pcr
        base = {"decision_at": decision_at.isoformat(), "bundle_id": bundle.bundle_id,
                "strategy_version": self.version, "config_hash": self.config_hash,
                "required_input_availability": {r: r not in bundle.missing_roles for r in bundle.required_roles}}
        if not bundle.complete:
            decision_id = sha256(f"{bundle.bundle_id}:incomplete_bundle".encode()).hexdigest()[:24]
            return (LiveDecision("REJECTEDDECISION", {
                **base,
                "decision_id": decision_id,
                "cell": "NONE",
                "direction": "NONE",
                "setup_type": "Skip",
                "outcome": "input rejection",
                "reason": "incomplete_bundle",
                "feature_values": {},
            }),)
        if self.portfolio.equity < self.portfolio.daily_baseline * (1 - self.max_daily_loss):
            return (LiveDecision("REJECTEDDECISION", {**base, "reason": "daily_loss_limit"}),)
        current_vix = bundle.bars.get(MarketRole.VIX) or _supporting_bar(bundle, MarketRole.VIX)
        if current_vix is not None:
            self._previous_vix = current_vix
            self._vix_history.append(current_vix)
            opening, _ = vix_open_and_event(tuple(self._vix_history), current_vix)
            if opening is not None:
                self._vix_open = opening
        # Canonical decisions require an event-time VIX value.  A prior VIX
        # observation is retained only for opening-value calculation; it must
        # not be carried forward as the current event input.
        vix_bar = current_vix
        if vix_bar is None:
            decision_id = sha256(f"{bundle.bundle_id}:missing_vix".encode()).hexdigest()[:24]
            return (LiveDecision("REJECTEDDECISION", {**base, "decision_id": decision_id, "cell": "NONE", "direction": "NONE", "setup_type": "Skip", "outcome": "policy rejection", "reason": "missing_vix", "required_input_availability": {**base["required_input_availability"], "vix": False}, "feature_values": {}}),)
        if self._vix_open is None:
            decision_id = sha256(f"{bundle.bundle_id}:missing_vix_open".encode()).hexdigest()[:24]
            return (LiveDecision("REJECTEDDECISION", {
                **base, "decision_id": decision_id, "cell": "NONE", "direction": "NONE",
                "setup_type": "Skip", "outcome": "policy rejection", "reason": "missing_vix",
                "required_input_availability": {
                    **base["required_input_availability"], "vix_open": False,
                }, "feature_values": {},
            }),)
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
        stop_basis = adaptive_stop_bp(
            prior, float(vix_bar.close),
            is_expiry_day=bundle.trading_date in self.expiry_dates,
            current_close=current,
        )
        feature_values = {"vwap": vwap, "session_high": session_high, "session_low": session_low,
                          "opening_range_high": or_high, "opening_range_low": or_low,
                          "atr": features.atr, "prior_day_high": features.prior_day_high,
                          "prior_day_low": features.prior_day_low,
                          "vix": vix_bar.close, "vix_open": self._vix_open,
                          "pcr": pcr}
        if location_snapshot.cell is None:
            return ()
        # Policy cells are canonical simultaneous-location composites. Paper
        # must require the detected composite to match exactly; subset
        # matching would accept session_high+or_high when VWAP is also active.
        configured = _configured_policies_for_cell(
            self._cell_policies, decision_session, location_snapshot.cell,
        )
        round_level = round(current / 50) * 50
        structural_proximity = (
            (self.prior_day_low is not None and abs(current - self.prior_day_low) <= 15)
            or (self.prior_day_high is not None and abs(current - self.prior_day_high) <= 15)
            or abs(current - round_level) <= 15
        )
        selling = compute_selling_structure(
            prior, futures, vix_open=float(self._vix_open),
            vix_at_event=float(vix_bar.close),
        )
        ist_decision_at = decision_at.astimezone(ZoneInfo("Asia/Kolkata"))
        minutes_from_open = ist_decision_at.hour * 60 + ist_decision_at.minute - (9 * 60 + 15)
        score, score_factors = calculate_setup_score(
            selling, {"minutes_from_open": float(minutes_from_open)}, float(vix_bar.close),
            float(self._vix_open), pcr, structural_proximity,
        )
        score_setup_type, score_multiplier = score_to_setup_type(score)
        score_skip = self.setup_score_skip_filter and score < SCORE_WEAK
        sequence = len(self._futures)
        events = []
        policies = configured or [(location_snapshot.cell, None)]
        session_selected = self._session_selected(decision_session)
        for cell, cell_policy in policies:
            direction = cell_policy.direction.value.lower() if cell_policy is not None else "NONE"
            profile_id = self.capital_context.profile.candidate_id if self.capital_context is not None else "baseline"
            candidate_id = sha256(f"{profile_id}:{decision_at.isoformat()}:{sequence}:{cell.name}".encode()).hexdigest()[:24]
            candidate = {**base, "decision_id": candidate_id, "cell": cell.name, "locations": [item.value for item in cell.ordered_locations], "direction": direction,
                         "setup_type": score_setup_type, "score": score, "score_factors": score_factors,
                         "score_multiplier": score_multiplier, "sequence": sequence,
                         "feature_values": feature_values, "entry_price": current, "outcome": "candidate",
                         "exit_mode": cell_policy.exit_mode.value if cell_policy is not None else "signal",
                         "candidate_id": candidate_id,
                         "research_candidate_id": profile_id,
                         "capital_policy_version": self.capital_context.profile.schema_version if self.capital_context is not None else 1}
            candidate.update(_synthetic_settlement_metadata(bundle))
            candidate.update(vix_at_event=vix_bar.close, pcr_at_event=pcr, stop_basis=stop_basis,
                             transitions=[{"reference": item.reference.value, "kind": item.kind.value,
                                           "from": item.from_side.value, "to": item.to_side.value}
                                          for item in location_snapshot.transitions])
            events.append(LiveDecision("CANDIDATEDECISION", candidate))
            reason = None
            if decision_session is Session.OUTSIDE:
                reason = "outside_session_window"
            elif not session_selected:
                reason = "session_not_selected"
            elif cell_policy is None:
                reason = "cell_not_configured"
            elif score_skip:
                reason = "setup_score_skip"
            elif not transition_patterns_allow(location_snapshot, cell_policy.transition_patterns or self.transition_patterns):
                reason = "transition_policy_mismatch"
            elif vix_bar.close <= 0:
                reason = "vix_gate"
            if reason:
                events.append(LiveDecision(
                    "REJECTEDDECISION",
                    {**candidate, "outcome": "policy rejection", "reason": reason,
                     "decision_id": candidate_id},
                ))
                continue
            if cell_policy is None:
                continue
            side = cell_policy.direction
            stop = stop_price(current, side.value, stop_basis)
            thesis_reason = self.risk_gate.thesis_rejection_reason(
                cell=cell.name, bar=sequence, date=bundle.trading_date,
            )
            if thesis_reason is not None:
                events.append(LiveDecision(
                    "REJECTEDDECISION",
                    {**candidate, "outcome": "thesis rejection", "reason": thesis_reason,
                      "decision_id": candidate_id},
                ))
                continue
            for vehicle in ("futures",):
                sizing = self._vehicle_sizers[vehicle].size(
                    capital=self.portfolio.initial_capital, equity=self.portfolio.equity, peak_equity=self.portfolio.peak_equity,
                    # Score qualifies the setup but is not a sizing input in
                    # the current canonical FTX capital path.
                    entry=current, stop=stop, score=None, vix=float(vix_bar.close),
                    is_expiry_day=bundle.trading_date in self.expiry_dates,
                    vehicle=vehicle,
                    open_margin_used=self.portfolio.open_margin,
                )
                stability = cell_policy.stability
                if self.capital_context is not None:
                    configured_stability = self.capital_context.profile.stability_for(
                        f"{decision_session.value}:{cell.name}"
                    )
                    if self.capital_context.profile.stability_policy:
                        stability = configured_stability
                cumulative_pnl = self.portfolio.equity - self.portfolio.initial_capital
                peak_pnl = self.portfolio.peak_equity - self.portfolio.initial_capital
                drawdown_scale = 1.0
                if self.capital_context is not None:
                    drawdown = cumulative_pnl - peak_pnl
                    for threshold, scale in sorted(self.capital_context.profile.drawdown_policy.tiers):
                        if drawdown <= threshold:
                            drawdown_scale = scale
                            break
                sizing_decision = self._sizing_pipeline.decide(SizingPipelineInput(
                    risk=sizing,
                    requested_quantity=sizing.quantity,
                    # RiskEngine exposes the raw permission ceiling; drawdown
                    # and policy stability are downstream sizing stages.
                    score_multiplier=1.0,
                    direction=direction,
                    net_directional_lots=self.risk_gate.net_directional_lots,
                    concurrency_limit_lots=self.risk_gate.max_net_directional_lots,
                    stability_multiplier=stability,
                    drawdown_multiplier=drawdown_scale,
                    vehicle_limit_lots=None,
                    candidate_id=candidate_id,
                ))
                normalized_direction = "long" if direction in {"long", "buy"} else "short"
                risk_scope = "|".join((
                    bundle.trading_date, decision_session.value, cell.name,
                    normalized_direction,
                ))
                risk_per_lot = abs(current - stop) * NIFTY_LOT_SIZE + FUTURES_RTD_COST_PER_LOT
                risk_allowance = (
                    self.capital_context.profile.initial_capital
                    * self.capital_context.profile.max_daily_loss
                    * self.capital_context.profile.cell_session_risk_buffer_fraction
                    if self.capital_context is not None
                    else self.capital * self.max_daily_loss
                )
                available_scoped_risk = None
                scoped_risk_lot_ceiling = sizing_decision.final_quantity
                if risk_allowance > 0:
                    available_scoped_risk = self.portfolio.available_scoped_risk(
                        risk_scope, risk_allowance,
                    )
                    scoped_risk_lot_ceiling = (
                        int(available_scoped_risk // risk_per_lot)
                        if risk_per_lot > 0 else 0
                    )
                quantity_before_stability = next(
                    (value for name, value in reversed(sizing_decision.stage_results) if name != "stability"),
                    sizing.quantity,
                )
                quantity = min(sizing_decision.final_quantity, scoped_risk_lot_ceiling)
                sizing_metadata = dict(sizing_decision.metadata)
                sizing_metadata.update({
                    "risk_scope": risk_scope,
                    "risk_per_lot_rs": risk_per_lot,
                    "risk_allowance_rs": risk_allowance,
                    "available_scoped_risk_rs": available_scoped_risk,
                    "scoped_risk_lot_ceiling": scoped_risk_lot_ceiling,
                })
                vehicle_candidate = {**candidate, "vehicle": vehicle,
                                     "strategy_request": self._vehicle_sizers[vehicle].config.lot_size,
                                     "requested_quantity": quantity_before_stability,
                                     "final_quantity": quantity,
                                     "score_multiplier": sizing_decision.score_multiplier,
                                     "stability": stability, "risk_amount": sizing.risk_amount,
                                     "quantity_before_stability": quantity_before_stability,
                                     "risk_budget": sizing.risk_budget, "stop_bp": sizing.stop_bp,
                                     "sizing_pipeline": sizing_metadata}
                vehicle_candidate.update({
                    "available_capital": sizing.available_capital,
                    "open_margin_used": sizing.open_margin_used,
                    "raw_risk_quantity": sizing.raw_quantity,
                    "margin_lots": sizing.margin_lots,
                    "risk_ceiling": sizing.risk_ceiling,
                    "drawdown_multiplier": drawdown_scale,
                    "risk_fraction": self._vehicle_sizers[vehicle].config.risk_fraction,
                    "margin_utilization_cap": self._vehicle_sizers[vehicle].config.margin_utilization_cap,
                    "margin_per_lot": self._vehicle_sizers[vehicle].vehicle_limits[vehicle].margin_per_lot,
                    "max_quantity": self._vehicle_sizers[vehicle].vehicle_limits[vehicle].max_quantity,
                    "shared_directional_headroom": self.risk_gate.remaining_directional_lots(direction),
                    "vehicle_directional_headroom": self.risk_gate.remaining_directional_lots(direction),
                    "risk_scope": risk_scope,
                    "risk_per_lot_rs": risk_per_lot,
                    "risk_allowance_rs": risk_allowance,
                    "available_scoped_risk_rs": available_scoped_risk,
                    "scoped_risk_lot_ceiling": scoped_risk_lot_ceiling,
                })
                if not sizing.approved:
                    events.append(LiveDecision("SIZING_REJECTED", {
                        **vehicle_candidate, "outcome": "sizing rejection", "reason": sizing.reason,
                    }))
                    continue
                # Gate the executable, stability-adjusted size rather than
                # the pre-stability risk ceiling.
                gate_reason = self.risk_gate.rejection_reason(
                    cell=cell.name, direction=direction, bar=sequence,
                    quantity=quantity, date=bundle.trading_date,
                )
                if quantity < 1:
                    events.append(LiveDecision("SIZING_REJECTED", {
                        **vehicle_candidate, "outcome": "sizing rejection",
                        "reason": (
                            "scoped_risk_buffer_exhausted"
                            if scoped_risk_lot_ceiling < 1
                            else "insufficient_risk_budget"
                        ),
                    }))
                    continue
                if gate_reason is not None:
                    events.append(LiveDecision(
                        "REJECTEDDECISION",
                        {**vehicle_candidate, "outcome": "risk rejection", "reason": gate_reason,
                          "decision_id": candidate_id},
                    ))
                    continue
                order = OrderIntent(candidate_id + f":{vehicle}", futures.instrument, side, quantity,
                                    reason="live_policy_accepted", cell=cell.name, stop_price=stop,
                                    exit_mode=cell_policy.exit_mode.value, entry_bar=sequence,
                    role=OrderRole.ENTRY, vehicle=vehicle)
                if risk_allowance > 0:
                    self.portfolio.reserve_scoped_risk(
                        order.client_order_id,
                        scope=risk_scope,
                        allowance=risk_allowance,
                        amount=risk_per_lot * quantity,
                    )
                events.append(LiveDecision("ACCEPTEDDECISION", {
                    **vehicle_candidate, "outcome": "accepted", "reason": "eligible",
                }, order=order))
                self.risk_gate.record_entry(
                    cell=cell.name, direction=direction, quantity=quantity,
                    date=bundle.trading_date, bar=sequence,
                )
        return tuple(events)

    def record_exit(self, *, cell: str, reason: str, entry_bar: int,
                    exit_bar: int, date: str, vehicle: str = "futures",
                    direction: str | None = None, quantity: int = 0) -> None:
        """Apply a completed exit to the replay/live risk state."""
        self.risk_gate.record_exit(
            cell=cell, reason=reason, entry_bar=entry_bar, exit_bar=exit_bar,
            date=date, direction=direction, quantity=quantity,
        )

    def cancel_entry(self, *, cell: str, direction: str, quantity: int,
                     date: str, vehicle: str = "futures") -> None:
        self.risk_gate.cancel_entry(cell=cell, direction=direction, quantity=quantity, date=date)

    def risk_snapshot(self) -> dict[str, object]:
        """Return JSON-safe gate state for persistence across segments."""
        return {
            "shared": self.risk_gate.snapshot(),
        }

    def restore_risk_snapshot(self, snapshot: dict[str, object]) -> None:
        shared = snapshot.get("shared", snapshot)
        if isinstance(shared, Mapping):
            self.risk_gate.restore(shared)


__all__ = ["IndependentLiveDecisionEngine", "LiveDecision"]
