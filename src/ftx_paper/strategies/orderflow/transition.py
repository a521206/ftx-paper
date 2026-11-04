"""Shared chronological owner for live and bounded orderflow transitions.

This module is deliberately an orchestration boundary.  Policy remains in the
canonical-compatible coordinators; this owner only gives live and replay one
mutable clock, one interaction owner, and one execution owner.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields, is_dataclass
from copy import deepcopy
from datetime import datetime
from enum import Enum
import math
from types import SimpleNamespace
from typing import Any, Mapping, Sequence

from ftx_paper.execution.events import ExecutionNotification

from .coordinator import OrderflowAdvance, OrderflowClockInput, OrderflowCoordinator
from .execution import (
    AdmissionStatus,
    EntryOrder,
    EntryReservation,
    ExecutionAdmission,
    ExecutionCoordinator,
    ExecutionEngine,
    ExecutionState,
    ExitOrder,
    Fill,
    IntentStatus,
    ReservationStatus,
)
from .contextual import build_context_scores
from .hierarchy import (
    ContextObservation,
    ContextVersionRef,
    ContextRelation,
    EntryPolicy,
    ExitIntent,
    IntentStatus as HierarchyIntentStatus,
    MarketContext,
    Permission,
    PositionManagement,
    PositionStatus,
    RegimePermission,
    ResolvedExitRule,
    StructuralSetup,
    TradeIntent,
    TriggerObservation,
)
from .lifecycle import EvidenceRef, LifecycleEvent, SetupOpportunity
from .opportunity_coordinator import LifecycleFormation, OpportunityCoordinator
from .opportunity_timeout import advance_opportunity_timeout
from .state_machine_types import (
    ExecutionConstraints,
    LocationRecord,
    LocationRegistry,
    LocationStatus,
    MarketContextStore,
    ReplayContext,
)
from .location_geometry import LocationBand
from .interactions import (
    CandleClassification,
    CloseSide,
    DepartureDirection,
    InteractionCandle,
    InteractionCloseReason,
    InteractionSnapshot,
    InteractionStatus,
    ProjectionSnapshot,
)
from .orderflow_common import Aggregate
from .templates import Direction, LocationTemplate, TRADE_TEMPLATES
from .timestamps import coerce_timestamp, nifty_label, serialize_utc


def _snapshot_value(value: Any) -> Any:
    """Convert canonical mutable state to deterministic persistence values."""
    if isinstance(value, datetime):
        return serialize_utc(value)
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {
            item.name: _snapshot_value(getattr(value, item.name))
            for item in fields(value)
        }
    if isinstance(value, Mapping):
        return {str(key): _snapshot_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_snapshot_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_snapshot_value(item) for item in value), key=str)
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("canonical state contains a non-finite number")
    return value


def _context_snapshot(context: ReplayContext) -> dict[str, Any]:
    store = context.market_contexts
    return {
        "versions": _snapshot_value(store.versions),
        "ordinary_context": _snapshot_value(store.ordinary_context),
        "interaction_contexts": _snapshot_value(store.interaction_contexts),
    }


def _location_snapshot(registry: LocationRegistry) -> dict[str, Any]:
    return {
        "stream_id": registry.stream_id,
        "records": _snapshot_value(registry.records),
        "aliases": _snapshot_value(registry.aliases),
    }


def _execution_claims_snapshot(engine: ExecutionEngine) -> dict[str, Any]:
    return {
        "entry_owners": [
            {"key": list(key), "owner": owner}
            for key, owner in engine._entry_owners.items()
        ],
        "execution_group_claims": _snapshot_value(engine._execution_group_claims),
        "position_owners": _snapshot_value(engine._position_owners),
        "position_transfer_claimed": _snapshot_value(engine._position_transfer_claimed),
    }


def _restore_context_store(raw: Mapping[str, Any]) -> MarketContextStore:
    store = MarketContextStore()
    versions = raw.get("versions", {})
    if not isinstance(versions, Mapping):
        raise ValueError("canonical context versions are invalid")
    for context_id, values in versions.items():
        if not isinstance(values, list):
            raise ValueError("canonical context versions must be lists")
        for value in values:
            if not isinstance(value, Mapping):
                raise ValueError("canonical context version is invalid")
            store.publish(MarketContext(
                context_id=str(value["context_id"]),
                version=str(value["version"]),
                timestamp=coerce_timestamp(value["timestamp"]),
                available_at=coerce_timestamp(value["available_at"]),
                location=value.get("location"),
                market_state=value.get("market_state"),
                long_score=value.get("long_score"),
                short_score=value.get("short_score"),
                evidence=tuple(value.get("evidence", ())),
                source_timeframe=str(value.get("source_timeframe", "5m")),
                parent_context_id=value.get("parent_context_id"),
                definition_version=str(value.get("definition_version", "1")),
                source_provenance=tuple(value.get("source_provenance", ())),
            ))
    ordinary = raw.get("ordinary_context")
    if isinstance(ordinary, Mapping):
        store.ordinary_context = ContextVersionRef(
            str(ordinary["context_id"]), str(ordinary["version"]),
        )
    interactions = raw.get("interaction_contexts", {})
    if isinstance(interactions, Mapping):
        store.interaction_contexts = {
            str(key): ContextVersionRef(
                str(value["context_id"]), str(value["version"]),
            )
            for key, value in interactions.items()
            if isinstance(value, Mapping)
        }
    return store


def _restore_location_registry(raw: Mapping[str, Any]) -> LocationRegistry:
    registry = LocationRegistry(str(raw.get("stream_id", "orderflow:paper")))
    records = raw.get("records", {})
    if not isinstance(records, Mapping):
        raise ValueError("canonical location records are invalid")
    for value in records.values():
        if not isinstance(value, Mapping):
            raise ValueError("canonical location record is invalid")
        band = value.get("band", {})
        if not isinstance(band, Mapping):
            raise ValueError("canonical location band is invalid")
        registry.register(LocationRecord(
            location_id=str(value["location_id"]),
            alias=str(value["alias"]),
            price=float(value["price"]),
            source_session=str(value["source_session"]),
            created_at=coerce_timestamp(value["created_at"]),
            available_at=coerce_timestamp(value["available_at"]),
            stream_id=registry.stream_id,
            band=LocationBand(float(band["low"]), float(band["high"])),
            generation=int(value.get("generation", 1)),
            status=LocationStatus(value.get("status", LocationStatus.ACTIVE)),
            frozen_atr=value.get("frozen_atr"),
            parent_extreme_generation=value.get("parent_extreme_generation"),
            source_aggregate_id=value.get("source_aggregate_id"),
            source_observation_id=value.get("source_observation_id"),
            timeframe=str(value.get("timeframe", "")),
            source_method=value.get("source_method"),
        ))
    return registry


def _restore_aggregate(raw: Mapping[str, Any] | None) -> Aggregate | None:
    if raw is None:
        return None
    return Aggregate(
        time=coerce_timestamp(raw["time"]),
        end_time=coerce_timestamp(raw["end_time"]),
        available_at=coerce_timestamp(raw["available_at"]),
        timeframe=str(raw["timeframe"]),
        source_bar_count=int(raw["source_bar_count"]),
        open=float(raw["open"]), high=float(raw["high"]),
        low=float(raw["low"]), close=float(raw["close"]),
        volume=int(raw["volume"]), symbol=str(raw["symbol"]),
        expiry=str(raw.get("expiry", "")),
        session_date=str(raw["session_date"]),
        source_fingerprint=str(raw["source_fingerprint"]),
    )


def _restore_interaction_snapshot(raw: Mapping[str, Any]) -> InteractionSnapshot:
    creation = _restore_aggregate(raw.get("creation_candle"))
    if creation is None:
        raise ValueError("canonical interaction is missing its creation candle")
    candles = tuple(
        InteractionCandle(
            candle=candle,
            classification=CandleClassification(item["classification"]),
            is_terminal=bool(item.get("is_terminal", False)),
        )
        for item in raw.get("candles", ())
        if isinstance(item, Mapping)
        for candle in (_restore_aggregate(item.get("candle")),)
        if candle is not None
    )
    raw_projections = raw.get("synthetic_snapshots", {})
    projections = tuple(
        (
            str(timeframe),
            tuple(
                ProjectionSnapshot(
                    timeframe=str(item["timeframe"]),
                    open=float(item["open"]), high=float(item["high"]),
                    low=float(item["low"]), close=float(item["close"]),
                    volume=float(item["volume"]),
                    source_candle_ids=tuple(item.get("source_candle_ids", ())),
                    first_source_start=coerce_timestamp(item["first_source_start"]),
                    last_source_end=coerce_timestamp(item["last_source_end"]),
                    available_at=coerce_timestamp(item["available_at"]),
                )
                for item in values if isinstance(item, Mapping)
            ),
        )
        for timeframe, values in raw_projections.items()
    ) if isinstance(raw_projections, Mapping) else ()
    band = raw.get("band")
    if not isinstance(band, Mapping):
        raise ValueError("canonical interaction is missing its band")
    return InteractionSnapshot(
        interaction_id=str(raw["interaction_id"]),
        instrument=str(raw["instrument"]), session_date=str(raw["session_date"]),
        location=str(raw["location"]), timeframe=str(raw["timeframe"]),
        template_version=str(raw["template_version"]), creation_candle=creation,
        reference_level=float(raw["reference_level"]),
        band=LocationBand(float(band["low"]), float(band["high"])),
        pre_contact_side=(CloseSide(raw["pre_contact_side"])
                          if raw.get("pre_contact_side") is not None else None),
        interaction_started_at=coerce_timestamp(raw["interaction_started_at"]),
        available_at=coerce_timestamp(raw["available_at"]),
        reference_generation=int(raw["reference_generation"]),
        sessions_carried=int(raw["sessions_carried"]),
        status=InteractionStatus(raw["status"]),
        departure_direction=(DepartureDirection(raw["departure_direction"])
                             if raw.get("departure_direction") is not None else None),
        terminal_close_side=(CloseSide(raw["terminal_close_side"])
                             if raw.get("terminal_close_side") is not None else None),
        terminal_candle_id=raw.get("terminal_candle_id"),
        close_reason=(InteractionCloseReason(raw["close_reason"])
                      if raw.get("close_reason") is not None else None),
        candles=candles, synthetic_snapshots=projections,
        pd_role=raw.get("pd_role"),
    )


def _restore_namespace(value: Any) -> Any:
    if isinstance(value, Mapping):
        restored = {
            str(key): _restore_namespace(item) for key, item in value.items()
        }
        for key in ("failed_reversal_locked",):
            if key in restored and isinstance(restored[key], list):
                restored[key] = set(restored[key])
        return SimpleNamespace(**restored)
    if isinstance(value, list):
        return [_restore_namespace(item) for item in value]
    return value


def _restore_evidence_ref(raw: Mapping[str, Any] | None) -> EvidenceRef | None:
    if raw is None:
        return None
    return EvidenceRef(
        evidence_id=str(raw["evidence_id"]), timeframe=str(raw["timeframe"]),
        observed_at=coerce_timestamp(raw["observed_at"]),
        available_at=coerce_timestamp(raw["available_at"]),
        source_fingerprint=str(raw["source_fingerprint"]),
        source_bar_count=int(raw["source_bar_count"]),
    )


def _restore_template(raw: Mapping[str, Any] | None) -> Any | None:
    if raw is None:
        return None
    template_id = raw.get("template_id")
    for template in TRADE_TEMPLATES.values():
        if template.template_id == template_id:
            return template
    raise ValueError(f"unknown canonical trade template: {template_id!r}")


def _restore_context_observation(raw: Mapping[str, Any] | None) -> ContextObservation | None:
    if raw is None:
        return None
    return ContextObservation(
        context_id=str(raw["context_id"]),
        context_version=str(raw["context_version"]),
        observed_at=coerce_timestamp(raw["observed_at"]),
        available_at=coerce_timestamp(raw["available_at"]),
        relation=ContextRelation(raw["relation"]),
        source_timeframe=str(raw["source_timeframe"]),
        evidence=dict(raw.get("evidence", {})),
    )


def _restore_context_ref(raw: Mapping[str, Any] | None) -> ContextVersionRef | None:
    if raw is None:
        return None
    return ContextVersionRef(str(raw["context_id"]), str(raw["version"]))


def _restore_exit_intent(raw: Mapping[str, Any] | None) -> ExitIntent | None:
    if raw is None:
        return None
    return ExitIntent(
        intent_id=str(raw["intent_id"]), position_id=str(raw["position_id"]),
        rule_id=str(raw["rule_id"]), location=str(raw["location"]),
        status=HierarchyIntentStatus(raw["status"]),
        created_at=coerce_timestamp(raw["created_at"]),
        expiry=coerce_timestamp(raw["expiry"]),
        interaction_id=raw.get("interaction_id"),
        formation_context=_restore_context_ref(raw.get("formation_context")),
        decision_context=_restore_context_ref(raw.get("decision_context")),
        cancel_reason=raw.get("cancel_reason"),
        triggered_at=(coerce_timestamp(raw["triggered_at"])
                      if raw.get("triggered_at") else None),
    )


def _restore_exit_plan(raw: Sequence[Any]) -> tuple[ResolvedExitRule, ...]:
    return tuple(
        ResolvedExitRule(
            rule_id=str(item["rule_id"]), location=str(item["location"]),
            mode=str(item["mode"]), level=item.get("level"),
            source_context=_restore_context_ref(item.get("source_context")),
            interaction_id=item.get("interaction_id"), response=item.get("response"),
            execution=item.get("execution"), score_direction=item.get("score_direction"),
            score_margin=item.get("score_margin"), required=bool(item.get("required", False)),
            trigger_policy=str(item.get("trigger_policy", "touch")),
        )
        for item in raw if isinstance(item, Mapping)
    )


def _restore_opportunity(raw: Mapping[str, Any]) -> SetupOpportunity:
    parent = raw.get("parent_context")
    parent_ref = (
        ContextVersionRef(str(parent["context_id"]), str(parent["version"]))
        if isinstance(parent, Mapping) else None
    )
    raw_events = raw.get("_events", ())
    events = tuple(
        LifecycleEvent(
            event_id=str(item["event_id"]), lifecycle_id=str(item["lifecycle_id"]),
            sequence=int(item["sequence"]), kind=str(item["kind"]),
            observed_at=coerce_timestamp(item["observed_at"]),
            available_at=coerce_timestamp(item["available_at"]),
            effective_at=(coerce_timestamp(item["effective_at"])
                          if item.get("effective_at") is not None else None),
            payload=dict(item.get("payload", {})),
        )
        for item in raw_events if isinstance(raw_events, list) and isinstance(item, Mapping)
    )
    response_evidence = tuple(
        aggregate for item in raw.get("response_evidence", ())
        if isinstance(item, Mapping)
        for aggregate in (_restore_aggregate(item),) if aggregate is not None
    )
    return SetupOpportunity(
        lifecycle_id=str(raw["lifecycle_id"]), direction=Direction(raw["direction"]),
        session_date=str(raw["session_date"]), instrument=str(raw["instrument"]),
        strategy=str(raw["strategy"]), formation_at=coerce_timestamp(raw["formation_at"]),
        formation_available_at=coerce_timestamp(raw["formation_available_at"]),
        candidate_id=str(raw["candidate_id"]), parent_context=parent_ref,
        location=(LocationTemplate(raw["location"]) if raw.get("location") is not None else None),
        reference_level=raw.get("reference_level"),
        context_candle=_restore_aggregate(raw.get("context_candle")),
        breakout_level=float(raw["breakout_level"]),
        failure_boundary=float(raw["failure_boundary"]),
        context_candle_lineage=_restore_evidence_ref(raw.get("context_candle_lineage")),
        origin_evidence_id=raw.get("origin_evidence_id"),
        response_evidence_ids=tuple(raw.get("response_evidence_ids", ())),
        response_evidence=response_evidence,
        response_evidence_lineage=tuple(
            ref for item in raw.get("response_evidence_lineage", ())
            if isinstance(item, Mapping)
            for ref in (_restore_evidence_ref(item),) if ref is not None
        ),
        policy_version=raw.get("policy_version"),
        reference_generation=int(raw.get("reference_generation", 0)),
        causal_identity=raw.get("causal_identity"),
        location_interaction_id=raw.get("location_interaction_id"),
        opening_interaction_kind=raw.get("opening_interaction_kind"),
        opening_interaction_evidence=list(raw.get("opening_interaction_evidence", ())),
        opening_policy_version=raw.get("opening_policy_version"),
        template_id=raw.get("template_id"), status=raw.get("status", "FORMED"),
        response_episode_id=raw.get("response_episode_id"),
        episode_confirmed=bool(raw.get("episode_confirmed", False)),
        execution_group_id=raw.get("execution_group_id"),
        clock=coerce_timestamp(raw["clock"]) if raw.get("clock") else None,
        decision_context_required=bool(raw.get("decision_context_required", True)),
        timeout_limit=int(raw.get("timeout_limit", 6)),
        timeout_count=int(raw.get("timeout_count", 0)),
        timeout_evidence_timeframe=str(raw.get("timeout_evidence_timeframe", "5m")),
        timeout_counted_source_ids=list(raw.get("timeout_counted_source_ids", ())),
        timeout_counted_snapshot_ids=list(raw.get("timeout_counted_snapshot_ids", ())),
        retest_touched=bool(raw.get("retest_touched", False)),
        retest_confirmed=bool(raw.get("retest_confirmed", False)), _events=events,
    )


def _restore_setup(raw: Mapping[str, Any] | None) -> StructuralSetup | None:
    if raw is None:
        return None
    parent_context = raw.get("parent_context_id")
    exit_plan = tuple(
        ResolvedExitRule(
            rule_id=str(item["rule_id"]), location=str(item["location"]),
            mode=str(item["mode"]), level=item.get("level"),
            source_context=(
                ContextVersionRef(str(item["source_context"]["context_id"]),
                                  str(item["source_context"]["version"]))
                if isinstance(item.get("source_context"), Mapping) else None
            ),
            interaction_id=item.get("interaction_id"), response=item.get("response"),
            execution=item.get("execution"), score_direction=item.get("score_direction"),
            score_margin=item.get("score_margin"), required=bool(item.get("required", False)),
            trigger_policy=str(item.get("trigger_policy", "touch")),
        )
        for item in (raw.get("exit_plan") or ()) if isinstance(item, Mapping)
    )
    return StructuralSetup(
        setup_id=str(raw["setup_id"]), version=str(raw["version"]),
        regime_version=str(raw["regime_version"]), direction=str(raw["direction"]),
        kind=str(raw["kind"]), entry_zone_low=float(raw["entry_zone_low"]),
        entry_zone_high=float(raw["entry_zone_high"]),
        structural_reference=float(raw["structural_reference"]),
        sweep_extreme=raw.get("sweep_extreme"), failure_boundary=float(raw["failure_boundary"]),
        expires_at=coerce_timestamp(raw["expires_at"]), active=bool(raw.get("active", True)),
        required_confirmation=tuple(raw.get("required_confirmation", ())),
        parent_context_id=parent_context,
        parent_context_version=raw.get("parent_context_version"),
        context_relation=ContextRelation(raw.get("context_relation", ContextRelation.CONFIRMED)),
        context_reference_level=raw.get("context_reference_level"),
        lifecycle_id=raw.get("lifecycle_id"),
        template=_restore_template(raw.get("template")),
        exit_plan=exit_plan,
    )


def _restore_regime(raw: Mapping[str, Any] | None) -> RegimePermission | None:
    if raw is None:
        return None
    return RegimePermission(
        version=str(raw["version"]), context=str(raw["context"]),
        long=Permission(raw["long"]), short=Permission(raw["short"]),
        restrictions=tuple(raw.get("restrictions", ())),
        opening_gap_state=raw.get("opening_gap_state"),
        opening_gap_direction=raw.get("opening_gap_direction"),
    )


def _restore_execution_state(raw: Mapping[str, Any]) -> ExecutionState:
    admission = raw.get("admission", {})
    reservation = raw.get("reservation")
    pending_entry = raw.get("pending_entry")
    pending_exit = raw.get("pending_exit")
    intent = raw.get("intent")
    position = raw.get("position")
    restored_intent = None
    if isinstance(intent, Mapping):
        restored_intent = TradeIntent(
            intent_id=str(intent["intent_id"]), setup_id=str(intent["setup_id"]),
            regime_version=str(intent["regime_version"]), direction=str(intent["direction"]),
            status=HierarchyIntentStatus(intent["status"]),
            created_at=coerce_timestamp(intent["created_at"]), trigger_price=float(intent["trigger_price"]),
            failure_boundary=float(intent["failure_boundary"]), expiry=coerce_timestamp(intent["expiry"]),
            confirmation=tuple(intent.get("confirmation", ())),
            cancel_reason=intent.get("cancel_reason"),
            triggered_at=(coerce_timestamp(intent["triggered_at"]) if intent.get("triggered_at") else None),
            setup_version=intent.get("setup_version"),
            entry_policy=EntryPolicy(intent.get("entry_policy", "intrabar_cross")),
            required_confirmation=tuple(intent.get("required_confirmation", ())),
            armed_at=(coerce_timestamp(intent["armed_at"]) if intent.get("armed_at") else None),
            effective_at=(coerce_timestamp(intent["effective_at"]) if intent.get("effective_at") else None),
            filled_at=(coerce_timestamp(intent["filled_at"]) if intent.get("filled_at") else None),
            lifecycle_id=intent.get("lifecycle_id"),
            formation_context=_restore_context_ref(intent.get("formation_context")),
            decision_context=_restore_context_ref(intent.get("decision_context")),
        )
    restored_admission = ExecutionAdmission(
        status=AdmissionStatus(admission.get("status", AdmissionStatus.NOT_EVALUATED)),
        reason=admission.get("reason"), decided_at=admission.get("decided_at"),
        intent_id=admission.get("intent_id"),
    ) if isinstance(admission, Mapping) else ExecutionAdmission()
    restored_reservation = None
    if isinstance(reservation, Mapping):
        restored_reservation = EntryReservation(
            reservation_id=str(reservation["reservation_id"]),
            opportunity_id=str(reservation["opportunity_id"]),
            direction=str(reservation["direction"]),
            session_date=str(reservation["session_date"]),
            reserved_at=str(reservation["reserved_at"]),
            status=ReservationStatus(reservation.get("status", "ACTIVE")),
            released_at=reservation.get("released_at"),
            release_reason=reservation.get("release_reason"),
        )
    restored_entry = None
    if isinstance(pending_entry, Mapping) and restored_intent is not None:
        restored_entry = EntryOrder(
            intent=restored_intent, available_at=str(pending_entry["available_at"]),
            fill_price=float(pending_entry["fill_price"]),
            session_date=pending_entry.get("session_date"),
        )
    restored_exit = None
    if isinstance(pending_exit, Mapping):
        restored_exit = ExitOrder(
            reason=str(pending_exit["reason"]), available_at=str(pending_exit["available_at"]),
            requested_price=pending_exit.get("requested_price"),
            execution_policy=str(pending_exit.get("execution_policy", "next_available_bar_open")),
            quantity=pending_exit.get("quantity"), session_date=pending_exit.get("session_date"),
            position_id=pending_exit.get("position_id"),
        )
    restored_position = None
    if isinstance(position, Mapping):
        restored_position = PositionManagement(
            position_id=str(position["position_id"]), intent_id=str(position["intent_id"]),
            setup_id=str(position["setup_id"]), regime_version=str(position["regime_version"]),
            direction=str(position["direction"]), status=PositionStatus(position["status"]),
            failure_boundary=float(position["failure_boundary"]),
            initial_risk_points=float(position["initial_risk_points"]),
            protective_stop=position.get("protective_stop"), target=position.get("target"),
            closed_at=position.get("closed_at"), exit_reason=position.get("exit_reason"),
            fill_price=position.get("fill_price"), quantity=float(position.get("quantity", 1.0)),
            lifecycle_id=position.get("lifecycle_id"),
            exit_plan=_restore_exit_plan(position.get("exit_plan", ())),
            exit_intent=_restore_exit_intent(position.get("exit_intent")),
            partial_exit_taken=bool(position.get("partial_exit_taken", False)),
            post_3r_activated=bool(position.get("post_3r_activated", False)),
        )
    fills = tuple(
        Fill(str(item["fill_id"]), str(item["opportunity_id"]), str(item["kind"]),
             float(item["quantity"]), float(item["price"]), str(item["filled_at"]),
             float(item.get("cost", 0.0)))
        for item in raw.get("fills", ()) if isinstance(item, Mapping)
    )
    return ExecutionState(
        opportunity_id=str(raw["opportunity_id"]), setup=_restore_setup(raw.get("setup")),
        setup_available_at=raw.get("setup_available_at"),
        exit_plan_frozen=bool(raw.get("exit_plan_frozen", False)), intent=restored_intent,
        admission=restored_admission, reservation=restored_reservation,
        pending_entry=restored_entry, pending_exit=restored_exit, position=restored_position,
        entry_at=raw.get("entry_at"), exit_at=raw.get("exit_at"),
        exit_price=raw.get("exit_price"), realized_points=raw.get("realized_points"),
        admission_history=tuple(
            ExecutionAdmission(
                status=AdmissionStatus(item.get("status", AdmissionStatus.NOT_EVALUATED)),
                reason=item.get("reason"), decided_at=item.get("decided_at"),
                intent_id=item.get("intent_id"),
            )
            for item in raw.get("admission_history", ()) if isinstance(item, Mapping)
        ),
        reservation_history=tuple(
            EntryReservation(
                reservation_id=str(item["reservation_id"]),
                opportunity_id=str(item["opportunity_id"]), direction=str(item["direction"]),
                session_date=str(item["session_date"]), reserved_at=str(item["reserved_at"]),
                status=ReservationStatus(item.get("status", "ACTIVE")),
                released_at=item.get("released_at"), release_reason=item.get("release_reason"),
            )
            for item in raw.get("reservation_history", ()) if isinstance(item, Mapping)
        ),
        fills=list(fills), entry_attempted=bool(raw.get("entry_attempted", False)),
        entry_aliases=tuple(raw.get("entry_aliases", ())),
        retest_touched=bool(raw.get("retest_touched", False)),
        retest_touch_observed_at=raw.get("retest_touch_observed_at"),
        retest_touch_available_at=raw.get("retest_touch_available_at"),
        retest_touch_evidence=raw.get("retest_touch_evidence"),
    )


def _execution_trace_state(state: Any) -> dict[str, Any]:
    position = state.position
    pending = state.pending_exit
    intent = state.intent

    def value(item: Any) -> Any:
        return getattr(item, "value", item)

    return {
        "intent": {
            "status": value(intent.status),
            "triggered_at": getattr(intent, "triggered_at", None),
        } if intent is not None else None,
        "pending_exit": {
            "reason": pending.reason,
            "available_at": pending.available_at,
            "requested_price": pending.requested_price,
            "execution_policy": pending.execution_policy,
            "quantity": pending.quantity,
        } if pending is not None else None,
        "position": {
            "status": value(position.status),
            "quantity": position.quantity,
            "fill_price": position.fill_price,
            "failure_boundary": position.failure_boundary,
            "protective_stop": position.protective_stop,
            "post_3r_activated": position.post_3r_activated,
            "partial_exit_taken": position.partial_exit_taken,
        } if position is not None else None,
    }


@dataclass(frozen=True, slots=True)
class TransitionAdvance:
    """Facts emitted by one idempotent shared transition."""

    observation: OrderflowAdvance
    execution: Mapping[str, tuple[dict[str, Any], ...]]


@dataclass(slots=True)
class OrderflowTransitionOwner:
    """Own the single chronological state boundary used by both modes."""

    location_registry: LocationRegistry
    observation_owner: OrderflowCoordinator = field(init=False)
    opportunity_owner: OpportunityCoordinator = field(init=False)
    execution_owner: ExecutionCoordinator = field(default_factory=ExecutionCoordinator)
    execution_adapter: Any | None = None
    rows_by_date: Mapping[str, Sequence[Mapping[str, object]]] = field(default_factory=dict, repr=False)
    atr_by_session: Mapping[str, float | None] | None = field(default=None, repr=False)
    context: ReplayContext = field(init=False)
    mode: str = "FULL"
    _clock: datetime | None = field(default=None, init=False, repr=False)
    _published_intents: set[str] = field(default_factory=set, init=False, repr=False)
    _execution_notifications: set[str] = field(default_factory=set, init=False, repr=False)

    @staticmethod
    def _intent_allows_requalification(state: Any) -> bool:
        intent = state.intent
        if intent is None:
            return state.admission.status is AdmissionStatus.NOT_EVALUATED
        return (
            intent.status is IntentStatus.CANCELLED
            and intent.cancel_reason in {
                "opening_gap_permission_changed",
                "decision_context_unavailable",
                "decision_context_scores_unavailable",
                "fill_permission_mismatch",
                "active_position_session_extreme_gate",
            }
        )

    def __post_init__(self) -> None:
        self.observation_owner = OrderflowCoordinator(self.location_registry)
        self.context = ReplayContext(
            rows_by_date=self.rows_by_date,
            atr_by_session=self.atr_by_session or {},
            location_registry=self.location_registry,
            execution_engine=ExecutionEngine(),
        )
        # ReplayContext is the canonical mutable execution owner.  The
        # coordinator is only a service facade over that same instance.
        self.execution_owner.engine = self.context.execution_engine
        self.execution_owner.machine = self
        from .opportunity_replay_host import build_opportunity_coordinator
        self.opportunity_owner = build_opportunity_coordinator(
            self,
            detector=self.observation_owner.detector,
        )

    def promote_episode(
        self,
        life: Any,
        *,
        observed_at: str,
        available_at: str,
        bar_events: list[dict[str, Any]],
    ) -> None:
        """Promote a provisional lifecycle after its final trigger checks."""
        if life.episode_confirmed:
            return
        life.episode_confirmed = True
        for competing in self.context.conflicting_unfilled_opportunities(life):
            if competing.active:
                self.execution_owner.invalidate_opportunity(
                    competing,
                    timestamp=observed_at,
                    observed_at=observed_at,
                    available_at=available_at,
                    bar_events=bar_events,
                    reason="opposing_episode_confirmed",
                )
        event = {
            "kind": "episode_confirmed",
            "lifecycle_id": life.lifecycle_id,
            "observed_at": observed_at,
            "available_at": available_at,
            "direction": life.direction.value,
            "conflict_key": life.conflict_key,
            "owner": "1m",
            "evidence_timeframe": "1m",
        }
        self.context.record_event(event)
        bar_events.append(event)

    @property
    def clock(self) -> datetime | None:
        return self._clock

    @property
    def parity_streams(self) -> dict[str, list[dict[str, Any]]]:
        return self.context.parity_snapshot()

    def snapshot(self) -> dict[str, Any]:
        """Return the complete canonical mutable state and producer prefix."""
        context = self.context
        return {
            "schema_version": 2,
            "clock": serialize_utc(self._clock) if self._clock is not None else None,
            "canonical_state": {
                "source": {
                    "rows": _snapshot_value(context.rows),
                    "rows_by_date": _snapshot_value(context.rows_by_date),
                    "prior_session_rows": _snapshot_value(context.prior_session_rows),
                    "atr_by_session": _snapshot_value(context.atr_by_session),
                    "aggregates": _snapshot_value(context.aggregates),
                    "current_session_date": context.current_session_date,
                },
                "context_store": _context_snapshot(context),
                "location_registry": _location_snapshot(self.location_registry),
                "interactions": _snapshot_value(context.interactions),
                "opportunities": _snapshot_value(context.opportunities),
                "execution_states": _snapshot_value(context.execution_engine.snapshot()),
                "execution_claims": _execution_claims_snapshot(context.execution_engine),
                "execution_proposals": _snapshot_value(context.execution_proposals),
                "regime": _snapshot_value(context.regime),
                "execution_constraints": _snapshot_value(context.execution_constraints),
                "opening_context": _snapshot_value(context.opening_context),
                "continuous": _snapshot_value(context.continuous),
                "session": _snapshot_value(context.session),
                "regime_gate": _snapshot_value(context.regime_gate),
                "market_state": context.market_state,
                "last_confirmation_context_time": _snapshot_value(
                    context.last_confirmation_context_time,
                ),
                "child_observation": _snapshot_value(context.child_observation),
                "regime_gate_blocked_lifecycles": _snapshot_value(
                    context.regime_gate_blocked_lifecycles,
                ),
                "session_extreme_gate_blocked_lifecycles": _snapshot_value(
                    context.session_extreme_gate_blocked_lifecycles,
                ),
                "pending_bar_events": _snapshot_value(context.pending_bar_events),
                "decision_context_evidence": _snapshot_value(context.decision_context_evidence),
                "opportunity_identities": _snapshot_value(context.opportunity_identities),
                "published_resolved_responses": _snapshot_value(
                    context.published_resolved_responses,
                ),
                "opportunity_outcomes": _snapshot_value(context.opportunity_outcomes),
                "or_seed_candles": _snapshot_value(context.or_seed_candles),
                "setup_number": context.setup_number,
                "neutral_event_ordinals": _snapshot_value(context.neutral_event_ordinals),
                "neutral_keys_by_causal_id": _snapshot_value(
                    context.neutral_keys_by_causal_id,
                ),
                "published_intents": sorted(self._published_intents),
                "execution_notifications": sorted(self._execution_notifications),
            },
            "events": deepcopy(context.events),
            "transitions": deepcopy(context.transitions),
            "parity_streams": deepcopy(context.parity_streams),
            "parity_observed": sorted(context.parity_observed),
            "parity_layers": deepcopy(context.parity_layers),
            "execution_diagnostics": deepcopy(context.execution_diagnostics),
        }

    def restore_snapshot(self, snapshot: Mapping[str, Any]) -> None:
        """Restore producer-owned facts and the transition clock idempotently."""
        if snapshot.get("schema_version") not in {1, 2}:
            raise ValueError("unsupported orderflow snapshot schema")
        raw_clock = snapshot.get("clock")
        if raw_clock is not None and not isinstance(raw_clock, str):
            raise ValueError("orderflow snapshot clock must be a string or null")
        for name in ("events", "transitions", "parity_streams", "parity_layers", "execution_diagnostics"):
            value = snapshot.get(name, [] if name in {"events", "transitions", "execution_diagnostics"} else {})
            if not isinstance(value, (list, dict)):
                raise ValueError(f"orderflow snapshot {name} is invalid")
        observed = snapshot.get("parity_observed", [])
        if not isinstance(observed, list) or any(not isinstance(item, str) for item in observed):
            raise ValueError("orderflow snapshot parity_observed is invalid")
        self._clock = coerce_timestamp(raw_clock) if raw_clock else None
        self.context.events = deepcopy(snapshot.get("events", []))
        self.context.transitions = deepcopy(snapshot.get("transitions", []))
        self.context.parity_streams = deepcopy(snapshot.get("parity_streams", {}))
        self.context.parity_observed = set(observed)
        self.context.parity_layers = deepcopy(snapshot.get("parity_layers", {}))
        self.context.execution_diagnostics = deepcopy(snapshot.get("execution_diagnostics", []))
        canonical = snapshot.get("canonical_state")
        if not isinstance(canonical, Mapping):
            return
        source = canonical.get("source")
        if isinstance(source, Mapping):
            if isinstance(source.get("rows"), list):
                self.context.rows = deepcopy(source["rows"])
            if isinstance(source.get("rows_by_date"), Mapping):
                self.context.rows_by_date = deepcopy(source["rows_by_date"])
            if isinstance(source.get("prior_session_rows"), list):
                self.context.prior_session_rows = tuple(
                    tuple(dict(item) for item in values)
                    for values in source["prior_session_rows"]
                    if isinstance(values, list)
                )
            if isinstance(source.get("atr_by_session"), Mapping):
                self.context.atr_by_session = deepcopy(source["atr_by_session"])
            if isinstance(source.get("aggregates"), Mapping):
                self.context.aggregates = deepcopy(source["aggregates"])
            current_session = source.get("current_session_date")
            self.context.current_session_date = (
                str(current_session) if current_session is not None else None
            )
        context_store = canonical.get("context_store")
        if isinstance(context_store, Mapping):
            self.context.market_contexts = _restore_context_store(context_store)
        location = canonical.get("location_registry")
        if isinstance(location, Mapping):
            restored_registry = _restore_location_registry(location)
            self.location_registry = restored_registry
            self.context.location_registry = restored_registry
            self.observation_owner.location_registry = restored_registry
        for name in (
            "pending_bar_events", "decision_context_evidence",
            "opportunity_identities", "opportunity_outcomes", "neutral_event_ordinals",
            "neutral_keys_by_causal_id",
        ):
            value = canonical.get(name)
            if value is not None:
                setattr(self.context, name, deepcopy(value))
        raw_interactions = canonical.get("interactions")
        if isinstance(raw_interactions, Mapping):
            self.context.interactions = {
                str(key): _restore_interaction_snapshot(value)
                for key, value in raw_interactions.items()
                if isinstance(value, Mapping)
            }
        raw_opportunities = canonical.get("opportunities")
        if isinstance(raw_opportunities, Mapping):
            restored_opportunities: dict[str, SetupOpportunity] = {}
            for lifecycle_id, value in raw_opportunities.items():
                if not isinstance(value, Mapping):
                    continue
                restored = _restore_opportunity(value)
                restored_opportunities[str(lifecycle_id)] = restored
                self.context.execution_engine.register(restored.lifecycle_id)
            self.context.opportunities = restored_opportunities
        raw_execution_states = canonical.get("execution_states")
        if isinstance(raw_execution_states, Mapping):
            self.context.execution_engine._states = {
                str(lifecycle_id): _restore_execution_state(value)
                for lifecycle_id, value in raw_execution_states.items()
                if isinstance(value, Mapping)
            }
        claims = canonical.get("execution_claims")
        if isinstance(claims, Mapping):
            entry_owners = claims.get("entry_owners")
            if isinstance(entry_owners, list):
                self.context.execution_engine._entry_owners = {
                    tuple(item["key"]): str(item["owner"])
                    for item in entry_owners
                    if isinstance(item, Mapping)
                    and isinstance(item.get("key"), list)
                    and len(item["key"]) == 5
                }
            for name in ("_execution_group_claims", "_position_owners"):
                value = claims.get(name[1:])
                if isinstance(value, Mapping):
                    setattr(
                        self.context.execution_engine, name,
                        {
                            str(key): tuple(item) if name == "_execution_group_claims" else str(item)
                            for key, item in value.items()
                        },
                    )
            transferred = claims.get("position_transfer_claimed")
            if isinstance(transferred, list):
                self.context.execution_engine._position_transfer_claimed = set(transferred)
        if isinstance(canonical.get("opening_context"), Mapping):
            self.context.opening_context = deepcopy(canonical["opening_context"])
        raw_proposals = canonical.get("execution_proposals")
        if isinstance(raw_proposals, list):
            self.context.execution_proposals = []
        for name in ("continuous", "session", "regime_gate"):
            if name in canonical:
                setattr(self.context, name, _restore_namespace(canonical[name]))
        regime = canonical.get("regime")
        if isinstance(regime, Mapping):
            self.context.regime = _restore_regime(regime)
        elif regime is None:
            self.context.regime = None
        if canonical.get("market_state") is not None:
            self.context.market_state = canonical["market_state"]
        raw_confirmation_time = canonical.get("last_confirmation_context_time")
        self.context.last_confirmation_context_time = (
            coerce_timestamp(raw_confirmation_time)
            if isinstance(raw_confirmation_time, str) else None
        )
        child_observation = canonical.get("child_observation")
        self.context.child_observation = (
            _restore_context_observation(child_observation)
            if isinstance(child_observation, Mapping) else None
        )
        for name in (
            "regime_gate_blocked_lifecycles",
            "session_extreme_gate_blocked_lifecycles",
        ):
            value = canonical.get(name)
            if isinstance(value, list):
                setattr(self.context, name, set(value))
        constraints = canonical.get("execution_constraints")
        if isinstance(constraints, Mapping):
            self.context.execution_constraints = ExecutionConstraints(
                max_open_positions=int(constraints.get("max_open_positions", 1)),
                max_session_entries=constraints.get("max_session_entries"),
                max_direction_entries=constraints.get("max_direction_entries"),
                post_trade_cooldown_minutes=int(
                    constraints.get("post_trade_cooldown_minutes", 0),
                ),
                fill_permission_margin=constraints.get("fill_permission_margin"),
                stop_atr_buffer=float(constraints.get("stop_atr_buffer", 0.25)),
                failed_reversal_lockout=bool(constraints.get("failed_reversal_lockout", False)),
            )
        if isinstance(canonical.get("published_resolved_responses"), list):
            self.context.published_resolved_responses = {
                tuple(item) for item in canonical["published_resolved_responses"]
                if isinstance(item, list) and len(item) == 2
            }
        if isinstance(canonical.get("or_seed_candles"), list):
            self.context.or_seed_candles = set(canonical["or_seed_candles"])
        if isinstance(canonical.get("setup_number"), int):
            self.context.setup_number = canonical["setup_number"]
        self._published_intents = set(canonical.get("published_intents", ()))
        self._execution_notifications = set(canonical.get("execution_notifications", ()))

    @property
    def engine(self) -> Any:
        """Expose the canonical execution state for lifecycle adapters."""
        return self.execution_owner.engine

    def attach_setup(self, formation: LifecycleFormation) -> None:
        self.execution_owner.attach_setup(formation)

    def on_execution_event(self, event: ExecutionNotification) -> None:
        """Record an account-owner notification without re-running policy."""
        identity = event.event_id or (
            f"{event.event_type}:{event.client_order_id}:"
            f"{event.occurred_at or event.available_at or ''}"
        )
        if identity in self._execution_notifications:
            return
        self._execution_notifications.add(identity)
        occurred_at = event.occurred_at or event.available_at or self._clock
        available_at = event.available_at or occurred_at
        if occurred_at is None or available_at is None:
            return
        kind = {
            "FILLED": "filled",
            "CANCELLED": "entry_cancelled",
            "REJECTED": "entry_cancelled",
        }.get(event.event_type.upper())
        if kind is None:
            return
        if any(
            fact.get("kind") == kind
            and fact.get("values", {}).get("client_order_id") == event.client_order_id
            for fact in self.context.parity_streams.get("execution_transitions", ())
        ):
            return
        self.context.record_event({
            "kind": kind,
            "identity": f"execution_notification:{identity}",
            "client_order_id": event.client_order_id,
            "status": event.status,
            "quantity": event.quantity,
            "filled_quantity": event.filled_quantity,
            "price": event.price,
            "reason": event.reason,
            "observed_at": occurred_at,
            "available_at": available_at,
            "causal_parent_id": event.client_order_id,
            "parity_stream": "execution_transitions",
        })

    def _accept_formations(
        self, formations: Sequence[LifecycleFormation],
    ) -> tuple[LifecycleFormation, ...]:
        accepted: list[LifecycleFormation] = []
        for formation in formations:
            life = formation.lifecycle
            if life is None:
                continue
            existing = self.context.opportunities.get(life.lifecycle_id)
            if existing is None and not self.context.register_opportunity(life):
                continue
            if existing is not None and existing is not life:
                continue
            if formation.setup is not None and self.engine.setup_for(
                life.lifecycle_id
            ) is None:
                self.attach_setup(formation)
            accepted.append(formation)
        return tuple(accepted)

    def advance(
        self,
        item: OrderflowClockInput,
        *,
        formations: Sequence[LifecycleFormation] = (),
        regime: Any = None,
        regime_publications: Sequence[tuple[str, Any]] = (),
        triggers: Mapping[str, TriggerObservation] | None = None,
        bar_opens: Mapping[str, float] | None = None,
        decision_scores: Mapping[str, tuple[float | None, float | None]] | None = None,
        constraints: ExecutionConstraints | None = None,
        instrument: Any | None = None,
    ) -> TransitionAdvance:
        """Advance one completed bar through the shared causal lifecycle.

        Existing requests are settled and positions are managed before the
        current bar is allowed to create a new entry proposal.  This mirrors
        the canonical replay boundary and keeps live/replay on one path.
        """
        available_at = coerce_timestamp(item.one_minute.available_at)
        if self._clock is not None and available_at < self._clock:
            raise ValueError("orderflow transition clock cannot move backwards")
        self.context.current_session_date = item.one_minute.session_date
        self.context.input_diagnostics.append({
            "kind": "orderflow_clock_input",
            "time": nifty_label(item.one_minute.time),
            "available_at": nifty_label(item.one_minute.available_at),
            **dict(item.input_diagnostic),
        })
        if (
            item.atr is not None
            and item.atr.value is not None
            and item.atr.available_at is not None
            and coerce_timestamp(item.atr.available_at) <= available_at
        ):
            self.context.atr_by_session = {
                **self.context.atr_by_session,
                item.one_minute.session_date: item.atr.value,
            }
        observation = self.observation_owner.advance(item)
        row = item.one_minute.to_dict()
        if not any(
            str(existing.get("time")) == str(item.one_minute.time)
            for existing in self.context.rows
        ):
            self.context.rows.append(row)
        for event in observation.events:
            self.context.record_event(event)
        if regime is not None:
            self.context.regime = regime
        if constraints is not None:
            self.context.execution_constraints = constraints
        timestamp = nifty_label(item.one_minute.time)
        next_time = nifty_label(item.one_minute.available_at)
        available5 = list(self.observation_owner.five_minute_aggregates)
        self.context.aggregates["5m"] = available5
        self._record_phase4_source_observations(
            (*self.observation_owner.one_minute_aggregates, *available5),
        )
        if item.five_minute is not None:
            latest = item.five_minute
            if self.context.last_confirmation_context_time != latest.time:
                self._publish_ordinary_context(latest)
        timeout_events: list[dict[str, Any]] = []
        self.advance_opportunity_timeouts(timestamp, available5, timeout_events)
        self.context.pending_bar_events.extend(timeout_events)
        bar_events = list(observation.events)
        self.opportunity_owner.handle_structure(
            timestamp, next_time, row, available5, bar_events,
        )
        self.opportunity_owner.handle_opening_interaction(
            row, timestamp, next_time, bar_events,
        )
        self._record_phase4_context_location(item)
        generated_formations = self.opportunity_owner.formations_for(next_time)
        accepted_formations = self._accept_formations(
            (*formations, *generated_formations),
        )
        if accepted_formations:
            self.execution_owner.advance(
                tuple(accepted_formations),
                regime=regime,
                regime_publications=regime_publications,
                triggers=triggers,
                bar_opens=bar_opens,
                decision_scores=decision_scores,
                constraints=constraints,
                decision_at=next_time,
            )
        self.process_execution_bar(
            tuple(accepted_formations),
            index=0,
            timestamp=timestamp,
            next_time=next_time,
            row=row,
            available5=available5,
            instrument=instrument,
        )
        self._clock = available_at
        return TransitionAdvance(
            observation=observation,
            execution={
                name: tuple(values)
                for name, values in self.context.parity_snapshot().items()
            },
        )

    def _record_phase4_context_location(self, item: OrderflowClockInput) -> None:
        """Publish canonical context/location facts for completed aggregates."""
        context = self.context
        ordinary = (
            context.market_contexts.resolve(context.market_contexts.ordinary_context)
            if context.market_contexts.ordinary_context is not None else None
        )
        observations_by_timeframe = {
            "1m": tuple(
                observation for observation in item.observations_1m
                if observation.timeframe == "1m"
                and observation.source_aggregate_id == nifty_label(item.one_minute.time)
                and observation.reference_source_timeframe in {None, "1m"}
            ),
        }
        aggregates = [(item.one_minute, observations_by_timeframe["1m"])]
        if item.five_minute is not None:
            observations_by_timeframe["5m"] = tuple(
                observation for observation in item.observations_5m
                if observation.timeframe == "5m"
                and observation.source_aggregate_id == nifty_label(item.five_minute.time)
            )
            aggregates.append((item.five_minute, observations_by_timeframe["5m"]))
        known = {
            str(fact.get("identity"))
            for fact in context.parity_streams.get("context_location", ())
        }
        for aggregate, observations in aggregates:
            identity = f"context-location:{aggregate.timeframe}:{nifty_label(aggregate.time)}"
            if identity in known:
                continue
            context.record_parity_fact("context_location", {
                "kind": "context_location_observed",
                "occurred_at": aggregate.end_time,
                "observed_at": aggregate.end_time,
                "available_at": aggregate.available_at,
                "identity": identity,
                "causal_parent_id": f"aggregate:{aggregate.timeframe}:{nifty_label(aggregate.time)}",
                "values": {
                    "timeframe": aggregate.timeframe,
                    "source_observation_id": f"aggregate:{aggregate.timeframe}:{nifty_label(aggregate.time)}",
                    "context": {
                        "context_id": ordinary.context_id,
                        "version": ordinary.version,
                        "location": ordinary.location,
                        "market_state": ordinary.market_state,
                        "available_at": ordinary.available_at,
                        "parent_context_id": ordinary.parent_context_id,
                        "definition_version": ordinary.definition_version,
                        "source_provenance": list(ordinary.source_provenance),
                    } if ordinary is not None else None,
                    "locations": [observation.to_dict() for observation in observations],
                },
                "unavailable": [] if ordinary is not None and observations else ["missing_context"],
            })
            known.add(identity)

    def _record_phase4_source_observations(
        self, aggregates: Sequence[Aggregate],
    ) -> None:
        """Record source facts once, at the owner that advances causal time."""
        known = {
            str(fact.get("identity"))
            for fact in self.context.parity_streams.get("source_observation", ())
        }
        one_minute = self.observation_owner.one_minute_aggregates
        for aggregate in aggregates:
            identity = f"aggregate:{aggregate.timeframe}:{nifty_label(aggregate.time)}"
            if identity in known:
                continue
            children = tuple(
                child for child in one_minute
                if aggregate.timeframe == "5m"
                and aggregate.time <= child.time <= aggregate.end_time
                and child.available_at <= aggregate.available_at
            )
            self.context.record_parity_fact("source_observation", {
                "kind": "source_observation",
                "occurred_at": aggregate.end_time,
                "observed_at": aggregate.end_time,
                "available_at": aggregate.available_at,
                "sequence": len(self.context.parity_streams.get("source_observation", ())),
                "identity": identity,
                "causal_parent_id": (
                    f"aggregate:1m:{nifty_label(children[-1].time)}"
                    if children else None
                ),
                "values": {
                    "aggregate": aggregate.to_dict(),
                    "source_fingerprint": aggregate.source_fingerprint,
                },
                "unavailable": [],
                "lineage": [
                    f"aggregate:1m:{nifty_label(child.time)}" for child in children
                ],
            })
            known.add(identity)

    def _publish_ordinary_context(self, latest: Aggregate) -> None:
        """Publish the canonical 5m decision context at bar availability."""
        context = self.context
        scores = build_context_scores(
            latest,
            raw_rows=context.rows,
            aggregates=context.aggregates,
            atr=context.atr_by_session.get(latest.session_date),
        )
        long_score, short_score = scores if scores is not None else (None, None)
        context_id = (
            f"5m-context-{latest.symbol}-{latest.session_date}-1m_5m"
        )
        published = MarketContext(
            context_id=context_id,
            version=f"v{len(context.market_contexts.versions.get(context_id, ())) + 1}",
            timestamp=latest.time,
            available_at=latest.available_at,
            location=None,
            market_state=context.market_state,
            definition_version="1m_5m",
            source_timeframe="5m",
            long_score=long_score,
            short_score=short_score,
            evidence=(
                {
                    "completed_bar": True,
                    "open": latest.open,
                    "high": latest.high,
                    "low": latest.low,
                    "close": latest.close,
                },
            ),
            source_provenance=(
                {
                    "source_candle_id": nifty_label(latest.time),
                    "available_at": latest.available_at,
                },
            ),
        )
        reference = context.publish_context(published)
        context.market_contexts.ordinary_context = reference
        context.last_confirmation_context_time = latest.time

    def advance_execution(
        self,
        formations: Sequence[LifecycleFormation],
        *,
        decision_at: str,
        regime: Any = None,
        regime_publications: Sequence[tuple[str, Any]] = (),
        triggers: Mapping[str, TriggerObservation] | None = None,
        bar_opens: Mapping[str, float] | None = None,
        decision_scores: Mapping[str, tuple[float | None, float | None]] | None = None,
        constraints: ExecutionConstraints | None = None,
    ) -> Mapping[str, tuple[dict[str, Any], ...]]:
        """Advance lifecycle execution without advancing market observation."""
        if regime is not None:
            self.context.regime = regime
        if constraints is not None:
            self.context.execution_constraints = constraints
        accepted_formations = self._accept_formations(formations)
        self.execution_owner.advance(
            tuple(accepted_formations),
            regime=regime,
            regime_publications=regime_publications,
            triggers=triggers,
            bar_opens=bar_opens,
            decision_scores=decision_scores,
            constraints=constraints,
            decision_at=decision_at,
        )
        return {
            name: tuple(values)
            for name, values in self.context.parity_snapshot().items()
        }

    def advance_replay_bar(
        self,
        formations: Sequence[LifecycleFormation],
        *,
        index: int,
        timestamp: str,
        next_time: str,
        row: Mapping[str, Any],
        available5: Sequence[Any] = (),
        instrument: Any | None = None,
    ) -> Mapping[str, tuple[dict[str, Any], ...]]:
        """Advance replay execution through the shared chronological boundary."""
        available_at = coerce_timestamp(next_time)
        if self._clock is not None and available_at < self._clock:
            raise ValueError("orderflow transition clock cannot move backwards")
        result = self.process_execution_bar(
            formations,
            index=index,
            timestamp=timestamp,
            next_time=next_time,
            row=row,
            available5=available5,
            instrument=instrument,
        )
        self._clock = available_at
        return result

    def advance_opportunity_timeouts(
        self,
        timestamp: str,
        available5: Sequence[Any],
        bar_events: list[dict[str, Any]],
    ) -> None:
        """Expire existing opportunities before current-bar structure admission."""
        for life in tuple(self.context.opportunities.values()):
            if not life.active or self.context.position_is_open(life):
                continue
            interaction = (
                self.context.interactions.get(life.location_interaction_id)
                if life.location_interaction_id is not None else None
            )
            timeout = advance_opportunity_timeout(
                life,
                interaction=interaction,
                evidence=available5,
                as_of=timestamp,
            )
            if life.timeout_count >= life.timeout_limit:
                self.execution_owner.expire_opportunity(
                    life,
                    timestamp=timeout.expiry_at or timestamp,
                    bar_events=bar_events,
                    reason="evidence_timeout",
                    timeout_event={
                        "timeout_count": life.timeout_count,
                        "timeout_limit": life.timeout_limit,
                        "evidence_timeframe": life.timeout_evidence_timeframe,
                        "counted_source_ids": list(life.timeout_counted_source_ids),
                        "counted_snapshot_ids": list(life.timeout_counted_snapshot_ids),
                    },
                )

    def process_execution_bar(
        self,
        formations: Sequence[LifecycleFormation],
        *,
        index: int,
        timestamp: str,
        next_time: str,
        row: Mapping[str, Any],
        available5: Sequence[Any] = (),
        instrument: Any | None = None,
    ) -> Mapping[str, tuple[dict[str, Any], ...]]:
        """Run the canonical execution clock for one completed 1m bar.

        Pending fills are settled before position management.  New trigger
        observations are evaluated only after that pass and are selected for
        the next bar, preventing same-bar trigger/fill transitions.
        """
        # The gate and position-management policies consume the same completed
        # 5m evidence that drove this execution clock. Keep it visible on the
        # shared context before any proposal is evaluated.
        if available5:
            self.context.aggregates["5m"] = list(available5)
        self.execution_owner.active_position_gate.update(self.context, timestamp)
        events = list(self.context.pending_bar_events)
        self.context.pending_bar_events.clear()
        self.context.execution_proposals.clear()
        if self.mode == "FULL":
            row_dict = dict(row)
            for life in tuple(self.context.opportunities.values()):
                if life.active or self.context.position_is_open(life):
                    self.execution_owner.handle_execution(
                        index,
                        timestamp,
                        next_time,
                        row_dict,
                        [],
                        events,
                        life,
                        evaluate_entries=False,
                    )
        managed: list[Any] = list(formations)
        formation_ids = {
            item.lifecycle.lifecycle_id
            for item in formations
            if item.lifecycle is not None
        }
        managed.extend(
            SimpleNamespace(
                lifecycle=life,
                setup=self.execution_owner.engine.setup_for(life.lifecycle_id),
                available_at=life.formation_available_at,
            )
            for life in self.context.opportunities.values()
            if life.lifecycle_id not in formation_ids
        )
        for formation in managed:
            life = formation.lifecycle
            if life is None:
                continue
            if coerce_timestamp(timestamp) < coerce_timestamp(life.formation_available_at):
                continue
            state = self.execution_owner.engine.state_for(life.lifecycle_id)
            if not life.active and state.position is None:
                continue
            self.execution_owner.apply_session_deadline(
                life,
                session_date=str(life.session_date),
                timestamp=timestamp,
                bar_events=events,
            )
            state = self.execution_owner.engine.state_for(life.lifecycle_id)
            if (
                self.mode == "FULL"
                and
                life.active
                and self.context.regime is not None
                and self._intent_allows_requalification(state)
                and not self.execution_owner.opportunity_expired(
                    life, timestamp=timestamp
                )
            ):
                self.execution_owner.admit_opportunity(life, next_time, events)
            trace_before = _execution_trace_state(
                self.execution_owner.engine.state_for(life.lifecycle_id)
            )
            self.execution_owner.handle_management(
                index, timestamp, next_time, dict(row), list(available5), events, life,
                defer_discretionary_execution=True,
            )
            if self.mode == "FULL" and life.active:
                self.execution_owner.handle_execution(
                    index, timestamp, next_time, dict(row), [], events, life,
                    evaluate_entries=True,
                )
            self.context.execution_diagnostics.append({
                "kind": "execution_lifecycle_tick",
                "lifecycle_id": life.lifecycle_id,
                "observed_at": timestamp,
                "available_at": next_time,
                "bar": {
                    "time": row.get("time"), "open": row.get("open"),
                    "high": row.get("high"), "low": row.get("low"),
                    "close": row.get("close"),
                },
                "available_5m": [
                    {"time": item.time, "available_at": item.available_at}
                    for item in available5[-2:]
                ],
                "before": trace_before,
                "after": _execution_trace_state(
                    self.execution_owner.engine.state_for(life.lifecycle_id)
                ),
            })
        self.execution_owner.select_execution_proposals(timestamp, next_time, events)
        if self.execution_adapter is not None and instrument is not None:
            self._publish_execution_proposals(
                managed, row=dict(row), timestamp=timestamp,
                next_time=next_time, instrument=instrument,
            )
        return {
            name: tuple(values)
            for name, values in self.context.parity_snapshot().items()
        }

    def _publish_execution_proposals(
        self,
        formations: Sequence[LifecycleFormation],
        *,
        row: Mapping[str, Any],
        timestamp: str,
        next_time: str,
        instrument: Any,
    ) -> None:
        from .execution import entry_fill_decision

        for formation in formations:
            life = formation.lifecycle
            if life is None:
                continue
            state = self.execution_owner.engine.state_for(life.lifecycle_id)
            intent = state.intent
            if (
                intent is None
                or intent.status.value != "TRIGGERED"
                or intent.intent_id in self._published_intents
            ):
                continue
            decision = entry_fill_decision(
                direction=life.direction.value,
                trigger_price=intent.trigger_price,
                requested_fill_price=intent.trigger_price,
                bar_open=float(row["open"]),
                failure_boundary=intent.failure_boundary,
            )
            if decision.quantity <= 0:
                continue
            adapter = self.execution_adapter
            assert adapter is not None
            proposal = adapter.proposal(
                intent,
                instrument=instrument,
                quantity=decision.quantity,
                observed_at=timestamp,
                available_at=next_time,
            )
            adapter.authorize(proposal, timestamp=next_time)
            self._published_intents.add(intent.intent_id)

    def observe_triggers(
        self,
        formations: Sequence[LifecycleFormation],
        observations: Mapping[str, TriggerObservation],
        **kwargs: Any,
    ) -> None:
        self.execution_owner.observe_triggers(tuple(formations), observations, **kwargs)

    def fill_triggered(
        self, formations: Sequence[LifecycleFormation], **kwargs: Any
    ) -> None:
        self.execution_owner.fill_triggered(tuple(formations), **kwargs)

    def queue_exit(
        self, formation: LifecycleFormation, order: Any, *, observed_at: str
    ) -> bool:
        return self.execution_owner.queue_exit_order(
            formation, order, observed_at=observed_at
        )

    def fill_exit(
        self, formation: LifecycleFormation, *, filled_at: str, open_price: float
    ) -> None:
        self.execution_owner.fill_exit(
            formation, filled_at=filled_at, open_price=open_price
        )

    def refresh_decision_context(self, life: Any, *, timestamp: str) -> Any | None:
        reference = self.context.market_contexts.ordinary_context
        context = self.context.market_contexts.resolve(reference) if reference is not None else None
        if context is not None and context.available_at > coerce_timestamp(timestamp):
            context = None
        if context is None:
            return self.decision_context_for_life(life, timestamp)
        self.execution_owner.engine.update_decision_context(
            life, ContextVersionRef(context.context_id, context.version)
        )
        return context

    def decision_context_for_life(self, life: Any, as_of: str) -> Any | None:
        reference = getattr(life, "parent_context", None)
        context = self.context.context(reference) if reference is not None else None
        if context is not None and context.available_at <= coerce_timestamp(as_of):
            return context
        reference = self.context.market_contexts.ordinary_context
        context = self.context.market_contexts.resolve(reference) if reference is not None else None
        return context if context is not None and context.available_at <= coerce_timestamp(as_of) else None

    def decision_context_for_location(
        self, location: str | None, *, as_of: str
    ) -> Any | None:
        if location is None:
            return None
        return self.context.market_contexts.latest_as_of(
            str(location), coerce_timestamp(as_of)
        )

    def scores_for_context_ref(
        self, reference: Any | None
    ) -> tuple[float | None, float | None]:
        context = self.context.context(reference) if reference is not None else None
        return (
            getattr(context, "long_score", None),
            getattr(context, "short_score", None),
        )

    def local_scores_at(self, timestamp: str) -> tuple[float | None, float | None]:
        context = self.decision_context_for_life(None, timestamp)
        return (
            getattr(context, "long_score", None),
            getattr(context, "short_score", None),
        )

    def context_relation_for_life(self, life: Any, context: Any) -> Any:
        setup = self.execution_owner.engine.setup_for(life.lifecycle_id)
        return getattr(setup, "context_relation", getattr(context, "relation", "neutral"))

    def record_exit_decision_context(
        self, life: Any, rule: Any, *, timestamp: str, reason: str
    ) -> Any | None:
        reference = self.decision_context_for_location(
            getattr(rule, "location", None), as_of=timestamp
        )
        self.context.record_decision_context(
            {
                "lifecycle_id": life.lifecycle_id,
                "rule_id": getattr(rule, "rule_id", None),
                "observed_at": timestamp,
                "reason": reason,
            }
        )
        return getattr(reference, "reference", None)


__all__ = ["OrderflowTransitionOwner", "TransitionAdvance"]
