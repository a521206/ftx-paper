"""Serialization helpers for the canonical orderflow state graph."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import fields, is_dataclass
from datetime import datetime
from enum import Enum
import math
from typing import Any, Mapping

from types import SimpleNamespace

from .execution import (
    AdmissionStatus, EntryOrder, EntryReservation, ExecutionAdmission,
    ExecutionState, ExitOrder, Fill, ReservationStatus,
)
from .hierarchy import (
    ContextObservation, ContextRelation, ContextVersionRef, EntryPolicy,
    ExitIntent, IntentStatus as HierarchyIntentStatus, MarketContext,
    Permission, PositionManagement, PositionStatus, RegimePermission,
    ResolvedExitRule, StructuralSetup, TradeIntent,
)
from .interactions import InteractionSnapshot, LocationInteraction
from .lifecycle import EvidenceRef, LifecycleEvent, SetupOpportunity
from .location_geometry import LocationBand
from .orderflow_common import Aggregate
from .state_machine_types import (
    ContinuousState,
    LocationRecord,
    LocationRegistry,
    LocationStatus,
    MarketContextStore,
    OrderflowState,
)
from .timestamps import coerce_timestamp, serialize_utc
from .templates import Direction, LocationTemplate, TRADE_TEMPLATES
from .value_publication import PriorSessionValueReference, ValuePublication


def snapshot_value(value: Any) -> Any:
    """Convert canonical mutable state to deterministic persistence values."""
    if isinstance(value, datetime):
        return serialize_utc(value)
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {
            item.name: snapshot_value(getattr(value, item.name))
            for item in fields(value)
        }
    if isinstance(value, Mapping):
        return {str(key): snapshot_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [snapshot_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((snapshot_value(item) for item in value), key=str)
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("canonical state contains a non-finite number")
    return value


def context_snapshot(context: OrderflowState) -> dict[str, Any]:
    store = context.market_contexts
    return {
        "versions": snapshot_value(store.versions),
        "ordinary_context": snapshot_value(store.ordinary_context),
        "interaction_contexts": snapshot_value(store.interaction_contexts),
    }


def location_snapshot(registry: LocationRegistry) -> dict[str, Any]:
    return {
        "stream_id": registry.stream_id,
        "records": snapshot_value(registry.records),
        "aliases": snapshot_value(registry.aliases),
    }


def tuple_mapping_snapshot(values: Mapping[tuple[str, ...], Any]) -> list[dict[str, Any]]:
    return [
        {"key": list(key), "value": snapshot_value(value)}
        for key, value in values.items()
    ]


def tuple_mapping_restore(raw: Any) -> dict[tuple[str, ...], Any]:
    if not isinstance(raw, list):
        return {}
    return {
        tuple(str(item) for item in entry["key"]): deepcopy(entry.get("value"))
        for entry in raw
        if isinstance(entry, Mapping)
        and isinstance(entry.get("key"), list)
    }


def restore_context_store(raw: Mapping[str, Any]) -> MarketContextStore:
    store = MarketContextStore()
    versions = raw.get("versions", {})
    if not isinstance(versions, Mapping):
        raise ValueError("canonical context versions are invalid")
    for values in versions.values():
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


def restore_location_registry(raw: Mapping[str, Any]) -> LocationRegistry:
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


def restore_aggregate(raw: Mapping[str, Any] | None) -> Aggregate | None:
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


def restore_continuous(raw: Mapping[str, Any], registry: LocationRegistry) -> ContinuousState:
    prior_values: list[ValuePublication] = []
    for item in raw.get("prior_values", ()):
        if not isinstance(item, Mapping):
            continue
        reference_raw = item.get("reference")
        reference = None
        if isinstance(reference_raw, Mapping):
            reference = PriorSessionValueReference(
                session_date=str(reference_raw["session_date"]),
                value_area_low=float(reference_raw["value_area_low"]),
                point_of_control=float(reference_raw["point_of_control"]),
                value_area_high=float(reference_raw["value_area_high"]),
                observed_at=coerce_timestamp(reference_raw["observed_at"]),
                available_at=coerce_timestamp(reference_raw["available_at"]),
                lineage=tuple(str(value) for value in reference_raw["lineage"]),
            )
        prior_values.append(ValuePublication(
            reference=reference,
            observed_at=(coerce_timestamp(item["observed_at"]) if item.get("observed_at") else None),
            available_at=(coerce_timestamp(item["available_at"]) if item.get("available_at") else None),
            lineage=tuple(str(value) for value in item.get("lineage", ())),
            unavailable_reason=item.get("unavailable_reason"),
        ))
    return ContinuousState(
        registry=registry,
        prior_values=prior_values,
        last_completed_bar=restore_aggregate(raw.get("last_completed_bar")),
        last_close=(float(raw["last_close"]) if raw.get("last_close") is not None else None),
        open_gaps=[dict(item) for item in raw.get("open_gaps", ()) if isinstance(item, Mapping)],
    )


def restore_interaction_snapshot(raw: Mapping[str, Any]) -> InteractionSnapshot:
    return LocationInteraction.from_snapshot(raw).evaluation_snapshot()


def restore_namespace(value: Any) -> Any:
    if isinstance(value, Mapping):
        restored = {str(key): restore_namespace(item) for key, item in value.items()}
        if isinstance(restored.get("failed_reversal_locked"), list):
            restored["failed_reversal_locked"] = set(restored["failed_reversal_locked"])
        return SimpleNamespace(**restored)
    if isinstance(value, list):
        return [restore_namespace(item) for item in value]
    return value


def restore_evidence_ref(raw: Mapping[str, Any] | None) -> EvidenceRef | None:
    if raw is None:
        return None
    return EvidenceRef(
        evidence_id=str(raw["evidence_id"]), timeframe=str(raw["timeframe"]),
        observed_at=coerce_timestamp(raw["observed_at"]),
        available_at=coerce_timestamp(raw["available_at"]),
        source_fingerprint=str(raw["source_fingerprint"]),
        source_bar_count=int(raw["source_bar_count"]),
    )


def restore_template(raw: Mapping[str, Any] | None) -> Any | None:
    if raw is None:
        return None
    template_id = raw.get("template_id")
    for template in TRADE_TEMPLATES.values():
        if template.template_id == template_id:
            return template
    raise ValueError(f"unknown canonical trade template: {template_id!r}")


def restore_context_observation(raw: Mapping[str, Any] | None) -> ContextObservation | None:
    if raw is None:
        return None
    return ContextObservation(
        context_id=str(raw["context_id"]), context_version=str(raw["context_version"]),
        observed_at=coerce_timestamp(raw["observed_at"]),
        available_at=coerce_timestamp(raw["available_at"]),
        relation=ContextRelation(raw["relation"]),
        source_timeframe=str(raw["source_timeframe"]),
        evidence=dict(raw.get("evidence", {})),
    )


def restore_context_ref(raw: Mapping[str, Any] | None) -> ContextVersionRef | None:
    if raw is None:
        return None
    return ContextVersionRef(str(raw["context_id"]), str(raw["version"]))


def restore_exit_intent(raw: Mapping[str, Any] | None) -> ExitIntent | None:
    if raw is None:
        return None
    return ExitIntent(
        intent_id=str(raw["intent_id"]), position_id=str(raw["position_id"]),
        rule_id=str(raw["rule_id"]), location=str(raw["location"]),
        status=HierarchyIntentStatus(raw["status"]),
        created_at=coerce_timestamp(raw["created_at"]),
        expiry=coerce_timestamp(raw["expiry"]), interaction_id=raw.get("interaction_id"),
        formation_context=restore_context_ref(raw.get("formation_context")),
        decision_context=restore_context_ref(raw.get("decision_context")),
        cancel_reason=raw.get("cancel_reason"),
        triggered_at=coerce_timestamp(raw["triggered_at"]) if raw.get("triggered_at") else None,
    )


def restore_exit_plan(raw: Any) -> tuple[ResolvedExitRule, ...]:
    return tuple(
        ResolvedExitRule(
            rule_id=str(item["rule_id"]), location=str(item["location"]),
            mode=str(item["mode"]), level=item.get("level"),
            source_context=restore_context_ref(item.get("source_context")),
            interaction_id=item.get("interaction_id"), response=item.get("response"),
            execution=item.get("execution"), score_direction=item.get("score_direction"),
            score_margin=item.get("score_margin"), required=bool(item.get("required", False)),
            trigger_policy=str(item.get("trigger_policy", "touch")),
        )
        for item in raw if isinstance(item, Mapping)
    )


def restore_opportunity(raw: Mapping[str, Any]) -> SetupOpportunity:
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
            effective_at=coerce_timestamp(item["effective_at"])
            if item.get("effective_at") is not None else None,
            payload=dict(item.get("payload", {})),
        )
        for item in raw_events if isinstance(raw_events, list) and isinstance(item, Mapping)
    )
    response_evidence = tuple(
        aggregate for item in raw.get("response_evidence", ())
        if isinstance(item, Mapping)
        for aggregate in (restore_aggregate(item),) if aggregate is not None
    )
    return SetupOpportunity(
        lifecycle_id=str(raw["lifecycle_id"]), direction=Direction(raw["direction"]),
        session_date=str(raw["session_date"]), instrument=str(raw["instrument"]),
        strategy=str(raw["strategy"]), formation_at=coerce_timestamp(raw["formation_at"]),
        formation_available_at=coerce_timestamp(raw["formation_available_at"]),
        candidate_id=str(raw["candidate_id"]), parent_context=parent_ref,
        location=LocationTemplate(raw["location"]) if raw.get("location") is not None else None,
        reference_level=raw.get("reference_level"),
        context_candle=restore_aggregate(raw.get("context_candle")),
        breakout_level=float(raw["breakout_level"]), failure_boundary=float(raw["failure_boundary"]),
        context_candle_lineage=restore_evidence_ref(raw.get("context_candle_lineage")),
        origin_evidence_id=raw.get("origin_evidence_id"),
        response_evidence_ids=tuple(raw.get("response_evidence_ids", ())),
        response_evidence=response_evidence,
        response_evidence_lineage=tuple(
            ref for item in raw.get("response_evidence_lineage", ())
            if isinstance(item, Mapping) for ref in (restore_evidence_ref(item),) if ref is not None
        ),
        policy_version=raw.get("policy_version"), reference_generation=int(raw.get("reference_generation", 0)),
        causal_identity=raw.get("causal_identity"), location_interaction_id=raw.get("location_interaction_id"),
        opening_interaction_kind=raw.get("opening_interaction_kind"),
        opening_interaction_evidence=list(raw.get("opening_interaction_evidence", ())),
        opening_policy_version=raw.get("opening_policy_version"), template_id=raw.get("template_id"),
        status=raw.get("status", "FORMED"), response_episode_id=raw.get("response_episode_id"),
        episode_confirmed=bool(raw.get("episode_confirmed", False)),
        execution_group_id=raw.get("execution_group_id"),
        clock=coerce_timestamp(raw["clock"]) if raw.get("clock") else None,
        decision_context_required=bool(raw.get("decision_context_required", True)),
        timeout_limit=int(raw.get("timeout_limit", 6)), timeout_count=int(raw.get("timeout_count", 0)),
        timeout_evidence_timeframe=str(raw.get("timeout_evidence_timeframe", "5m")),
        timeout_counted_source_ids=list(raw.get("timeout_counted_source_ids", ())),
        timeout_counted_snapshot_ids=list(raw.get("timeout_counted_snapshot_ids", ())),
        retest_touched=bool(raw.get("retest_touched", False)),
        retest_confirmed=bool(raw.get("retest_confirmed", False)), _events=events,
    )


def restore_setup(raw: Mapping[str, Any] | None) -> StructuralSetup | None:
    if raw is None:
        return None
    exit_plan = tuple(
        ResolvedExitRule(
            rule_id=str(item["rule_id"]), location=str(item["location"]), mode=str(item["mode"]),
            level=item.get("level"), source_context=restore_context_ref(item.get("source_context")),
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
        entry_zone_high=float(raw["entry_zone_high"]), structural_reference=float(raw["structural_reference"]),
        sweep_extreme=raw.get("sweep_extreme"), failure_boundary=float(raw["failure_boundary"]),
        expires_at=coerce_timestamp(raw["expires_at"]), active=bool(raw.get("active", True)),
        required_confirmation=tuple(raw.get("required_confirmation", ())),
        parent_context_id=raw.get("parent_context_id"), parent_context_version=raw.get("parent_context_version"),
        context_relation=ContextRelation(raw.get("context_relation", ContextRelation.CONFIRMED)),
        context_reference_level=raw.get("context_reference_level"), lifecycle_id=raw.get("lifecycle_id"),
        template=restore_template(raw.get("template")), exit_plan=exit_plan,
    )


def restore_regime(raw: Mapping[str, Any] | None) -> RegimePermission | None:
    if raw is None:
        return None
    return RegimePermission(
        version=str(raw["version"]), context=str(raw["context"]),
        long=Permission(raw["long"]), short=Permission(raw["short"]),
        restrictions=tuple(raw.get("restrictions", ())),
        opening_gap_state=raw.get("opening_gap_state"), opening_gap_direction=raw.get("opening_gap_direction"),
    )


def restore_execution_state(raw: Mapping[str, Any]) -> ExecutionState:
    admission = raw.get("admission", {})
    intent = raw.get("intent")
    restored_intent = None
    if isinstance(intent, Mapping):
        restored_intent = TradeIntent(
            intent_id=str(intent["intent_id"]), setup_id=str(intent["setup_id"]),
            regime_version=str(intent["regime_version"]), direction=str(intent["direction"]),
            status=HierarchyIntentStatus(intent["status"]),
            created_at=coerce_timestamp(intent["created_at"]), trigger_price=float(intent["trigger_price"]),
            failure_boundary=float(intent["failure_boundary"]), expiry=coerce_timestamp(intent["expiry"]),
            confirmation=tuple(intent.get("confirmation", ())), cancel_reason=intent.get("cancel_reason"),
            triggered_at=coerce_timestamp(intent["triggered_at"]) if intent.get("triggered_at") else None,
            setup_version=intent.get("setup_version"), entry_policy=EntryPolicy(intent.get("entry_policy", "intrabar_cross")),
            required_confirmation=tuple(intent.get("required_confirmation", ())),
            armed_at=coerce_timestamp(intent["armed_at"]) if intent.get("armed_at") else None,
            effective_at=coerce_timestamp(intent["effective_at"]) if intent.get("effective_at") else None,
            filled_at=coerce_timestamp(intent["filled_at"]) if intent.get("filled_at") else None,
            lifecycle_id=intent.get("lifecycle_id"), formation_context=restore_context_ref(intent.get("formation_context")),
            decision_context=restore_context_ref(intent.get("decision_context")),
        )
    restored_admission = ExecutionAdmission(
        status=AdmissionStatus(admission.get("status", AdmissionStatus.NOT_EVALUATED)),
        reason=admission.get("reason"), decided_at=admission.get("decided_at"), intent_id=admission.get("intent_id"),
    ) if isinstance(admission, Mapping) else ExecutionAdmission()
    reservation = raw.get("reservation")
    restored_reservation = EntryReservation(
        reservation_id=str(reservation["reservation_id"]), opportunity_id=str(reservation["opportunity_id"]),
        direction=str(reservation["direction"]), session_date=str(reservation["session_date"]),
        reserved_at=str(reservation["reserved_at"]), status=ReservationStatus(reservation.get("status", "ACTIVE")),
        released_at=reservation.get("released_at"), release_reason=reservation.get("release_reason"),
    ) if isinstance(reservation, Mapping) else None
    pending_entry = raw.get("pending_entry")
    restored_entry = EntryOrder(
        intent=restored_intent, available_at=str(pending_entry["available_at"]),
        fill_price=float(pending_entry["fill_price"]), session_date=pending_entry.get("session_date"),
    ) if isinstance(pending_entry, Mapping) and restored_intent is not None else None
    pending_exit = raw.get("pending_exit")
    restored_exit = ExitOrder(
        reason=str(pending_exit["reason"]), available_at=str(pending_exit["available_at"]),
        requested_price=pending_exit.get("requested_price"),
        execution_policy=str(pending_exit.get("execution_policy", "next_available_bar_open")),
        quantity=pending_exit.get("quantity"), session_date=pending_exit.get("session_date"),
        position_id=pending_exit.get("position_id"),
    ) if isinstance(pending_exit, Mapping) else None
    position = raw.get("position")
    restored_position = None
    if isinstance(position, Mapping):
        restored_position = PositionManagement(
            position_id=str(position["position_id"]), intent_id=str(position["intent_id"]),
            setup_id=str(position["setup_id"]), regime_version=str(position["regime_version"]),
            direction=str(position["direction"]), status=PositionStatus(position["status"]),
            failure_boundary=float(position["failure_boundary"]), initial_risk_points=float(position["initial_risk_points"]),
            protective_stop=position.get("protective_stop"), target=position.get("target"),
            closed_at=position.get("closed_at"), exit_reason=position.get("exit_reason"),
            fill_price=position.get("fill_price"), quantity=float(position.get("quantity", 1.0)),
            lifecycle_id=position.get("lifecycle_id"), exit_plan=restore_exit_plan(position.get("exit_plan", ())),
            exit_intent=restore_exit_intent(position.get("exit_intent")),
            partial_exit_taken=bool(position.get("partial_exit_taken", False)), post_3r_activated=bool(position.get("post_3r_activated", False)),
        )
    return ExecutionState(
        opportunity_id=str(raw["opportunity_id"]), setup=restore_setup(raw.get("setup")),
        setup_available_at=raw.get("setup_available_at"), exit_plan_frozen=bool(raw.get("exit_plan_frozen", False)),
        intent=restored_intent, admission=restored_admission, reservation=restored_reservation,
        pending_entry=restored_entry, pending_exit=restored_exit, position=restored_position,
        entry_at=raw.get("entry_at"), exit_at=raw.get("exit_at"), exit_price=raw.get("exit_price"),
        realized_points=raw.get("realized_points"),
        admission_history=tuple(ExecutionAdmission(
            status=AdmissionStatus(item.get("status", AdmissionStatus.NOT_EVALUATED)), reason=item.get("reason"),
            decided_at=item.get("decided_at"), intent_id=item.get("intent_id"),
        ) for item in raw.get("admission_history", ()) if isinstance(item, Mapping)),
        reservation_history=tuple(EntryReservation(
            reservation_id=str(item["reservation_id"]), opportunity_id=str(item["opportunity_id"]),
            direction=str(item["direction"]), session_date=str(item["session_date"]), reserved_at=str(item["reserved_at"]),
            status=ReservationStatus(item.get("status", "ACTIVE")), released_at=item.get("released_at"), release_reason=item.get("release_reason"),
        ) for item in raw.get("reservation_history", ()) if isinstance(item, Mapping)),
        fills=[Fill(str(item["fill_id"]), str(item["opportunity_id"]), str(item["kind"]), float(item["quantity"]), float(item["price"]), str(item["filled_at"]), float(item.get("cost", 0.0))) for item in raw.get("fills", ()) if isinstance(item, Mapping)],
        entry_attempted=bool(raw.get("entry_attempted", False)), entry_aliases=tuple(raw.get("entry_aliases", ())),
        retest_touched=bool(raw.get("retest_touched", False)), retest_touch_observed_at=raw.get("retest_touch_observed_at"),
        retest_touch_available_at=raw.get("retest_touch_available_at"), retest_touch_evidence=raw.get("retest_touch_evidence"),
    )


__all__ = [
    "context_snapshot", "location_snapshot", "restore_aggregate",
    "restore_context_store", "restore_continuous", "restore_interaction_snapshot",
    "restore_location_registry", "snapshot_value", "tuple_mapping_restore",
    "tuple_mapping_snapshot",
]
