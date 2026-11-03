"""Orderflow daily setup task declarations."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timezone
import hashlib
from typing import Any

from ftx_paper.application.setup_worker import DailySetupTask
from ftx_paper.contracts.setup import SetupRunRequest
from ftx_paper.infrastructure.artifacts.setup_store import SetupArtifactStore, build_envelope, build_payload_envelope, canonical_json
from ftx_paper.runtime.store import RuntimeStore
from ftx_paper.runtime.futures_source import FuturesSessionSourceResolver
from ftx_paper.strategies.orderflow.canonical.orderflow.value_publication import (
    prior_profile_is_complete, publish_value_references,
)
from ftx_paper.strategies.orderflow.value_publication import PriorSessionValueReference, ValuePublication
from ftx_paper.strategies.orderflow.features import daily_atr_cache
from ftx_paper.strategies.orderflow.timestamps import coerce_timestamp, parse_nifty_label, serialize_utc


STRATEGY_ID = "ftx-paper-orderflow"
TASK_ID = "prior-session-value"
ALGORITHM_VERSION = "orderflow.prior-session-value.v1"
PD_LEVELS_TASK_ID = "prior-session-levels"
PD_LEVELS_ALGORITHM_VERSION = "orderflow.prior-session-levels.v1"
ATR_TASK_ID = "daily-atr"
ATR_ALGORITHM_VERSION = "orderflow.daily-atr.wilder.v1"


def _rows(source: Any, session_date: str) -> list[dict[str, Any]]:
    if source is None:
        return []
    result = []
    for row in source.bars:
        result.append({
            "time": str(row["minute"]), "open": float(row["open"]), "high": float(row["high"]),
            "low": float(row["low"]), "close": float(row["close"]), "volume": int(row.get("volume") or 0),
            "oi": int(row.get("open_interest") or 0), "symbol": "NIFTY", "exchange": str(row.get("exchange", "NFO")), "expiry": "",
            "session_date": session_date,
        })
    return result


def _selected_source(source: Any) -> dict[str, Any]:
    """Describe both the virtual series and its dated storage contract."""
    return {
        "exchange": source.identity.exchange,
        "symbol": "NIFTY",
        "expiry": "",
        "storage_identity": {
            "symbol": source.identity.symbol,
            "exchange": source.identity.exchange,
            "instrument_type": source.identity.instrument_type,
            "expiry": source.identity.expiry,
        },
        "rollover_policy_version": source.identity.policy_version,
    }


def _history_fingerprint(sources_by_date: Mapping[str, Any], sessions: list[str]) -> str:
    return hashlib.sha256(canonical_json([
        {"session": session, "source_fingerprint": sources_by_date[session].source_fingerprint}
        for session in sessions
    ])).hexdigest()


def _observed_at(rows: list[dict[str, Any]]) -> datetime:
    return coerce_timestamp(str(rows[-1]["time"]))


def build_prior_session_task(store: RuntimeStore, artifact_store: SetupArtifactStore) -> DailySetupTask:
    def materialize(request: SetupRunRequest, cancelled: Callable[[], bool] = lambda: False) -> Mapping[str, int]:
        all_dates = sorted(str(item) for item in store.read_dates())
        dates = list(all_dates)
        if request.from_session:
            dates = [item for item in dates if item >= request.from_session]
        if request.through_session:
            dates = [item for item in dates if item <= request.through_session]
        processed = materialized = reused = unavailable = failed = 0
        target_available: int | None = None
        source_resolver = FuturesSessionSourceResolver(store)
        sources_by_date = {session: source_resolver.resolve(session) for session in all_dates}
        rows_by_date = {
            session: _rows(sources_by_date[session], session) if sources_by_date[session] is not None else []
            for session in all_dates
        }
        completed_dates = [session for session in all_dates if rows_by_date[session]]
        targets = [session for session in dates if session in completed_dates and session != completed_dates[0]]
        for target in targets:
            if cancelled():
                break
            index = completed_dates.index(target)
            prior = completed_dates[index - 1]
            prior_rows, target_rows = rows_by_date[prior], rows_by_date[target]
            if not prior_rows or not target_rows:
                continue
            prior_source = sources_by_date[prior]
            if prior_source is None:
                continue
            processed += 1
            source_fp = prior_source.provenance()["source_fingerprint"]
            identity = {"strategy_id": STRATEGY_ID, "task_id": TASK_ID, "algorithm_version": ALGORITHM_VERSION,
                        "config_fingerprint": "value-profile-default", "instrument": "NIFTY"}
            if request.mode != "rebuild":
                existing = artifact_store.read(identity, target)
                existing_fingerprints = (
                    existing.get("source", {}).get("source_fingerprints", [])
                    if existing is not None else []
                )
                if source_fp in existing_fingerprints:
                    reused += 1
                    target_available = int(
                        isinstance(existing, Mapping) and existing.get("status") == "available"
                    )
                    continue
            try:
                refs = publish_value_references(
                    prior_rows, target_rows, target,
                    prior_profile_complete=prior_profile_is_complete(prior_rows),
                    trading_session_dates=tuple(dates),
                )
                ref = next((item for item in refs if item.scope == "prior_session"), None)
                observed_at = _observed_at(prior_rows)
                if ref is None:
                    publication = ValuePublication(None, None, None, (), "prior_session_incomplete")
                else:
                    available_at = datetime.fromisoformat(ref.available_at.replace("Z", "+00:00"))
                    lineage = (ref.reference_id, ref.fingerprint)
                    publication = ValuePublication(
                        PriorSessionValueReference(ref.source_sessions[0], ref.val, ref.poc, ref.vah, observed_at, available_at, lineage),
                        observed_at, available_at, lineage,
                    )
                source = {"sessions": [prior], "selected_source": _selected_source(prior_source),
                          "source_fingerprints": [source_fp], "source_fingerprint_recipe": "normalized-continuous-bars-v1", "source_bar_count": len(prior_rows),
                          "session_finalization": {"timezone": "Asia/Kolkata", "close": "15:30", "finalized_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}}
                envelope = build_envelope(strategy_id=STRATEGY_ID, task_id=TASK_ID, algorithm_version=ALGORITHM_VERSION,
                                          target_session=target, source=source,
                                          configuration={"config_fingerprint": identity["config_fingerprint"], "profile_version": "value_profile.v1"},
                                          publication=publication)
                artifact_store.write(envelope, source_fingerprint=source_fp, output_root=request.output_root)
                materialized += 1
                unavailable += publication.reference is None
                target_available = int(publication.reference is not None)
            except Exception:
                failed += 1
        return {"processed_sessions": processed, "materialized": materialized, "reused": reused, "unavailable": unavailable, "failed": failed, "target_available": target_available if target_available is not None else 0}

    return DailySetupTask(STRATEGY_ID, TASK_ID, ALGORITHM_VERSION, materialize)


def build_prior_session_levels_task(store: RuntimeStore, artifact_store: SetupArtifactStore) -> DailySetupTask:
    def materialize(request: SetupRunRequest, cancelled: Callable[[], bool] = lambda: False) -> Mapping[str, int]:
        all_dates = sorted(str(item) for item in store.read_dates())
        dates = [item for item in all_dates if (not request.from_session or item >= request.from_session) and (not request.through_session or item <= request.through_session)]
        sources_by_date = {session: FuturesSessionSourceResolver(store).resolve(session) for session in all_dates}
        rows_by_date = {
            session: _rows(sources_by_date[session], session) if sources_by_date[session] is not None else []
            for session in all_dates
        }
        processed = materialized = reused = unavailable = 0
        target_available: int | None = None
        for target in dates:
            if cancelled():
                break
            processed += 1
            prior = max((session for session in all_dates if session < target), default=None)
            prior_rows = rows_by_date.get(prior, []) if prior is not None else []
            prior_source = sources_by_date.get(prior) if prior is not None else None
            provenance = prior_source.provenance() if prior_source is not None else {}
            source_fp = str(provenance.get("source_fingerprint", "none"))
            identity = {"strategy_id": STRATEGY_ID, "task_id": PD_LEVELS_TASK_ID, "algorithm_version": PD_LEVELS_ALGORITHM_VERSION,
                        "config_fingerprint": "prior-session-levels-NIFTY", "instrument": "NIFTY"}
            if request.mode != "rebuild":
                existing = artifact_store.read(identity, target)
                if existing is not None and source_fp in existing.get("source", {}).get("source_fingerprints", []):
                    reused += 1
                    target_available = int(existing.get("status") == "available")
                    continue
            complete = (
                provenance.get("ordered_unique_minutes") is True
                and provenance.get("missing_minute_count") == 0
            )
            available = bool(prior_rows and prior_source is not None and complete)
            if available:
                high = max(float(row["high"]) for row in prior_rows)
                low = min(float(row["low"]) for row in prior_rows)
                observed_at = _observed_at(prior_rows)
                available_at = parse_nifty_label(f"{target}T09:15")
                lineage = [f"source:{source_fp}", *(f"bar:{row['time']}" for row in prior_rows)]
                payload = {"target_session": target, "source_session": prior, "symbol": "NIFTY",
                           "high": high, "low": low, "observed_at": serialize_utc(observed_at),
                           "available_at": serialize_utc(available_at), "lineage": lineage}
                status, reason = "available", None
            else:
                payload, status, reason = None, "unavailable", (
                    "prior_session_source_incomplete"
                    if prior_source is not None and not complete
                    else "prior_session_source_unavailable"
                )
                unavailable += 1
            source = {
                "sessions": [prior] if prior is not None and available else [],
                "selected_source": _selected_source(prior_source) if prior_source is not None else {"symbol": "NIFTY", "expiry": ""},
                "source_fingerprints": [source_fp], "source_fingerprint_recipe": "normalized-continuous-bars-v1",
                "source_bar_count": len(prior_rows),
                "ordered_unique_minutes": provenance.get("ordered_unique_minutes", False),
                "missing_minute_count": provenance.get("missing_minute_count", 0),
                "session_finalization": {"timezone": "Asia/Kolkata", "close": "15:30", "finalized_at": serialize_utc(datetime.now(timezone.utc))},
            }
            envelope = build_payload_envelope(
                strategy_id=STRATEGY_ID, task_id=PD_LEVELS_TASK_ID,
                algorithm_version=PD_LEVELS_ALGORITHM_VERSION, target_session=target,
                source=source,
                configuration={"config_fingerprint": identity["config_fingerprint"], "symbol": "NIFTY"},
                payload=payload, status=status, unavailable_reason=reason,
            )
            artifact_store.write(envelope, source_fingerprint=source_fp, output_root=request.output_root)
            materialized += 1
            target_available = int(available)
        return {"processed_sessions": processed, "materialized": materialized, "reused": reused,
                "unavailable": unavailable, "failed": 0, "target_available": target_available or 0}

    return DailySetupTask(STRATEGY_ID, PD_LEVELS_TASK_ID, PD_LEVELS_ALGORITHM_VERSION, materialize)


def build_daily_atr_task(store: RuntimeStore, artifact_store: SetupArtifactStore) -> DailySetupTask:
    def materialize(request: SetupRunRequest, cancelled: Callable[[], bool] = lambda: False) -> Mapping[str, int]:
        all_dates = sorted(str(item) for item in store.read_dates())
        source_resolver = FuturesSessionSourceResolver(store)
        sources_by_date = {session: source_resolver.resolve(session) for session in all_dates}
        rows_by_date = {
            session: _rows(sources_by_date[session], session) if sources_by_date[session] is not None else []
            for session in all_dates
        }
        values = daily_atr_cache(rows_by_date)
        completed_dates = [session for session in all_dates if rows_by_date[session]]
        processed = materialized = reused = unavailable = 0
        target_available: int | None = None
        targets = [item for item in all_dates if (not request.from_session or item >= request.from_session) and (not request.through_session or item <= request.through_session)]
        for target in targets:
            if cancelled():
                break
            processed += 1
            identity = {"strategy_id": STRATEGY_ID, "task_id": ATR_TASK_ID, "algorithm_version": ATR_ALGORITHM_VERSION,
                        "config_fingerprint": "daily-atr-NIFTY-14", "instrument": "NIFTY"}
            value = values.get(target)
            source_index = completed_dates.index(target) if target in completed_dates else None
            if source_index is None:
                source_date = None
                source_rows = []
            else:
                source_date = completed_dates[source_index - 1] if source_index else None
                source_rows = [
                    row for session in completed_dates[:source_index]
                    for row in rows_by_date[session]
                ]
            completed_source_count = source_index if source_index is not None else 0
            source_fingerprint = _history_fingerprint(
                sources_by_date, completed_dates[:source_index]
            ) if source_rows else "none"
            if request.mode != "rebuild":
                existing = artifact_store.read(identity, target)
                existing_fingerprints = (
                    existing.get("source", {}).get("source_fingerprints", [])
                    if existing is not None else []
                )
                if source_fingerprint in existing_fingerprints:
                    reused += 1
                    target_available = int(
                        isinstance(existing, Mapping) and existing.get("status") == "available"
                    )
                    continue
            if value is None or source_date is None:
                envelope = build_payload_envelope(
                    strategy_id=STRATEGY_ID, task_id=ATR_TASK_ID, algorithm_version=ATR_ALGORITHM_VERSION,
                    target_session=target, source={"sessions": completed_dates[:source_index] if source_index is not None else [], "selected_source": _selected_source(sources_by_date[completed_dates[source_index - 1]]) if source_index else {"symbol": "NIFTY", "expiry": ""}, "source_bar_count": len(source_rows)},
                    configuration={"config_fingerprint": identity["config_fingerprint"], "symbol": "NIFTY", "period": 14, "method": "wilder"},
                    payload=None, status="unavailable", unavailable_reason="insufficient_daily_atr_history",
                )
                unavailable += 1
                target_available = 0
            else:
                observed = _observed_at(rows_by_date[source_date])
                available = parse_nifty_label(f"{target}T09:15")
                envelope = build_payload_envelope(
                    strategy_id=STRATEGY_ID, task_id=ATR_TASK_ID, algorithm_version=ATR_ALGORITHM_VERSION,
                    target_session=target, source={"sessions": completed_dates[:completed_source_count], "selected_source": _selected_source(sources_by_date[source_date]) if source_date else {"symbol": "NIFTY", "expiry": ""}, "source_fingerprints": [source_fingerprint], "source_fingerprint_recipe": "ordered-storage-sources-v1", "source_bar_count": len(source_rows), "completed_session_count": completed_source_count},
                    configuration={"config_fingerprint": identity["config_fingerprint"], "symbol": "NIFTY", "period": 14, "method": "wilder"},
                    payload={"target_session": target, "source_session": source_date, "symbol": "NIFTY", "period": 14, "method": "wilder", "value": value,
                             "observed_at": serialize_utc(observed), "available_at": serialize_utc(available),
                             "lineage": [f"daily_atr:NIFTY:{target}:{value:.12g}", f"source:{source_fingerprint}"]},
                )
                target_available = 1
            artifact_store.write(envelope, source_fingerprint=source_fingerprint, output_root=request.output_root)
            materialized += 1
        return {"processed_sessions": processed, "materialized": materialized, "reused": reused, "unavailable": unavailable, "failed": 0, "target_available": target_available if target_available is not None else 0}

    return DailySetupTask(STRATEGY_ID, ATR_TASK_ID, ATR_ALGORITHM_VERSION, materialize)
