from __future__ import annotations

import os
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Flask, jsonify, redirect, request
from typing import Any

from ftx_paper.runtime import ProcessAlreadyRunningError, RuntimeSession
from ftx_paper.runtime.store import RuntimeStore
from ftx_paper.runtime.replay_worker import ReplayWorker
from ftx_paper.application.runtime_operations import RuntimeOperations
from ftx_paper.domain.capital import ResearchCapitalProfile, RESEARCH_CAPITAL_PROFILE
from ftx_paper.runtime.events import (
    EXECUTION_EVENT_TYPES, RISK_EVENT_TYPES, DecisionTimestampError,
)
from ftx_paper.runtime.events import serialize_datetime
from .schemas import error_payload, openapi_document


def create_app(store: RuntimeStore, zerodha_auth: Any | None = None, auth_token: str | None = None,
               controller: Any | None = None, session: RuntimeSession | None = None,
               replay_worker: ReplayWorker | None = None,
               capital_profile: ResearchCapitalProfile | None = None,
               runtime_operations: RuntimeOperations | None = None) -> Flask:
    app = Flask(__name__)
    store.recover_interrupted()
    session = session or RuntimeSession(store, zerodha_auth, [])
    replay_worker = replay_worker or ReplayWorker(
        store, capital_profile=capital_profile or RESEARCH_CAPITAL_PROFILE,
    )
    operations = runtime_operations or RuntimeOperations(session, replay_worker, controller)

    def json_safe(value: Any) -> Any:
        if isinstance(value, datetime):
            return serialize_datetime(value)
        if isinstance(value, dict):
            return {key: json_safe(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [json_safe(item) for item in value]
        return value

    @app.errorhandler(DecisionTimestampError)
    def invalid_decision_timestamp(error: DecisionTimestampError):
        return jsonify({"error": {"code": "invalid_decision_at", "message": str(error)}}), 422

    @app.after_request
    def add_cors_headers(response):
        origin = request.headers.get("Origin", "")
        allowed = {"http://127.0.0.1:8502", "http://localhost:8502"}
        response.headers["Access-Control-Allow-Origin"] = origin if origin in allowed else "http://127.0.0.1:8502"
        response.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        return response

    @app.before_request
    def authenticate():
        if request.endpoint == "health" or (auth_token is None and request.remote_addr in {"127.0.0.1", "::1"}):
            return None
        header = request.headers.get("Authorization", "")
        if not header.startswith("Bearer ") or header[7:].strip() != auth_token:
            return jsonify({"error": {"code": "unauthorized", "message": "Bearer authentication required"}}), 401
        return None

    @app.get("/api/v1/health")
    def health():
        return jsonify({"status": "pass", "service": "ftx-paper-api"})

    @app.post("/api/v1/replay")
    def create_replay():
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify(error_payload("invalid_request", "JSON object body required")), 400
        if "vehicle" in payload:
            return jsonify(error_payload("invalid_request", "use vehicles array; vehicle is no longer supported")), 400
        if set(payload) - {"session_date", "vehicles", "emit_rejected_decisions"}:
            return jsonify(error_payload("invalid_request", "unsupported replay request fields")), 400
        if "emit_rejected_decisions" in payload and not isinstance(
            payload["emit_rejected_decisions"], bool,
        ):
            return jsonify(error_payload(
                "invalid_request", "emit_rejected_decisions must be a boolean",
            )), 400
        vehicles = payload.get("vehicles")
        if vehicles is not None and (
            not isinstance(vehicles, list)
            or not vehicles
            or any(not isinstance(item, str) for item in vehicles)
            or any(item not in {"futures", "synthetic"} for item in vehicles)
            or "futures" not in vehicles
        ):
            return jsonify(error_payload("invalid_request", "vehicles must include futures; synthetic is reporting-only")), 400
        requested_date = payload.get("session_date")
        if not isinstance(requested_date, str):
            return jsonify(error_payload("invalid_date", "session_date is required and must be YYYY-MM-DD")), 400
        try:
            parsed_date = datetime.strptime(requested_date, "%Y-%m-%d").date().isoformat()
        except ValueError:
            return jsonify(error_payload("invalid_date", "session_date must be YYYY-MM-DD")), 400
        if parsed_date != requested_date or parsed_date not in store.read_market_dates():
            return jsonify(error_payload("no_data", "no market data for session_date")), 404
        payload["session_date"] = parsed_date
        try:
            run_id = operations.submit_replay(payload)
        except RuntimeError as exc:
            return jsonify(error_payload("replay_queue_full", str(exc))), 409
        return jsonify(store.read_replay_run(run_id)), 202

    @app.get("/api/v1/replay")
    def list_replays():
        return jsonify(json_safe({"runs": store.read_replay_run_summaries()}))

    @app.get("/api/v1/replay/dates")
    def replay_dates():
        return jsonify({"dates": store.read_market_dates()})

    @app.get("/api/v1/replay/inputs")
    def replay_inputs():
        session_date = request.args.get("date", "")
        try:
            parsed = datetime.strptime(session_date, "%Y-%m-%d").date().isoformat()
        except ValueError:
            return jsonify(error_payload("invalid_date", "date must be YYYY-MM-DD")), 400
        if parsed != session_date:
            return jsonify(error_payload("invalid_date", "date must be YYYY-MM-DD")), 400
        if session_date not in store.read_market_dates():
            return jsonify(error_payload("no_data", "no market data for session_date")), 404
        return jsonify(json_safe(replay_worker.input_manifest(session_date)))

    @app.get("/api/v1/replay/<run_id>")
    def replay_detail(run_id: str):
        run = store.read_replay_run(run_id)
        if run is None:
            return jsonify(error_payload("not_found", "replay not found")), 404
        result_raw = run.get("result")
        result: dict[str, Any] = result_raw if isinstance(result_raw, dict) else {}
        run["replay_metadata"] = {
            "run_id": run_id,
            "status": run.get("status"),
            "request": run.get("request"),
            "result_schema_version": result.get("result_schema_version"),
            "strategy": result.get("strategy"),
            "configuration": result.get("configuration"),
            "metadata": result.get("metadata", {}),
        }
        return jsonify(json_safe(run))

    @app.get("/api/v1/replay/<run_id>/trace")
    def replay_trace(run_id: str):
        run = store.read_replay_run(run_id)
        if run is None:
            return jsonify(error_payload("not_found", "replay not found")), 404
        result_raw = run.get("result")
        result: dict[str, Any] = result_raw if isinstance(result_raw, dict) else {}
        return jsonify(json_safe({"run_id": run_id, "metadata": result.get("metadata", {}),
                                  "schema_version": result.get("trace_schema_version", 1),
                                  "trace": result.get("trace", [])}))

    @app.get("/api/v1/replay/<run_id>/events")
    def replay_events(run_id: str):
        run = store.read_replay_run(run_id)
        if run is None:
            return jsonify(error_payload("not_found", "replay not found")), 404
        result_raw = run.get("result")
        result: dict[str, Any] = result_raw if isinstance(result_raw, dict) else {}
        events = list(result.get("events", []))
        try:
            offset = max(0, int(request.args.get("offset", 0)))
            limit = min(10_000, max(1, int(request.args.get("limit", 1_000))))
        except ValueError:
            return jsonify(error_payload("invalid_request", "offset and limit must be integers")), 400
        return jsonify(json_safe({
            "run_id": run_id,
            "metadata": result.get("metadata", {}),
            "offset": offset,
            "limit": limit,
            "total": len(events),
            "events": events[offset:offset + limit],
            "has_more": offset + limit < len(events),
            "next_offset": offset + limit if offset + limit < len(events) else None,
        }))

    @app.post("/api/v1/replay/<run_id>/cancel")
    def cancel_replay(run_id: str):
        if not operations.cancel_replay(run_id):
            return jsonify(error_payload("not_cancellable", "replay cannot be cancelled")), 409
        return jsonify(json_safe(store.read_replay_run(run_id)))

    @app.get("/api/v1/openapi.json")
    def openapi():
        return jsonify(openapi_document())

    @app.get("/api/v1/broker/zerodha/login")
    def zerodha_login():
        if zerodha_auth is None:
            return jsonify(error_payload("broker_not_configured", "Zerodha is not configured")), 503
        return jsonify({"provider": "zerodha", "login_url": zerodha_auth.login_url()})

    @app.post("/api/v1/broker/zerodha/callback")
    def zerodha_callback():
        if zerodha_auth is None:
            return jsonify({"error": {"code": "broker_not_configured", "message": "Zerodha is not configured"}}), 503
        request_token = (request.get_json(silent=True) or {}).get("request_token", "")
        try:
            zerodha_auth.exchange(str(request_token))
        except ValueError as exc:
            return jsonify({"error": {"code": "invalid_request_token", "message": str(exc)}}), 400
        except Exception as exc:  # broker errors are not safe to expose as 500 details
            return jsonify({"error": {"code": "broker_auth_failed", "message": str(exc)}}), 502
        operations.request_start()
        return jsonify({"provider": "zerodha", "authenticated": True}), 200

    @app.get("/zerodha/callback")
    def zerodha_browser_callback():
        if zerodha_auth is None:
            return "Zerodha is not configured", 503
        if request.args.get("status", "success").lower() != "success":
            return "Zerodha login was not completed. You may close this window.", 400
        request_token = request.args.get("request_token", "")
        try:
            zerodha_auth.exchange(request_token)
        except ValueError as exc:
            return f"Zerodha login failed: {exc}", 400
        except Exception:
            return "Zerodha token exchange failed. Check the API logs.", 502
        operations.request_start()
        dashboard_url = os.getenv("FTX_UI_BASE_URL", "http://127.0.0.1:8502/")
        return redirect(dashboard_url)

    @app.get("/api/v1/broker/zerodha/status")
    def zerodha_status():
        authenticated = zerodha_auth is not None and zerodha_auth.access_token() is not None
        return jsonify({"provider": "zerodha", "configured": zerodha_auth is not None, "authenticated": authenticated})

    @app.get("/api/v1/runtime")
    def runtime():
        status = store.read_status()
        if status.get("bars_seen") or store.read_events(1):
            status["event_counts"] = store.event_counts()
        return jsonify(status)

    @app.get("/api/v1/diagnostics")
    def diagnostics():
        return jsonify({"status": store.read_status(), "event_counts": store.event_counts()})

    @app.post("/api/v1/runtime/<command>")
    def runtime_command(command: str):
        actions = {"start": operations.request_start, "stop": operations.request_stop, "restart": operations.request_restart}
        action = actions.get(command)
        if action is None:
            return jsonify({"error": {"code": "unknown_command", "message": command}}), 404
        if command in {"start", "restart"} and zerodha_auth is not None and zerodha_auth.access_token() is None:
            return jsonify({"error": {"code": "auth_required", "message": "Connect Zerodha before starting the runtime session", "login_url": zerodha_auth.login_url()}}), 409
        try:
            action()
        except ProcessAlreadyRunningError as exc:
            return jsonify(error_payload("runtime_already_running", str(exc))), 409
        return jsonify(store.read_status()), 202

    @app.get("/api/v1/events")
    def events():
        try:
            limit = min(int(request.args.get("limit", "100")), 1000)
        except ValueError:
            return jsonify({"error": {"code": "invalid_limit", "message": "limit must be an integer"}}), 400
        return jsonify(json_safe({"events": store.read_events(limit)}))

    @app.get("/api/v1/decisions")
    def decisions():
        selected_date = request.args.get("date") or datetime.now(ZoneInfo("Asia/Kolkata")).date().isoformat()
        try:
            parsed_date = datetime.strptime(selected_date, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({"error": {"code": "invalid_date", "message": "date must be YYYY-MM-DD"}}), 400
        if parsed_date.isoformat() != selected_date:
            return jsonify({"error": {"code": "invalid_date", "message": "date must be YYYY-MM-DD"}}), 400
        try:
            limit = int(request.args.get("limit", "50"))
            before_id = int(request.args["before"]) if request.args.get("before") else None
        except ValueError:
            return jsonify(error_payload("invalid_pagination", "limit and before must be integers")), 400
        if not 1 <= limit <= 100:
            return jsonify(error_payload("invalid_limit", "limit must be between 1 and 100")), 400
        if before_id is not None and before_id < 1:
            return jsonify(error_payload("invalid_cursor", "before must be a positive event cursor")), 400
        selected_session = request.args.get("session")
        if selected_session not in {None, "", "Pre", "Morning", "Mid", "Afternoon", "Post"}:
            return jsonify(error_payload("invalid_session", "session must be Pre, Morning, Mid, Afternoon, or Post")), 400

        summary = store.read_decision_summary(selected_date)
        decision_events = store.read_decision_events(
            session_date=selected_date, limit=limit + 1, before_id=before_id,
            session_name=selected_session or None,
        )
        has_more = len(decision_events) > limit
        decision_events = decision_events[:limit]
        decision_ids = {
            str(event["payload"]["decision_id"])
            for event in decision_events if event.get("payload", {}).get("decision_id")
        }
        executions_by_decision: dict[str, list[dict[str, Any]]] = {}
        risks_by_decision: dict[str, list[dict[str, Any]]] = {}
        for event in store.read_decision_lifecycle_events(decision_ids):
            event_type = str(event.get("event_type", "")).upper()
            decision_id = event.get("payload", {}).get("decision_id")
            if not decision_id:
                continue
            if event_type in EXECUTION_EVENT_TYPES:
                executions_by_decision.setdefault(str(decision_id), []).append(event)
            elif event_type in RISK_EVENT_TYPES:
                risks_by_decision.setdefault(str(decision_id), []).append(event)

        def enrich(event: dict[str, Any]) -> dict[str, Any]:
            payload = dict(event.get("payload", {}))
            decision_id = payload.get("decision_id")
            lifecycle = executions_by_decision.get(str(decision_id), []) if decision_id else []
            risks = risks_by_decision.get(str(decision_id), []) if decision_id else []
            execution = lifecycle[0] if lifecycle else None
            risk = risks[0] if risks else None
            if execution is not None:
                execution_type = str(execution["event_type"]).upper()
                status = {
                    "ORDER_SUPPRESSED": "replayed",
                    "ORDER_ACK": "acknowledged",
                    "EXECUTEDDECISION": "filled",
                    "FILL": "filled",
                    "ORDER_UNFILLED": "unfilled",
                    "EXECUTION_ERROR": "execution_error",
                }.get(execution_type, "unknown")
                payload["execution_status"] = status
                payload["execution_event_type"] = execution_type
                payload["execution"] = execution
            if risk is not None:
                payload["risk_status"] = "sizing_rejected"
                payload["risk_event"] = risk
            if execution is None and str(payload.get("decision_source", "")).lower() == "replay":
                payload["execution_status"] = "replayed"
            return {**event, "category": "decision", "payload": payload}

        enriched_events = []
        for event in decision_events:
            cursor_id = event.pop("_cursor_id")
            enriched_events.append({**enrich(event), "event_id": cursor_id})
        next_cursor = str(enriched_events[-1]["event_id"]) if has_more and enriched_events else None
        enriched_events.sort(key=lambda event: event["payload"]["decision_at"], reverse=True)
        return jsonify(json_safe({
            "date": selected_date,
            "summary": summary,
            "decisions": enriched_events,
            "has_more": has_more,
            "next_cursor": next_cursor,
        }))

    @app.get("/api/v1/logs")
    def logs():
        return jsonify(json_safe({"logs": store.read_events(1000)}))

    @app.get("/api/v1/positions")
    def positions():
        status = store.read_status()
        return jsonify({"positions": status.get("open_positions", [])})

    @app.get("/api/v1/capital")
    def capital():
        status = store.read_status()
        portfolio = session.portfolio
        if portfolio is not None:
            values = portfolio.capital_snapshot()
            return jsonify({**values, "capital": values["current_equity"], "currency": "INR"})
        raw = status.get("capital", 0)
        values = raw if isinstance(raw, dict) else {}
        return jsonify({
            "capital": values.get("capital", raw if not isinstance(raw, dict) else None),
            "initial_capital": values.get("initial_capital"),
            "current_equity": values.get("current_equity", values.get("capital")),
            "realized_pnl": values.get("realized_pnl", values.get("total_pnl")),
            "total_pnl": values.get("total_pnl", values.get("realized_pnl")),
            "open_margin": values.get("open_margin", values.get("open_margin_used")),
            "drawdown": values.get("drawdown", values.get("drawdown_pct")),
            "currency": "INR",
        })

    @app.get("/api/v1/trades")
    def trades():
        rows = [event for event in store.read_events(1000) if event["event_type"] in {"FILL", "TRADE"}]
        return jsonify({"trades": rows})

    @app.get("/api/v1/audit")
    def audit():
        return jsonify(json_safe({"events": store.read_events(1000)}))

    return app
