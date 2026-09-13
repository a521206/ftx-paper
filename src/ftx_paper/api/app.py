from __future__ import annotations

from datetime import datetime

from flask import Flask, jsonify, request
from typing import Any

from ftx_paper.runtime import RuntimeController, RuntimeSession, RuntimeStore
from ftx_paper.runtime.replay_worker import ReplayWorker
from ftx_paper.capital_config import FtxCapitalConfig
from ftx_paper.runtime.events import (
    EXECUTION_EVENT_TYPES, RISK_EVENT_TYPES, DecisionTimestampError,
)
from ftx_paper.runtime.events import serialize_datetime
from .schemas import error_payload, openapi_document


def create_app(store: RuntimeStore, zerodha_auth: Any | None = None, auth_token: str | None = None,
               controller: RuntimeController | None = None, session: RuntimeSession | None = None,
               replay_worker: ReplayWorker | None = None,
               capital_config: FtxCapitalConfig | None = None) -> Flask:
    app = Flask(__name__)
    store.recover_interrupted()
    session = session or RuntimeSession(store, zerodha_auth, [])
    controller = controller or RuntimeController(store, session)
    replay_worker = replay_worker or ReplayWorker(
        store, capital_config=capital_config or FtxCapitalConfig.from_file(),
    )

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
        if auth_token is None or request.endpoint == "health":
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
        requested_date = payload.get("session_date", payload.get("date"))
        if requested_date not in (None, ""):
            if not isinstance(requested_date, str):
                return jsonify(error_payload("invalid_date", "session_date must be YYYY-MM-DD")), 400
            try:
                parsed_date = datetime.strptime(requested_date, "%Y-%m-%d").date().isoformat()
            except ValueError:
                return jsonify(error_payload("invalid_date", "session_date must be YYYY-MM-DD")), 400
            if parsed_date != requested_date or parsed_date not in store.read_market_dates():
                return jsonify(error_payload("no_data", "no market data for session_date")), 404
            payload["session_date"] = parsed_date
        elif not store.read_market_dates():
            return jsonify(error_payload("no_data", "no market data available for replay")), 404
        try:
            run_id = replay_worker.submit(payload)
        except RuntimeError as exc:
            return jsonify(error_payload("replay_queue_full", str(exc))), 409
        return jsonify(store.read_replay_run(run_id)), 202

    @app.get("/api/v1/replay")
    def list_replays():
        return jsonify(json_safe({"runs": store.read_replay_runs()}))

    @app.get("/api/v1/replay/<run_id>")
    def replay_detail(run_id: str):
        run = store.read_replay_run(run_id)
        return jsonify(json_safe(run)) if run is not None else (jsonify(error_payload("not_found", "replay not found")), 404)

    @app.post("/api/v1/replay/<run_id>/cancel")
    def cancel_replay(run_id: str):
        if not replay_worker.cancel(run_id):
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
        controller.request_start()
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
        controller.request_start()
        return "Zerodha connected successfully. You may close this window and refresh the dashboard.", 200

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
        actions = {"start": controller.request_start, "stop": controller.request_stop, "restart": controller.request_restart}
        action = actions.get(command)
        if action is None:
            return jsonify({"error": {"code": "unknown_command", "message": command}}), 404
        if command in {"start", "restart"} and zerodha_auth is not None and zerodha_auth.access_token() is None:
            return jsonify({"error": {"code": "auth_required", "message": "Connect Zerodha before starting the runtime session", "login_url": zerodha_auth.login_url()}}), 409
        action()
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
        selected_date = request.args.get("date")
        if selected_date:
            try:
                datetime.strptime(selected_date, "%Y-%m-%d").date()
            except ValueError:
                return jsonify({"error": {"code": "invalid_date", "message": "date must be YYYY-MM-DD"}}), 400
        decision_events = store.read_decision_events(session_date=selected_date, limit=1000)
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

        decision_events = [enrich(event) for event in decision_events]
        decision_events.sort(key=lambda event: event["payload"]["decision_at"], reverse=True)
        return jsonify(json_safe({"decisions": decision_events}))

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
