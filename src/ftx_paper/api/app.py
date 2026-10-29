from __future__ import annotations

import os
from datetime import datetime

from flask import Flask, jsonify, redirect, request
from typing import Any

from ftx_paper.config import IST
from ftx_paper.contracts import ReplayRequest, validate_decision_view
from ftx_paper.infrastructure.sqlite.runtime_store import ProcessAlreadyRunningError
from ftx_paper.runtime.session import RuntimeSession
from ftx_paper.runtime.store import RuntimeStore
from ftx_paper.runtime.replay_worker import ReplayWorker
from ftx_paper.runtime.parity_artifacts import PARITY_LAYERS, read_parity_artifact
from ftx_paper.application.runtime_operations import RuntimeOperations
from ftx_paper.domain.capital import ResearchCapitalProfile, RESEARCH_CAPITAL_PROFILE
from ftx_paper.runtime.events import (
    EXECUTION_EVENT_TYPES, RISK_EVENT_TYPES, DecisionTimestampError,
)
from ftx_paper.runtime.events import serialize_datetime
from .schemas import error_payload, openapi_document
from .orderflow import build_orderflow_projection

INVALID_STRATEGY = object()


def create_app(store: RuntimeStore, zerodha_auth: Any | None = None, auth_token: str | None = None,
               controller: Any | None = None, session: RuntimeSession | None = None,
               replay_worker: ReplayWorker | None = None,
               capital_profile: ResearchCapitalProfile | None = None,
               runtime_operations: RuntimeOperations | None = None,
               broker_auth: Any | None = None) -> Flask:
    app = Flask(__name__)
    store.recover_interrupted()
    effective_auth = broker_auth or zerodha_auth
    session = session or RuntimeSession(store, effective_auth, [])
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

    def strategy_filter() -> str | None | object:
        selected = request.args.get("strategy_id")
        if not selected:
            return None
        known = {
            str(item["strategy_id"])
            for item in configured_strategy_statuses()
            if item.get("strategy_id")
        }
        if selected not in known:
            return INVALID_STRATEGY
        return selected

    def configured_strategy_statuses() -> list[dict[str, Any]]:
        """Keep configured strategies visible while the runtime is stopped.

        A fresh process may not have an in-memory engine yet, but the last
        runtime status still contains the strategy identities needed by the UI
        and by strategy_id validation.
        """
        current = list(session.strategy_status())
        if current:
            return current
        stored = store.read_status().get("strategies", [])
        return [item for item in stored if isinstance(item, dict)]

    def filter_attributed(rows: list[dict[str, Any]], selected: str | None | object) -> list[dict[str, Any]]:
        if selected is None or selected is INVALID_STRATEGY:
            return rows
        return [row for row in rows if row.get("payload", {}).get("strategy_id") == selected]

    def incomplete_replay_response(run: dict[str, Any]):
        return jsonify({
            "run_id": run.get("run_id"),
            "status": run.get("status"),
            "request": run.get("request"),
            "error": run.get("error"),
        }), 202

    def parity_artifact_for(run: dict[str, Any]) -> tuple[dict[str, Any] | None, Any]:
        if run.get("status") not in {"completed", "failed", "cancelled"}:
            return None, incomplete_replay_response(run)
        result = run.get("result")
        artifact = result.get("parity_artifact") if isinstance(result, dict) else None
        if not isinstance(artifact, dict):
            return None, (jsonify(error_payload("parity_unavailable", "replay has no parity artifact")), 404)
        try:
            return read_parity_artifact(artifact), None
        except ValueError as exc:
            return None, (jsonify(error_payload("invalid_parity_artifact", str(exc))), 500)

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
        required_fields = {"session_date", "strategy_id", "vehicles", "mode", "include_parity_layers"}
        missing_fields = required_fields - set(payload)
        if missing_fields:
            return jsonify(error_payload(
                "invalid_request", "missing replay request fields: " + ", ".join(sorted(missing_fields)),
            )), 400
        try:
            replay_request = ReplayRequest.from_mapping(payload)
        except (TypeError, ValueError) as exc:
            return jsonify(error_payload("invalid_request", str(exc))), 400
        requested_date = replay_request.session_date
        try:
            parsed_date = datetime.strptime(requested_date, "%Y-%m-%d").date().isoformat()
        except ValueError:
            return jsonify(error_payload("invalid_date", "session_date must be YYYY-MM-DD")), 400
        if parsed_date != requested_date or parsed_date not in store.read_market_dates():
            return jsonify(error_payload("no_data", "no market data for session_date")), 404
        if replay_request.strategy_id == "ftx-paper-orderflow":
            coverage = store.read_market_coverage(parsed_date)
            symbols = {
                str(row["symbol"]).strip().upper()
                for row in coverage["rows"]
                if str(row["instrument_type"]).upper() == "INDEX"
            }
            missing = sorted(
                symbol for symbol in ("NIFTY", "NIFTY 50", "INDIA VIX")
                if symbol not in symbols
            )
            # NIFTY and NIFTY 50 are equivalent storage symbols.
            if "NIFTY" in missing and "NIFTY 50" in symbols:
                missing.remove("NIFTY")
            if "NIFTY 50" in missing and "NIFTY" in symbols:
                missing.remove("NIFTY 50")
            if missing:
                return jsonify(error_payload(
                    "incomplete_market_data",
                    "required index input is missing: " + ", ".join(missing),
                )), 409
        payload = replay_request.to_dict()
        payload["session_date"] = parsed_date
        try:
            run_id = operations.submit_replay(payload)
        except RuntimeError as exc:
            return jsonify(error_payload("replay_queue_full", str(exc))), 409
        queued = store.read_replay_run(run_id)
        if queued is None:
            return jsonify(error_payload("not_found", "replay not found")), 404
        return jsonify(json_safe(queued)), 202

    @app.get("/api/v1/replay")
    def list_replays():
        return jsonify(json_safe({"runs": store.read_replay_run_summaries()}))

    @app.get("/api/v1/replay/<run_id>")
    def replay_detail(run_id: str):
        run = store.read_replay_run(run_id)
        if run is None:
            return jsonify(error_payload("not_found", "replay not found")), 404
        if run.get("status") not in {"completed", "failed", "cancelled"}:
            return incomplete_replay_response(run)
        return jsonify(json_safe(run))

    @app.get("/api/v1/replay/<run_id>/parity/manifest")
    def replay_parity_manifest(run_id: str):
        run = store.read_replay_run(run_id)
        if run is None:
            return jsonify(error_payload("not_found", "replay not found")), 404
        artifact, error = parity_artifact_for(run)
        if error is not None:
            return error
        assert artifact is not None
        return jsonify(json_safe(artifact["manifest"]))

    @app.get("/api/v1/replay/<run_id>/parity/<layer>")
    def replay_parity_layer(run_id: str, layer: str):
        if layer not in PARITY_LAYERS:
            return jsonify(error_payload("invalid_layer", f"unsupported parity layer: {layer}")), 400
        run = store.read_replay_run(run_id)
        if run is None:
            return jsonify(error_payload("not_found", "replay not found")), 404
        artifact, error = parity_artifact_for(run)
        if error is not None:
            return error
        assert artifact is not None
        try:
            offset = int(request.args.get("offset", "0"))
            limit = int(request.args.get("limit", "100"))
        except ValueError:
            return jsonify(error_payload("invalid_pagination", "offset and limit must be integers")), 400
        if offset < 0 or limit < 1 or limit > 1000:
            return jsonify(error_payload("invalid_pagination", "offset must be non-negative and limit must be between 1 and 1000")), 400
        records = artifact["layers"][layer]
        page = records[offset:offset + limit]
        next_offset = offset + len(page) if offset + len(page) < len(records) else None
        return jsonify(json_safe({
            "run_id": run_id,
            "layer": layer,
            "offset": offset,
            "limit": limit,
            "next_offset": next_offset,
            "records": page,
            "manifest": artifact["manifest"]["layers"][layer],
        }))

    @app.get("/api/v1/openapi.json")
    def openapi():
        return jsonify(openapi_document())

    shoonya_auth = broker_auth if getattr(broker_auth, "provider", "") == "shoonya" else None
    xts_auth = broker_auth if getattr(broker_auth, "provider", "") == "xts" else None
    active_auth = broker_auth or zerodha_auth

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

    @app.route("/api/v1/broker/xts/login", methods=["GET", "POST"])
    def xts_login():
        auth = xts_auth or (active_auth if getattr(active_auth, "provider", "") == "xts" else None)
        if auth is None:
            return jsonify(error_payload("broker_not_configured", "XTS is not configured")), 503

        try:
            token = auth.login()
            operations.request_start()
            return jsonify({"provider": "xts", "authenticated": True, "token_obtained": bool(token)})
        except Exception as exc:
            return jsonify(error_payload("broker_auth_failed", str(exc))), 502

    @app.get("/api/v1/broker/xts/status")
    def xts_status():
        auth = xts_auth or (active_auth if getattr(active_auth, "provider", "") == "xts" else None)
        authenticated = auth is not None and auth.access_token() is not None
        return jsonify({"provider": "xts", "configured": auth is not None, "authenticated": authenticated})

    @app.route("/api/v1/broker/shoonya/login", methods=["GET", "POST"])
    def shoonya_login():
        auth = shoonya_auth or (active_auth if getattr(active_auth, "provider", "") == "shoonya" else None)
        if auth is None:
            return jsonify(error_payload("broker_not_configured", "Shoonya is not configured")), 503

        if request.method == "POST":
            payload = request.get_json(silent=True) or {}
            token = str(payload.get("token", "")).strip()
            raw_code = str(payload.get("code") or payload.get("auth_code", "")).strip()
            if "code=" in raw_code:
                from urllib.parse import urlparse, parse_qs
                parsed = urlparse(raw_code)
                qs = parse_qs(parsed.query or parsed.path)
                auth_code = qs.get("code", [raw_code])[0]
            else:
                auth_code = raw_code
            try:
                if token:
                    auth.save_token(token)
                elif auth_code:
                    auth.exchange(auth_code)
                else:
                    auth.login()
                operations.request_start()
                return jsonify({"provider": "shoonya", "authenticated": True}), 200
            except Exception as exc:
                return jsonify(error_payload("broker_auth_failed", str(exc))), 502

        # GET request
        try:
            token = auth.login()
            operations.request_start()
            return jsonify({"provider": "shoonya", "authenticated": True, "token_obtained": bool(token)})
        except Exception as exc:
            login_url = getattr(auth, "login_url", lambda: None)()
            return (
                jsonify(
                    {
                        "provider": "shoonya",
                        "authenticated": False,
                        "login_url": login_url,
                        "error": str(exc),
                    }
                ),
                200 if login_url else 502,
            )

    @app.get("/shoonya/callback")
    def shoonya_browser_callback():
        auth = shoonya_auth or (active_auth if getattr(active_auth, "provider", "") == "shoonya" else None)
        if auth is None:
            return "Shoonya is not configured", 503
        code = request.args.get("code") or request.args.get("auth_code", "")
        if not code:
            return "Shoonya login did not provide an authorization code. You may close this window.", 400
        try:
            auth.exchange(code)
        except Exception as exc:
            return f"Shoonya token exchange failed: {exc}", 502
        operations.request_start()
        dashboard_url = os.getenv("FTX_UI_BASE_URL", "http://127.0.0.1:8502/")
        return redirect(dashboard_url)

    @app.get("/api/v1/broker/shoonya/status")
    def shoonya_status():
        auth = shoonya_auth or (active_auth if getattr(active_auth, "provider", "") == "shoonya" else None)
        authenticated = auth is not None and auth.access_token() is not None
        return jsonify({"provider": "shoonya", "configured": auth is not None, "authenticated": authenticated})

    @app.get("/api/v1/broker/status")
    def active_broker_status():
        auth = active_auth
        provider = getattr(auth, "provider", "none") if auth is not None else "none"
        authenticated = auth is not None and auth.access_token() is not None
        return jsonify({"provider": provider, "configured": auth is not None, "authenticated": authenticated})

    @app.get("/api/v1/runtime")
    def runtime():
        status = store.read_status()
        statuses = configured_strategy_statuses()
        failed_id = status.get("failed_strategy_id")
        if failed_id:
            statuses = [
                {**item, "state": "failed" if item["strategy_id"] == failed_id else "stopped"}
                for item in statuses
            ]
        status["strategies"] = statuses
        if status.get("bars_seen") or store.read_events(1):
            status["event_counts"] = store.event_counts()
        return jsonify(status)

    @app.get("/api/v1/strategies")
    def strategies():
        """Return the canonical configured strategy catalog for every UI page."""
        return jsonify(json_safe({"strategies": configured_strategy_statuses()}))

    @app.get("/api/v1/diagnostics")
    def diagnostics():
        selected = strategy_filter()
        if selected is INVALID_STRATEGY:
            return jsonify(error_payload("invalid_strategy", "strategy_id is not configured")), 400
        events = filter_attributed(store.read_events(1000), selected)
        counts: dict[str, int] = {}
        for event in events:
            event_type = str(event.get("event_type", "UNKNOWN"))
            counts[event_type] = counts.get(event_type, 0) + 1
        return jsonify({"status": store.read_status(), "event_counts": counts})

    @app.post("/api/v1/runtime/<command>")
    def runtime_command(command: str):
        actions: dict[str, Any] = {
            "start": operations.request_start,
            "stop": operations.request_stop,
            "restart": operations.request_restart,
        }
        action = actions.get(command)
        if action is None:
            return jsonify({"error": {"code": "unknown_command", "message": command}}), 404
        if command in {"start", "restart"} and active_auth is not None and active_auth.access_token() is None:
            login_url = getattr(active_auth, "login_url", lambda: None)()
            provider = getattr(active_auth, "provider", "broker")
            return jsonify({"error": {"code": "auth_required", "message": f"Connect {provider.capitalize()} before starting the runtime session", "login_url": login_url}}), 409
        try:
            action()
        except ProcessAlreadyRunningError as exc:
            return jsonify(error_payload("runtime_already_running", str(exc))), 409
        return jsonify(store.read_status()), 202

    @app.get("/api/v1/decisions")
    def decisions():
        selected_date = request.args.get("date") or datetime.now(IST).date().isoformat()
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
        selected_strategy = strategy_filter()
        if selected_strategy is INVALID_STRATEGY:
            return jsonify(error_payload("invalid_strategy", "strategy_id is not configured")), 400

        strategy_id = selected_strategy if isinstance(selected_strategy, str) else None
        summary = store.read_decision_summary(selected_date, strategy_id=strategy_id, canonical_only=True)
        decision_events = store.read_decision_events(
            session_date=selected_date, strategy_id=strategy_id,
            limit=limit + 1, before_id=before_id,
            session_name=selected_session or None,
            canonical_only=True,
        )
        has_more = len(decision_events) > limit
        decision_events = decision_events[:limit]
        decision_ids = {
            str(event["payload"]["decision_id"])
            for event in decision_events if event.get("payload", {}).get("decision_id")
        }
        executions_by_decision: dict[str, list[dict[str, Any]]] = {}
        risks_by_decision: dict[str, list[dict[str, Any]]] = {}
        lifecycle_events = store.read_decision_lifecycle_events(decision_ids)
        if strategy_id:
            lifecycle_events = filter_attributed(lifecycle_events, strategy_id)
        for event in lifecycle_events:
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
                    "FILL": "filled",
                    "ORDER_UNFILLED": "unfilled",
                    "EXECUTION_ERROR": "execution_error",
                    "RISK_REJECTED": "risk_rejected",
                }.get(execution_type, "unknown")
                payload["execution_status"] = status
                payload["execution_event_type"] = execution_type
                payload["execution"] = execution
            if risk is not None:
                payload["risk_status"] = "sizing_rejected"
                payload["risk_event"] = risk
            if execution is None and str(payload.get("decision_source", "")).lower() == "replay":
                payload["execution_status"] = "replayed"
            # Prefer the persisted strategy-neutral contract. The fallback is
            # only for decisions written before the contract was introduced.
            raw_view = payload.get("decision_view")
            if not isinstance(raw_view, dict):
                raise ValueError("canonical decision event is missing decision_view")
            view = validate_decision_view(raw_view)
            # Lifecycle evidence is newer than the policy decision and changes
            # the aggregate status without rewriting policy.outcome.
            outcome = payload.get("risk_status") or payload.get("execution_status") or view.get("outcome") or "recorded"
            view["outcome"] = outcome
            view["display_outcome"] = outcome
            lifecycle = {}
            if execution is not None:
                lifecycle.update({"status": payload.get("execution_status"), "event_type": payload.get("execution_event_type")})
            if risk is not None:
                lifecycle.update({"status": payload.get("risk_status"), "event_type": risk.get("event_type")})
            if lifecycle:
                view["execution"] = lifecycle
            return {**event, "category": "decision", "decision_view": view, "payload": payload}

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

    @app.get("/api/v1/positions")
    def positions():
        status = store.read_status()
        return jsonify({"positions": status.get("open_positions", [])})

    @app.get("/api/v1/capital")
    def capital():
        status = store.read_status()
        portfolio = session.portfolio if session is not None else None
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
        selected = strategy_filter()
        if selected is INVALID_STRATEGY:
            return jsonify(error_payload("invalid_strategy", "strategy_id is not configured")), 400
        rows = [event for event in filter_attributed(store.read_events(1000), selected)
                if event["event_type"] in {"FILL", "TRADE"}]
        return jsonify({"trades": rows})

    @app.get("/api/v1/orderflow")
    def orderflow():
        selected_date = request.args.get("date") or datetime.now(IST).date().isoformat()
        try:
            parsed_date = datetime.strptime(selected_date, "%Y-%m-%d").date()
        except ValueError:
            return jsonify(error_payload("invalid_date", "date must be YYYY-MM-DD")), 400
        if parsed_date.isoformat() != selected_date:
            return jsonify(error_payload("invalid_date", "date must be YYYY-MM-DD")), 400
        strategy_id = request.args.get("strategy_id", "orderflow-research")
        if strategy_id != "orderflow-research":
            return jsonify(error_payload("invalid_strategy", "orderflow endpoint requires orderflow-research")), 400
        decision_rows = store.read_decision_events(session_date=selected_date, strategy_id=strategy_id, limit=100, canonical_only=True)
        decisions_for_projection = []
        for row in decision_rows:
            payload = dict(row.get("payload", {}))
            view = validate_decision_view(payload["decision_view"])
            decisions_for_projection.append({**row, "decision_view": view})
        lifecycle = store.read_decision_lifecycle_events([str(_row.get("payload", {}).get("decision_id")) for _row in decision_rows])
        events = [*decisions_for_projection, *lifecycle]
        trades = [event for event in filter_attributed(store.read_events(1000), strategy_id) if event["event_type"] in {"FILL", "TRADE"}]
        return jsonify(json_safe(build_orderflow_projection(
            decisions_for_projection, events, store.read_status().get("open_positions", []), trades, as_of=datetime.now(IST).isoformat(),
        )))

    return app
