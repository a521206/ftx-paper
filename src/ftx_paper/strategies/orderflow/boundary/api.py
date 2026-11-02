from __future__ import annotations

from typing import Any, Callable

from flask import Flask, jsonify, request

from ftx_paper.strategies.shared.decision_projection import read_strategy_decisions


STRATEGY_ID = "ftx-paper-orderflow"


def register_decision_api(app: Flask, store: Any, json_safe: Callable[[Any], Any]) -> None:
    """Register the orderflow strategy's causal-decision API."""

    @app.get(f"/api/v1/strategies/{STRATEGY_ID}/decisions")
    def orderflow_decisions():
        result, error = read_strategy_decisions(
            store,
            strategy_id=STRATEGY_ID,
            selected_date=request.args.get("date"),
            limit_text=request.args.get("limit", "50"),
            before_text=request.args.get("before"),
        )
        if error is not None:
            payload, status = error
            return jsonify(payload), status
        assert result is not None
        return jsonify(json_safe({**result, "strategy_id": STRATEGY_ID, "api": "orderflow"}))

    @app.get(f"/api/v1/strategies/{STRATEGY_ID}/facts")
    def orderflow_facts():
        stream = request.args.get("stream")
        selected_date = request.args.get("date")
        try:
            limit = max(1, min(int(request.args.get("limit", "100")), 1000))
        except ValueError:
            return jsonify({"error": "limit must be an integer"}), 400
        facts = store.read_orderflow_facts(limit=limit, stream=stream)
        if selected_date:
            facts = [
                item for item in facts
                if item.get("payload", {}).get("session_date") == selected_date
            ]
        return jsonify(json_safe({
            "strategy_id": STRATEGY_ID, "stream": stream, "date": selected_date,
            "facts": facts,
        }))
