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
