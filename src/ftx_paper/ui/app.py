from __future__ import annotations

import os

from flask import Flask, redirect, render_template, request, url_for


def create_ui_app(api_base_url: str | None = None) -> Flask:
    """Create the presentation-only server; it has no backend imports."""
    app = Flask(__name__, template_folder="templates", static_folder="static")
    configured_api = api_base_url or os.getenv("FTX_API_BASE_URL", "http://127.0.0.1:8501")

    @app.after_request
    def disable_html_caching(response):
        if response.mimetype == "text/html":
            response.headers["Cache-Control"] = "no-store"
        return response

    @app.get("/")
    @app.get("/<page>")
    def index(page: str = "dashboard"):
        pages = {"dashboard", "decisions", "positions", "trades", "activity", "capital", "replay"}
        if page not in pages:
            return "Not found", 404
        strategy_id = request.args.get("strategy_id", "")
        strategy_templates = {
            ("ftx-paper-research", "decisions"): "strategies/ftx-paper-research.html",
            ("ftx-paper-orderflow", "replay"): "strategies/ftx-paper-orderflow.html",
        }
        template = strategy_templates.get((strategy_id, page), "index.html")
        known_strategy = strategy_id if strategy_id in {"ftx-paper-research", "ftx-paper-orderflow"} else ""
        return render_template(
            template,
            api_base_url=configured_api.rstrip("/"),
            page=page,
            strategy_id=known_strategy,
        )

    @app.get("/replay/investigate")
    def replay_investigation():
        return redirect(url_for("index", page="replay"))

    return app
