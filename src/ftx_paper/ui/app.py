from __future__ import annotations

import os

from flask import Flask, render_template


def create_ui_app(api_base_url: str | None = None) -> Flask:
    """Create the presentation-only server; it has no backend imports."""
    app = Flask(__name__, template_folder="templates", static_folder="static")
    configured_api = api_base_url or os.getenv("FTX_API_BASE_URL", "http://127.0.0.1:8501")

    @app.get("/")
    @app.get("/<page>")
    def index(page: str = "dashboard"):
        pages = {"dashboard", "decisions", "positions", "trades", "events", "logs"}
        if page not in pages:
            return "Not found", 404
        return render_template("index.html", api_base_url=configured_api.rstrip("/"), page=page)

    return app
