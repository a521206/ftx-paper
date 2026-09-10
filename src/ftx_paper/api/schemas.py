from __future__ import annotations

from typing import Any


def error_payload(code: str, message: str) -> dict[str, dict[str, str]]:
    return {"error": {"code": code, "message": message}}


def openapi_document() -> dict[str, Any]:
    return {
        "openapi": "3.0.3",
        "info": {"title": "FTX Paper API", "version": "1.0.0"},
        "paths": {
            "/api/v1/health": {"get": {"responses": {"200": {"description": "Healthy"}}}},
            "/api/v1/runtime": {"get": {"responses": {"200": {"description": "Runtime status"}}}},
            "/api/v1/events": {"get": {"responses": {"200": {"description": "Runtime events"}}}},
            "/api/v1/decisions": {"get": {"responses": {"200": {"description": "Strategy decisions"}}}},
            "/api/v1/logs": {"get": {"responses": {"200": {"description": "Runtime logs"}}}},
            "/api/v1/positions": {"get": {"responses": {"200": {"description": "Open positions"}}}},
            "/api/v1/capital": {"get": {"responses": {"200": {"description": "Capital"}}}},
            "/api/v1/trades": {"get": {"responses": {"200": {"description": "Trades"}}}},
        },
    }
