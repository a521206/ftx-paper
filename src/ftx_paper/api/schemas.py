from __future__ import annotations

from typing import Any


def error_payload(code: str, message: str) -> dict[str, dict[str, str]]:
    return {"error": {"code": code, "message": message}}


def openapi_document() -> dict[str, Any]:
    decision_event_schema = {
        "type": "object",
        "required": ["event_type", "payload", "created_at"],
        "properties": {
            "event_type": {
                "type": "string",
                "enum": ["CANDIDATEDECISION", "REJECTEDDECISION", "ACCEPTEDDECISION"],
            },
            "payload": {"type": "object", "additionalProperties": True},
            "created_at": {"type": "string", "format": "date-time"},
        },
    }
    return {
        "openapi": "3.0.3",
        "info": {"title": "FTX Paper API", "version": "1.0.0"},
        "paths": {
            "/api/v1/health": {"get": {"responses": {"200": {"description": "Healthy"}}}},
            "/api/v1/runtime": {"get": {"responses": {"200": {"description": "Runtime status; interrupted sessions recover to STOPPED"}}}},
            "/api/v1/diagnostics": {"get": {"responses": {"200": {"description": "Cached runtime event and rejection counts"}}}},
            "/api/v1/events": {"get": {"responses": {"200": {"description": "Runtime events"}}}},
            "/api/v1/decisions": {"get": {"responses": {"200": {
                "description": "Persisted live decision lifecycle events",
                "content": {"application/json": {"schema": {
                    "type": "object",
                    "required": ["decisions"],
                    "properties": {"decisions": {"type": "array", "items": decision_event_schema}},
                }}},
            }}}},
            "/api/v1/logs": {"get": {"responses": {"200": {"description": "Runtime logs"}}}},
            "/api/v1/positions": {"get": {"responses": {"200": {"description": "Open positions"}}}},
            "/api/v1/capital": {"get": {"responses": {"200": {"description": "Capital"}}}},
            "/api/v1/trades": {"get": {"responses": {"200": {"description": "Trades"}}}},
        },
    }
