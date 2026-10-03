from __future__ import annotations

from typing import Any


def error_payload(code: str, message: str) -> dict[str, dict[str, str]]:
    return {"error": {"code": code, "message": message}}


def openapi_document() -> dict[str, Any]:
    decision_event_schema = {
        "type": "object",
        "required": ["event_id", "event_type", "category", "payload", "created_at"],
        "properties": {
            "event_id": {"type": "integer", "description": "Stable event identifier used for pagination"},
            "event_type": {
                "type": "string",
                "enum": ["CANDIDATEDECISION", "REJECTEDDECISION", "ACCEPTEDDECISION", "EXITDECISION"],
            },
            "category": {"type": "string", "enum": ["decision"]},
            "payload": {
                "type": "object",
                "required": ["decision_at"],
                "properties": {"decision_at": {"type": "string", "format": "date-time"}},
                "additionalProperties": True,
            },
            "created_at": {"type": "string", "format": "date-time", "description": "Persistence/audit timestamp"},
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
            "/api/v1/decisions": {"get": {
                "parameters": [
                    {"name": "date", "in": "query", "schema": {"type": "string", "format": "date"}, "description": "IST session date; defaults to today"},
                    {"name": "session", "in": "query", "schema": {"type": "string", "enum": ["Pre", "Morning", "Mid", "Afternoon", "Post"]}},
                    {"name": "limit", "in": "query", "schema": {"type": "integer", "minimum": 1, "maximum": 100, "default": 50}},
                    {"name": "before", "in": "query", "schema": {"type": "integer", "minimum": 1}, "description": "Event cursor from the prior page"},
                ],
                "responses": {"200": {
                "description": "Full-day monitoring summary and a cursor-paginated page of recent decisions",
                "content": {"application/json": {"schema": {
                    "type": "object",
                    "required": ["date", "summary", "decisions", "has_more", "next_cursor"],
                    "properties": {
                        "date": {"type": "string", "format": "date"},
                        "summary": {"type": "object", "description": "Complete totals and session buckets for the date"},
                        "decisions": {"type": "array", "items": decision_event_schema},
                        "has_more": {"type": "boolean"},
                        "next_cursor": {"type": "string", "nullable": True},
                    },
                }}},
            }}}},
            "/api/v1/logs": {"get": {"responses": {"200": {"description": "Runtime logs"}}}},
            "/api/v1/positions": {"get": {"responses": {"200": {"description": "Open positions"}}}},
            "/api/v1/capital": {"get": {"responses": {"200": {"description": "Capital"}}}},
            "/api/v1/trades": {"get": {"responses": {"200": {"description": "Trades"}}}},
            "/api/v1/replay": {
                "get": {"responses": {"200": {"description": "Replay runs"}}},
                "post": {"responses": {"202": {"description": "Replay queued"}, "400": {"description": "Invalid request"}}},
            },
            "/api/v1/replay/dates": {"get": {"responses": {"200": {"description": "Available replay dates"}}}},
            "/api/v1/replay/inputs": {"get": {"parameters": [{"name": "date", "in": "query", "required": True, "schema": {"type": "string", "format": "date"}}], "responses": {"200": {"description": "Deterministic replay input manifest"}}}},
            "/api/v1/replay/{run_id}": {"get": {"parameters": [{"name": "run_id", "in": "path", "required": True, "schema": {"type": "string"}}], "responses": {"200": {"description": "Replay run"}, "404": {"description": "Not found"}}}},
            "/api/v1/replay/{run_id}/trace": {"get": {"parameters": [{"name": "run_id", "in": "path", "required": True, "schema": {"type": "string"}}], "responses": {"200": {"description": "Causal feature, location, and transition trace"}}}},
            "/api/v1/replay/{run_id}/events": {"get": {"parameters": [{"name": "run_id", "in": "path", "required": True, "schema": {"type": "string"}}, {"name": "offset", "in": "query", "schema": {"type": "integer", "minimum": 0}}, {"name": "limit", "in": "query", "schema": {"type": "integer", "minimum": 1, "maximum": 10000}}], "responses": {"200": {"description": "Paginated replay events"}}}},
            "/api/v1/replay/{run_id}/cancel": {"post": {"parameters": [{"name": "run_id", "in": "path", "required": True, "schema": {"type": "string"}}], "responses": {"200": {"description": "Replay cancelled"}, "409": {"description": "Replay cannot be cancelled"}}}},
        },
    }
