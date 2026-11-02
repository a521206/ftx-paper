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
                "enum": ["POLICY_DECISION"],
            },
            "category": {"type": "string", "enum": ["decision"]},
            "decision_view": {
                "type": "object",
                "description": "Strategy-neutral summary for decision review clients",
                "required": ["schema_version", "at", "detail", "outcome", "subject", "policy", "evidence"],
                "properties": {
                    "schema_version": {"type": "integer", "minimum": 1},
                    "at": {"type": "string", "format": "date-time"},
                    "strategy_id": {"type": "string", "nullable": True},
                    "strategy_version": {"type": "string", "nullable": True},
                    "config_hash": {"type": "string", "nullable": True},
                    "decision_id": {"type": "string", "nullable": True},
                    "stage": {"type": "string", "nullable": True},
                    "context": {},
                    "direction": {},
                    "detail": {},
                    "outcome": {},
                    "display_outcome": {},
                    "execution": {"type": "object", "additionalProperties": True},
                    "subject": {"type": "object", "additionalProperties": True},
                    "policy": {"type": "object", "additionalProperties": True},
                    "evidence": {"type": "object", "additionalProperties": True},
                },
                "additionalProperties": True,
            },
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
            "/api/v1/runtime": {"get": {"responses": {"200": {"description": "Runtime status with configured strategies; interrupted sessions recover to STOPPED"}}}},
            "/api/v1/diagnostics": {"get": {"parameters": [{"name": "strategy_id", "in": "query", "schema": {"type": "string"}}], "responses": {"200": {"description": "Runtime event and rejection counts, optionally filtered by strategy"}}}},
            "/api/v1/strategies/{strategy_id}/activity": {"get": {"parameters": [{"name": "strategy_id", "in": "path", "required": True, "schema": {"type": "string"}}, {"name": "date", "in": "query", "schema": {"type": "string", "format": "date"}}, {"name": "event_type", "in": "query", "schema": {"type": "string"}}, {"name": "limit", "in": "query", "schema": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100}}, {"name": "before", "in": "query", "schema": {"type": "integer", "minimum": 1}}], "responses": {"200": {"description": "Operational activity for one strategy"}}}},
            "/api/v1/decisions": {"get": {
                "parameters": [
                    {"name": "date", "in": "query", "schema": {"type": "string", "format": "date"}, "description": "IST session date; defaults to today"},
                    {"name": "session", "in": "query", "schema": {"type": "string", "enum": ["Pre", "Morning", "Mid", "Afternoon", "Post"]}},
                    {"name": "strategy_id", "in": "query", "schema": {"type": "string"}},
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
            "/api/v1/strategies/ftx-paper-research/decisions": {"get": {"$ref": "#/paths/~1api~1v1~1decisions/get"}},
            "/api/v1/strategies/ftx-paper-orderflow/decisions": {"get": {"$ref": "#/paths/~1api~1v1~1decisions/get"}},
            "/api/v1/strategies/ftx-paper-orderflow/facts": {"get": {"parameters": [{"name": "stream", "in": "query", "schema": {"type": "string"}}, {"name": "date", "in": "query", "schema": {"type": "string", "format": "date"}}, {"name": "limit", "in": "query", "schema": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100}}], "responses": {"200": {"description": "Persisted canonical orderflow facts"}}}},
            "/api/v1/positions": {"get": {"responses": {"200": {"description": "Open positions"}}}},
            "/api/v1/capital": {"get": {"responses": {"200": {"description": "Capital"}}}},
            "/api/v1/trades": {"get": {"parameters": [{"name": "strategy_id", "in": "query", "schema": {"type": "string"}}], "responses": {"200": {"description": "Trades, optionally filtered by strategy"}}}},
            "/api/v1/strategies/{strategy_id}/replays": {
                "parameters": [{"name": "strategy_id", "in": "path", "required": True, "schema": {"type": "string"}}],
                "get": {"responses": {"200": {"description": "Replay runs for one strategy"}}},
                "post": {"requestBody": {"required": True, "content": {"application/json": {"schema": {
                    "type": "object", "additionalProperties": False,
                    "required": ["session_date", "vehicles", "mode", "include_parity_layers"],
                    "properties": {
                        "session_date": {"type": "string", "format": "date"},
                        "vehicles": {"type": "array", "items": {"type": "string", "enum": ["futures", "synthetic"]}},
                        "mode": {"type": "string", "enum": ["replay"]},
                        "include_parity_layers": {"type": "boolean"},
                    },
                }}}}, "responses": {"202": {"description": "Replay queued"}, "400": {"description": "Invalid request"}}},
            },
            "/api/v1/strategies/{strategy_id}/replays/{run_id}": {"get": {"parameters": [{"name": "strategy_id", "in": "path", "required": True, "schema": {"type": "string"}}, {"name": "run_id", "in": "path", "required": True, "schema": {"type": "string"}}], "responses": {"200": {"description": "Replay bookkeeping with the canonical result envelope"}, "202": {"description": "Replay is not complete"}, "404": {"description": "Not found"}}}},
            "/api/v1/strategies/{strategy_id}/replays/{run_id}/parity/manifest": {"get": {"parameters": [{"name": "strategy_id", "in": "path", "required": True, "schema": {"type": "string"}}, {"name": "run_id", "in": "path", "required": True, "schema": {"type": "string"}}], "responses": {"200": {"description": "Parity manifest"}, "202": {"description": "Replay is not complete"}, "404": {"description": "Not found or parity unavailable"}}}},
            "/api/v1/strategies/{strategy_id}/replays/{run_id}/parity/{layer}": {"get": {"parameters": [{"name": "strategy_id", "in": "path", "required": True, "schema": {"type": "string"}}, {"name": "run_id", "in": "path", "required": True, "schema": {"type": "string"}}, {"name": "layer", "in": "path", "required": True, "schema": {"type": "string", "enum": ["run_scope", "decision", "execution_settlement", "portfolio_ledger"]}}, {"name": "offset", "in": "query", "schema": {"type": "integer", "minimum": 0, "default": 0}}, {"name": "limit", "in": "query", "schema": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100}}], "responses": {"200": {"description": "Bounded parity layer records"}, "400": {"description": "Invalid layer or pagination"}, "202": {"description": "Replay is not complete"}, "404": {"description": "Not found or parity unavailable"}}}},
        },
    }
