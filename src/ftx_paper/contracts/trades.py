"""Typed trade records shared by execution and reporting boundaries."""

from __future__ import annotations

from typing import NotRequired, TypedDict


class TradePayload(TypedDict):
    """Generic normalized entry-to-exit trade payload."""

    status: str
    date: str
    session: str
    vehicle: str
    cell: str
    direction: str
    entry_time: str
    exit_time: str
    quantity: float
    entry_price: float
    stop_price: float | None
    initial_risk_points: float | None
    initial_risk_rs: float | None
    exit_price: float | None
    gross_pnl_rs: float | None
    cost_rs: float | None
    net_pnl_rs: float | None
    exit_reason: str | None
    ce_strike: NotRequired[float | None]
    ce_entry_price: NotRequired[float | None]
    pe_entry_price: NotRequired[float | None]
    ce_exit_price: NotRequired[float | None]
    pe_exit_price: NotRequired[float | None]
    synthetic_entry_price: NotRequired[float | None]
    synthetic_exit_price: NotRequired[float | None]
    ce_symbol: NotRequired[str | None]
    pe_symbol: NotRequired[str | None]
    expiry: NotRequired[str | None]
    quote_timestamp: NotRequired[str | None]
    quote_source: NotRequired[str | None]
    ce_exit_symbol: NotRequired[str | None]
    pe_exit_symbol: NotRequired[str | None]
    exit_quote_timestamp: NotRequired[str | None]
    exit_quote_source: NotRequired[str | None]


__all__ = ["TradePayload"]
