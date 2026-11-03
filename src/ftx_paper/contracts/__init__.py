"""Stable contracts shared by the engine, runtime, API, and UI."""

from .market import (
    Instrument, MarketBar, MarketRole, OptionRole, OptionType, Role, SyntheticFutureQuote, SyntheticPremiumPair, SyntheticQuoteSelection, market_minute_key, select_synthetic_quote, synthetic_future_quote,
    canonical_market_exchange, canonical_market_symbol, normalize_exchange_timestamp, parse_role, role_to_key,
    validate_market_bar,
)
from .orders import (
    ExitPlanRule, exit_plan_from_payload, OrderAck, OrderIntent, OrderLifecycle, OrderLifecycleState, OrderRole, OrderSide, OrderType,
    is_legal_order_transition,
)
from .decision_artifacts import DecisionTrace, FuturesExecutionPlan, SyntheticSettlement, TradePlan
from .trades import TradePayload
from .decision_view import DECISION_VIEW_SCHEMA_VERSION, build_decision_view, validate_decision_view
from .events import RuntimeEvent
from .replay import (
    ExecutionScope, LedgerScope, REPLAY_REPORT_FIELDS, REPLAY_SCHEMA_VERSION, ReplayEvent, ReplayLedgerEntry,
    ReplayReport, ReplayRequest, ReplayResult, ReplayScope, ReplaySummary, ReplayTrade,
)
from .futures_rollover import (
    FuturesContract, ROLLOVER_RULE, RolloverSelection, SELECTION_POLICY_VERSION,
    futures_contract_from_row, parse_expiry, select_front_futures_contract,
)

__all__ = [
    "Instrument", "MarketBar", "MarketRole", "OptionRole", "OptionType", "Role", "SyntheticFutureQuote", "SyntheticPremiumPair", "SyntheticQuoteSelection", "select_synthetic_quote", "synthetic_future_quote",
    "market_minute_key", "canonical_market_exchange", "canonical_market_symbol", "normalize_exchange_timestamp", "parse_role", "role_to_key",
    "validate_market_bar",
    "ExitPlanRule", "exit_plan_from_payload", "OrderAck", "OrderIntent", "OrderLifecycle", "OrderLifecycleState", "OrderRole", "OrderSide", "OrderType",
    "is_legal_order_transition",
    "TradePlan", "FuturesExecutionPlan", "SyntheticSettlement", "DecisionTrace", "TradePayload",
    "DECISION_VIEW_SCHEMA_VERSION", "build_decision_view", "validate_decision_view",
    "RuntimeEvent",
    "ExecutionScope", "LedgerScope", "REPLAY_REPORT_FIELDS", "REPLAY_SCHEMA_VERSION", "ReplayEvent", "ReplayLedgerEntry",
    "ReplayReport", "ReplayRequest", "ReplayResult", "ReplayScope", "ReplaySummary", "ReplayTrade",
    "FuturesContract", "ROLLOVER_RULE", "RolloverSelection", "SELECTION_POLICY_VERSION",
    "futures_contract_from_row", "parse_expiry", "select_front_futures_contract",
]
