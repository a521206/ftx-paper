"""Minimal orderflow strategy boundary for the canonical port."""

from .orderflow import OrderflowStrategy
from .orderflow_config import ORDERFLOW_NAME, ORDERFLOW_VERSION, OrderflowConfig
from .factory import OrderflowStrategyFactory

STRATEGY_ID = ORDERFLOW_NAME

__all__ = ["ORDERFLOW_NAME", "ORDERFLOW_VERSION", "OrderflowConfig", "OrderflowStrategy", "OrderflowStrategyFactory", "STRATEGY_ID"]
