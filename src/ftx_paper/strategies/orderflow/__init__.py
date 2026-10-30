"""Minimal orderflow strategy boundary for the canonical port."""

from .orderflow import OrderflowStrategy
from .orderflow_config import ORDERFLOW_NAME, ORDERFLOW_VERSION, OrderflowConfig

STRATEGY_ID = "orderflow-research"

__all__ = ["ORDERFLOW_NAME", "ORDERFLOW_VERSION", "OrderflowConfig", "OrderflowStrategy", "STRATEGY_ID"]
