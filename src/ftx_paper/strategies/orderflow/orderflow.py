"""Compatibility import for the isolated orderflow policy package."""

from ftx_paper.strategies.orderflow.policy import (
    OrderflowPolicy,
    OrderflowStrategy,
    _admission_projection,  # noqa: F401 - legacy import compatibility
    _score_rejection,  # noqa: F401 - legacy import compatibility
)
from ftx_paper.strategies.orderflow.orderflow_config import OrderflowConfig

__all__ = ["OrderflowConfig", "OrderflowPolicy", "OrderflowStrategy"]
