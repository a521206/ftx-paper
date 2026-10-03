from typing import TYPE_CHECKING

from .config import STRATEGY_NAME, STRATEGY_VERSION

if TYPE_CHECKING:
    from .live import ConfiguredLiveStrategy
    from .factory import ConfiguredStrategyFactory, StrategyFactory


def __getattr__(name: str):
    if name == "ConfiguredLiveStrategy":
        from .live import ConfiguredLiveStrategy
        return ConfiguredLiveStrategy
    if name in {"ConfiguredStrategyFactory", "StrategyFactory"}:
        from .factory import ConfiguredStrategyFactory, StrategyFactory
        return {"ConfiguredStrategyFactory": ConfiguredStrategyFactory, "StrategyFactory": StrategyFactory}[name]
    raise AttributeError(name)

__all__ = ["ConfiguredLiveStrategy", "ConfiguredStrategyFactory", "StrategyFactory", "STRATEGY_NAME", "STRATEGY_VERSION"]
