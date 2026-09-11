from typing import TYPE_CHECKING

from .config import CAPITAL, STRATEGY_NAME, STRATEGY_VERSION

if TYPE_CHECKING:
    from .live import ConfiguredLiveStrategy


def __getattr__(name: str):
    if name == "ConfiguredLiveStrategy":
        from .live import ConfiguredLiveStrategy
        return ConfiguredLiveStrategy
    raise AttributeError(name)

__all__ = ["CAPITAL", "ConfiguredLiveStrategy", "STRATEGY_NAME", "STRATEGY_VERSION"]
