from types import SimpleNamespace

import pytest

from ftx_paper.runtime.engine import MultiStrategyEngine
from ftx_paper.strategies.shared.protocol import StrategyMetadata


class _Strategy:
    metadata = StrategyMetadata("one", "1", "hash")


def test_unknown_execution_strategy_id_fails_closed() -> None:
    engine = MultiStrategyEngine({"one": _Strategy()})
    with pytest.raises(KeyError, match="unknown strategy_id"):
        engine.on_execution_event(SimpleNamespace(strategy_id="missing"))
