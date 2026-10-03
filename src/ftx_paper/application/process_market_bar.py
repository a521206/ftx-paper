"""Application use case for processing one completed market bar bundle."""

from collections.abc import Mapping
from typing import Any

from ftx_paper.ports.unit_of_work import UnitOfWorkFactory


class ProcessMarketBar:
    """Coordinate one strategy evaluation in an independent transaction.

    Broker/network work belongs outside this persistence operation. The caller
    supplies an already-normalized bar and a strategy boundary.
    """

    def __init__(self, uow_factory: UnitOfWorkFactory, strategy: Any) -> None:
        self.uow_factory = uow_factory
        self.strategy = strategy

    def execute(self, bar: Any) -> Mapping[str, object]:
        result = self.strategy.on_bar(bar)
        with self.uow_factory() as uow:
            uow.events.append("MARKET_BAR_PROCESSED", {"bar": bar, "result": result})
            uow.commit()
        return result
