from ftx_paper.domain import RESEARCH_CAPITAL_PROFILE
from ftx_paper.strategies import orderflow as orderflow_entry
from ftx_paper.strategies import research as research_entry
from ftx_paper.strategies.research import ConfiguredStrategyFactory


def test_strategy_factory_is_the_strategy_selection_boundary() -> None:
    factory = ConfiguredStrategyFactory(capital_profile=RESEARCH_CAPITAL_PROFILE)
    strategy = factory.create()

    assert strategy.metadata.name
    assert strategy.metadata.version


def test_strategy_factory_selects_orderflow_explicitly() -> None:
    factory = ConfiguredStrategyFactory(capital_profile=RESEARCH_CAPITAL_PROFILE)
    strategy = factory.create(strategy="ftx-paper-orderflow")

    assert strategy.metadata.name == "ftx-paper-orderflow"
    assert strategy.metadata.version == "0.1.0"


def test_strategy_entry_points_are_explicitly_separate() -> None:
    assert research_entry.STRATEGY_ID == "ftx-paper-research"
    assert orderflow_entry.STRATEGY_ID == "orderflow-research"
    assert research_entry.ConfiguredLiveStrategy is not orderflow_entry.OrderflowStrategy
