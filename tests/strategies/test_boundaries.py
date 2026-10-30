import os
import subprocess
import sys

from ftx_paper.domain import RESEARCH_CAPITAL_PROFILE
from ftx_paper.application import StrategyRegistry
from ftx_paper.strategies import orderflow as orderflow_entry
from ftx_paper.strategies import research as research_entry
from ftx_paper.strategies.research import ResearchStrategyFactory
from ftx_paper.strategies.shared.factory import StrategyContext


def test_research_factory_constructs_research_only() -> None:
    factory = ResearchStrategyFactory(capital_profile=RESEARCH_CAPITAL_PROFILE)
    strategy = factory.create(StrategyContext())

    assert strategy.metadata.name
    assert strategy.metadata.version


def test_strategy_registry_selects_orderflow_explicitly() -> None:
    registry = StrategyRegistry(capital_profile=RESEARCH_CAPITAL_PROFILE)
    strategy = registry.create(
        "ftx-paper-orderflow", StrategyContext(),
    )

    assert strategy.metadata.name == "ftx-paper-orderflow"
    assert strategy.metadata.version == "scaffold"


def test_strategy_entry_points_are_explicitly_separate() -> None:
    assert research_entry.STRATEGY_ID == "ftx-paper-research"
    assert orderflow_entry.STRATEGY_ID == "ftx-paper-orderflow"
    assert research_entry.ConfiguredLiveStrategy is not orderflow_entry.OrderflowStrategy


def test_research_import_does_not_load_orderflow() -> None:
    environment = dict(os.environ)
    source_root = str(research_entry.__file__).split("ftx_paper", 1)[0]
    environment["PYTHONPATH"] = source_root
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import ftx_paper.strategies.research; "
            "assert 'ftx_paper.strategies.orderflow' not in sys.modules",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )
    assert completed.returncode == 0
