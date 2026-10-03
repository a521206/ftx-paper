from datetime import datetime, timezone

import pytest

from ftx_paper.contracts import Instrument, OrderIntent, OrderRole, OrderSide
from ftx_paper.core import (
    AccountAggregate, CapitalRuntimeContext, DecisionContext, PortfolioState,
    RESEARCH_CAPITAL_PROFILE,
)
from ftx_paper.strategy import ConfiguredStrategyFactory


def _order(order_id: str, quantity: int = 1) -> OrderIntent:
    return OrderIntent(
        order_id,
        Instrument("NIFTYFUT", "NFO", "FUTURES"),
        OrderSide.BUY,
        quantity,
        role=OrderRole.ENTRY,
    )


def test_account_context_is_read_only_and_includes_revision() -> None:
    account = AccountAggregate(
        PortfolioState(2_500_000),
        CapitalRuntimeContext(RESEARCH_CAPITAL_PROFILE),
    )

    context = account.context(as_of=datetime(2026, 1, 1, tzinfo=timezone.utc))

    assert isinstance(context, DecisionContext)
    assert context.account_revision == 0
    assert context.capital.available_capital == 2_500_000
    assert context.__dataclass_params__ is not None


def test_authorization_rechecks_current_account_and_reserves_once() -> None:
    account = AccountAggregate(
        PortfolioState(2_500_000),
        CapitalRuntimeContext(RESEARCH_CAPITAL_PROFILE),
    )
    first = _order("first", quantity=1)
    second = _order("second", quantity=1)

    account.authorize_and_reserve(first)
    account.authorize_and_reserve(second)

    assert account.portfolio.open_margin == pytest.approx(350_000)
    assert account.revision == 2
    assert account.context().margin.used == pytest.approx(350_000)


def test_authorization_rejects_current_margin_and_exposure_limits() -> None:
    account = AccountAggregate(
        PortfolioState(100_000),
        CapitalRuntimeContext(RESEARCH_CAPITAL_PROFILE),
    )

    with pytest.raises(ValueError, match="insufficient_available_capital"):
        account.authorize_and_reserve(_order("too-large"))


def test_strategy_factory_is_the_strategy_selection_boundary() -> None:
    factory = ConfiguredStrategyFactory(capital_profile=RESEARCH_CAPITAL_PROFILE)
    strategy = factory.create()

    assert strategy.metadata.name
    assert strategy.metadata.version
