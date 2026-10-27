from ftx_paper.strategy import (
    ExecutionAuthorization, ExecutionProposal, ExecutionStatus,
    OrderflowExecutionCoordinator,
)


def _proposal() -> ExecutionProposal:
    return ExecutionProposal("intent-1", "NIFTYFUT", "long", 1, 100, 98, 104)


def test_unapproved_execution_is_non_mutating() -> None:
    coordinator = OrderflowExecutionCoordinator(ExecutionAuthorization(False, "diagnostic"))

    assert coordinator.submit(_proposal()) is None
    assert coordinator.states == ()


def test_explicitly_authorized_paper_execution_tracks_fill() -> None:
    coordinator = OrderflowExecutionCoordinator(ExecutionAuthorization(True, "approved"))
    proposed = coordinator.submit(_proposal())
    assert proposed is not None and proposed.status is ExecutionStatus.PROPOSED

    filled = coordinator.fill("intent-1", 100.5)
    assert filled.status is ExecutionStatus.FILLED
    assert filled.filled_price == 100.5
