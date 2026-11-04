from threading import Event

from ftx_paper.runtime.replay_runners import ResearchReplayRunner
from ftx_paper.strategies.orderflow.replay_runner import OrderflowReplayRunner


class _Worker:
    def execute_orderflow(self, request, cancel, *, session_date, vehicles, include_parity_layers):
        return {
            "runner": "orderflow", "request": request, "cancel": cancel,
            "session_date": session_date, "vehicles": vehicles,
            "include_parity_layers": include_parity_layers,
        }

    def _execute_research(self, request, cancel):
        return {"runner": "research", "request": request, "cancel": cancel}


def test_strategy_runners_delegate_isolated_execution_contracts():
    worker = _Worker()
    cancel = Event()
    request = {"session_date": "2026-01-02"}

    orderflow = OrderflowReplayRunner(worker).run(request, cancel)
    research = ResearchReplayRunner(worker).run(request, cancel)

    assert orderflow["runner"] == "orderflow"
    assert orderflow["session_date"] == "2026-01-02"
    assert orderflow["vehicles"] == ("futures",)
    assert research["runner"] == "research"
    assert orderflow["cancel"] is cancel
    assert research["request"] == request
