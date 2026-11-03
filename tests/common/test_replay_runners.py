from threading import Event

from ftx_paper.runtime.replay_runners import OrderflowReplayRunner, ResearchReplayRunner


class _Worker:
    def _execute_orderflow_impl(self, request, cancel):
        return {"runner": "orderflow", "request": request, "cancel": cancel}

    def _execute_research(self, request, cancel):
        return {"runner": "research", "request": request, "cancel": cancel}


def test_strategy_runners_delegate_isolated_execution_contracts():
    worker = _Worker()
    cancel = Event()
    request = {"session_date": "2026-01-02"}

    orderflow = OrderflowReplayRunner(worker).run(request, cancel)
    research = ResearchReplayRunner(worker).run(request, cancel)

    assert orderflow["runner"] == "orderflow"
    assert research["runner"] == "research"
    assert orderflow["cancel"] is cancel
    assert research["request"] == request
