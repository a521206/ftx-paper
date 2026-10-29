from ftx_paper.application.composition import build_application, build_runtime_session
from ftx_paper.broker import PaperBroker
from ftx_paper.runtime import RuntimeStore


def test_composition_builds_application_without_shared_query_connection(tmp_path):
    application = build_application(tmp_path / "runtime.sqlite3", object(), lambda *_args: None)
    assert application.start_runtime is not None
    assert application.query_decisions is not None


def test_runtime_composition_enforces_paper_execution(tmp_path):
    session = build_runtime_session(
        RuntimeStore(tmp_path / "runtime"), None, [],
        broker_factory=lambda _client: object(),
    )
    assert session.broker_factory is not None
    assert isinstance(session.broker_factory(None), PaperBroker)
    assert session.pnl_manager.snapshot()["initial_capital"] > 0
