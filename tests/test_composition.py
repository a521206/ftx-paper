from ftx_paper.application.composition import build_application


def test_composition_builds_application_without_shared_query_connection(tmp_path):
    application = build_application(tmp_path / "runtime.sqlite3", object(), lambda *_args: None)
    assert application.start_runtime is not None
    assert application.query_decisions is not None
