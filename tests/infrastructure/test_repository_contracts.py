import sqlite3

from ftx_paper.infrastructure.sqlite.replay_repository import SqliteReplayRepository
from ftx_paper.infrastructure.sqlite.runtime_store import SqliteRuntimeStore


def test_replay_transition_is_compare_and_set(tmp_path):
    store = SqliteRuntimeStore(tmp_path)
    store.initialize()
    connection = sqlite3.connect(store.database)
    connection.execute(
        "INSERT INTO replay_runs VALUES ('r', 'queued', '{}', NULL, NULL, "
        "'2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')"
    )
    repository = SqliteReplayRepository(connection)
    assert repository.transition("r", "queued", "running")
    assert not repository.transition("r", "queued", "cancelled")
