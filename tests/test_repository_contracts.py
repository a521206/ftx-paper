import sqlite3

from ftx_paper.infrastructure.sqlite.replay_repository import SqliteReplayRepository


def test_replay_transition_is_compare_and_set():
    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE replay_runs (run_id TEXT PRIMARY KEY, status TEXT, request TEXT, result TEXT, error TEXT, created_at TEXT, updated_at TEXT)")
    connection.execute("INSERT INTO replay_runs VALUES ('r', 'queued', '{}', NULL, NULL, '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')")
    repository = SqliteReplayRepository(connection)
    assert repository.transition("r", "queued", "running")
    assert not repository.transition("r", "queued", "cancelled")
