import sqlite3

from ftx_paper.infrastructure.sqlite.event_repository import SqliteEventRepository
from .fakes import InMemoryEventRepository


def test_event_repository_contract_supports_idempotent_append_and_recent_reads():
    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE runtime_events (id INTEGER PRIMARY KEY, event_type TEXT, payload TEXT, created_at TEXT, idempotency_key TEXT UNIQUE)")
    repository = SqliteEventRepository(connection)
    assert repository.append("WARMUP", {"ok": True}, "same")
    assert not repository.append("WARMUP", {"ok": True}, "same")
    assert repository.read_recent()[0].event_type == "WARMUP"


def test_event_repository_contract_is_shared_by_in_memory_backend():
    repository = InMemoryEventRepository()
    assert repository.append("WARMUP", {"ok": True}, "same")
    assert not repository.append("WARMUP", {"ok": True}, "same")
    assert repository.read_recent()[0].event_type == "WARMUP"
