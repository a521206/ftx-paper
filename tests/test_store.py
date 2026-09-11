import os
import sqlite3

from ftx_paper.runtime import RuntimeStore


def test_runtime_store_replaces_dead_lease(tmp_path):
    store = RuntimeStore(tmp_path)
    with sqlite3.connect(store.database) as connection:
        connection.execute(
            "INSERT INTO process_leases(service, pid, started_at, instance_id) VALUES (?, ?, ?, ?)",
            ("api", 999999, "2026-01-01T00:00:00+00:00", "stale"),
        )

    instance_id = store.acquire_process_lease("api")
    with sqlite3.connect(store.database) as connection:
        row = connection.execute(
            "SELECT pid, instance_id FROM process_leases WHERE service = ?", ("api",)
        ).fetchone()
    assert row == (os.getpid(), instance_id)


def test_runtime_store_release_cannot_remove_newer_lease(tmp_path):
    store = RuntimeStore(tmp_path)
    old_instance = store.acquire_process_lease("api")
    with sqlite3.connect(store.database) as connection:
        connection.execute(
            "UPDATE process_leases SET instance_id = ? WHERE service = ?",
            ("newer", "api"),
        )

    store.release_process_lease("api", old_instance)
    with sqlite3.connect(store.database) as connection:
        row = connection.execute(
            "SELECT instance_id FROM process_leases WHERE service = ?", ("api",)
        ).fetchone()
    assert row == ("newer",)
