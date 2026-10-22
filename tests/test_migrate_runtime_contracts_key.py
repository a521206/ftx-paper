import importlib.util
import sqlite3
from pathlib import Path

import pytest

from ftx_paper.runtime import RuntimeStore

_SCRIPT = Path(__file__).parents[1] / "scripts" / "migrate_runtime_contracts_key.py"
_spec = importlib.util.spec_from_file_location("migrate_runtime_contracts_key", _SCRIPT)
assert _spec is not None and _spec.loader is not None
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)
main = _module.main
migrate = _module.migrate

TARGET_KEY = ["exchange", "underlying", "role", "tradingsymbol"]


def _primary_key(database: Path) -> list[str]:
    with sqlite3.connect(database) as connection:
        return [
            row[1] for row in connection.execute("PRAGMA table_info(runtime_contracts)")
            if row[5]
        ]


def _legacy_database(database: Path, primary_key: str) -> None:
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE runtime_contracts ("
            "exchange TEXT NOT NULL, underlying TEXT NOT NULL, role TEXT NOT NULL, "
            "tradingsymbol TEXT NOT NULL, instrument_token INTEGER NOT NULL, "
            "expiry TEXT NOT NULL, selected_at TEXT NOT NULL, "
            f"PRIMARY KEY ({primary_key}))",
        )
        connection.execute(
            "INSERT INTO runtime_contracts VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("NFO", "NIFTY", "futures", "NIFTY26OCTFUT", 2, "2026-10-27", "2026-10-26T10:00:00+00:00"),
        )


@pytest.mark.parametrize(
    "primary_key",
    ["exchange, underlying, role", "exchange, tradingsymbol"],
)
def test_migrate_rebuilds_legacy_contract_key(tmp_path, primary_key):
    database = tmp_path / "runtime.sqlite3"
    _legacy_database(database, primary_key)

    result = migrate(database)

    assert "rebuilt runtime_contracts" in result
    assert _primary_key(database) == TARGET_KEY
    assert RuntimeStore(tmp_path).read_runtime_contracts(
        exchange="NFO", underlying="NIFTY", role="futures",
    ) == []


def test_migrate_is_idempotent_on_target_key(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    _legacy_database(database, "exchange, underlying, role, tradingsymbol")

    assert migrate(database) == "runtime_contracts already uses the target key"
    assert _primary_key(database) == TARGET_KEY


def test_migrate_leaves_database_without_contract_table_alone(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE runtime_status (id INTEGER PRIMARY KEY)")

    assert migrate(database) == "no runtime_contracts table; nothing to migrate"


def test_migrate_recovers_interrupted_drop_and_rename(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE runtime_contracts ("
            "exchange TEXT NOT NULL, underlying TEXT NOT NULL, role TEXT NOT NULL, "
            "tradingsymbol TEXT NOT NULL, instrument_token INTEGER NOT NULL, "
            "expiry TEXT NOT NULL, selected_at TEXT NOT NULL, "
            "PRIMARY KEY (exchange, underlying, role, tradingsymbol))",
        )
        connection.execute("ALTER TABLE runtime_contracts RENAME TO runtime_contracts_v3")

    result = migrate(database)

    assert "already uses the target key" in result
    assert _primary_key(database) == TARGET_KEY


def test_migrate_rejects_missing_database(tmp_path):
    with pytest.raises(FileNotFoundError):
        migrate(tmp_path / "runtime.sqlite3")


def test_main_accepts_runtime_directory(tmp_path, capsys):
    database = tmp_path / "runtime.sqlite3"
    _legacy_database(database, "exchange, tradingsymbol")

    assert main([str(tmp_path)]) == 0
    assert "rebuilt runtime_contracts" in capsys.readouterr().out
