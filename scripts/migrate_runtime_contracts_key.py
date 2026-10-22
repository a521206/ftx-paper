"""One-time migration: re-key runtime_contracts to include underlying and role.

The runtime store no longer migrates this table at startup. Run this once
against an existing runtime database before starting a build that expects the
new primary key. Data is not preserved by design: the only production caller
persists futures selections that are re-derived from the broker instrument
master on every startup.

Usage:
    .venv\\Scripts\\python.exe scripts\\migrate_runtime_contracts_key.py <runtime-dir-or-db>
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sqlite3
import sys

TARGET_KEY = ("exchange", "underlying", "role", "tradingsymbol")
NEW_TABLE = "runtime_contracts_v3"
LEGACY_TABLES = ("runtime_contracts_v2",)


def _primary_key(connection: sqlite3.Connection, table: str) -> list[str] | None:
    exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,),
    ).fetchone()
    if exists is None:
        return None
    return [
        str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")
        if row[5]
    ]


def _recover_interrupted_migration(connection: sqlite3.Connection) -> None:
    """Restore a table left behind by an interrupted create/drop/rename."""
    current = _primary_key(connection, "runtime_contracts")
    leftover = _primary_key(connection, NEW_TABLE)
    if leftover is not None and (current is None or current != list(TARGET_KEY)):
        connection.execute("DROP TABLE IF EXISTS runtime_contracts")
        connection.execute(f"ALTER TABLE {NEW_TABLE} RENAME TO runtime_contracts")


def migrate(database: Path) -> str:
    if not database.is_file():
        raise FileNotFoundError(f"runtime database not found: {database}")

    connection = sqlite3.connect(database)
    try:
        connection.isolation_level = None
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("BEGIN IMMEDIATE")
        try:
            _recover_interrupted_migration(connection)
            for table in LEGACY_TABLES:
                if _primary_key(connection, table) is not None:
                    connection.execute(f"DROP TABLE {table}")

            primary_key = _primary_key(connection, "runtime_contracts")
            if primary_key is None:
                connection.execute("COMMIT")
                return "no runtime_contracts table; nothing to migrate"
            if primary_key == list(TARGET_KEY):
                connection.execute("COMMIT")
                return "runtime_contracts already uses the target key"

            connection.execute(f"DROP TABLE IF EXISTS {NEW_TABLE}")
            connection.execute(
                f"""
                CREATE TABLE {NEW_TABLE} (
                    exchange TEXT NOT NULL,
                    underlying TEXT NOT NULL,
                    role TEXT NOT NULL,
                    tradingsymbol TEXT NOT NULL,
                    instrument_token INTEGER NOT NULL,
                    expiry TEXT NOT NULL,
                    selected_at TEXT NOT NULL,
                    PRIMARY KEY (exchange, underlying, role, tradingsymbol)
                )
                """
            )
            connection.execute("DROP TABLE runtime_contracts")
            connection.execute(f"ALTER TABLE {NEW_TABLE} RENAME TO runtime_contracts")
            connection.execute("COMMIT")
            return f"rebuilt runtime_contracts: {primary_key} -> {list(TARGET_KEY)}"
        except BaseException:
            connection.execute("ROLLBACK")
            raise
    finally:
        connection.close()


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "target",
        help="runtime directory (containing runtime.sqlite3) or the database file itself",
    )
    args = parser.parse_args(argv)
    database = Path(args.target)
    if database.is_dir():
        database = database / "runtime.sqlite3"
    print(migrate(database))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
