"""SQLite persistence for canonical daily value profiles."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from pathlib import Path

from .connection import SqliteDatabase


_TABLE = "orderflow_daily_value_profiles"
_COLUMNS = (
    "snapshot_id, instrument, timeframe_minutes, session_date, poc, vah, val, vwap, "
    "window_first_bar_ts, window_last_bar_ts, bar_count, total_volume, gap_count, quality, "
    "observed_at, available_at, config_fingerprint, profile_version, aggregate_source_fingerprint"
)


class SqliteDailyProfileRepository:
    """Persist canonical daily profile records in the Paper runtime database."""

    def __init__(self, path: str | Path) -> None:
        self._database = SqliteDatabase(path)

    def ensure_schema(self) -> None:
        with self._database.connect() as connection:
            connection.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {_TABLE} (
                    snapshot_id TEXT NOT NULL,
                    instrument TEXT NOT NULL,
                    timeframe_minutes INTEGER NOT NULL,
                    session_date TEXT NOT NULL,
                    poc REAL NOT NULL,
                    vah REAL NOT NULL,
                    val REAL NOT NULL,
                    vwap REAL,
                    window_first_bar_ts TEXT NOT NULL,
                    window_last_bar_ts TEXT NOT NULL,
                    bar_count INTEGER NOT NULL,
                    total_volume REAL NOT NULL,
                    gap_count INTEGER NOT NULL,
                    quality TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    available_at TEXT NOT NULL,
                    config_fingerprint TEXT NOT NULL,
                    profile_version TEXT NOT NULL,
                    aggregate_source_fingerprint TEXT NOT NULL,
                    PRIMARY KEY (instrument, timeframe_minutes, config_fingerprint, session_date)
                )
                """,
            )

    def load(
        self,
        *,
        instrument: str,
        timeframe_minutes: int,
        config_fingerprint: str,
        session_date: str,
    ) -> dict[str, object] | None:
        self.ensure_schema()
        with self._database.connect() as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute(
                f"SELECT {_COLUMNS} FROM {_TABLE} "
                "WHERE instrument=? AND timeframe_minutes=? AND config_fingerprint=? AND session_date=?",
                (instrument, timeframe_minutes, config_fingerprint, session_date),
            ).fetchone()
        return dict(row) if row is not None else None

    def load_all(
        self,
        *,
        instrument: str,
        timeframe_minutes: int,
        config_fingerprint: str,
    ) -> list[dict[str, object]]:
        self.ensure_schema()
        with self._database.connect() as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                f"SELECT {_COLUMNS} FROM {_TABLE} "
                "WHERE instrument=? AND timeframe_minutes=? AND config_fingerprint=? "
                "ORDER BY session_date",
                (instrument, timeframe_minutes, config_fingerprint),
            ).fetchall()
        return [dict(row) for row in rows]

    def save(self, payload: Mapping[str, object]) -> None:
        self.ensure_schema()
        columns = _COLUMNS.replace(" ", "").split(",")
        values = [payload[column] for column in columns]
        updates = ", ".join(
            f"{column}=excluded.{column}"
            for column in columns
            if column not in {"instrument", "timeframe_minutes", "config_fingerprint", "session_date"}
        )
        with self._database.connect() as connection:
            connection.execute(
                f"INSERT INTO {_TABLE} ({_COLUMNS}) VALUES ({','.join('?' for _ in values)}) "
                "ON CONFLICT (instrument, timeframe_minutes, config_fingerprint, session_date) "
                f"DO UPDATE SET {updates}",
                values,
            )


__all__ = ["SqliteDailyProfileRepository"]
