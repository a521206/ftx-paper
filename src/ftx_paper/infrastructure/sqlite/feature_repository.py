"""SQLite persistence for immutable causal feature snapshots."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from ftx_paper.market.feature_cache import FeatureCacheKey, FeatureSnapshot
from ftx_paper.ports.repositories import FeatureRepository


class SqliteFeatureRepository(FeatureRepository):
    def __init__(self, database: str | Path, *, read_only: bool = False) -> None:
        self.database = Path(database)
        self.read_only = read_only
        self.writable = not read_only

    def read_feature_snapshot(self, key: FeatureCacheKey) -> FeatureSnapshot | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT values_json, source_bar_ids_json, computed_at, availability "
                "FROM feature_cache WHERE cache_key = ?",
                (key.value,),
            ).fetchone()
        if row is None:
            return None
        return FeatureSnapshot(
            key=key,
            values=json.loads(row[0]),
            source_bar_ids=tuple(json.loads(row[1])),
            computed_at=_parse_datetime(row[2]),
            availability=str(row[3]),
        )

    def write_feature_snapshot(self, snapshot: FeatureSnapshot) -> None:
        if self.read_only:
            raise RuntimeError("cannot write feature cache through a read-only repository")
        with self._connect() as connection:
            connection.execute(
                """INSERT OR REPLACE INTO feature_cache
                   (cache_key, instrument, session_date, timeframe, cutoff_minute,
                    feature_set, calculator_version, parameters_json, input_fingerprint,
                    values_json, source_bar_ids_json, provenance_json, availability, computed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    snapshot.key.value,
                    snapshot.key.instrument,
                    snapshot.key.session_date,
                    snapshot.key.timeframe,
                    snapshot.key.cutoff_minute,
                    snapshot.key.feature_set,
                    snapshot.key.calculator_version,
                    json.dumps(snapshot.key.parameters, separators=(",", ":")),
                    snapshot.key.input_fingerprint,
                    json.dumps(dict(snapshot.values), separators=(",", ":")),
                    json.dumps(snapshot.source_bar_ids, separators=(",", ":")),
                    json.dumps({"source_bar_ids": snapshot.source_bar_ids}, separators=(",", ":")),
                    snapshot.availability,
                    snapshot.computed_at.isoformat(),
                ),
            )

    def _connect(self) -> sqlite3.Connection:
        if self.read_only:
            uri = f"file:{self.database.resolve().as_posix()}?mode=ro"
            connection = sqlite3.connect(uri, uri=True, timeout=30)
        else:
            connection = sqlite3.connect(self.database, timeout=30)
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection


def _parse_datetime(value: str):
    from datetime import datetime

    return datetime.fromisoformat(str(value))


__all__ = ["SqliteFeatureRepository"]
