"""Compatibility facade for the SQLite runtime persistence adapter.

New application code should depend on ports and a unit of work. This facade is
kept temporarily for existing runtime/API callers during the migration.
"""

from pathlib import Path

from ftx_paper.infrastructure.sqlite.runtime_store import (
    ProcessAlreadyRunningError,
    SqliteRuntimeStore as _SqliteRuntimeStore,
    _json_safe,
)


# Kept as an import compatibility alias for integrations that still import the
# historical module path. Production code imports the adapter directly.
RuntimeStore = _SqliteRuntimeStore

__all__ = ["ProcessAlreadyRunningError", "RuntimeStore", "_json_safe"]
