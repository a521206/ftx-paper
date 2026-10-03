"""Compatibility facade for the SQLite runtime persistence adapter.

New application code should depend on ports and a unit of work. This facade is
kept temporarily for existing runtime/API callers during the migration.
"""

from ftx_paper.infrastructure.sqlite.runtime_store import (
    ProcessAlreadyRunningError,
    SqliteRuntimeStore as _SqliteRuntimeStore,
    _json_safe,
)


# Keep the adapter behind the runtime-facing facade so API and CLI code do not
# select a persistence implementation themselves.
RuntimeStore = _SqliteRuntimeStore

__all__ = ["ProcessAlreadyRunningError", "RuntimeStore", "_json_safe"]
