"""Schema initialization entry point for SQLite infrastructure."""

from pathlib import Path

from .runtime_store import SqliteRuntimeStore


def initialize(path: str | Path) -> None:
    """Initialize the runtime schema through the legacy adapter during migration."""
    SqliteRuntimeStore(Path(path).parent).initialize()
