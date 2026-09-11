"""One-time migration of canonical DuckDB expiry dates into paper SQLite."""

from __future__ import annotations

import argparse
from pathlib import Path

from ftx_paper.runtime import RuntimeStore


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-dir", type=Path, required=True)
    parser.add_argument("--source-db", type=Path, default=None)
    args = parser.parse_args()

    from src.data.db import new_connection
    from src.data.repos.index_repo import load_weekly_expiry_dates

    with new_connection(db_path=args.source_db, read_only=True) as connection:
        dates = load_weekly_expiry_dates(con=connection)
    store = RuntimeStore(args.runtime_dir)
    copied = store.import_expiry_dates_once(dates, source=str(args.source_db or "default DuckDB"))
    print(f"expiry migration: {copied} dates copied; {len(store.read_expiry_dates())} stored")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
