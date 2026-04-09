from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Callable, Iterable

from helpers.runtime_paths import OLX_ITEMS_DB_FILE, SHAFA_ITEMS_DB_FILE, SHOES_DB_FILE

RUNTIME_DB_PRAGMA_STATEMENTS = (
    "PRAGMA journal_mode=WAL;",
    "PRAGMA synchronous=NORMAL;",
    "PRAGMA busy_timeout=5000;",
    # Spare RAM is available on the Oracle instance, so keep a larger page cache in memory.
    "PRAGMA cache_size=-20000;",
    "PRAGMA temp_store=MEMORY;",
    "PRAGMA mmap_size=268435456;",
)

RUNTIME_DB_MAINTENANCE_STATEMENTS = (
    *RUNTIME_DB_PRAGMA_STATEMENTS,
    "PRAGMA wal_checkpoint(TRUNCATE);",
    "PRAGMA optimize;",
    "ANALYZE;",
)


def apply_runtime_pragmas(conn: sqlite3.Connection, *, include_foreign_keys: bool = False) -> None:
    if include_foreign_keys:
        conn.execute("PRAGMA foreign_keys=ON;")
    for stmt in RUNTIME_DB_PRAGMA_STATEMENTS:
        conn.execute(stmt)


def runtime_db_files() -> tuple[Path, ...]:
    return (SHOES_DB_FILE, OLX_ITEMS_DB_FILE, SHAFA_ITEMS_DB_FILE)


def run_runtime_db_maintenance(
    db_files: Iterable[Path] | None = None,
    *,
    vacuum: bool = False,
    retention_callback: Callable[[sqlite3.Connection, Path], None] | None = None,
) -> None:
    for db_path in tuple(db_files or runtime_db_files()):
        if not db_path.exists():
            continue
        conn = sqlite3.connect(db_path)
        try:
            for stmt in RUNTIME_DB_MAINTENANCE_STATEMENTS:
                conn.execute(stmt)
            if retention_callback is not None:
                retention_callback(conn, db_path)
            if vacuum:
                conn.execute("VACUUM;")
            conn.commit()
        finally:
            conn.close()
