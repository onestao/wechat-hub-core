"""SQLite staging repair primitive shared by legacy and account-aware workers."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path


def repair_memory_indexes(memory_db: Path) -> dict:
    """Preserve the upstream REINDEX + integrity_check recovery behavior."""
    started = time.time()
    with sqlite3.connect(memory_db, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("REINDEX")
        check = conn.execute("PRAGMA integrity_check").fetchone()[0]
    return {
        "ok": check == "ok",
        "integrity_check": check,
        "elapsed_seconds": round(time.time() - started, 3),
    }
