from __future__ import annotations

import gzip
import os
import shutil
import time
from pathlib import Path

from helpers.runtime_paths import RUNTIME_CACHE_DIR, RUNTIME_DEBUG_DIR, RUNTIME_LOGS_DIR, RUNTIME_TMP_DIR


def _iter_files(root: Path):
    if not root.exists():
        return
    for path in root.rglob("*"):
        if path.is_file():
            yield path


def _older_than(path: Path, age_seconds: float, *, now_ts: float) -> bool:
    try:
        return (now_ts - path.stat().st_mtime) > age_seconds
    except FileNotFoundError:
        return False


def _gzip_file(path: Path) -> Path | None:
    gz_path = path.with_suffix(path.suffix + ".gz")
    try:
        with path.open("rb") as src, gzip.open(gz_path, "wb", compresslevel=6) as dst:
            shutil.copyfileobj(src, dst)
        path.unlink()
        return gz_path
    except Exception:
        return None


def run_runtime_housekeeping() -> dict[str, int]:
    now_ts = time.time()
    stats = {
        "compressed": 0,
        "deleted_tmp": 0,
        "deleted_cache": 0,
        "deleted_old_archives": 0,
    }

    # Compress older logs and debug text-ish artifacts rather than deleting them immediately.
    for root in (RUNTIME_LOGS_DIR, RUNTIME_DEBUG_DIR):
        for path in list(_iter_files(root) or []):
            if path.suffix.lower() not in {".log", ".txt", ".html", ".json"}:
                continue
            if path.name.endswith(".gz"):
                continue
            if _older_than(path, 24 * 3600, now_ts=now_ts) and _gzip_file(path) is not None:
                stats["compressed"] += 1

    for path in list(_iter_files(RUNTIME_TMP_DIR) or []):
        if _older_than(path, 3 * 24 * 3600, now_ts=now_ts):
            try:
                path.unlink()
                stats["deleted_tmp"] += 1
            except Exception:
                pass

    for path in list(_iter_files(RUNTIME_CACHE_DIR) or []):
        if _older_than(path, 7 * 24 * 3600, now_ts=now_ts):
            try:
                path.unlink()
                stats["deleted_cache"] += 1
            except Exception:
                pass

    for root in (RUNTIME_LOGS_DIR, RUNTIME_DEBUG_DIR):
        for path in list(_iter_files(root) or []):
            if not path.name.endswith(".gz"):
                continue
            if _older_than(path, 30 * 24 * 3600, now_ts=now_ts):
                try:
                    path.unlink()
                    stats["deleted_old_archives"] += 1
                except Exception:
                    pass

    return stats
