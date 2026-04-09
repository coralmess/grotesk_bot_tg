from __future__ import annotations

import hashlib
import io
import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from helpers.runtime_paths import RUNTIME_CACHE_DIR, ensure_runtime_dirs

MARKET_DATA_CACHE_DIR = RUNTIME_CACHE_DIR / "market_data"
_CACHE_LOCK = threading.Lock()


def _write_text_atomic(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident()}.{time.time_ns()}.tmp")
    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass


def _cache_key(symbol: str, **params: Any) -> str:
    payload = json.dumps(
        {
            "symbol": symbol.upper(),
            "params": params,
        },
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _cache_file(symbol: str, **params: Any) -> Path:
    ensure_runtime_dirs()
    MARKET_DATA_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return MARKET_DATA_CACHE_DIR / f"{symbol.upper()}-{_cache_key(symbol, **params)}.json"


def _load_dataframe_from_cache(path: Path, *, ttl_seconds: int):
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        fetched_at = float(payload["fetched_at"])
        if (time.time() - fetched_at) > ttl_seconds:
            return None
        frame_payload = payload["frame"]
        frame = pd.read_json(io.StringIO(frame_payload), orient="split")
        return frame
    except Exception:
        return None


def _save_dataframe_to_cache(path: Path, frame) -> None:
    payload = json.dumps(
        {
            "fetched_at": time.time(),
            "frame": frame.to_json(orient="split", date_format="iso"),
        },
        ensure_ascii=False,
    )
    _write_text_atomic(path, payload)


def cached_history(
    symbol: str,
    *,
    ttl_seconds: int,
    fetch_history: Callable[[], Any],
    **params: Any,
):
    cache_path = _cache_file(symbol, **params)
    cached = _load_dataframe_from_cache(cache_path, ttl_seconds=ttl_seconds)
    if cached is not None:
        return cached

    with _CACHE_LOCK:
        cached = _load_dataframe_from_cache(cache_path, ttl_seconds=ttl_seconds)
        if cached is not None:
            return cached
        frame = fetch_history()
        if frame is not None and not getattr(frame, "empty", True):
            _save_dataframe_to_cache(cache_path, frame)
        return frame
