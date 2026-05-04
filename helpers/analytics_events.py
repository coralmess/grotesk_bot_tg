from __future__ import annotations

import hashlib
import json
import os
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import urlparse

from helpers.runtime_paths import RUNTIME_ANALYTICS_DIR

_LOCK = threading.Lock()
_SECRET_KEY_RE = re.compile(r"(token|secret|password|authorization|api[_-]?key)", re.IGNORECASE)
_CHAT_ID_KEY_RE = re.compile(r"(^|_)chat(_id)?$", re.IGNORECASE)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sanitize_name(value: str) -> str:
    sanitized = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(value or "").strip().lower())
    sanitized = re.sub(r"_+", "_", sanitized).strip("_")
    return sanitized or "default"


def stable_hash(value: Any) -> str:
    raw = str(value or "").encode("utf-8", errors="ignore")
    return "sha256:" + hashlib.sha256(raw).hexdigest()[:16]


def fingerprint_url(url: str | None) -> dict[str, Any]:
    if not url:
        return {}
    parsed = urlparse(str(url))
    host = parsed.netloc.lower()
    path = parsed.path or "/"
    result: dict[str, Any] = {
        "url_scheme": parsed.scheme.lower(),
        "url_host": host,
        "url_path_hash": stable_hash(path),
    }
    if path:
        result["url_ext"] = Path(path).suffix.lower()[:12]
    return result


def sanitize_payload(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    return _sanitize_mapping(dict(payload or {}))


def _sanitize_mapping(payload: dict[str, Any]) -> dict[str, Any]:
    safe: dict[str, Any] = {}
    for key, value in payload.items():
        key_str = str(key)
        if _SECRET_KEY_RE.search(key_str):
            safe[key_str] = "[redacted]"
            continue
        if _CHAT_ID_KEY_RE.search(key_str):
            safe[f"{key_str}_hash"] = stable_hash(value)
            continue
        safe[key_str] = _sanitize_value(value)
    return safe


def _sanitize_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return _sanitize_mapping(dict(value))
    if isinstance(value, list):
        return [_sanitize_value(item) for item in value]
    if isinstance(value, tuple):
        return [_sanitize_value(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(tmp_path, path)


class AnalyticsSink:
    def __init__(self, root_dir: Path = RUNTIME_ANALYTICS_DIR, *, now_func: Callable[[], str] = utc_now_iso) -> None:
        self.root_dir = Path(root_dir)
        self._now_func = now_func

    def append_event(self, stream: str, payload: Mapping[str, Any] | None = None) -> Path:
        ts_utc = self._now_func()
        stream_name = sanitize_name(stream)
        date_key = _date_key_from_iso(ts_utc)
        path = self.root_dir / "events" / f"{date_key}.{stream_name}.jsonl"
        event = {
            "schema_version": 1,
            "ts_utc": ts_utc,
            "stream": stream_name,
        }
        event.update(sanitize_payload(payload))
        # Analytics are append-only and machine-readable so production debugging can rely on
        # stable facts instead of parsing noisy human logs after the issue already happened.
        with _LOCK:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
        return path

    def add_daily_counters(
        self,
        domain: str,
        *,
        dimensions: Mapping[str, Any] | None = None,
        counters: Mapping[str, int | float] | None = None,
        date_key: str | None = None,
    ) -> Path:
        domain_name = sanitize_name(domain)
        date_key = date_key or _date_key_from_iso(self._now_func())
        path = self.root_dir / "daily" / f"{date_key}.{domain_name}.json"
        safe_dimensions = sanitize_payload(dimensions)
        group_key = _group_key(safe_dimensions)
        with _LOCK:
            if path.exists():
                try:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                except Exception:
                    payload = {}
            else:
                payload = {}
            payload.setdefault("schema_version", 1)
            payload["date"] = date_key
            payload["domain"] = domain_name
            groups = payload.setdefault("groups", {})
            group = groups.setdefault(group_key, {"dimensions": safe_dimensions, "counters": {}})
            group["dimensions"] = safe_dimensions
            group_counters = group.setdefault("counters", {})
            for key, value in (counters or {}).items():
                if not isinstance(value, (int, float)):
                    continue
                group_counters[str(key)] = group_counters.get(str(key), 0) + value
            _write_json_atomic(path, payload)
        return path


def append_analytics_event(stream: str, payload: Mapping[str, Any] | None = None) -> Path:
    return AnalyticsSink().append_event(stream, payload)


def add_daily_counters(
    domain: str,
    *,
    dimensions: Mapping[str, Any] | None = None,
    counters: Mapping[str, int | float] | None = None,
    date_key: str | None = None,
) -> Path:
    return AnalyticsSink().add_daily_counters(domain, dimensions=dimensions, counters=counters, date_key=date_key)


def _date_key_from_iso(value: str) -> str:
    return str(value or utc_now_iso())[:10]


def _group_key(dimensions: Mapping[str, Any]) -> str:
    if not dimensions:
        return "total"
    return "|".join(f"{key}={dimensions[key]}" for key in sorted(dimensions))
