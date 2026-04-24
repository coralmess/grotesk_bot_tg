from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class CloudflareBackoffDecision:
    key: str
    failure_count: int
    cooldown_sec: int
    blocked_until_ts: float


class CloudflareBackoff:
    def __init__(self, path: Path, *, base_cooldown_sec: int, max_cooldown_sec: int) -> None:
        self._path = path
        self._base = max(1, int(base_cooldown_sec))
        self._max = max(self._base, int(max_cooldown_sec))
        # Persist penalties so a service restart does not immediately hammer the
        # same LYST source/country pair that just triggered Cloudflare.
        self._state = self._load()

    def _key(self, source_name: str, country: str) -> str:
        return f"{source_name.strip()}::{country.strip()}".lower()

    def should_allow(self, source_name: str, country: str, *, now_ts: float | None = None) -> bool:
        now = time.time() if now_ts is None else now_ts
        entry = self._state.get(self._key(source_name, country), {})
        return now >= float(entry.get("blocked_until_ts") or 0)

    def record_failure(
        self,
        source_name: str,
        country: str,
        *,
        now_ts: float | None = None,
    ) -> CloudflareBackoffDecision:
        now = time.time() if now_ts is None else now_ts
        key = self._key(source_name, country)
        entry = self._state.get(key, {})
        failure_count = int(entry.get("failure_count") or 0) + 1
        # Exponential cooldown slows repeated challenges while keeping the first
        # miss recoverable without waiting for the normal full scheduler interval.
        cooldown_sec = min(self._max, self._base * (2 ** (failure_count - 1)))
        blocked_until_ts = now + cooldown_sec
        self._state[key] = {
            "failure_count": failure_count,
            "cooldown_sec": cooldown_sec,
            "blocked_until_ts": blocked_until_ts,
            "source_name": source_name,
            "country": country,
            "updated_ts": now,
        }
        self._save()
        return CloudflareBackoffDecision(key, failure_count, cooldown_sec, blocked_until_ts)

    def record_success(self, source_name: str, country: str) -> None:
        self._state.pop(self._key(source_name, country), None)
        self._save()

    def snapshot(self) -> dict:
        return dict(self._state)

    def _load(self) -> dict:
        try:
            return json.loads(self._path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return {}

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.write_text(json.dumps(self._state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self._path)
