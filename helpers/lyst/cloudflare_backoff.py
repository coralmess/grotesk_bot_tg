from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from helpers.analytics_events import AnalyticsSink


@dataclass(slots=True)
class CloudflareBackoffDecision:
    key: str
    failure_count: int
    cooldown_sec: int
    blocked_until_ts: float


class CloudflareBackoff:
    def __init__(
        self,
        path: Path,
        *,
        base_cooldown_sec: int,
        max_cooldown_sec: int,
        analytics_sink: AnalyticsSink | None = None,
    ) -> None:
        self._path = path
        self._base = max(1, int(base_cooldown_sec))
        self._max = max(self._base, int(max_cooldown_sec))
        self._analytics_sink = analytics_sink
        # Persist penalties so a service restart does not immediately hammer the
        # same LYST source/country pair that just triggered Cloudflare.
        self._state = self._load()

    def _key(self, source_name: str, country: str) -> str:
        return f"{source_name.strip()}::{country.strip()}".lower()

    def should_allow(self, source_name: str, country: str, *, now_ts: float | None = None) -> bool:
        now = time.time() if now_ts is None else now_ts
        entry = self._state.get(self._key(source_name, country), {})
        blocked_until_ts = float(entry.get("blocked_until_ts") or 0)
        allowed = now >= blocked_until_ts
        if not allowed:
            self._record_analytics(
                "cooldown_skip",
                source_name=source_name,
                country=country,
                failure_count=int(entry.get("failure_count") or 0),
                cooldown_seconds=int(entry.get("cooldown_sec") or max(0, blocked_until_ts - now)),
                remaining_seconds=max(0, int(blocked_until_ts - now)),
            )
        return allowed

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
        self._record_analytics(
            "cooldown_set",
            source_name=source_name,
            country=country,
            failure_count=failure_count,
            cooldown_seconds=cooldown_sec,
            blocked_until_ts=blocked_until_ts,
        )
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

    def _record_analytics(self, event: str, **fields: Any) -> None:
        if self._analytics_sink is None:
            return
        try:
            payload = {"event": event}
            payload.update(fields)
            self._analytics_sink.append_event("lyst_cloudflare_cooldown", payload)
            # These counters show whether longer cooldowns reduce repeated blocks
            # without requiring operators to parse the verbose LYST journal.
            self._analytics_sink.add_daily_counters(
                "lyst_cloudflare_cooldown",
                dimensions={
                    "event": event,
                    "source_name": str(fields.get("source_name") or "unknown")[:120],
                    "country": str(fields.get("country") or "unknown"),
                },
                counters={
                    "events": 1,
                    "cooldown_seconds": int(fields.get("cooldown_seconds") or 0),
                    "remaining_seconds": int(fields.get("remaining_seconds") or 0),
                },
            )
        except Exception:
            return
