from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class RunStatsCollector:
    def __init__(self, scraper: str, *, now_func: Callable[[], str] = utc_now_iso) -> None:
        self.scraper = scraper
        self._now_func = now_func
        self.started_at_utc = self._now_func()
        self.counters: dict[str, int] = {}
        self.fields: dict[str, Any] = {}
        self.sources: list[dict[str, Any]] = []

    def inc(self, name: str, amount: int = 1) -> None:
        self.counters[name] = self.counters.get(name, 0) + amount

    def set_field(self, name: str, value: Any) -> None:
        self.fields[name] = value

    def record_source(self, name: str, **fields: Any) -> None:
        source = {"name": name}
        source.update(fields)
        self.sources.append(source)

    def finish(self, *, outcome: str, finished_at_utc: str | None = None) -> dict[str, Any]:
        return {
            "scraper": self.scraper,
            "outcome": outcome,
            "started_at_utc": self.started_at_utc,
            "finished_at_utc": finished_at_utc or self._now_func(),
            "counters": dict(sorted(self.counters.items())),
            "fields": dict(sorted(self.fields.items())),
            "sources": list(self.sources),
        }

    def write_jsonl(self, path: Path, summary: dict[str, Any], *, suppress_errors: bool = True) -> None:
        # JSONL gives future debugging/AI analysis a stable, append-only run ledger
        # without forcing it to reverse-engineer meaning from human log text.
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(summary, ensure_ascii=False, sort_keys=True) + "\n")
        except Exception:
            if not suppress_errors:
                raise
