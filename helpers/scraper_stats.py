from __future__ import annotations

import json
import os
import platform
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from helpers.analytics_events import AnalyticsSink


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class RunStatsCollector:
    def __init__(
        self,
        scraper: str,
        *,
        now_func: Callable[[], str] = utc_now_iso,
        run_id: str | None = None,
    ) -> None:
        self.scraper = scraper
        self.run_id = run_id or uuid.uuid4().hex
        self._now_func = now_func
        self._started_perf = time.perf_counter()
        self.started_at_utc = self._now_func()
        self.counters: dict[str, int] = {}
        self.fields: dict[str, Any] = {}
        self.sources: list[dict[str, Any]] = []
        self.errors: list[dict[str, Any]] = []
        self.error_counts: dict[str, int] = {}

    def inc(self, name: str, amount: int = 1) -> None:
        self.counters[name] = self.counters.get(name, 0) + amount

    def set_field(self, name: str, value: Any) -> None:
        self.fields[name] = value

    def record_source(self, name: str, **fields: Any) -> None:
        source = {"name": name}
        source.update(fields)
        self._add_source_efficiency_fields(source)
        self.sources.append(source)

    def record_error(self, category: str, *, message: str = "", source: str = "", **fields: Any) -> None:
        normalized = _normalize_category(category)
        self.error_counts[normalized] = self.error_counts.get(normalized, 0) + 1
        self.inc("errors_total")
        entry: dict[str, Any] = {"category": normalized}
        if source:
            entry["source"] = source
        if message:
            entry["message"] = message[:200]
        entry.update({key: value for key, value in fields.items() if value is not None})
        self.errors.append(entry)

    def set_coverage(
        self,
        *,
        expected: int,
        attempted: int,
        completed: int,
        blocked: int = 0,
        skipped: int = 0,
    ) -> None:
        expected = max(0, int(expected or 0))
        attempted = max(0, int(attempted or 0))
        completed = max(0, int(completed or 0))
        blocked = max(0, int(blocked or 0))
        skipped = max(0, int(skipped or 0))
        self.fields["coverage"] = {
            "expected": expected,
            "attempted": attempted,
            "completed": completed,
            "blocked": blocked,
            "skipped": skipped,
            "attempted_percent": _percent(attempted, expected),
            "completed_percent": _percent(completed, expected),
        }

    def set_notification_funnel(
        self,
        *,
        seen: int = 0,
        candidates: int = 0,
        new: int = 0,
        persisted_without_send: int = 0,
        sent: int = 0,
        failed: int = 0,
        skipped: int = 0,
    ) -> None:
        seen = max(0, int(seen or 0))
        sent = max(0, int(sent or 0))
        self.fields["notification_funnel"] = {
            "seen": seen,
            "candidates": max(0, int(candidates or 0)),
            "new": max(0, int(new or 0)),
            "persisted_without_send": max(0, int(persisted_without_send or 0)),
            "sent": sent,
            "failed": max(0, int(failed or 0)),
            "skipped": max(0, int(skipped or 0)),
            "sent_per_1000_seen": round((sent / seen) * 1000, 3) if seen else 0.0,
        }

    def set_data_freshness(self, **fields: Any) -> None:
        self.fields["data_freshness"] = {key: value for key, value in fields.items() if value is not None}

    def set_deploy_metadata(self, **fields: Any) -> None:
        metadata = {
            "pid": os.getpid(),
            "hostname": platform.node(),
            "git_sha": _git_sha(),
        }
        metadata.update({key: value for key, value in fields.items() if value is not None})
        self.fields["deploy"] = metadata

    def set_resource_snapshot(self, **fields: Any) -> None:
        snapshot = _process_resource_snapshot()
        snapshot.update({key: value for key, value in fields.items() if value is not None})
        self.fields["resources"] = snapshot

    def finish(self, *, outcome: str, finished_at_utc: str | None = None) -> dict[str, Any]:
        summary = {
            "schema_version": 2,
            "run_id": self.run_id,
            "scraper": self.scraper,
            "outcome": outcome,
            "started_at_utc": self.started_at_utc,
            "finished_at_utc": finished_at_utc or self._now_func(),
            "duration_seconds": round(time.perf_counter() - self._started_perf, 6),
            "counters": dict(sorted(self.counters.items())),
            "fields": dict(sorted(self.fields.items())),
            "sources": list(self.sources),
        }
        if self.errors:
            summary["errors"] = list(self.errors)
            summary["error_counts"] = dict(sorted(self.error_counts.items()))
        return summary

    def write_jsonl(
        self,
        path: Path,
        summary: dict[str, Any],
        *,
        suppress_errors: bool = True,
        analytics_sink: AnalyticsSink | None = None,
    ) -> None:
        # JSONL gives future debugging/AI analysis a stable, append-only run ledger
        # without forcing it to reverse-engineer meaning from human log text.
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(summary, ensure_ascii=False, sort_keys=True) + "\n")
            self._write_analytics(summary, analytics_sink=analytics_sink)
        except Exception:
            if not suppress_errors:
                raise

    @staticmethod
    def _add_source_efficiency_fields(source: dict[str, Any]) -> None:
        items = _int_value(source.get("items_scraped") or source.get("items_seen"))
        sent = _int_value(source.get("sent_items") or source.get("sent"))
        new = _int_value(source.get("new_items") or source.get("new"))
        skipped = _int_value(source.get("skipped_items") or source.get("skipped"))
        if items > 0:
            source.setdefault("new_per_1000_items", round((new / items) * 1000, 3))
            source.setdefault("sent_per_1000_items", round((sent / items) * 1000, 3))
            source.setdefault("skipped_per_1000_items", round((skipped / items) * 1000, 3))

    @staticmethod
    def _write_analytics(summary: dict[str, Any], *, analytics_sink: AnalyticsSink | None = None) -> None:
        sink = analytics_sink or AnalyticsSink()
        scraper = str(summary.get("scraper") or "unknown")
        outcome = str(summary.get("outcome") or "unknown")
        counters = summary.get("counters") if isinstance(summary.get("counters"), dict) else {}
        event_payload = {
            "run_id": summary.get("run_id"),
            "scraper": scraper,
            "outcome": outcome,
            "duration_seconds": summary.get("duration_seconds"),
            "errors_total": counters.get("errors_total", 0),
            "items_scraped": counters.get("items_scraped", counters.get("items_seen", 0)),
            "items_sent": counters.get("items_sent", 0),
            "sources_attempted": counters.get("sources_attempted", 0),
        }
        sink.append_event("scraper_run", event_payload)
        # Daily rollups make the instance analyzable without repeatedly parsing the large
        # scraper_runs.jsonl ledger or grep-heavy human logs.
        sink.add_daily_counters(
            "scraper_runs",
            dimensions={"scraper": scraper, "outcome": outcome},
            counters={
                "runs": 1,
                "duration_seconds": float(summary.get("duration_seconds") or 0),
                "items_scraped": _int_value(event_payload["items_scraped"]),
                "items_sent": _int_value(event_payload["items_sent"]),
                "errors_total": _int_value(event_payload["errors_total"]),
            },
        )
        for source in summary.get("sources") or []:
            if not isinstance(source, dict):
                continue
            sink.add_daily_counters(
                "scraper_sources",
                dimensions={
                    "scraper": scraper,
                    "source": str(source.get("name") or "unknown")[:120],
                    "status": str(source.get("status") or "unknown"),
                },
                counters={
                    "runs": 1,
                    "items_scraped": _int_value(source.get("items_scraped") or source.get("items_seen")),
                    "sent_items": _int_value(source.get("sent_items") or source.get("sent")),
                    "new_items": _int_value(source.get("new_items") or source.get("new")),
                    "skipped_items": _int_value(source.get("skipped_items") or source.get("skipped")),
                },
            )


def _percent(part: int, whole: int) -> float:
    return round((part / whole) * 100, 3) if whole else 0.0


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _normalize_category(category: str) -> str:
    value = "".join(ch if ch.isalnum() else "_" for ch in (category or "unknown").strip().lower())
    return "_".join(part for part in value.split("_") if part) or "unknown"


def _process_resource_snapshot() -> dict[str, Any]:
    snapshot: dict[str, Any] = {"pid": os.getpid()}
    try:
        import resource

        usage = resource.getrusage(resource.RUSAGE_SELF)
        # Linux reports ru_maxrss in KiB. This is good enough for trend analysis and
        # avoids adding psutil as a dependency on the zero-cost instance.
        snapshot["max_rss_mb"] = round(float(usage.ru_maxrss) / 1024, 3)
        snapshot["user_cpu_seconds"] = round(float(usage.ru_utime), 6)
        snapshot["system_cpu_seconds"] = round(float(usage.ru_stime), 6)
    except Exception:
        pass
    return snapshot


def _git_sha() -> str:
    try:
        git_dir = Path(__file__).resolve().parents[1] / ".git"
        head = (git_dir / "HEAD").read_text(encoding="utf-8").strip()
        if head.startswith("ref:"):
            ref = head.split(" ", 1)[1].strip()
            return (git_dir / ref).read_text(encoding="utf-8").strip()[:12]
        return head[:12]
    except Exception:
        return ""
