from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

from helpers.runtime_paths import service_health_file

LOGGER = logging.getLogger(__name__)

DEFAULT_METRICS_PORTS = {
    "grotesk-market": 9101,
    "grotesk-lyst": 9102,
    "usefulbot": 9103,
    "svitlobot": 9104,
    "tsekbot": 9105,
    "auto-ria-bot": 9106,
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident()}.{time.time_ns()}.tmp")
    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass


@dataclass
class ServiceMetricsConfig:
    service_name: str
    health_file: Path
    metrics_port: Optional[int]
    metrics_host: str = "127.0.0.1"
    heartbeat_interval_sec: int = 30

    @classmethod
    def for_service(
        cls,
        service_name: str,
        *,
        metrics_port: Optional[int] = None,
        metrics_host: str = "127.0.0.1",
        heartbeat_interval_sec: int = 30,
    ) -> "ServiceMetricsConfig":
        if metrics_port is None:
            metrics_port = DEFAULT_METRICS_PORTS.get(service_name)
        return cls(
            service_name=service_name,
            health_file=service_health_file(service_name),
            metrics_port=metrics_port,
            metrics_host=metrics_host,
            heartbeat_interval_sec=heartbeat_interval_sec,
        )


class ServiceHealthReporter:
    def __init__(self, config: ServiceMetricsConfig) -> None:
        self._config = config
        self._lock = threading.Lock()
        self._started_at = utc_now_iso()
        self._last_heartbeat_utc = self._started_at
        self._last_success_utc: Optional[str] = None
        self._last_failure_utc: Optional[str] = None
        self._last_error = ""
        self._status = "starting"
        self._note = ""
        self._operation_stats: dict[str, dict[str, Any]] = {}
        self._metrics_started = False
        self._registry = CollectorRegistry()
        self._service_up = Gauge(
            "grotesk_service_up",
            "Whether the service process is currently up",
            ["service"],
            registry=self._registry,
        )
        self._service_ready = Gauge(
            "grotesk_service_ready",
            "Whether the service reports itself ready",
            ["service"],
            registry=self._registry,
        )
        self._heartbeat = Gauge(
            "grotesk_service_heartbeat_unixtime",
            "Latest service heartbeat unix timestamp",
            ["service"],
            registry=self._registry,
        )
        self._last_success = Gauge(
            "grotesk_service_last_success_unixtime",
            "Latest successful operation unix timestamp",
            ["service"],
            registry=self._registry,
        )
        self._last_failure = Gauge(
            "grotesk_service_last_failure_unixtime",
            "Latest failed operation unix timestamp",
            ["service"],
            registry=self._registry,
        )
        self._run_counter = Counter(
            "grotesk_service_runs_total",
            "Count of tracked operations by service, operation, and outcome",
            ["service", "operation", "outcome"],
            registry=self._registry,
        )
        self._run_duration = Histogram(
            "grotesk_service_run_duration_seconds",
            "Tracked operation duration",
            ["service", "operation", "outcome"],
            registry=self._registry,
            buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600),
        )

    @property
    def service_name(self) -> str:
        return self._config.service_name

    @property
    def heartbeat_interval_sec(self) -> int:
        return self._config.heartbeat_interval_sec

    def start(self) -> None:
        if self._config.metrics_port and not self._metrics_started:
            start_http_server(
                self._config.metrics_port,
                addr=self._config.metrics_host,
                registry=self._registry,
            )
            self._metrics_started = True
        service = self._config.service_name
        self._service_up.labels(service=service).set(1)
        self._service_ready.labels(service=service).set(0)
        self._heartbeat.labels(service=service).set(time.time())
        self._write_snapshot()

    def mark_ready(self, note: str = "") -> None:
        self._status = "ready"
        self._note = note
        self._service_ready.labels(service=self._config.service_name).set(1)
        self.heartbeat(note=note or None)

    def mark_degraded(self, note: str = "") -> None:
        self._status = "degraded"
        self._note = note
        self._service_ready.labels(service=self._config.service_name).set(0)
        self.heartbeat(note=note or None)

    def mark_stopping(self, note: str = "") -> None:
        self._status = "stopping"
        self._note = note
        service = self._config.service_name
        self._service_ready.labels(service=service).set(0)
        self._service_up.labels(service=service).set(0)
        self.heartbeat(note=note or None)

    def heartbeat(self, *, note: Optional[str] = None) -> None:
        with self._lock:
            self._last_heartbeat_utc = utc_now_iso()
            if note is not None:
                self._note = note
            self._heartbeat.labels(service=self._config.service_name).set(time.time())
            self._write_snapshot_locked()

    def record_success(self, operation: str, *, duration_seconds: Optional[float] = None, note: str = "") -> None:
        now_iso = utc_now_iso()
        service = self._config.service_name
        self._run_counter.labels(service=service, operation=operation, outcome="success").inc()
        if duration_seconds is not None:
            self._run_duration.labels(service=service, operation=operation, outcome="success").observe(duration_seconds)
        self._last_success.labels(service=service).set(time.time())
        with self._lock:
            stats = self._operation_stats.setdefault(operation, {"success_count": 0, "failure_count": 0})
            stats["success_count"] += 1
            stats["last_success_utc"] = now_iso
            if duration_seconds is not None:
                stats["last_duration_seconds"] = round(duration_seconds, 6)
            if note:
                stats["last_note"] = note
            self._last_success_utc = now_iso
            self._last_error = ""
            self._status = "ready"
            if note:
                self._note = note
            self._last_heartbeat_utc = now_iso
            self._write_snapshot_locked()

    def record_failure(self, operation: str, error: Any, *, duration_seconds: Optional[float] = None) -> None:
        now_iso = utc_now_iso()
        error_text = str(error)
        service = self._config.service_name
        self._run_counter.labels(service=service, operation=operation, outcome="failure").inc()
        if duration_seconds is not None:
            self._run_duration.labels(service=service, operation=operation, outcome="failure").observe(duration_seconds)
        self._last_failure.labels(service=service).set(time.time())
        with self._lock:
            stats = self._operation_stats.setdefault(operation, {"success_count": 0, "failure_count": 0})
            stats["failure_count"] += 1
            stats["last_failure_utc"] = now_iso
            stats["last_error"] = error_text
            if duration_seconds is not None:
                stats["last_duration_seconds"] = round(duration_seconds, 6)
            self._last_failure_utc = now_iso
            self._last_error = error_text
            self._status = "degraded"
            self._note = operation
            self._last_heartbeat_utc = now_iso
            self._write_snapshot_locked()

    async def monitor_async(self, operation: str, awaitable):
        started = time.perf_counter()
        try:
            result = await awaitable
        except Exception as exc:
            self.record_failure(operation, exc, duration_seconds=time.perf_counter() - started)
            raise
        self.record_success(operation, duration_seconds=time.perf_counter() - started)
        return result

    async def heartbeat_loop(self, *, note: str = "running") -> None:
        while True:
            try:
                self.heartbeat(note=note)
                await asyncio.sleep(self._config.heartbeat_interval_sec)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.warning("Service heartbeat loop failed for %s: %s", self._config.service_name, exc)
                await asyncio.sleep(5)

    def _write_snapshot(self) -> None:
        with self._lock:
            self._write_snapshot_locked()

    def _write_snapshot_locked(self) -> None:
        payload = {
            "service_name": self._config.service_name,
            "pid": os.getpid(),
            "hostname": socket.gethostname(),
            "started_at_utc": self._started_at,
            "last_heartbeat_utc": self._last_heartbeat_utc,
            "last_success_utc": self._last_success_utc,
            "last_failure_utc": self._last_failure_utc,
            "status": self._status,
            "note": self._note,
            "last_error": self._last_error,
            "metrics_host": self._config.metrics_host,
            "metrics_port": self._config.metrics_port,
            "heartbeat_interval_sec": self._config.heartbeat_interval_sec,
            "operation_stats": self._operation_stats,
        }
        _write_json_atomic(self._config.health_file, payload)


def build_service_health(
    service_name: str,
    *,
    metrics_port: Optional[int] = None,
    metrics_host: str = "127.0.0.1",
    heartbeat_interval_sec: int = 30,
) -> ServiceHealthReporter:
    return ServiceHealthReporter(
        ServiceMetricsConfig.for_service(
            service_name,
            metrics_port=metrics_port,
            metrics_host=metrics_host,
            heartbeat_interval_sec=heartbeat_interval_sec,
        )
    )
