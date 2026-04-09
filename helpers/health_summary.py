from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from helpers.runtime_paths import (
    LAST_RUNS_JSON_FILE,
    MARKET_OLX_RUN_STATUS_FILE,
    MARKET_SHAFA_RUN_STATUS_FILE,
    RUNTIME_HEALTH_DIR,
    RUNTIME_STATUS_DIR,
    runtime_file,
    service_health_file,
)

SUMMARY_JSON_FILE = runtime_file(RUNTIME_HEALTH_DIR, "summary.json")
SUMMARY_TEXT_FILE = runtime_file(RUNTIME_HEALTH_DIR, "summary.txt")
KNOWN_SERVICES = (
    "grotesk-market",
    "grotesk-lyst",
    "usefulbot",
    "svitlobot",
    "tsekbot",
)


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def build_health_summary_payload() -> dict[str, Any]:
    services: dict[str, Any] = {}
    for service_name in KNOWN_SERVICES:
        services[service_name] = _read_json(service_health_file(service_name)) or {}

    payload = {
        "generated_at_utc": _iso_now(),
        "services": services,
        "lyst": _read_json(LAST_RUNS_JSON_FILE) or {},
        "market": {
            "olx": _read_json(MARKET_OLX_RUN_STATUS_FILE) or {},
            "shafa": _read_json(MARKET_SHAFA_RUN_STATUS_FILE) or {},
        },
    }
    return payload


def render_health_summary_text(payload: dict[str, Any]) -> str:
    lines = [f"Generated: {payload.get('generated_at_utc', '')}"]
    for service_name, info in (payload.get("services") or {}).items():
        status = info.get("status", "unknown")
        note = info.get("note", "")
        heartbeat = info.get("last_heartbeat_utc", "never")
        line = f"{service_name}: {status} | heartbeat={heartbeat}"
        if note:
            line += f" | note={note}"
        lines.append(line)

    lyst = payload.get("lyst") or {}
    if lyst:
        lines.append(
            "lyst: "
            f"last={lyst.get('last_lyst_run_end_utc', 'never')} | "
            f"ok={lyst.get('last_lyst_run_ok', 'unknown')} | "
            f"note={lyst.get('last_lyst_run_note', '')}"
        )

    market = payload.get("market") or {}
    for source_name in ("olx", "shafa"):
        source = market.get(source_name) or {}
        lines.append(
            f"{source_name}: last={source.get('last_run_end_utc', 'never')} | "
            f"ok={source.get('last_run_ok', 'unknown')} | note={source.get('last_run_note', '')}"
        )
    return "\n".join(lines) + "\n"


def write_health_summary_files() -> dict[str, Any]:
    payload = build_health_summary_payload()
    SUMMARY_JSON_FILE.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    SUMMARY_TEXT_FILE.write_text(render_health_summary_text(payload), encoding="utf-8")
    return payload
