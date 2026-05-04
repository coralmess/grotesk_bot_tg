from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from helpers.runtime_paths import RUNTIME_ANALYTICS_DIR


def build_analytics_daily_summary(
    analytics_dir: Path = RUNTIME_ANALYTICS_DIR,
    *,
    date_key: str | None = None,
) -> dict[str, Any]:
    root = Path(analytics_dir)
    daily_dir = root / "daily"
    if date_key is None:
        date_key = _latest_daily_date(daily_dir)
    payload = {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "date": date_key,
        "domains": {},
    }
    if not date_key:
        return payload

    for path in sorted(daily_dir.glob(f"{date_key}.*.json")):
        try:
            domain_payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        domain = str(domain_payload.get("domain") or path.stem.split(".", 1)[-1])
        payload["domains"][domain] = {
            "groups": domain_payload.get("groups") or {},
        }
    return payload


def write_analytics_summary(analytics_dir: Path = RUNTIME_ANALYTICS_DIR, *, date_key: str | None = None) -> Path:
    root = Path(analytics_dir)
    summary = build_analytics_daily_summary(root, date_key=date_key)
    path = root / "summary" / "latest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(tmp_path, path)
    return path


def _latest_daily_date(daily_dir: Path) -> str:
    dates = []
    if daily_dir.exists():
        for path in daily_dir.glob("????-??-??.*.json"):
            dates.append(path.name[:10])
    return max(dates) if dates else ""
