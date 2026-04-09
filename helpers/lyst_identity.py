from __future__ import annotations

import logging
from pathlib import Path

from helpers.runtime_paths import RUNTIME_BROWSER_DIR, ensure_runtime_dirs


LYST_BROWSER_DIR = RUNTIME_BROWSER_DIR / "lyst"
LYST_BROWSER_CACHE_DIR = LYST_BROWSER_DIR / "cache"
LYST_STORAGE_STATE_DIR = LYST_BROWSER_DIR / "storage_state"


def ensure_lyst_identity_dirs() -> None:
    ensure_runtime_dirs()
    for directory in (LYST_BROWSER_DIR, LYST_BROWSER_CACHE_DIR, LYST_STORAGE_STATE_DIR):
        directory.mkdir(parents=True, exist_ok=True)


def country_storage_state_path(country: str) -> Path:
    ensure_lyst_identity_dirs()
    return LYST_STORAGE_STATE_DIR / f"{country.strip().upper()}.json"


def browser_launch_args() -> list[str]:
    ensure_lyst_identity_dirs()
    return [
        f"--disk-cache-dir={LYST_BROWSER_CACHE_DIR}",
        "--disk-cache-size=1073741824",
    ]


async def persist_context_storage_state(country: str, context, logger: logging.Logger) -> None:
    state_path = country_storage_state_path(country)
    try:
        await context.storage_state(path=str(state_path))
    except Exception as exc:
        logger.debug("Failed to persist Lyst storage state for %s: %s", country, exc)
