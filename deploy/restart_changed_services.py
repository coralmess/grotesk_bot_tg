#!/usr/bin/env python3
"""Restart instance services only when relevant files changed.

This is meant to be called by the instance update flow right after `git pull`.
It intentionally keeps restart rules narrow so unrelated commits do not bounce
other bots.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SYSTEMD_UNIT_DIR = Path("/etc/systemd/system")

# Keep rules explicit. Each service restart should be tied only to files that
# can actually change its runtime behavior after a pull.
SERVICE_RULES = {
    "grotesk-market.service": (
        "grotesk_market_service.py",
        "grotesk-market.service",
        "olx_scraper.py",
        "shafa_scraper.py",
        "helpers/dynamic_sources.py",
        "helpers/process_pool.py",
        "helpers/scraper_unsubscribes.py",
        "helpers/service_health.py",
        "helpers/telegram_runtime.py",
        "helpers/scheduler.py",
        "helpers/runtime_paths.py",
        "GroteskBotStatus.py",
        "config.py",
        "config_olx_urls.py",
        "config_shafa_urls.py",
    ),
    "grotesk-lyst.service": (
        "grotesk_lyst_service.py",
        "grotesk-lyst.service",
        "GroteskBotTg.py",
        "helpers/service_health.py",
        "helpers/telegram_runtime.py",
        "helpers/scheduler.py",
        "helpers/runtime_paths.py",
        "helpers/lyst_identity.py",
        "helpers/lyst_",
        "GroteskBotStatus.py",
        "config.py",
        "config_lyst.py",
    ),
    "svitlobot.service": (
        "svitlo_bot.py",
        "svitlobot.service",
        "helpers/runtime_paths.py",
        "helpers/service_health.py",
        "config.py",
    ),
    "tsekbot.service": (
        "tsek_bot/",
        "helpers/service_health.py",
        "config.py",
    ),
    "usefulbot.service": (
        "useful_bot/",
        "usefulbot.service",
        "helpers/process_pool.py",
        "helpers/runtime_paths.py",
        "helpers/service_health.py",
        "config.py",
    ),
    "auto-ria-bot.service": (
        "auto_ria_bot.py",
        "auto-ria-bot.service",
        "config_auto_ria_urls.py",
        "helpers/auto_ria/",
        "helpers/process_pool.py",
        "helpers/runtime_paths.py",
        "helpers/service_health.py",
        "config.py",
    ),
}

TRACKED_SERVICE_UNITS = {service_name for service_name in SERVICE_RULES if (PROJECT_ROOT / service_name).exists()}


def run_git_diff(base_ref: str, head_ref: str) -> list[str]:
    result = subprocess.run(
        ["git", "diff", "--name-only", f"{base_ref}..{head_ref}"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def should_restart(service_name: str, changed_files: list[str]) -> bool:
    prefixes = SERVICE_RULES[service_name]
    for path in changed_files:
        if any(path == prefix or path.startswith(prefix) for prefix in prefixes):
            return True
    return False


def changed_unit_files(changed_files: list[str]) -> list[str]:
    return [path for path in changed_files if path in TRACKED_SERVICE_UNITS]


def install_changed_unit_files(changed_files: list[str]) -> None:
    units = changed_unit_files(changed_files)
    if not units:
        return
    for unit_name in units:
        # Service units live in git, but systemd only reads /etc/systemd/system.
        # Copy changed units before restart so the process uses the pulled config.
        subprocess.run(
            ["sudo", "cp", str(PROJECT_ROOT / unit_name), (SYSTEMD_UNIT_DIR / unit_name).as_posix()],
            check=True,
        )
    subprocess.run(["sudo", "systemctl", "daemon-reload"], check=True)


def restart_service(service_name: str) -> None:
    subprocess.run(["sudo", "systemctl", "restart", service_name], check=True)


def verify_service_active(service_name: str) -> None:
    subprocess.run(["systemctl", "is-active", "--quiet", service_name], check=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-ref", required=True, help="Old git ref before pull")
    parser.add_argument("--to-ref", required=True, help="New git ref after pull")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print which services would restart without restarting them",
    )
    args = parser.parse_args()

    changed_files = run_git_diff(args.from_ref, args.to_ref)
    if not changed_files:
        print("No changed files detected.")
        return 0

    print("Changed files:")
    for path in changed_files:
        print(f" - {path}")

    changed_units = changed_unit_files(changed_files)
    if changed_units:
        print("Installing changed systemd units:")
        for unit_name in changed_units:
            print(f" - {unit_name}")
        if not args.dry_run:
            install_changed_unit_files(changed_files)

    restarted_any = False
    restarted_services: list[str] = []
    for service_name in SERVICE_RULES:
        if not should_restart(service_name, changed_files):
            print(f"Skip {service_name}: no relevant changes.")
            continue

        restarted_any = True
        if args.dry_run:
            print(f"Would restart {service_name}")
            continue

        print(f"Restarting {service_name}")
        restart_service(service_name)
        restarted_services.append(service_name)

    if restarted_services:
        print("Verifying restarted services:")
        for service_name in restarted_services:
            verify_service_active(service_name)
            print(f" - {service_name}: active")

    if not restarted_any:
        print("No service restarts required.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as exc:
        print(exc.stderr or str(exc), file=sys.stderr)
        raise SystemExit(exc.returncode)
