from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any


@dataclass
class LystRuntimeState:
    """Mutable state for one Lyst service process.

    The production module still exposes compatibility globals, but this object is
    the migration target: state that changes per run should live together instead
    of being scattered across module-level variables.
    """

    abort_event: asyncio.Event = field(default_factory=asyncio.Event)
    run_failed: bool = False
    run_progress: dict[str, int] = field(default_factory=dict)
    cycle_started_in_resume: bool = False
    resume_entry_outcomes: dict[str, str] = field(default_factory=dict)
    last_cloudflare_event: dict[str, Any] | None = None

    def begin_cycle(self, *, resume_active: bool) -> None:
        self.abort_event.clear()
        self.run_failed = False
        self.run_progress.clear()
        self.cycle_started_in_resume = bool(resume_active)
        self.resume_entry_outcomes.clear()
        self.last_cloudflare_event = None

    def mark_failed(self) -> None:
        self.run_failed = True
        self.abort_event.set()

    def record_cloudflare_event(self, *, source_name: str, country: str, page: int | None) -> None:
        self.last_cloudflare_event = {
            "source_name": source_name,
            "country": country,
            "page": page,
        }

    def should_restart_after_terminal_resume(self, all_shoes: list[dict]) -> bool:
        if all_shoes or not self.cycle_started_in_resume:
            return False
        if not self.resume_entry_outcomes:
            return False
        return all(outcome == "terminal_only_resume" for outcome in self.resume_entry_outcomes.values())
