from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class FetchStatus(str, Enum):
    # The fetch layer now returns explicit statuses so cycle orchestration can
    # react without inferring meaning from ad-hoc tuples and string branches.
    OK = "ok"
    FAILED = "failed"
    TERMINAL = "terminal"
    ABORTED = "aborted"
    CLOUDFLARE = "cloudflare"


@dataclass(slots=True)
class FetchResult:
    # This typed result exists to make fetch/cycle boundaries explicit before the
    # rest of the Lyst orchestration is moved out of GroteskBotTg.py.
    status: FetchStatus
    content: str | None = None
    soup: Any | None = None
    final_url: str | None = None
    page_debug_events: list[str] = field(default_factory=list)
    screenshot_bytes: bytes | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def is_ok(self) -> bool:
        return self.status == FetchStatus.OK

    @property
    def is_failed(self) -> bool:
        return self.status == FetchStatus.FAILED

    @property
    def is_terminal(self) -> bool:
        return self.status == FetchStatus.TERMINAL


@dataclass(slots=True)
class PageProgress:
    # Explicit progress records make the Lyst run state easier to move between
    # orchestrator, diagnostics, and resume helpers without more global dicts.
    step: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ResumeEntry:
    # The monolith historically mutated bare dicts in many places; this model
    # documents the expected resume fields before deeper extraction work lands.
    key: str
    data: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CycleOutcome:
    # Cycle aggregation will eventually return structured outcomes instead of
    # several loosely coupled globals and helper return values.
    shoes: list[dict[str, Any]] = field(default_factory=list)
    run_failed: bool = False
    restarted_from_terminal_resume: bool = False
    entry_outcomes: dict[str, str] = field(default_factory=dict)
