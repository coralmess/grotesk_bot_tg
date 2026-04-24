from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class LystRunState(str, Enum):
    SUCCESS_FULL = "success_full"
    SUCCESS_PARTIAL = "success_partial"
    FAILED_CLOUDFLARE = "failed_cloudflare"
    FAILED_STALLED = "failed_stalled"
    FAILED = "failed"
    SKIPPED_DISABLED = "skipped_disabled"


@dataclass(slots=True)
class LystRunOutcome:
    state: LystRunState
    note: str = ""
    items_seen: int = 0
    new_items: int = 0
    source_name: str = ""
    country: str = ""
    page: int | None = None

    @property
    def ok(self) -> bool:
        return self.state in {LystRunState.SUCCESS_FULL, LystRunState.SUCCESS_PARTIAL}

    @property
    def phase(self) -> str:
        if self.state == LystRunState.SUCCESS_FULL:
            return "succeeded"
        if self.state == LystRunState.SUCCESS_PARTIAL:
            return "succeeded_partial"
        if self.state == LystRunState.FAILED_CLOUDFLARE:
            return "failed_cloudflare"
        if self.state == LystRunState.FAILED_STALLED:
            return "failed_stalled"
        if self.state == LystRunState.SKIPPED_DISABLED:
            return "skipped_disabled"
        return "failed"

    @classmethod
    def full_success(cls, *, items_seen: int, new_items: int) -> "LystRunOutcome":
        return cls(LystRunState.SUCCESS_FULL, items_seen=items_seen, new_items=new_items)

    @classmethod
    def partial_success(cls, *, note: str, items_seen: int, new_items: int) -> "LystRunOutcome":
        return cls(LystRunState.SUCCESS_PARTIAL, note=note, items_seen=items_seen, new_items=new_items)

    @classmethod
    def cloudflare_partial(
        cls,
        *,
        source_name: str,
        country: str,
        page: int | None,
        items_seen: int,
        new_items: int,
    ) -> "LystRunOutcome":
        location = " ".join(part for part in (source_name, country, f"page {page}" if page else "") if part)
        note = f"Cloudflare challenge: {location}" if location else "Cloudflare challenge"
        return cls(
            LystRunState.FAILED_CLOUDFLARE,
            note=note,
            items_seen=items_seen,
            new_items=new_items,
            source_name=source_name,
            country=country,
            page=page,
        )

    @classmethod
    def failed(cls, note: str) -> "LystRunOutcome":
        state = LystRunState.FAILED_STALLED if note == "stalled" else LystRunState.FAILED
        return cls(state, note=note or "failed")

    def service_state_fields(self) -> dict[str, object]:
        return {
            "lyst_last_run_ok": self.ok,
            "lyst_last_run_note": self.note,
            "lyst_cycle_phase": self.phase,
            "lyst_items_seen": self.items_seen,
            "lyst_new_items": self.new_items,
            "lyst_failure_source": self.source_name,
            "lyst_failure_country": self.country,
            "lyst_failure_page": self.page,
        }
