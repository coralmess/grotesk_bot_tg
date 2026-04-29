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


def _cloudflare_note(*, source_name: str, country: str, page: int | None) -> str:
    location = " ".join(part for part in (source_name, country, f"page {page}" if page else "") if part)
    return f"Cloudflare challenge: {location}" if location else "Cloudflare challenge"


@dataclass(slots=True)
class LystRunOutcome:
    state: LystRunState
    note: str = ""
    items_seen: int = 0
    new_items: int = 0
    source_name: str = ""
    country: str = ""
    page: int | None = None
    blocked_reason: str = ""

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
    def blocked_partial_success(
        cls,
        *,
        reason: str,
        items_seen: int,
        new_items: int,
        source_name: str = "",
        country: str = "",
        page: int | None = None,
    ) -> "LystRunOutcome":
        if reason == "cloudflare":
            note = _cloudflare_note(source_name=source_name, country=country, page=page)
        else:
            note = f"Partial coverage: {reason or 'blocked'}"
        return cls(
            LystRunState.SUCCESS_PARTIAL,
            note=note,
            items_seen=items_seen,
            new_items=new_items,
            source_name=source_name,
            country=country,
            page=page,
            blocked_reason=reason,
        )

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
        note = _cloudflare_note(source_name=source_name, country=country, page=page)
        return cls(
            LystRunState.FAILED_CLOUDFLARE,
            note=note,
            items_seen=items_seen,
            new_items=new_items,
            source_name=source_name,
            country=country,
            page=page,
            blocked_reason="cloudflare",
        )

    @classmethod
    def cloudflare_partial_success(
        cls,
        *,
        source_name: str,
        country: str,
        page: int | None,
        items_seen: int,
        new_items: int,
    ) -> "LystRunOutcome":
        note = _cloudflare_note(source_name=source_name, country=country, page=page)
        return cls(
            LystRunState.SUCCESS_PARTIAL,
            note=note,
            items_seen=items_seen,
            new_items=new_items,
            source_name=source_name,
            country=country,
            page=page,
            blocked_reason="cloudflare",
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
            "lyst_blocked_reason": self.blocked_reason,
        }


def _blocked_reason_from_resume_outcomes(resume_outcomes: dict[str, str]) -> str:
    blocked_order = ("cloudflare", "failed", "aborted", "cloudflare_cooldown")
    values = set(resume_outcomes.values())
    for outcome in blocked_order:
        if outcome in values:
            return "cloudflare" if outcome == "cloudflare_cooldown" else outcome
    return ""


def build_lyst_run_outcome(
    *,
    run_failed: bool,
    items_seen: int,
    new_items: int,
    cloudflare_event: dict | None,
    fallback_note: str,
    resume_outcomes: dict[str, str] | None = None,
) -> LystRunOutcome:
    """Convert low-level cycle flags into the single status model used by logs and Telegram.

    Lyst can hit Cloudflare or page fetch failures after already scraping useful
    items. Keeping this decision in one helper avoids future edits accidentally
    marking partial coverage as either a clean full success or a hard failure.
    """
    resume_outcomes = resume_outcomes or {}
    blocked_reason = "cloudflare" if cloudflare_event else _blocked_reason_from_resume_outcomes(resume_outcomes)
    # A later blocked source should not erase useful pages already collected; this
    # keeps status honest without pretending the whole LYST catalog completed.
    has_usable_coverage = items_seen > 0
    stalled = (fallback_note or "").strip() == "stalled"

    if blocked_reason and has_usable_coverage and not stalled:
        if cloudflare_event:
            return LystRunOutcome.blocked_partial_success(
                reason="cloudflare",
                source_name=str(cloudflare_event.get("source_name") or ""),
                country=str(cloudflare_event.get("country") or ""),
                page=cloudflare_event.get("page"),
                items_seen=items_seen,
                new_items=new_items,
            )
        return LystRunOutcome.blocked_partial_success(
            reason=blocked_reason,
            items_seen=items_seen,
            new_items=new_items,
        )

    if cloudflare_event:
        return LystRunOutcome.cloudflare_partial(
            source_name=str(cloudflare_event.get("source_name") or ""),
            country=str(cloudflare_event.get("country") or ""),
            page=cloudflare_event.get("page"),
            items_seen=items_seen,
            new_items=new_items,
        )
    if run_failed:
        return LystRunOutcome.failed(fallback_note or "failed")
    return LystRunOutcome.full_success(items_seen=items_seen, new_items=new_items)


def format_lyst_completion_message(outcome: LystRunOutcome) -> str:
    """Return a compact operational log line for the end of a Lyst cycle."""
    if outcome.ok:
        if outcome.note:
            return (
                f"LYST run {outcome.phase}: {outcome.note}; "
                f"items_seen={outcome.items_seen}, new_items={outcome.new_items}"
            )
        return (
            f"LYST run {outcome.phase}: "
            f"items_seen={outcome.items_seen}, new_items={outcome.new_items}"
        )
    return (
        f"LYST run {outcome.phase}: {outcome.note}; "
        f"items_seen={outcome.items_seen}, new_items={outcome.new_items}"
    )


def has_pending_lyst_resume_outcome(entry_outcomes: dict[str, str]) -> bool:
    """Tell resume finalization whether source-level failures must remain resumable.

    Local Cloudflare/cancel outcomes may still leave useful already-scraped items.
    We keep resume state for those sources instead of clearing it with the rest of a
    partial-but-processed run.
    """
    return any(
        outcome in {"cloudflare", "cloudflare_cooldown", "failed", "aborted"}
        for outcome in entry_outcomes.values()
    )
