from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from helpers.service_health import ServiceHealthReporter


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class LystStatusManager:
    def __init__(
        self,
        *,
        reporter: ServiceHealthReporter,
        legacy_write_status: Callable[..., Any] | None = None,
    ) -> None:
        self._reporter = reporter
        self._legacy_write_status = legacy_write_status
        self._started_at_utc: str | None = None
        self._issues: list[str] = []
        self._finished = False

    def begin_cycle(self) -> None:
        self._started_at_utc = _utc_now_iso()
        self._issues.clear()
        self._finished = False
        # The canonical snapshot mirrors legacy fields so the heartbeat can switch
        # readers without changing user-facing status text or compatibility files.
        self._reporter.set_state_fields(
            lyst_last_run_start_utc=self._started_at_utc,
            lyst_last_run_end_utc=None,
            lyst_last_run_ok=None,
            lyst_last_run_note="",
            lyst_cycle_phase="running",
        )
        self._reporter.mark_ready("lyst cycle running")
        self._write_legacy(ok=None, note="", end_utc=None)

    def mark_issue(self, note: str) -> None:
        if note:
            self._issues.append(note)
            self._reporter.set_state_fields(
                lyst_last_run_note=note,
                lyst_cycle_phase="degraded",
            )
            self._reporter.mark_degraded(note)

    def set_state_fields(self, **fields: Any) -> None:
        self._reporter.set_state_fields(**fields)

    def finish_outcome(self, outcome, *, duration_seconds: float | None = None) -> None:
        if self._finished:
            return
        self._finished = True
        finished_at_utc = _utc_now_iso()
        state_fields = outcome.service_state_fields()
        state_fields["lyst_last_run_end_utc"] = finished_at_utc
        self._reporter.set_state_fields(**state_fields)
        if outcome.ok:
            self._reporter.record_success("lyst_run", duration_seconds=duration_seconds, note=outcome.note)
        else:
            self._reporter.record_failure("lyst_run", outcome.note, duration_seconds=duration_seconds)
        self._write_legacy(ok=outcome.ok, note=outcome.note, end_utc=finished_at_utc)

    def finish_success(self, *, duration_seconds: float | None = None) -> None:
        if self._finished:
            return
        self._finished = True
        finished_at_utc = _utc_now_iso()
        if self._issues:
            note = "; ".join(sorted(set(self._issues)))
            self._reporter.set_state_fields(
                lyst_last_run_end_utc=finished_at_utc,
                lyst_last_run_ok=False,
                lyst_last_run_note=note,
                lyst_cycle_phase="failed",
            )
            self._reporter.record_failure("lyst_run", note, duration_seconds=duration_seconds)
            self._write_legacy(ok=False, note=note, end_utc=finished_at_utc)
            return

        self._reporter.set_state_fields(
            lyst_last_run_end_utc=finished_at_utc,
            lyst_last_run_ok=True,
            lyst_last_run_note="",
            lyst_cycle_phase="succeeded",
        )
        self._reporter.record_success("lyst_run", duration_seconds=duration_seconds)
        self._write_legacy(ok=True, note="", end_utc=finished_at_utc)

    def finish_failure(self, error: Any, *, duration_seconds: float | None = None) -> None:
        if self._finished:
            return
        self._finished = True
        note = str(error) if error else "failed"
        finished_at_utc = _utc_now_iso()
        self._reporter.set_state_fields(
            lyst_last_run_end_utc=finished_at_utc,
            lyst_last_run_ok=False,
            lyst_last_run_note=note,
            lyst_cycle_phase="failed",
        )
        self._reporter.record_failure("lyst_run", note, duration_seconds=duration_seconds)
        self._write_legacy(ok=False, note=note, end_utc=finished_at_utc)

    def _write_legacy(self, *, ok: bool | None, note: str, end_utc: str | None) -> None:
        if self._legacy_write_status is None:
            return
        self._legacy_write_status(
            start_utc=_parse_iso_or_none(self._started_at_utc),
            end_utc=_parse_iso_or_none(end_utc),
            ok=ok,
            note=note,
        )


def _parse_iso_or_none(value: str | None):
    if not value:
        return None
    return datetime.fromisoformat(value)
