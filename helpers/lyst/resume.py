from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from helpers import lyst_state as lyst_state_helpers


class LystResumeController:
    # GroteskBotTg.py historically owned resume globals, persistence, and
    # transition rules directly. This controller centralizes that truth so the
    # service entrypoint can become orchestration-only instead of stateful.
    def __init__(
        self,
        *,
        resume_file: Path,
        kyiv_tz: ZoneInfo,
        logger,
        abort_event: asyncio.Event | None = None,
        resume_lock: asyncio.Lock | None = None,
    ) -> None:
        self.resume_file = resume_file
        self.kyiv_tz = kyiv_tz
        self.logger = logger
        self.abort_event = abort_event or asyncio.Event()
        self.resume_lock = resume_lock or asyncio.Lock()
        self.state: dict = {"resume_active": False, "entries": {}}

    def resume_key(self, base_url: dict, country: str) -> str:
        return f"{base_url['url_name']}::{country}"

    def now_kyiv_str(self) -> str:
        return datetime.now(self.kyiv_tz).strftime("%Y-%m-%d %H:%M:%S")

    def load_state(self) -> dict:
        self.state = lyst_state_helpers.load_resume_state(
            resume_file=self.resume_file,
            logger=self.logger,
        )
        return self.state

    def save_state(self, state: dict | None = None) -> None:
        payload = state if state is not None else self.state
        lyst_state_helpers.save_resume_state(
            resume_file=self.resume_file,
            state=payload,
            logger=self.logger,
        )

    def init_run(self, loaded_state: dict | None = None) -> dict:
        if loaded_state is not None:
            self.state = loaded_state
        self.state, _, _ = lyst_state_helpers.init_resume_state(
            loaded_state=self.state,
            abort_event=self.abort_event,
        )
        return self.state

    async def update_entry(self, key: str, **fields) -> None:
        await lyst_state_helpers.update_resume_entry(
            resume_lock=self.resume_lock,
            resume_state=self.state,
            key=key,
            fields=fields,
            now_kyiv_str_fn=self.now_kyiv_str,
            save_state_fn=self.save_state,
        )

    async def mark_run_failed(self, reason: str, run_progress: dict) -> bool:
        return await lyst_state_helpers.mark_run_failed(
            reason=reason,
            resume_lock=self.resume_lock,
            resume_state=self.state,
            run_progress=run_progress,
            now_kyiv_str_fn=self.now_kyiv_str,
            save_state_fn=self.save_state,
            abort_event=self.abort_event,
        )

    def log_run_progress_summary(self, run_progress: dict) -> None:
        lyst_state_helpers.log_run_progress_summary(
            run_progress=run_progress,
            logger=self.logger,
        )

    async def finalize_after_processing(self, *, run_failed: bool, preserve_resume: bool = False) -> None:
        await lyst_state_helpers.finalize_resume_after_processing(
            resume_lock=self.resume_lock,
            resume_state=self.state,
            run_failed=run_failed,
            preserve_resume=preserve_resume,
            save_state_fn=self.save_state,
        )

    async def clear(self) -> None:
        async with self.resume_lock:
            self.state["resume_active"] = False
            self.state["entries"] = {}
            for key in ("last_run_progress", "last_failure_reason", "last_failure_at"):
                self.state.pop(key, None)
            self.save_state()

    def should_restart_after_terminal_resume(
        self,
        *,
        all_shoes: list[dict],
        cycle_started_in_resume: bool,
        entry_outcomes: dict[str, str],
    ) -> bool:
        if all_shoes or not cycle_started_in_resume:
            return False
        if not entry_outcomes:
            return False
        return all(outcome == "terminal_only_resume" for outcome in entry_outcomes.values())
