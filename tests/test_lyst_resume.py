import asyncio
import logging
import tempfile
import unittest
from pathlib import Path
from zoneinfo import ZoneInfo

from helpers.lyst.models import FetchResult, FetchStatus
from helpers.lyst.resume import LystResumeController


class LystModelsTests(unittest.TestCase):
    def test_fetch_result_exposes_status_helpers(self) -> None:
        result = FetchResult(status=FetchStatus.OK, content="<html></html>")

        self.assertTrue(result.is_ok)
        self.assertFalse(result.is_terminal)
        self.assertFalse(result.is_failed)


class LystResumeControllerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.resume_file = Path(self._tmpdir.name) / "resume.json"
        self.controller = LystResumeController(
            resume_file=self.resume_file,
            kyiv_tz=ZoneInfo("Europe/Kyiv"),
            logger=logging.getLogger("test_lyst_resume"),
        )

    async def asyncTearDown(self) -> None:
        self._tmpdir.cleanup()

    async def test_update_entry_persists_timestamped_resume_state(self) -> None:
        self.controller.load_state()

        await self.controller.update_entry("url::PL", next_page=3, completed=False)

        self.assertTrue(self.resume_file.exists())
        entry = self.controller.state["entries"]["url::PL"]
        self.assertEqual(entry["next_page"], 3)
        self.assertFalse(entry["completed"])
        self.assertIn("updated_at", entry)

    async def test_mark_run_failed_sets_abort_and_persists_progress(self) -> None:
        self.controller.load_state()

        await self.controller.mark_run_failed("Cloudflare", {"url::PL": 2})

        self.assertTrue(self.controller.abort_event.is_set())
        self.assertTrue(self.controller.state["resume_active"])
        self.assertEqual(self.controller.state["last_failure_reason"], "Cloudflare")
        self.assertEqual(self.controller.state["last_run_progress"], {"url::PL": 2})

    async def test_finalize_after_processing_advances_next_page(self) -> None:
        self.controller.state = {
            "resume_active": True,
            "entries": {
                "url::PL": {
                    "last_scraped_page": 4,
                    "scrape_complete": False,
                    "next_page": 1,
                },
                "url::US": {
                    "last_scraped_page": 2,
                    "scrape_complete": True,
                    "next_page": 1,
                },
            },
        }

        await self.controller.finalize_after_processing(run_failed=False)

        pl_entry = self.controller.state["entries"]["url::PL"]
        us_entry = self.controller.state["entries"]["url::US"]
        self.assertEqual(pl_entry["last_success_page"], 4)
        self.assertEqual(pl_entry["next_page"], 5)
        self.assertFalse(pl_entry["completed"])
        self.assertEqual(us_entry["last_success_page"], 2)
        self.assertEqual(us_entry["next_page"], 1)
        self.assertTrue(us_entry["completed"])

    async def test_finalize_after_processing_preserves_resume_without_treating_clean_entries_as_failed(self) -> None:
        self.controller.state = {
            "resume_active": True,
            "entries": {
                "blocked::IT": {
                    "last_scraped_page": 0,
                    "next_page": 1,
                    "completed": False,
                    "failure_reason": "Cloudflare challenge",
                },
                "clean::US": {
                    "last_scraped_page": 2,
                    "scrape_complete": True,
                    "next_page": 1,
                    "completed": False,
                },
            },
        }

        await self.controller.finalize_after_processing(run_failed=False, preserve_resume=True)

        blocked_entry = self.controller.state["entries"]["blocked::IT"]
        clean_entry = self.controller.state["entries"]["clean::US"]
        self.assertTrue(self.controller.state["resume_active"])
        self.assertEqual(blocked_entry["next_page"], 1)
        self.assertFalse(blocked_entry["completed"])
        self.assertEqual(clean_entry["last_success_page"], 2)
        self.assertEqual(clean_entry["next_page"], 1)
        self.assertTrue(clean_entry["completed"])

    async def test_should_restart_after_terminal_resume_requires_empty_resume_only_outcomes(self) -> None:
        self.assertTrue(
            self.controller.should_restart_after_terminal_resume(
                all_shoes=[],
                cycle_started_in_resume=True,
                entry_outcomes={"pl": "terminal_only_resume", "us": "terminal_only_resume"},
            )
        )
        self.assertFalse(
            self.controller.should_restart_after_terminal_resume(
                all_shoes=[{"name": "shoe"}],
                cycle_started_in_resume=True,
                entry_outcomes={"pl": "terminal_only_resume"},
            )
        )
        self.assertFalse(
            self.controller.should_restart_after_terminal_resume(
                all_shoes=[],
                cycle_started_in_resume=False,
                entry_outcomes={"pl": "terminal_only_resume"},
            )
        )


if __name__ == "__main__":
    unittest.main()
