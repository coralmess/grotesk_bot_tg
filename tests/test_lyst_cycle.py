import unittest

from helpers.lyst.cycle import (
    build_scrape_target_url,
    collect_successful_results,
    should_restart_after_terminal_resume,
)


class LystCycleTests(unittest.TestCase):
    def test_build_scrape_target_url_respects_single_page_mode(self) -> None:
        base_url = {"url": "https://www.lyst.com/shop"}

        self.assertEqual(build_scrape_target_url(base_url, 1, False), "https://www.lyst.com/shop")
        self.assertEqual(build_scrape_target_url(base_url, 1, True), "https://www.lyst.com/shop")
        self.assertEqual(build_scrape_target_url(base_url, 3, True), "https://www.lyst.com/shop&page=3")

    def test_collect_successful_results_skips_exceptions(self) -> None:
        class _Logger:
            def __init__(self) -> None:
                self.messages = []

            def error(self, message):
                self.messages.append(message)

        logger = _Logger()
        result = collect_successful_results([[{"name": "a"}], RuntimeError("boom"), [{"name": "b"}]], logger=logger)

        self.assertEqual(result, [{"name": "a"}, {"name": "b"}])
        self.assertEqual(len(logger.messages), 1)

    def test_should_restart_after_terminal_resume_requires_resume_empty_terminal_only(self) -> None:
        self.assertTrue(
            should_restart_after_terminal_resume(
                all_shoes=[],
                cycle_started_in_resume=True,
                entry_outcomes={"pl": "terminal_only_resume", "us": "terminal_only_resume"},
            )
        )
        self.assertFalse(
            should_restart_after_terminal_resume(
                all_shoes=[{"name": "shoe"}],
                cycle_started_in_resume=True,
                entry_outcomes={"pl": "terminal_only_resume"},
            )
        )


if __name__ == "__main__":
    unittest.main()
