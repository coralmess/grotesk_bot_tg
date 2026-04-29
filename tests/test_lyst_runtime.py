import asyncio
import unittest
from unittest import mock

from helpers.lyst_runtime import build_shoe_message, get_sale_emoji


class LystRuntimeTests(unittest.TestCase):
    def test_get_sale_emoji_restores_expected_visual_tiers(self) -> None:
        self.assertEqual(get_sale_emoji(95, 5000), "🚀🚀🚀")
        self.assertEqual(get_sale_emoji(70, 3500), "✨✨✨")
        self.assertEqual(get_sale_emoji(70, 5000), "🍄🍄🍄")

    def test_build_shoe_message_matches_operator_preferred_labels(self) -> None:
        shoe = {
            "name": "Test Shoe",
            "original_price": "€300",
            "sale_price": "€180",
            "lowest_price": "€170",
            "lowest_price_uah": 7650,
            "store": "Lyst",
            "country": "IT",
            "shoe_link": "https://www.lyst.com/shoe",
        }

        message = build_shoe_message(
            shoe,
            sale_percentage=40,
            uah_sale=8100,
            kurs=45.0,
            kurs_symbol="€",
        )

        self.assertIn("🍄🍄🍄", message)
        self.assertIn("💀 Prices", message)
        self.assertIn("🤑 Grivniki", message)
        self.assertIn("🧊 Kurs", message)
        self.assertIn("🔗 Store", message)
        self.assertIn("🌍 Country", message)
        self.assertNotIn("????", message)
        self.assertNotIn("?? ", message)

    def test_format_lyst_completion_message_distinguishes_cloudflare_failure(self):
        from GroteskBotTg import _format_lyst_completion_message
        from helpers.lyst.outcome import LystRunOutcome

        message = _format_lyst_completion_message(
            LystRunOutcome.cloudflare_partial(
                source_name="Main brands",
                country="US",
                page=3,
                items_seen=120,
                new_items=0,
            )
        )

        self.assertIn("failed_cloudflare", message)
        self.assertIn("Cloudflare challenge", message)
        self.assertNotEqual(message, "LYST run completed")

    def test_format_lyst_completion_message_includes_partial_success_note(self):
        from GroteskBotTg import _format_lyst_completion_message
        from helpers.lyst.outcome import LystRunOutcome

        message = _format_lyst_completion_message(
            LystRunOutcome.cloudflare_partial_success(
                source_name="Main brands",
                country="US",
                page=3,
                items_seen=120,
                new_items=2,
            )
        )

        self.assertIn("succeeded_partial", message)
        self.assertIn("Cloudflare challenge", message)
        self.assertIn("Main brands", message)

    def test_build_lyst_run_outcome_treats_cloudflare_with_items_as_partial_success(self):
        from GroteskBotTg import _build_lyst_run_outcome

        outcome = _build_lyst_run_outcome(
            run_failed=True,
            items_seen=120,
            new_items=0,
            cloudflare_event={"source_name": "Main brands", "country": "US", "page": 3},
            fallback_note="failed",
            resume_outcomes={},
        )

        self.assertEqual(outcome.phase, "succeeded_partial")
        self.assertIn("Main brands", outcome.note)

    def test_build_lyst_run_outcome_marks_cloudflare_with_items_as_partial_success(self):
        from GroteskBotTg import _build_lyst_run_outcome

        outcome = _build_lyst_run_outcome(
            run_failed=False,
            items_seen=120,
            new_items=2,
            cloudflare_event={"source_name": "Main brands", "country": "US", "page": 3},
            fallback_note="",
            resume_outcomes={},
        )

        self.assertTrue(outcome.ok)
        self.assertEqual(outcome.phase, "succeeded_partial")
        self.assertIn("Cloudflare challenge", outcome.note)
        self.assertEqual(outcome.source_name, "Main brands")

    def test_build_lyst_run_outcome_keeps_zero_item_cloudflare_failed(self):
        from GroteskBotTg import _build_lyst_run_outcome

        outcome = _build_lyst_run_outcome(
            run_failed=False,
            items_seen=0,
            new_items=0,
            cloudflare_event={"source_name": "Main brands", "country": "US", "page": 3},
            fallback_note="",
            resume_outcomes={},
        )

        self.assertFalse(outcome.ok)
        self.assertEqual(outcome.phase, "failed_cloudflare")

    def test_build_lyst_run_outcome_marks_failed_run_with_scraped_items_as_partial(self):
        from GroteskBotTg import _build_lyst_run_outcome

        outcome = _build_lyst_run_outcome(
            run_failed=True,
            items_seen=112,
            new_items=0,
            cloudflare_event={"source_name": "Main brands [3]", "country": "GB", "page": 3},
            fallback_note="failed",
            resume_outcomes={"Main brands [3]::GB": "cloudflare", "Grotesk Shoes::US": "scraped"},
        )

        self.assertTrue(outcome.ok)
        self.assertEqual(outcome.phase, "succeeded_partial")
        self.assertEqual(outcome.service_state_fields()["lyst_blocked_reason"], "cloudflare")

    def test_pending_resume_outcome_detects_local_cloudflare(self):
        from GroteskBotTg import _has_pending_lyst_resume_outcome

        self.assertTrue(_has_pending_lyst_resume_outcome({"Main:US": "cloudflare"}))
        self.assertTrue(_has_pending_lyst_resume_outcome({"Main:US": "cloudflare_cooldown"}))
        self.assertFalse(_has_pending_lyst_resume_outcome({"Main:US": "terminal"}))

    def test_should_skip_lyst_source_when_cloudflare_backoff_blocks_it(self):
        from GroteskBotTg import _should_skip_lyst_source_for_backoff

        class Backoff:
            def should_allow(self, source_name, country):
                return False

        self.assertTrue(_should_skip_lyst_source_for_backoff("Main brands", "US", Backoff()))

    def test_image_url_wrapper_delegates_to_parsing_helper(self):
        import GroteskBotTg

        with mock.patch.object(
            GroteskBotTg.lyst_parsing_helpers,
            "upgrade_lyst_image_url",
            return_value="https://img.example/item.jpg",
        ) as helper:
            result = GroteskBotTg._upgrade_lyst_image_url("https://raw.example/item.jpg")

        self.assertEqual(result, "https://img.example/item.jpg")
        helper.assert_called_once_with("https://raw.example/item.jpg")

    def test_image_candidates_wrapper_delegates_to_parsing_helper(self):
        import GroteskBotTg

        with mock.patch.object(
            GroteskBotTg.lyst_parsing_helpers,
            "image_url_candidates",
            return_value=["https://img.example/a.jpg"],
        ) as helper:
            result = GroteskBotTg._image_url_candidates("https://img.example/a.jpg")

        self.assertEqual(result, ["https://img.example/a.jpg"])
        helper.assert_called_once_with("https://img.example/a.jpg")

    def test_extract_shoe_data_wrapper_passes_runtime_context_to_helper(self):
        import GroteskBotTg

        card = object()
        fallback_map = {"product": "image"}
        expected = {"name": "Delegated Shoe"}
        with mock.patch.object(
            GroteskBotTg.lyst_parsing_helpers,
            "extract_shoe_data",
            return_value=expected,
        ) as helper:
            result = GroteskBotTg.extract_shoe_data(card, "US", fallback_map)

        self.assertEqual(result, expected)
        helper.assert_called_once()
        _, country = helper.call_args.args
        self.assertEqual(country, "US")
        self.assertIs(helper.call_args.kwargs["logger"], GroteskBotTg.logger)
        self.assertIs(helper.call_args.kwargs["skipped_items"], GroteskBotTg.SKIPPED_ITEMS)
        self.assertIs(helper.call_args.kwargs["normalize_product_link"], GroteskBotTg._normalize_lyst_product_link)
        self.assertEqual(helper.call_args.kwargs["image_fallback_map"], fallback_map)

    def test_scrape_page_wrapper_delegates_runtime_dependencies(self):
        import GroteskBotTg

        async def run_case():
            expected = ([{"name": "Delegated Shoe"}], "<html></html>", "ok")
            with mock.patch.object(
                GroteskBotTg.lyst_page_scraper,
                "scrape_page",
                new=mock.AsyncMock(return_value=expected),
            ) as helper:
                result = await GroteskBotTg.scrape_page(
                    "https://www.lyst.com/shop",
                    "US",
                    max_scroll_attempts=4,
                    url_name="Test URL",
                    page_num=2,
                    use_pagination=True,
                )
            return expected, result, helper

        expected, result, helper = asyncio.run(run_case())
        self.assertEqual(result, expected)
        helper.assert_awaited_once()
        args, kwargs = helper.await_args
        self.assertEqual(args, ("https://www.lyst.com/shop", "US"))
        self.assertIs(kwargs["get_soup_and_content"], GroteskBotTg.get_soup_and_content)
        self.assertIs(kwargs["extract_ldjson_image_map"], GroteskBotTg.extract_ldjson_image_map)
        self.assertIs(kwargs["extract_shoe_data"], GroteskBotTg.extract_shoe_data)
        self.assertIs(kwargs["mark_issue"], GroteskBotTg._mark_lyst_issue)
        self.assertIs(kwargs["cloudflare_exception"], GroteskBotTg.LystCloudflareChallenge)
        self.assertIs(kwargs["aborted_exception"], GroteskBotTg.LystRunAborted)
        self.assertIs(kwargs["terminal_exception"], GroteskBotTg.LystHttpTerminalPage)
        self.assertEqual(kwargs["max_scroll_attempts"], 4)
        self.assertEqual(kwargs["url_name"], "Test URL")
        self.assertEqual(kwargs["page_num"], 2)
        self.assertTrue(kwargs["use_pagination"])

    def test_scrape_all_pages_wrapper_delegates_runtime_dependencies(self):
        import GroteskBotTg

        async def run_case():
            expected = [{"name": "Delegated Shoe"}]
            with mock.patch.object(
                GroteskBotTg.lyst_page_runner,
                "scrape_all_pages",
                new=mock.AsyncMock(return_value=expected),
            ) as helper:
                result = await GroteskBotTg.scrape_all_pages(
                    {"url": "https://www.lyst.com/shop", "url_name": "Main"},
                    "US",
                    use_pagination=False,
                )
            return expected, result, helper

        expected, result, helper = asyncio.run(run_case())
        self.assertEqual(result, expected)
        helper.assert_awaited_once()
        args, kwargs = helper.await_args
        self.assertEqual(args, ({"url": "https://www.lyst.com/shop", "url_name": "Main"}, "US"))
        self.assertIsInstance(kwargs["config"], GroteskBotTg.lyst_page_runner.PageRunConfig)
        self.assertIsInstance(kwargs["hooks"], GroteskBotTg.lyst_page_runner.PageRunHooks)
        self.assertIs(kwargs["resume_state"], GroteskBotTg.LYST_RESUME_STATE)
        self.assertIs(kwargs["resume_entry_outcomes"], GroteskBotTg.LYST_RESUME_ENTRY_OUTCOMES)
        self.assertIs(kwargs["run_progress"], GroteskBotTg.LYST_RUN_PROGRESS)
        self.assertIs(kwargs["abort_event"], GroteskBotTg.LYST_ABORT_EVENT)
        self.assertFalse(kwargs["use_pagination"])


if __name__ == "__main__":
    unittest.main()
