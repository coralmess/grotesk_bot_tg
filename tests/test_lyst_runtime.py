import unittest

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

    def test_build_lyst_run_outcome_prefers_cloudflare_failure_event(self):
        from GroteskBotTg import _build_lyst_run_outcome

        outcome = _build_lyst_run_outcome(
            run_failed=True,
            items_seen=120,
            new_items=0,
            cloudflare_event={"source_name": "Main brands", "country": "US", "page": 3},
            fallback_note="failed",
        )

        self.assertEqual(outcome.phase, "failed_cloudflare")
        self.assertIn("Main brands", outcome.note)

    def test_should_skip_lyst_source_when_cloudflare_backoff_blocks_it(self):
        from GroteskBotTg import _should_skip_lyst_source_for_backoff

        class Backoff:
            def should_allow(self, source_name, country):
                return False

        self.assertTrue(_should_skip_lyst_source_for_backoff("Main brands", "US", Backoff()))


if __name__ == "__main__":
    unittest.main()
