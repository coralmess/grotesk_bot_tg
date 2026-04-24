import asyncio
import json
import logging
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

from helpers.lyst import pricing


class LystPricingTests(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger("test_lyst_pricing")

    def test_extract_price_tokens_handles_trailing_currency(self):
        tokens = pricing.extract_price_tokens("Now only 215€ and 300$")
        self.assertIn("215€", tokens)
        self.assertIn("300$", tokens)

    def test_calculate_sale_percentage(self):
        self.assertEqual(pricing.calculate_sale_percentage("€200", "€100", "PL"), 50)

    def test_convert_to_uah_rounds_to_tens(self):
        result = pricing.convert_to_uah("$103", "US", {"USD": 0.025}, "Item", logger=self.logger)
        self.assertEqual(result.uah_amount, 4120)
        self.assertEqual(result.exchange_rate, 40.0)
        self.assertEqual(result.currency_symbol, "$")

    def test_load_exchange_rates_uses_fresh_cache_without_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rates.json"
            path.write_text(
                json.dumps(
                    {
                        "last_update": datetime.now().isoformat(),
                        "rates": {"EUR": 0.04, "USD": 0.025, "GBP": 0.03},
                    }
                ),
                encoding="utf-8",
            )
            rates = pricing.load_exchange_rates(
                exchange_rate_api_key="unused",
                exchange_rates_file=path,
                logger=self.logger,
            )
        self.assertEqual(rates["USD"], 0.025)

    def test_async_load_exchange_rates_uses_fresh_cache_without_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rates.json"
            path.write_text(
                json.dumps(
                    {
                        "last_update": datetime.now().isoformat(),
                        "rates": {"EUR": 0.04, "USD": 0.025, "GBP": 0.03},
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.object(pricing, "async_update_exchange_rates", side_effect=AssertionError("network not expected")):
                rates = asyncio.run(
                    pricing.async_load_exchange_rates(
                        exchange_rate_api_key="unused",
                        exchange_rates_file=path,
                        logger=self.logger,
                    )
                )
        self.assertEqual(rates["USD"], 0.025)

    def test_load_cached_exchange_rates_uses_injected_now_for_freshness(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rates.json"
            path.write_text(
                '{"last_update":"2026-04-21T10:00:00","rates":{"EUR":0.04,"USD":0.025,"GBP":0.03}}',
                encoding="utf-8",
            )

            rates, is_fresh = pricing._load_cached_exchange_rates(
                path,
                now=datetime.fromisoformat("2026-04-22T09:00:00"),
            )

        self.assertTrue(is_fresh)
        self.assertEqual(rates["USD"], 0.025)
