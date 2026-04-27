import unittest

from config_olx_urls import OLX_URLS
from config_shafa_urls import SHAFA_URLS
from helpers.dynamic_sources import detect_source


class MarketplaceSourceConfigTests(unittest.TestCase):
    def test_static_olx_urls_are_only_olx_sources(self) -> None:
        # Static config feeds source-stat DB tables, so wrong marketplaces pollute long-term run accounting.
        bad = [entry.get("url", "") for entry in OLX_URLS if detect_source(entry.get("url", "")) != "olx"]
        self.assertEqual(bad, [])

    def test_static_shafa_urls_are_only_shafa_sources(self) -> None:
        # This prevents an OLX grail-watch URL from being scanned by SHAFA and written into shafa_sources.
        bad = [entry.get("url", "") for entry in SHAFA_URLS if detect_source(entry.get("url", "")) != "shafa"]
        self.assertEqual(bad, [])
