import logging
import unittest
from pathlib import Path

from bs4 import BeautifulSoup

import olx_scraper
import shafa_scraper
from helpers.lyst import parsing as lyst_parsing


FIXTURE_DIR = Path(__file__).with_name("fixtures")


def _fixture_soup(name: str) -> BeautifulSoup:
    return BeautifulSoup((FIXTURE_DIR / name).read_text(encoding="utf-8"), "lxml")


class ScraperParserFixtureTests(unittest.TestCase):
    def test_olx_fixture_keeps_decimal_price_and_best_srcset_image(self) -> None:
        soup = _fixture_soup("olx_listing_decimal_price.html")
        cards = olx_scraper.collect_cards_with_stop(soup)

        self.assertEqual(len(cards), 1)
        item = olx_scraper.parse_card(cards[0])
        self.assertIsNotNone(item)
        self.assertEqual(item.name, "Test Sneakers")
        self.assertEqual(item.price_text, "10220.15 грн")
        self.assertEqual(item.price_int, 10220)
        self.assertEqual(item.first_image_url, "https://img.olx.ua/images/high.jpg")

    def test_shafa_fixture_uses_current_sale_price_and_same_anchor_image(self) -> None:
        soup = _fixture_soup("shafa_sale_card.html")
        cards = shafa_scraper.collect_cards(soup)

        self.assertEqual(len(cards), 1)
        item = shafa_scraper.parse_card(cards[0])
        self.assertIsNotNone(item)
        self.assertEqual(item.id, "151937764")
        self.assertEqual(item.name, "Брендова куртка zimtstern switzerland.")
        self.assertEqual(item.price_text, "1032 грн")
        self.assertEqual(item.price_int, 1032)
        self.assertEqual(item.brand, "Zimmerli of Switzerland")
        self.assertEqual(item.first_image_url, "https://image-thumbs.shafastatic.net/1997969041")

    def test_lyst_fixture_uses_ldjson_image_when_card_image_is_lazy_missing(self) -> None:
        soup = _fixture_soup("lyst_ldjson_lazy_card.html")
        image_map = lyst_parsing.extract_ldjson_image_map(soup)
        card = soup.find("div", class_="_693owt3")

        item = lyst_parsing.extract_shoe_data(
            card,
            "GB",
            logger=logging.getLogger("test_scraper_parser_fixtures"),
            skipped_items=set(),
            normalize_product_link=lambda value: value,
            image_fallback_map=image_map,
        )

        self.assertIsNotNone(item)
        self.assertEqual(item["name"], "CamperLab Vamonos Loafers - White")
        self.assertEqual(item["store"], "SSENSE")
        self.assertEqual(item["original_price"], "€320")
        self.assertEqual(item["sale_price"], "€160")
        self.assertEqual(
            item["image_url"],
            "https://cdna.lystit.com/photos/ssense/936712a3/camperlab-White-Vamonos-Loafers.jpeg",
        )


if __name__ == "__main__":
    unittest.main()
