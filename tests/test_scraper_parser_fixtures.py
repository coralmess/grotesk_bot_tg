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

    def test_olx_fixture_stops_before_recommendation_boundary(self) -> None:
        soup = _fixture_soup("olx_listing_recommendation_boundary.html")
        cards = olx_scraper.collect_cards_with_stop(soup)

        self.assertEqual(len(cards), 1)
        item = olx_scraper.parse_card(cards[0])
        self.assertIsNotNone(item)
        self.assertEqual(item.name, "Real search item")
        self.assertNotIn("Recommended unrelated item", cards[0].get_text(" ", strip=True))

    def test_olx_extended_search_card_is_rejected_even_if_boundary_is_missing(self) -> None:
        soup = BeautifulSoup(
            """
            <div data-testid="l-card">
              <a href="/d/uk/obyavlenie/orciani-bag-IDbad.html?reason=extended_search_extended_category&search_reason=search%7Corganic">
                Orciani unrelated category recommendation
              </a>
              <p data-testid="ad-price">2 500 грн.</p>
              <img src="https://img.olx.ua/images/bad.jpg" />
            </div>
            """,
            "lxml",
        )

        self.assertIsNone(olx_scraper.parse_card(soup.find("div", attrs={"data-testid": "l-card"})))

    def test_olx_save_search_banner_does_not_stop_real_cards_after_it(self) -> None:
        soup = BeautifulSoup(
            """
            <div data-testid="l-card">
              <a href="/d/uk/obyavlenie/first-real-IDone.html">First real item</a>
              <p data-testid="ad-price">1 000 грн.</p>
            </div>
            <div>
              <div class="css-1vnmjfl">
                <p data-nx-name="P3" data-nx-legacy="true">Зберегти параметри пошуку</p>
                <p data-nx-name="P4" data-nx-legacy="true">Якщо з’являться схожі оголошення, ми повідомимо.</p>
              </div>
            </div>
            <div data-testid="l-card">
              <a href="/d/uk/obyavlenie/second-real-IDtwo.html">Second real item</a>
              <p data-testid="ad-price">2 000 грн.</p>
            </div>
            """,
            "lxml",
        )

        cards = olx_scraper.collect_cards_with_stop(soup)

        self.assertEqual([olx_scraper.parse_card(card).name for card in cards], ["First real item", "Second real item"])

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
