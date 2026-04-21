import logging
import unittest
from bs4 import BeautifulSoup

from helpers.lyst import parsing


class LystParsingTests(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger("test_lyst_parsing")

    def test_upgrade_lyst_image_url_removes_resize_prefix(self):
        upgraded = parsing.upgrade_lyst_image_url("https://cdna.lystit.com/200/300/photos/store/item.jpg")
        self.assertEqual(upgraded, "https://cdna.lystit.com/photos/store/item.jpg")

    def test_find_price_strings_prefers_del_for_original(self):
        soup = BeautifulSoup("<div><del>€200</del><span>€120</span></div>", "html.parser")
        self.assertEqual(parsing.find_price_strings(soup.div), ("€200", "€120"))

    def test_extract_ldjson_image_map_keeps_product_url_mapping(self):
        soup = BeautifulSoup(
            """
            <script type="application/ld+json">
            {"@type":"ItemList","itemListElement":[{"item":{"url":"https://www.lyst.com/shoes/foo/","image":["https://cdna.lystit.com/200/300/photos/store/item.jpg"]}}]}
            </script>
            """,
            "html.parser",
        )
        image_map = parsing.extract_ldjson_image_map(soup)
        self.assertEqual(
            image_map["https://www.lyst.com/shoes/foo/"],
            "https://cdna.lystit.com/photos/store/item.jpg",
        )

    def test_extract_shoe_data_uses_ldjson_image_fallback(self):
        html = """
        <div class="_693owt3">
          <div data-testid="product-card" id="item-1"></div>
          <span class="vjlibs5">Rick Owens Jumbo Tee</span>
          <div data-testid="product-price"><del>€200</del><span>€100</span></div>
          <span data-testid="retailer-name"><span class="_1fcx6l24">SSENSE</span></span>
          <a href="/track/lead/123/"></a>
          <a href="/shoes/rick-owens-jumbo-tee/"></a>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        item = parsing.extract_shoe_data(
            soup.div,
            "PL",
            logger=self.logger,
            skipped_items=set(),
            normalize_product_link=lambda value: value,
            image_fallback_map={"https://www.lyst.com/track/lead/123/": "https://cdna.lystit.com/photos/store/item.jpg"},
        )
        self.assertEqual(item["image_url"], "https://cdna.lystit.com/photos/store/item.jpg")
        self.assertEqual(item["store"], "SSENSE")

