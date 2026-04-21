import tempfile
import unittest
from pathlib import Path

from helpers.auto_ria.parsing import (
    build_auto_ria_caption,
    extract_vin_from_detail_html,
    normalize_auto_ria_image_url,
    parse_auto_ria_search_html,
    parse_vin_decoder_html,
)
from helpers.auto_ria.storage import AutoRiaStorage


SEARCH_HTML = """
<a class="link product-card horizontal" href="/uk/auto_audi_a3_39769491.html" id="39769491" data-car-id="39769491">
  <div class="product-card-template">
    <div class="product-card-gallery">
      <picture>
        <img src="https://cdn1.riastatic.com/photosnew/auto/photo/_a3__638617591fx.jpg" title="Седан Audi A3 2015 в Львові">
      </picture>
    </div>
    <div class="product-card-content">
      <div class="common-text size-16-20 titleS fw-bold mb-4"> Audi A3 2015</div>
      <div class="common-text size-14-16 ellipsis-1 mb-8">8V  •  2.0T S-Tronic (220 к.с.) Quattro  •  Basis</div>
      <div><span class="common-text titleM c-green">10 490 $ </span><span class="common-text body"> · 544 236 грн </span></div>
      <div class="grid-wrapper">
        <div class="structure-row ai-center gap-8 flex-1"><span class="common-text ellipsis-1 body">109 тис. км</span></div>
        <div class="structure-row ai-center gap-8 flex-1"><span class="common-text ellipsis-1 body">Робот</span></div>
        <div class="structure-row ai-center gap-8 flex-1"><span class="common-text ellipsis-1 body">Бензин, 1.98 л</span></div>
        <div class="structure-row ai-center gap-8 flex-1"><span class="common-text ellipsis-1 body">Львів (Львівська)</span></div>
      </div>
    </div>
  </div>
</a>
"""

DETAIL_HTML = """
<div id="badgesVin">
  <span>WAUB8GFF5G1020011</span>
</div>
"""

VIN_DECODER_HTML = """
<table>
  <tr><th>Trim</th><td>Premium</td></tr>
  <tr><th>Transmission</th><td>QYZ(6A)</td></tr>
</table>
"""


class AutoRiaParsingTests(unittest.TestCase):
    def test_parse_search_html_extracts_listing_fields(self) -> None:
        listings = parse_auto_ria_search_html(SEARCH_HTML)

        self.assertEqual(len(listings), 1)
        listing = listings[0]
        self.assertEqual(listing.id, "39769491")
        self.assertEqual(listing.title, "Audi A3 2015")
        self.assertEqual(listing.subtitle, "8V  •  2.0T S-Tronic (220 к.с.) Quattro  •  Basis")
        self.assertEqual(listing.price_usd, 10490)
        self.assertEqual(listing.price_text, "10 490 $")
        self.assertEqual(listing.mileage_text, "109 тис. км")
        self.assertEqual(listing.fuel_engine_text, "Бензин, 1.98 л")
        self.assertEqual(
            listing.image_url,
            "https://cdn1.riastatic.com/photosnew/auto/photo/_a3__638617591fhd.jpg",
        )
        self.assertEqual(listing.url, "https://auto.ria.com/uk/auto_audi_a3_39769491.html")

    def test_extract_vin_from_detail_html(self) -> None:
        self.assertEqual(extract_vin_from_detail_html(DETAIL_HTML), "WAUB8GFF5G1020011")

    def test_parse_vin_decoder_html(self) -> None:
        vin_details = parse_vin_decoder_html(VIN_DECODER_HTML)
        self.assertEqual(vin_details.trim, "Premium")
        self.assertEqual(vin_details.transmission, "QYZ(6A)")

    def test_build_caption_includes_optional_vin_fields(self) -> None:
        listing = parse_auto_ria_search_html(SEARCH_HTML)[0]
        caption = build_auto_ria_caption(
            listing,
            transmission="QYZ(6A)",
            trim="Premium",
        )
        self.assertEqual(
            caption,
            "<b>Audi A3 2015</b>\n"
            "8V  •  2.0T S-Tronic (220 к.с.) Quattro  •  Basis\n\n"
            "Ціна: <b>10 490 $</b>\n"
            "Пробіг: 109 тис. км\n"
            "Бензин, 1.98 л\n"
            "Коробка: QYZ(6A)\n"
            "Комплектація: Premium",
        )

    def test_build_caption_omits_vin_fields_when_unavailable(self) -> None:
        listing = parse_auto_ria_search_html(SEARCH_HTML)[0]
        caption = build_auto_ria_caption(listing, transmission=None, trim=None)
        self.assertNotIn("Коробка:", caption)
        self.assertNotIn("Комплектація:", caption)
        self.assertIn("Ціна: <b>10 490 $</b>", caption)

    def test_normalize_auto_ria_image_url_switches_to_hd_variant(self) -> None:
        self.assertEqual(
            normalize_auto_ria_image_url("https://cdn1.riastatic.com/photosnew/auto/photo/_a3__638617591fx.jpg"),
            "https://cdn1.riastatic.com/photosnew/auto/photo/_a3__638617591fhd.jpg",
        )


class AutoRiaStorageTests(unittest.TestCase):
    def test_storage_tracks_seen_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "auto_ria_items.db"
            storage = AutoRiaStorage(db_path)
            storage.create_tables()

            self.assertEqual(storage.fetch_seen_ids(["39769491"]), set())

            storage.mark_sent(
                car_id="39769491",
                title="Audi A3 2015",
                url="https://auto.ria.com/uk/auto_audi_a3_39769491.html",
                price_usd=10490,
            )

            self.assertEqual(storage.fetch_seen_ids(["39769491", "other"]), {"39769491"})


if __name__ == "__main__":
    unittest.main()
