import unittest

from helpers.lyst.diagnostics import (
    page_progress_data,
    safe_page_final_url,
    url_suffix,
)


class _Page:
    def __init__(self, url: str) -> None:
        self.url = url


class _BrokenPage:
    @property
    def url(self) -> str:
        raise RuntimeError("closed")


class LystDiagnosticsTests(unittest.TestCase):
    def test_url_suffix_formats_optional_page_context(self) -> None:
        self.assertEqual(url_suffix(url_name="sale", page_num=3), " | url_name=sale page=3")
        self.assertEqual(url_suffix(), "")

    def test_page_progress_data_includes_attempt_only_when_present(self) -> None:
        data = page_progress_data("https://example.com", "PL", "sale", 2, attempt=4)
        self.assertEqual(data["attempt"], 4)
        self.assertEqual(data["country"], "PL")
        self.assertNotIn("attempt", page_progress_data("https://example.com", "PL"))

    def test_safe_page_final_url_returns_none_for_closed_page(self) -> None:
        self.assertEqual(safe_page_final_url(_Page("https://example.com")), "https://example.com")
        self.assertIsNone(safe_page_final_url(_BrokenPage()))


if __name__ == "__main__":
    unittest.main()
