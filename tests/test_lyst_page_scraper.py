import unittest
from unittest import mock

from bs4 import BeautifulSoup

from helpers.lyst import page_scraper


class CloudflareChallenge(Exception):
    pass


class RunAborted(Exception):
    pass


class TerminalPage(Exception):
    def __init__(self, content):
        self.content = content
        super().__init__("terminal")


class LystPageScraperTests(unittest.IsolatedAsyncioTestCase):
    async def test_scrape_page_returns_ok_shoes_and_content(self):
        content = '<html><body><div class="_693owt3">shoe</div></body></html>'
        soup = BeautifulSoup(content, "lxml")
        fallback_map = {"shoe": "https://img.example/shoe.jpg"}
        parsed_shoe = {"name": "Parsed Shoe"}
        parser_calls = []

        async def get_soup_and_content(*args, **kwargs):
            return soup, content

        def extract_ldjson_image_map(received_soup):
            self.assertIs(received_soup, soup)
            return fallback_map

        def extract_shoe_data(card, country, image_fallback_map):
            parser_calls.append((card, country, image_fallback_map))
            return parsed_shoe

        shoes, returned_content, status = await page_scraper.scrape_page(
            "https://www.lyst.com/shop",
            "US",
            get_soup_and_content=get_soup_and_content,
            extract_ldjson_image_map=extract_ldjson_image_map,
            extract_shoe_data=extract_shoe_data,
            mark_issue=mock.Mock(),
            cloudflare_exception=CloudflareChallenge,
            aborted_exception=RunAborted,
            terminal_exception=TerminalPage,
            max_scroll_attempts=4,
            url_name="Test URL",
            page_num=2,
            use_pagination=True,
        )

        self.assertEqual(shoes, [parsed_shoe])
        self.assertEqual(returned_content, content)
        self.assertEqual(status, "ok")
        self.assertEqual(len(parser_calls), 1)
        self.assertEqual(parser_calls[0][1], "US")
        self.assertIs(parser_calls[0][2], fallback_map)

    async def test_scrape_page_maps_fetch_exceptions_to_runtime_statuses(self):
        async def cloudflare_fetch(*args, **kwargs):
            raise CloudflareChallenge()

        self.assertEqual(
            await page_scraper.scrape_page(
                "https://www.lyst.com/shop",
                "US",
                get_soup_and_content=cloudflare_fetch,
                extract_ldjson_image_map=mock.Mock(),
                extract_shoe_data=mock.Mock(),
                mark_issue=mock.Mock(),
                cloudflare_exception=CloudflareChallenge,
                aborted_exception=RunAborted,
                terminal_exception=TerminalPage,
            ),
            ([], None, "cloudflare"),
        )

        async def aborted_fetch(*args, **kwargs):
            raise RunAborted()

        self.assertEqual(
            await page_scraper.scrape_page(
                "https://www.lyst.com/shop",
                "US",
                get_soup_and_content=aborted_fetch,
                extract_ldjson_image_map=mock.Mock(),
                extract_shoe_data=mock.Mock(),
                mark_issue=mock.Mock(),
                cloudflare_exception=CloudflareChallenge,
                aborted_exception=RunAborted,
                terminal_exception=TerminalPage,
            ),
            ([], None, "aborted"),
        )

        async def terminal_fetch(*args, **kwargs):
            raise TerminalPage("terminal content")

        self.assertEqual(
            await page_scraper.scrape_page(
                "https://www.lyst.com/shop",
                "US",
                get_soup_and_content=terminal_fetch,
                extract_ldjson_image_map=mock.Mock(),
                extract_shoe_data=mock.Mock(),
                mark_issue=mock.Mock(),
                cloudflare_exception=CloudflareChallenge,
                aborted_exception=RunAborted,
                terminal_exception=TerminalPage,
            ),
            ([], "terminal content", "terminal"),
        )

    async def test_scrape_page_marks_failed_when_soup_missing(self):
        async def get_soup_and_content(*args, **kwargs):
            return None, "broken content"

        mark_issue = mock.Mock()
        result = await page_scraper.scrape_page(
            "https://www.lyst.com/shop",
            "US",
            get_soup_and_content=get_soup_and_content,
            extract_ldjson_image_map=mock.Mock(),
            extract_shoe_data=mock.Mock(),
            mark_issue=mark_issue,
            cloudflare_exception=CloudflareChallenge,
            aborted_exception=RunAborted,
            terminal_exception=TerminalPage,
        )

        self.assertEqual(result, ([], "broken content", "failed"))
        mark_issue.assert_called_once_with("Failed to get soup")

    async def test_scrape_page_keeps_empty_product_page_as_ok(self):
        content = "<html><body>No product cards here</body></html>"
        soup = BeautifulSoup(content, "lxml")

        async def get_soup_and_content(*args, **kwargs):
            return soup, content

        result = await page_scraper.scrape_page(
            "https://www.lyst.com/shop",
            "US",
            get_soup_and_content=get_soup_and_content,
            extract_ldjson_image_map=mock.Mock(return_value={}),
            extract_shoe_data=mock.Mock(),
            mark_issue=mock.Mock(),
            cloudflare_exception=CloudflareChallenge,
            aborted_exception=RunAborted,
            terminal_exception=TerminalPage,
        )

        self.assertEqual(result, ([], content, "ok"))


if __name__ == "__main__":
    unittest.main()
