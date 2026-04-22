import unittest

from helpers.lyst.fetch import (
    LystHttpTerminalPage,
    http_base_url,
    http_content_has_product_cards,
    is_cloudflare_challenge,
    is_pipe_closed_error,
    is_target_closed_error,
)


class TargetClosedError(Exception):
    pass


class LystFetchTests(unittest.TestCase):
    def test_is_cloudflare_challenge_detects_common_markers(self) -> None:
        self.assertTrue(is_cloudflare_challenge("<html>cf-browser-verification Just a moment...</html>"))
        self.assertFalse(is_cloudflare_challenge("<html>normal page</html>"))

    def test_http_base_url_strips_query_and_fragment(self) -> None:
        self.assertEqual(
            http_base_url("https://www.lyst.com/shop?foo=1#bar"),
            "https://www.lyst.com/shop",
        )

    def test_http_content_detects_product_cards(self) -> None:
        self.assertTrue(http_content_has_product_cards('<div class="_693owt3"></div>'))
        self.assertFalse(http_content_has_product_cards("<div>empty</div>"))

    def test_pipe_and_target_closed_detection_match_runtime_errors(self) -> None:
        self.assertTrue(is_pipe_closed_error(RuntimeError("pipe closed while writing")))
        self.assertFalse(is_pipe_closed_error(RuntimeError("other")))

        self.assertTrue(is_target_closed_error(TargetClosedError("boom")))
        self.assertTrue(is_target_closed_error(RuntimeError("Target page, context or browser has been closed")))
        self.assertFalse(is_target_closed_error(RuntimeError("other")))

    def test_terminal_page_exception_keeps_content(self) -> None:
        exc = LystHttpTerminalPage("<html></html>")
        self.assertEqual(exc.content, "<html></html>")


if __name__ == "__main__":
    unittest.main()
