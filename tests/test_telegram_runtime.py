import unittest

from helpers import telegram_runtime


class TelegramRuntimeTests(unittest.TestCase):
    def test_unique_bot_tokens_filters_empty_and_duplicate_values(self) -> None:
        self.assertEqual(
            telegram_runtime.unique_bot_tokens(
                " token-a ",
                "",
                None,
                "token-b",
                "'token-a'",
                '"token-b"',
            ),
            ["token-a", "token-b"],
        )


if __name__ == "__main__":
    unittest.main()
