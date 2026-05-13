import unittest

from second_brain_bot.bot import _shorten_for_telegram, build_help_text


class SecondBrainBotTests(unittest.TestCase):
    def test_shorten_for_telegram_preserves_short_text(self) -> None:
        self.assertEqual(_shorten_for_telegram("hello", limit=20), "hello")

    def test_shorten_for_telegram_truncates_with_marker(self) -> None:
        text = _shorten_for_telegram("a" * 30, limit=20)
        self.assertLessEqual(len(text), 20)
        self.assertTrue(text.endswith("..."))

    def test_help_text_explains_commands(self) -> None:
        text = build_help_text()

        self.assertIn("/brain_ask <question> - ask something based on your saved notes.", text)
        self.assertIn("/brain_accept <id> - accept AI title/tags/folder for a note.", text)
        self.assertIn("🧠Thinking🧠", text)


if __name__ == "__main__":
    unittest.main()
