import unittest

from second_brain_bot.bot import _format_capture_confirmation, _format_note_preview_html, _shorten_for_telegram, build_help_text
from second_brain_bot.models import NoteRecord, SearchResult


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

    def test_capture_confirmation_uses_readable_breadcrumb_and_ai_provider(self) -> None:
        note = NoteRecord(
            note_id="20260513122336-cafc88a2",
            title="Феномен Баадера — Майнхоф",
            path="/home/ubuntu/LystTgFirefox/runtime_data/second_brain_vault/3-Resources/Psychology/Феномен Баадера — Майнхоф.md",
            tags=[],
            entities=[],
            body="",
            status="Reference",
            created_at="2026-05-13T12:23:36Z",
            updated_at="2026-05-13T12:23:36Z",
        )
        note = type("CapturedNote", (), {**note.__dict__, "provider": "gemini"})()

        text = _format_capture_confirmation(note)

        self.assertEqual(
            text,
            "🧠 Memorized: Resources -> Psychology -> Феномен Баадера — Майнхоф\n"
            "📄 ID: 20260513122336-cafc88a2 (Gemini)",
        )
        self.assertNotIn("/home/ubuntu", text)
        self.assertNotIn(".md", text)
        self.assertNotIn("Captured:", text)

    def test_note_preview_uses_telegram_html_not_raw_markdown_headings(self) -> None:
        result = SearchResult(
            note_id="n1",
            title="Money <strategy>",
            path="00_Inbox/money.md",
            tags=[],
            entities=[],
            body="## Raw Capture\nAmazon FBA strategy.",
            status="inbox",
        )

        preview = _format_note_preview_html(result)

        self.assertIn("<b>Money &lt;strategy&gt;</b>", preview)
        self.assertIn("<u>00_Inbox/money.md</u>", preview)
        self.assertIn("<b>Capture</b>", preview)
        self.assertNotIn("## Raw Capture", preview)

    def test_note_preview_formats_structured_note_for_telegram(self) -> None:
        result = SearchResult(
            note_id="n1",
            title="Репрезентативна евристика",
            path="3-Resources/Psychology/Репрезентативна евристика.md",
            tags=[],
            entities=[],
            body=(
                "# Репрезентативна евристика\n\n"
                "Parent: [[Psychology MOC]]\n\n"
                "Related: [[Cognitive Biases MOC]], [[Decision Making]]\n\n"
                "## Executive Summary\n"
                "Запит на пояснення когнітивного упередження.\n\n"
                "## Polished Capture\n"
                "🧠 **Репрезентативна евристика**\n"
                "Хотів би дізнатись, що це таке.\n\n"
                "## Source Capture\n"
                "Репрезентативная эвристика - хотів би дізнатись що це\n\n"
                "## Catalog\n"
                "- Type: Concept\n"
                "- Tags: #психологія, #когнітивні_упередження\n"
                "- Entities: Репрезентативна евристика, Амос Тверські\n\n"
                "### Action Items\n"
                "- Вивчити основні приклади.\n\n"
                "### Questions\n"
                "- Чим вона відрізняється від евристики доступності?\n\n"
                "### Useful Context\n"
                "- Репрезентативна евристика — це ментальне скорочення.\n\n"
                "### Scored Suggestions\n"
                "- Прочитати Thinking Fast and Slow (Score: 95/100) - Першоджерело.\n"
            ),
            status="Reference",
        )

        preview = _format_note_preview_html(result)

        self.assertIn("<b>Summary</b>", preview)
        self.assertIn("Запит на пояснення когнітивного упередження.", preview)
        self.assertIn("<b>Capture</b>", preview)
        self.assertIn("🧠 Репрезентативна евристика", preview)
        self.assertIn("<b>Useful Context</b>", preview)
        self.assertIn("• Репрезентативна евристика", preview)
        self.assertIn("<b>Actions</b>", preview)
        self.assertIn("• Вивчити основні приклади.", preview)
        self.assertIn("<b>Questions</b>", preview)
        self.assertIn("<b>Suggestions</b>", preview)
        self.assertNotIn("**", preview)
        self.assertNotIn("Catalog", preview)
        self.assertNotIn("#психологія", preview)
        self.assertNotIn("Entities:", preview)


if __name__ == "__main__":
    unittest.main()
