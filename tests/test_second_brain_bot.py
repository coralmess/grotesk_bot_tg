import unittest

from second_brain_bot.bot import (
    _format_capture_confirmation,
    _format_learning_result,
    _format_note_preview_html,
    _format_vault_results,
    _shorten_for_telegram,
    _split_for_telegram,
    build_help_text,
)
from second_brain_bot.models import NoteRecord, SearchResult


class SecondBrainBotTests(unittest.TestCase):
    def test_shorten_for_telegram_preserves_short_text(self) -> None:
        self.assertEqual(_shorten_for_telegram("hello", limit=20), "hello")

    def test_shorten_for_telegram_truncates_with_marker(self) -> None:
        text = _shorten_for_telegram("a" * 30, limit=20)
        self.assertLessEqual(len(text), 20)
        self.assertTrue(text.endswith("..."))

    def test_split_for_telegram_splits_long_text_into_numbered_parts(self) -> None:
        text = "\n\n".join(["paragraph " + str(i) + " " + ("x" * 80) for i in range(8)])

        parts = _split_for_telegram(text, limit=220)

        self.assertGreater(len(parts), 1)
        self.assertTrue(parts[0].startswith("Part 1/"))
        self.assertTrue(parts[1].startswith("Part 2/"))
        self.assertTrue(all(len(part) <= 220 for part in parts))

    def test_help_text_shows_simple_daily_commands(self) -> None:
        text = build_help_text()

        self.assertIn("/ask <question>", text)
        self.assertIn("/vault [query]", text)
        self.assertIn("/note <id>", text)
        self.assertIn("/learn <id or topic>", text)
        self.assertIn("/review", text)
        self.assertIn("/status", text)
        self.assertNotIn("/brain_ask", text)
        self.assertNotIn("/brain_accept", text)

    def test_vault_results_are_grouped_and_readable(self) -> None:
        results = [
            SearchResult(
                note_id="n1",
                title="Representativeness Heuristic",
                path="3-Resources/Psychology/Representativeness Heuristic.md",
                tags=["#psychology"],
                entities=[],
                body="",
                status="Reference",
            ),
            SearchResult(
                note_id="n2",
                title="Knife Purchase",
                path="4-Incubator/Purchases/Knife Purchase.md",
                tags=["#purchase"],
                entities=[],
                body="",
                status="Incubating",
            ),
        ]

        text = _format_vault_results(results)

        self.assertIn("Resources / Psychology", text)
        self.assertIn("Representativeness Heuristic", text)
        self.assertIn("ID: n1", text)
        self.assertIn("Incubator / Purchases", text)

    def test_capture_confirmation_uses_readable_breadcrumb_and_ai_provider(self) -> None:
        note = NoteRecord(
            note_id="20260513122336-cafc88a2",
            title="Baader Meinhof Phenomenon",
            path="/home/ubuntu/LystTgFirefox/runtime_data/second_brain_vault/3-Resources/Psychology/Baader Meinhof Phenomenon.md",
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
            "🧠 Memorized: Resources -> Psychology -> Baader Meinhof Phenomenon\n"
            "📄 ID: 20260513122336-cafc88a2 (Gemini)",
        )
        self.assertNotIn("/home/ubuntu", text)
        self.assertNotIn(".md", text)
        self.assertNotIn("Captured:", text)

    def test_learning_result_formats_flashcard_answers_as_telegram_spoilers(self) -> None:
        note = type(
            "Note",
            (),
            {
                "note_id": "learn-1",
                "path": "3-Resources/Learning/Learning - Bias.md",
                "provider": "gemini",
            },
        )()
        result = type(
            "Result",
            (),
            {
                "note": note,
                "provider": "gemini",
                "text": "Flashcards\nQ: What is bias?\nA: A systematic thinking error.",
            },
        )()

        text = _format_learning_result(result)

        self.assertIn("Q: What is bias?", text)
        self.assertIn('<span class="tg-spoiler">A systematic thinking error.</span>', text)

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
            title="Representativeness Heuristic",
            path="3-Resources/Psychology/Representativeness Heuristic.md",
            tags=[],
            entities=[],
            body=(
                "# Representativeness Heuristic\n\n"
                "Parent: [[Psychology MOC]]\n\n"
                "Related: [[Cognitive Biases MOC]], [[Decision Making]]\n\n"
                "## Executive Summary\n"
                "Request to explain a cognitive bias.\n\n"
                "## Polished Capture\n"
                "🧠 **Representativeness Heuristic**\n"
                "I want to understand what it is.\n\n"
                "## Catalog\n"
                "- Type: Concept\n"
                "- Tags: #psychology, #cognitive-biases\n"
                "- Entities: Representativeness Heuristic, Amos Tversky\n\n"
                "### Action Items\n"
                "- Study the Linda problem.\n\n"
                "### Questions\n"
                "- How is it different from availability heuristic?\n\n"
                "### Useful Context\n"
                "- It is a mental shortcut.\n\n"
                "### Scored Suggestions\n"
                "- Read Thinking Fast and Slow (Score: 95/100) - Primary source.\n"
            ),
            status="Reference",
        )

        preview = _format_note_preview_html(result)

        self.assertIn("<b>Summary</b>", preview)
        self.assertIn("Request to explain a cognitive bias.", preview)
        self.assertIn("<b>Capture</b>", preview)
        self.assertIn("🧠 Representativeness Heuristic", preview)
        self.assertIn("<b>Useful Context</b>", preview)
        self.assertIn("• It is a mental shortcut.", preview)
        self.assertIn("<b>Actions</b>", preview)
        self.assertIn("• Study the Linda problem.", preview)
        self.assertIn("<b>Questions</b>", preview)
        self.assertIn("<b>Suggestions</b>", preview)
        self.assertNotIn("**", preview)
        self.assertNotIn("Catalog", preview)
        self.assertNotIn("#psychology", preview)
        self.assertNotIn("Entities:", preview)


if __name__ == "__main__":
    unittest.main()
