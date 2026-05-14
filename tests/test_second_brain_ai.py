import asyncio
import tempfile
import unittest
from pathlib import Path

from helpers.analytics_events import AnalyticsSink
from second_brain_bot.ai import (
    AIEnrichment,
    AIOrchestrator,
    ModelProvider,
    OpenAICompatibleProvider,
    ProviderResult,
    clean_human_response,
    format_note_context,
)
from second_brain_bot.models import SearchResult


class FakeProvider(ModelProvider):
    def __init__(self, name: str, *, result: ProviderResult | None = None, error: Exception | None = None) -> None:
        self.name = name
        self.result = result
        self.error = error
        self.calls: list[tuple[str, str, int]] = []

    async def complete_json(self, *, task: str, prompt: str, max_tokens: int = 800) -> ProviderResult:
        self.calls.append((task, prompt, max_tokens))
        if self.error is not None:
            raise self.error
        if self.result is None:
            raise RuntimeError("missing result")
        return self.result

    async def complete_text(self, *, task: str, prompt: str, max_tokens: int = 1200) -> ProviderResult:
        return await self.complete_json(task=task, prompt=prompt, max_tokens=max_tokens)


class PlainTextOnlyProvider(FakeProvider):
    async def complete_json(self, *, task: str, prompt: str, max_tokens: int = 800) -> ProviderResult:
        raise AssertionError("ask should not use JSON completion")

    async def complete_text(self, *, task: str, prompt: str, max_tokens: int = 1200) -> ProviderResult:
        self.calls.append((task, prompt, max_tokens))
        if self.result is None:
            raise RuntimeError("missing result")
        return self.result


class SequencedProvider(FakeProvider):
    def __init__(self, name: str, results: list[ProviderResult | Exception]) -> None:
        super().__init__(name)
        self.results = list(results)

    async def complete_json(self, *, task: str, prompt: str, max_tokens: int = 800) -> ProviderResult:
        self.calls.append((task, prompt, max_tokens))
        if not self.results:
            raise RuntimeError("missing result")
        item = self.results.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class AIOrchestratorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.analytics_sink = AnalyticsSink(
            Path(self._tmp.name) / "analytics",
            now_func=lambda: "2026-05-13T00:00:00Z",
        )

    def _ai(self, *, providers, **kwargs) -> AIOrchestrator:
        return AIOrchestrator(providers=providers, analytics_sink=self.analytics_sink, **kwargs)

    async def test_heavy_tasks_prefer_gemini_when_available(self) -> None:
        gemini = FakeProvider(
            "gemini",
            result=ProviderResult(provider="gemini", model="gemini-3-flash-preview", payload={"answer": "from gemini"}),
        )
        modal = FakeProvider(
            "modal_glm",
            result=ProviderResult(provider="modal_glm", model="glm", payload={"answer": "from glm"}),
        )
        ai = self._ai(providers={"gemini": gemini, "modal_glm": modal})

        result = await ai.ask("What should I buy?", context="Buy knife", heavy=True)

        self.assertEqual(result.provider, "gemini")
        self.assertEqual(len(gemini.calls), 1)
        self.assertEqual(len(modal.calls), 0)

    async def test_heavy_tasks_fall_back_to_modal_when_gemini_missing(self) -> None:
        modal = FakeProvider(
            "modal_glm",
            result=ProviderResult(provider="modal_glm", model="glm", payload={"answer": "from glm"}),
        )
        groq = FakeProvider(
            "groq",
            result=ProviderResult(provider="groq", model="qwen", payload={"answer": "from groq"}),
        )
        ai = self._ai(providers={"modal_glm": modal, "groq": groq})

        result = await ai.ask("What should I buy?", context="Buy knife", heavy=True)

        self.assertEqual(result.provider, "modal_glm")
        self.assertEqual(len(modal.calls), 1)
        self.assertEqual(len(groq.calls), 0)

    async def test_default_enrichment_prefers_gemini_when_available(self) -> None:
        gemini = FakeProvider(
            "gemini",
            result=ProviderResult(provider="gemini", model="gemini-3-flash-preview", payload={"title": "Gemini note"}),
        )
        cerebras = FakeProvider(
            "cerebras",
            result=ProviderResult(provider="cerebras", model="qwen", payload={"title": "Cerebras note"}),
        )
        ai = self._ai(providers={"gemini": gemini, "cerebras": cerebras})

        enrichment = await ai.enrich_capture("I want COPX stocks")

        self.assertEqual(enrichment.provider, "gemini")
        self.assertEqual(enrichment.title, "Gemini note")
        self.assertEqual(len(cerebras.calls), 0)

    async def test_relation_judging_prefers_gemini_when_available(self) -> None:
        gemini = FakeProvider(
            "gemini",
            result=ProviderResult(
                provider="gemini",
                model="gemini-3-flash-preview",
                payload={"related_notes": [{"note_id": "n1", "reason": "same topic", "confidence": 0.8}]},
            ),
        )
        modal = FakeProvider(
            "modal_glm",
            result=ProviderResult(provider="modal_glm", model="glm", payload={"related_notes": []}),
        )
        candidate = SearchResult(
            note_id="n1",
            title="COPX - ETF Note",
            path="4-Incubator/Investments/COPX - ETF Note.md",
            tags=["#investment"],
            entities=["COPX"],
            body="Copper miners ETF",
            status="Incubating",
        )
        ai = self._ai(providers={"gemini": gemini, "modal_glm": modal})

        relations = await ai.suggest_relations("I want COPX stocks", [candidate])

        self.assertEqual(relations[0].note_id, "n1")
        self.assertEqual(len(gemini.calls), 1)
        self.assertEqual(len(modal.calls), 0)

    async def test_combined_enrichment_and_relations_uses_one_provider_request(self) -> None:
        gemini = FakeProvider(
            "gemini",
            result=ProviderResult(
                provider="gemini",
                model="gemini-3-flash-preview",
                payload={
                    "title": "Knife Brand - Purchase Research Note",
                    "summary": "A captured note about a good knife brand.",
                    "related_notes": [{"note_id": "n1", "reason": "same purchase topic", "confidence": 0.9}],
                },
            ),
        )
        candidate = SearchResult(
            note_id="n1",
            title="Knife Wishlist",
            path="4-Incubator/Purchases/Knife Wishlist.md",
            tags=["#wishlist"],
            entities=["knife"],
            body="Buy knife",
            status="Incubating",
        )
        ai = self._ai(providers={"gemini": gemini})

        enrichment, relations = await ai.enrich_capture_with_relations("This knife brand is great", [candidate])

        self.assertEqual(enrichment.provider, "gemini")
        self.assertEqual(relations[0].note_id, "n1")
        self.assertEqual(len(gemini.calls), 1)
        self.assertEqual(gemini.calls[0][0], "enrich")
        self.assertIn("Candidates", gemini.calls[0][1])

    async def test_gemini_json_tasks_retry_before_fallback(self) -> None:
        gemini = SequencedProvider(
            "gemini",
            [
                RuntimeError("temporary 429"),
                RuntimeError("temporary 429"),
                ProviderResult(
                    provider="gemini",
                    model="gemini-3-flash-preview",
                    payload={"title": "Retried Gemini note"},
                ),
            ],
        )
        modal = FakeProvider(
            "modal_glm",
            result=ProviderResult(provider="modal_glm", model="glm", payload={"title": "Fallback note"}),
        )
        ai = self._ai(providers={"gemini": gemini, "modal_glm": modal})

        enrichment = await ai.enrich_capture("retry this")

        self.assertEqual(enrichment.provider, "gemini")
        self.assertEqual(enrichment.title, "Retried Gemini note")
        self.assertEqual(len(gemini.calls), 3)
        self.assertEqual(len(modal.calls), 0)

    async def test_falls_back_when_preferred_provider_fails(self) -> None:
        cerebras = FakeProvider("cerebras", error=RuntimeError("rate limited"))
        modal = FakeProvider(
            "modal_glm",
            result=ProviderResult(
                provider="modal_glm",
                model="glm",
                payload={
                    "title": "Knife brand",
                    "summary": "A note about a knife brand.",
                    "suggested_folder": "03_Resources",
                    "suggested_tags": ["knife"],
                    "entities": ["knife"],
                    "action_items": [],
                    "questions": [],
                },
            ),
        )
        ai = self._ai(providers={"cerebras": cerebras, "modal_glm": modal})

        enrichment = await ai.enrich_capture("A great knife brand", preferred_provider="cerebras")

        self.assertIsInstance(enrichment, AIEnrichment)
        self.assertEqual(enrichment.provider, "modal_glm")
        self.assertEqual(enrichment.title, "Knife brand")

    async def test_failed_provider_is_temporarily_cooled_down(self) -> None:
        failing = FakeProvider("gemini", error=RuntimeError("temporary outage"))
        fallback = FakeProvider(
            "modal_glm",
            result=ProviderResult(provider="modal_glm", model="glm", payload={"answer": "fallback answer"}),
        )
        ai = self._ai(
            providers={"gemini": failing, "modal_glm": fallback},
            provider_cooldown_after=1,
            provider_cooldown_sec=300,
        )

        first = await ai.ask("What should I do?", context="Buy knife", heavy=True)
        second = await ai.ask("What should I do next?", context="Buy scarf", heavy=True)

        self.assertEqual(first.provider, "modal_glm")
        self.assertEqual(second.provider, "modal_glm")
        self.assertEqual(len(failing.calls), 3)
        self.assertEqual(len(fallback.calls), 2)

    async def test_non_ai_fallback_preserves_text_when_all_providers_fail(self) -> None:
        ai = self._ai(providers={"modal_glm": FakeProvider("modal_glm", error=RuntimeError("down"))})

        enrichment = await ai.enrich_capture("Remember to buy a knife")

        self.assertEqual(enrichment.title, "Remember to buy a knife")
        self.assertEqual(enrichment.provider, "local_fallback")
        self.assertIn("#buy", enrichment.suggested_tags)
        self.assertEqual(enrichment.suggested_folder, "5-Todo List")
        self.assertEqual(enrichment.parent_moc, "Purchase Tasks MOC")
        self.assertEqual(enrichment.note_status, "Active")

    async def test_image_bytes_are_not_sent_to_ai_prompt(self) -> None:
        modal = FakeProvider(
            "modal_glm",
            result=ProviderResult(provider="modal_glm", model="glm", payload={"title": "Photo note"}),
        )
        ai = self._ai(providers={"modal_glm": modal})

        await ai.enrich_capture("caption only", image_bytes=b"private-image-bytes")

        self.assertEqual(len(modal.calls), 1)
        self.assertNotIn("private-image-bytes", modal.calls[0][1])

    async def test_enrichment_prompt_prefers_useful_context_without_noisy_guesses(self) -> None:
        modal = FakeProvider(
            "modal_glm",
            result=ProviderResult(provider="modal_glm", model="glm", payload={"title": "COPX stock"}),
        )
        ai = self._ai(providers={"modal_glm": modal})

        await ai.enrich_capture("I want COPX stocks", preferred_provider="modal_glm")

        prompt = modal.calls[0][1]
        self.assertIn("recognizable ticker", prompt)
        self.assertIn("Do not add noisy guesses", prompt)
        self.assertIn("polished_text", prompt)
        self.assertIn("split inline numbered steps", prompt)
        self.assertIn("score from 1 to 100", prompt)
        self.assertIn("Catalog metadata must be English-only", prompt)
        self.assertIn("Keep the user's visible note content in the user's language", prompt)
        self.assertIn("5-Todo List", prompt)
        self.assertIn("estimated_completion_time", prompt)

    async def test_enrichment_parses_useful_context_and_scored_suggestions(self) -> None:
        modal = FakeProvider(
            "modal_glm",
            result=ProviderResult(
                provider="modal_glm",
                model="glm",
                payload={
                    "title": "Improve mental resilience",
                    "summary": "Goal note.",
                    "polished_text": "I want to improve my mental resilience.",
                    "enrichment_notes": ["Cognitive defusion means noticing thoughts as thoughts."],
                    "scored_suggestions": [
                        {
                            "title": "Spotting Cognitive Distortions",
                            "score": 95,
                            "reason": "Useful CBT skill.",
                        }
                    ],
                },
            ),
        )
        ai = self._ai(providers={"modal_glm": modal})

        enrichment = await ai.enrich_capture("I care too much about opinions", preferred_provider="modal_glm")

        self.assertEqual(enrichment.enrichment_notes, ["Cognitive defusion means noticing thoughts as thoughts."])
        self.assertEqual(enrichment.polished_text, "I want to improve my mental resilience.")
        self.assertEqual(enrichment.scored_suggestions[0]["score"], 95)

    async def test_local_enrichment_routes_clear_todo_capture_to_todo_list(self) -> None:
        ai = self._ai(providers={})

        enrichment = await ai.enrich_capture("Треба пошукати нормальну бутилку для води")

        self.assertEqual(enrichment.suggested_folder, "5-Todo List")
        self.assertEqual(enrichment.note_type, "Plan")
        self.assertEqual(enrichment.note_status, "Active")
        self.assertEqual(enrichment.parent_moc, "Purchase Tasks MOC")
        self.assertTrue(enrichment.estimated_completion_time)

    async def test_local_enrichment_formats_inline_steps_without_adding_knowledge(self) -> None:
        ai = self._ai(providers={})

        enrichment = await ai.enrich_capture("Steps: 1) Open account 2) Compare fees 3) Save notes")

        self.assertIn("1. Open account", enrichment.polished_text)
        self.assertIn("\n2. Compare fees", enrichment.polished_text)
        self.assertNotIn("AI Suggestions", enrichment.polished_text)

    async def test_ask_returns_readable_answer_not_thinking_or_json(self) -> None:
        modal = FakeProvider(
            "modal_glm",
            result=ProviderResult(
                provider="modal_glm",
                model="glm",
                payload={
                    "answer": "<think>private chain</think>{\"action_items\":[{\"task\":\"Buy knife\",\"source\":\"Wishlist\"}]}"
                },
            ),
        )
        ai = self._ai(providers={"modal_glm": modal})

        result = await ai.ask("what do I need to do?", context="Buy knife", heavy=True)

        self.assertNotIn("<think>", result.text)
        self.assertNotIn("action_items", result.text)
        self.assertIn("Buy knife", result.text)
        self.assertIn("Wishlist", result.text)

    async def test_ask_prompt_requires_grounded_answer_sections(self) -> None:
        gemini = PlainTextOnlyProvider(
            "gemini",
            result=ProviderResult(provider="gemini", model="gemini", payload={}, text="Answer"),
        )
        ai = self._ai(providers={"gemini": gemini})

        await ai.ask("What should I do?", context="Title: Buy knife\nPath: note.md\nExcerpt: Buy knife")

        prompt = gemini.calls[0][1]
        self.assertIn("Answer", prompt)
        self.assertIn("Evidence from notes", prompt)
        self.assertIn("Assumptions", prompt)
        self.assertIn("Confidence", prompt)
        self.assertIn("Next actions", prompt)

    async def test_ask_prompt_does_not_duplicate_global_system_prompt(self) -> None:
        gemini = PlainTextOnlyProvider(
            "gemini",
            result=ProviderResult(provider="gemini", model="gemini", payload={}, text="Answer"),
        )
        ai = self._ai(providers={"gemini": gemini})

        await ai.ask("What is in my vault?", context="Title: Test\nPath: test.md\nExcerpt: saved fact")

        prompt = gemini.calls[0][1]
        self.assertNotIn("You are Grotesk Brain", prompt)
        self.assertIn("Evidence from notes", prompt)

    async def test_provider_uses_task_specific_system_instructions(self) -> None:
        provider = OpenAICompatibleProvider(
            name="test",
            api_key="key",
            base_url="https://example.test/v1",
            model="model",
        )

        self.assertIn("answering questions", provider._system_instructions_for_task("ask"))
        self.assertIn("creating learning sessions", provider._system_instructions_for_task("learn"))
        self.assertIn("cataloging captured notes", provider._system_instructions_for_task("enrich"))

    async def test_ask_accepts_plain_text_provider_response(self) -> None:
        gemini = PlainTextOnlyProvider(
            "gemini",
            result=ProviderResult(
                provider="gemini",
                model="gemini-3-flash-preview",
                payload={},
                text="🧠 У vault є нотатка про репрезентативну евристику.",
            ),
        )
        ai = self._ai(providers={"gemini": gemini})

        result = await ai.ask("Що в мене є про Репрезентативна евристика?", context="Title: Репрезентативна евристика")

        self.assertEqual(result.provider, "gemini")
        self.assertIn("репрезентативну евристику", result.text.lower())

    async def test_ask_local_fallback_does_not_dump_raw_markdown(self) -> None:
        ai = self._ai(providers={"modal_glm": FakeProvider("modal_glm", error=RuntimeError("down"))})
        context = (
            "Title: Money strategy\n"
            "Path: 00_Inbox/money.md\n"
            "Tags: inbox\n"
            "Excerpt: ## Raw Capture\n"
            "Amazon FBA strategy. 1) Open LLC 2) Research niche 3) Test ads. "
            + "Extra context " * 80
        )

        result = await ai.ask("ways to earn money?", context=context, heavy=True)

        self.assertIn("Money strategy", result.text)
        self.assertIn("00_Inbox/money.md", result.text)
        self.assertNotIn("## Raw Capture", result.text)
        self.assertNotIn("Extra context " * 20, result.text)

    async def test_ask_local_fallback_prefers_executive_summary(self) -> None:
        ai = self._ai(providers={"gemini": FakeProvider("gemini", error=RuntimeError("down"))})
        context = (
            "Title: Репрезентативна евристика\n"
            "Path: 3-Resources/Psychology/Репрезентативна евристика.md\n"
            "Tags: #psychology\n"
            "Excerpt: Репрезентативна евристика Parent: Psychology MOC Related: Cognitive Biases MOC "
            "Executive Summary Запит на пояснення когнітивного упередження. "
            "Source Capture Репрезентативна евристика"
        )

        result = await ai.ask("Що в мене є?", context=context, heavy=True)

        self.assertIn("Репрезентативна евристика", result.text)
        self.assertIn("Запит на пояснення когнітивного упередження", result.text)
        self.assertNotIn("Parent:", result.text)
        self.assertNotIn("Related:", result.text)

    def test_format_note_context_strips_obsidian_headings(self) -> None:
        item = SearchResult(
            note_id="n1",
            title="Money strategy",
            path="00_Inbox/money.md",
            tags=["inbox"],
            entities=[],
            body="# Money strategy\n\n## Raw Capture\nAmazon FBA strategy.",
            status="inbox",
        )

        context = format_note_context(item)

        self.assertIn("Money strategy", context)
        self.assertNotIn("## Raw Capture", context)

    def test_clean_human_response_removes_thinking_and_formats_json_tasks(self) -> None:
        raw = '<think>hidden</think>{"action_items":[{"task":"Buy scarf","source":"Wishlist"}]}'

        cleaned = clean_human_response(raw)

        self.assertNotIn("<think>", cleaned)
        self.assertIn("🧭 Things to do", cleaned)
        self.assertIn("Buy scarf", cleaned)

    def test_clean_human_response_formats_scored_suggestions(self) -> None:
        raw = '{"suggestions":[{"title":"Cognitive Defusion","score":92,"reason":"Helps detach from thoughts."}]}'

        cleaned = clean_human_response(raw)

        self.assertIn("Suggested options", cleaned)
        self.assertIn("Cognitive Defusion (Score: 92/100)", cleaned)

    def test_clean_human_response_removes_raw_markdown_for_telegram(self) -> None:
        raw = "### 🧠 Основна інформація\n*   **Визначення:** текст\n1. **Крок:** зробити"

        cleaned = clean_human_response(raw)

        self.assertIn("🧠 Основна інформація", cleaned)
        self.assertIn("- Визначення: текст", cleaned)
        self.assertIn("1. Крок: зробити", cleaned)
        self.assertNotIn("###", cleaned)
        self.assertNotIn("**", cleaned)


if __name__ == "__main__":
    unittest.main()
