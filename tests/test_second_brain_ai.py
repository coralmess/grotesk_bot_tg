import asyncio
import unittest

from second_brain_bot.ai import (
    AIEnrichment,
    AIOrchestrator,
    ModelProvider,
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


class AIOrchestratorTests(unittest.IsolatedAsyncioTestCase):
    async def test_heavy_tasks_prefer_gemini_when_available(self) -> None:
        gemini = FakeProvider(
            "gemini",
            result=ProviderResult(provider="gemini", model="gemini-3-flash-preview", payload={"answer": "from gemini"}),
        )
        modal = FakeProvider(
            "modal_glm",
            result=ProviderResult(provider="modal_glm", model="glm", payload={"answer": "from glm"}),
        )
        ai = AIOrchestrator(providers={"gemini": gemini, "modal_glm": modal})

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
        ai = AIOrchestrator(providers={"modal_glm": modal, "groq": groq})

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
        ai = AIOrchestrator(providers={"gemini": gemini, "cerebras": cerebras})

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
        ai = AIOrchestrator(providers={"gemini": gemini, "modal_glm": modal})

        relations = await ai.suggest_relations("I want COPX stocks", [candidate])

        self.assertEqual(relations[0].note_id, "n1")
        self.assertEqual(len(gemini.calls), 1)
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
        ai = AIOrchestrator(providers={"cerebras": cerebras, "modal_glm": modal})

        enrichment = await ai.enrich_capture("A great knife brand", preferred_provider="cerebras")

        self.assertIsInstance(enrichment, AIEnrichment)
        self.assertEqual(enrichment.provider, "modal_glm")
        self.assertEqual(enrichment.title, "Knife brand")

    async def test_non_ai_fallback_preserves_text_when_all_providers_fail(self) -> None:
        ai = AIOrchestrator(providers={"modal_glm": FakeProvider("modal_glm", error=RuntimeError("down"))})

        enrichment = await ai.enrich_capture("Remember to buy a knife")

        self.assertEqual(enrichment.title, "Remember to buy a knife")
        self.assertEqual(enrichment.provider, "local_fallback")
        self.assertIn("#buy", enrichment.suggested_tags)
        self.assertEqual(enrichment.parent_moc, "Purchases MOC")

    async def test_image_bytes_are_not_sent_to_ai_prompt(self) -> None:
        modal = FakeProvider(
            "modal_glm",
            result=ProviderResult(provider="modal_glm", model="glm", payload={"title": "Photo note"}),
        )
        ai = AIOrchestrator(providers={"modal_glm": modal})

        await ai.enrich_capture("caption only", image_bytes=b"private-image-bytes")

        self.assertEqual(len(modal.calls), 1)
        self.assertNotIn("private-image-bytes", modal.calls[0][1])

    async def test_enrichment_prompt_prefers_useful_context_without_noisy_guesses(self) -> None:
        modal = FakeProvider(
            "modal_glm",
            result=ProviderResult(provider="modal_glm", model="glm", payload={"title": "COPX stock"}),
        )
        ai = AIOrchestrator(providers={"modal_glm": modal})

        await ai.enrich_capture("I want COPX stocks", preferred_provider="modal_glm")

        prompt = modal.calls[0][1]
        self.assertIn("recognizable ticker", prompt)
        self.assertIn("Do not add noisy guesses", prompt)
        self.assertIn("polished_text", prompt)
        self.assertIn("split inline numbered steps", prompt)
        self.assertIn("score from 1 to 100", prompt)

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
        ai = AIOrchestrator(providers={"modal_glm": modal})

        enrichment = await ai.enrich_capture("I care too much about opinions", preferred_provider="modal_glm")

        self.assertEqual(enrichment.enrichment_notes, ["Cognitive defusion means noticing thoughts as thoughts."])
        self.assertEqual(enrichment.polished_text, "I want to improve my mental resilience.")
        self.assertEqual(enrichment.scored_suggestions[0]["score"], 95)

    async def test_local_enrichment_formats_inline_steps_without_adding_knowledge(self) -> None:
        ai = AIOrchestrator(providers={})

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
        ai = AIOrchestrator(providers={"modal_glm": modal})

        result = await ai.ask("what do I need to do?", context="Buy knife", heavy=True)

        self.assertNotIn("<think>", result.text)
        self.assertNotIn("action_items", result.text)
        self.assertIn("Buy knife", result.text)
        self.assertIn("Wishlist", result.text)

    async def test_ask_local_fallback_does_not_dump_raw_markdown(self) -> None:
        ai = AIOrchestrator(providers={"modal_glm": FakeProvider("modal_glm", error=RuntimeError("down"))})
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


if __name__ == "__main__":
    unittest.main()
