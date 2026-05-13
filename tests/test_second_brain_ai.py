import asyncio
import unittest

from second_brain_bot.ai import AIEnrichment, AIOrchestrator, ModelProvider, ProviderResult


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
    async def test_heavy_tasks_prefer_modal_glm(self) -> None:
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
        self.assertIn("inbox", enrichment.suggested_tags)

    async def test_image_bytes_are_not_sent_to_ai_prompt(self) -> None:
        modal = FakeProvider(
            "modal_glm",
            result=ProviderResult(provider="modal_glm", model="glm", payload={"title": "Photo note"}),
        )
        ai = AIOrchestrator(providers={"modal_glm": modal})

        await ai.enrich_capture("caption only", image_bytes=b"private-image-bytes")

        self.assertEqual(len(modal.calls), 1)
        self.assertNotIn("private-image-bytes", modal.calls[0][1])


if __name__ == "__main__":
    unittest.main()
