import tempfile
import unittest
from pathlib import Path

from second_brain_bot.ai import AIEnrichment, RelatedNoteSuggestion
from second_brain_bot.service import SecondBrainService


class FakeAI:
    def __init__(self) -> None:
        self.enrich_calls: list[str] = []
        self.ask_calls: list[tuple[str, str, bool]] = []

    async def enrich_capture(self, text: str, *, image_bytes=None, preferred_provider=None, allow_web=False):
        self.enrich_calls.append(text)
        return AIEnrichment(
            title="Knife Brand - Purchase Research Note",
            summary="A captured note about a good knife brand.",
            suggested_folder="4-Incubator",
            suggested_tags=["#knife", "#wishlist"],
            entities=["knife"],
            note_type="Purchase",
            note_status="Incubating",
            parent_moc="Purchases MOC",
            moc_category="Purchases",
            moc_description="Tracks potential purchases, buying criteria, comparisons, and follow-up decisions.",
            related_links=["Things to Buy MOC"],
            action_items=["Compare with wishlist"],
            questions=[],
            provider="fake",
        )

    async def suggest_relations(self, note_text, candidates):
        return [
            RelatedNoteSuggestion(
                note_id=candidates[0].note_id,
                title=candidates[0].title,
                reason="Both mention knives.",
                confidence=0.9,
            )
        ] if candidates else []

    async def ask(self, question: str, *, context: str, heavy: bool = True):
        self.ask_calls.append((question, context, heavy))
        return type("Result", (), {"text": "Use the collected knife notes.", "provider": "fake"})()


class SecondBrainServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_capture_indexes_note_and_creates_relation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = SecondBrainService(vault_dir=Path(tmp), ai=FakeAI())
            first = await service.capture_text("Buy knife", telegram_message_id=1, created_at="2026-05-13T09:00:00Z")
            second = await service.capture_text(
                "This knife brand is great",
                telegram_message_id=2,
                created_at="2026-05-13T10:00:00Z",
            )

            self.assertNotEqual(first.note_id, second.note_id)
            relations = service.index.relations_for(second.note_id)
            self.assertEqual(len(relations), 1)
            self.assertEqual(relations[0].target_note_id, first.note_id)
            self.assertIn("[[Knife Brand - Purchase Research Note]]", second.path.read_text(encoding="utf-8"))

    async def test_ask_uses_local_retrieval_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ai = FakeAI()
            service = SecondBrainService(vault_dir=Path(tmp), ai=ai)
            await service.capture_text("Buy knife", telegram_message_id=1)

            answer = await service.ask("What should I buy?")

            self.assertIn("knife", ai.ask_calls[0][1].lower())
            self.assertNotIn("## Raw Capture", ai.ask_calls[0][1])
            self.assertIn("Use the collected", answer)

    async def test_digest_writes_daily_note(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = SecondBrainService(vault_dir=Path(tmp), ai=FakeAI())
            await service.capture_text("Buy knife", telegram_message_id=1, created_at="2026-05-13T09:00:00Z")

            digest = await service.build_daily_digest(now_iso="2026-05-13T13:00:00Z")

            self.assertIn("Daily Second Brain Digest", digest)
            self.assertTrue((Path(tmp) / "2-Areas" / "Daily Reviews" / "2026-05-13 - Daily Second Brain Digest.md").exists())


if __name__ == "__main__":
    unittest.main()
