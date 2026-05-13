import tempfile
import unittest
from pathlib import Path

from second_brain_bot.ai import AIEnrichment, RelatedNoteSuggestion
from second_brain_bot.models import NoteRecord, RelationRecord
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
        if "learning session" in question.lower():
            return type("Result", (), {"text": "Lesson body with examples, answers, and scored next steps.", "provider": "fake"})()
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

    async def test_ask_uses_related_notes_from_deep_retrieval(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ai = FakeAI()
            service = SecondBrainService(vault_dir=Path(tmp), ai=ai)
            source = await service.capture_text(
                "Representativeness heuristic",
                telegram_message_id=1,
                created_at="2026-05-13T09:00:00Z",
            )
            related = await service.capture_text(
                "Thinking Fast and Slow explains cognitive bias examples.",
                telegram_message_id=2,
                created_at="2026-05-13T10:00:00Z",
            )
            service.index.upsert_relation(
                RelationRecord(
                    source_note_id=source.note_id,
                    target_note_id=related.note_id,
                    target_title=related.title,
                    reason="Useful source for learning.",
                    confidence=0.9,
                )
            )

            await service.ask("Representativeness")

            context = ai.ask_calls[-1][1]
            self.assertIn(source.title, context)
            self.assertIn(related.title, context)

    async def test_capture_stores_action_items_for_task_questions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ai = FakeAI()
            service = SecondBrainService(vault_dir=Path(tmp), ai=ai)
            await service.capture_text("Buy knife", telegram_message_id=1)

            actions = service.index.search_actions("what should I do?", limit=5)

            self.assertEqual(len(actions), 1)
            self.assertIn("Compare with wishlist", actions[0].action_text)

    async def test_ask_includes_open_actions_for_task_questions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ai = FakeAI()
            service = SecondBrainService(vault_dir=Path(tmp), ai=ai)
            await service.capture_text("Buy knife", telegram_message_id=1)

            await service.ask("what are the things I need to do?")

            context = ai.ask_calls[-1][1]
            self.assertIn("Open Actions", context)
            self.assertIn("Compare with wishlist", context)

    async def test_digest_writes_daily_note(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = SecondBrainService(vault_dir=Path(tmp), ai=FakeAI())
            await service.capture_text("Buy knife", telegram_message_id=1, created_at="2026-05-13T09:00:00Z")

            digest = await service.build_daily_digest(now_iso="2026-05-13T13:00:00Z")

            self.assertIn("Daily Second Brain Digest", digest)
            self.assertTrue((Path(tmp) / "2-Areas" / "Daily Reviews" / "2026-05-13 - Daily Second Brain Digest.md").exists())

    async def test_vault_health_reports_weak_and_orphan_notes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = SecondBrainService(vault_dir=Path(tmp), ai=FakeAI())
            service.index.upsert_note(
                NoteRecord(
                    note_id="bad-note",
                    title="A note",
                    path="3-Resources/Knowledge/A note.md",
                    tags=["#знання"],
                    entities=["Репрезентативна евристика"],
                    body="# A note\n\nNo parent MOC link.",
                    status="Reference",
                    created_at="2026-05-13T10:00:00Z",
                    updated_at="2026-05-13T10:00:00Z",
                )
            )

            report = service.vault_health()

            self.assertIn("Vault Health", report)
            self.assertIn("Weak titles: 1", report)
            self.assertIn("Missing parent MOC links: 1", report)
            self.assertIn("Non-English metadata: 1", report)

    async def test_consolidate_writes_new_review_note(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ai = FakeAI()
            service = SecondBrainService(vault_dir=Path(tmp), ai=ai)
            await service.capture_text("Buy knife", telegram_message_id=1, created_at="2026-05-13T09:00:00Z")

            text = await service.consolidate("week", now_iso="2026-05-13T13:00:00Z")

            self.assertIn("Use the collected", text)
            review_path = Path(tmp) / "2-Areas" / "Vault Reviews" / "2026-05-13 - Vault Consolidation.md"
            self.assertTrue(review_path.exists())
            self.assertIn("Vault Consolidation", review_path.read_text(encoding="utf-8"))

    async def test_learn_creates_linked_learning_note_from_note_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ai = FakeAI()
            service = SecondBrainService(vault_dir=Path(tmp), ai=ai)
            source = await service.capture_text(
                "Representativeness heuristic",
                telegram_message_id=1,
                created_at="2026-05-13T09:00:00Z",
            )

            lesson = await service.learn(source.note_id)

            self.assertIn("Lesson body with examples", lesson.text)
            self.assertIn("recall questions", ai.ask_calls[-1][0].lower())
            self.assertIn("practice exercise", ai.ask_calls[-1][0].lower())
            self.assertIn("Learning", lesson.note.title)
            self.assertTrue(lesson.note.path.exists())
            body = lesson.note.path.read_text(encoding="utf-8")
            self.assertIn("Related: [[Knife Brand - Purchase Research Note]]", body)
            self.assertIn("Lesson body with examples, answers, and scored next steps.", body)
            self.assertIsNotNone(service.index.get_note(lesson.note.note_id))

    async def test_list_notes_returns_recent_notes_without_query(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = SecondBrainService(vault_dir=Path(tmp), ai=FakeAI())
            await service.capture_text("Buy knife", telegram_message_id=1)

            notes = service.list_notes(limit=10)

            self.assertEqual(len(notes), 1)
            self.assertEqual(notes[0].title, "Knife Brand - Purchase Research Note")


if __name__ == "__main__":
    unittest.main()
