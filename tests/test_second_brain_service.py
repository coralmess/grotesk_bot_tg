import tempfile
import unittest
from pathlib import Path

from helpers.analytics_events import AnalyticsSink
from second_brain_bot.ai import AIEnrichment, RelatedNoteSuggestion
from second_brain_bot.models import NoteRecord, RelationRecord
from second_brain_bot.service import SecondBrainService
from second_brain_bot.vault import CaptureInput


def make_test_service(tmp: str, ai) -> SecondBrainService:
    return SecondBrainService(
        vault_dir=Path(tmp),
        ai=ai,
        analytics_sink=AnalyticsSink(Path(tmp) / "analytics", now_func=lambda: "2026-05-13T00:00:00Z"),
    )


class FakeAI:
    def __init__(self) -> None:
        self.enrich_calls: list[str] = []
        self.relation_calls = 0
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
        self.relation_calls += 1
        return [
            RelatedNoteSuggestion(
                note_id=candidates[0].note_id,
                title=candidates[0].title,
                reason="Both mention knives.",
                confidence=0.9,
            )
        ] if candidates else []

    async def ask(self, question: str, *, context: str, heavy: bool = True, **kwargs):
        self.ask_calls.append((question, context, heavy))
        if "learning session" in question.lower():
            return type("Result", (), {"text": "Lesson body with examples, answers, and scored next steps.", "provider": "fake"})()
        return type("Result", (), {"text": "Use the collected knife notes.", "provider": "fake"})()


class RetryAI:
    def __init__(self) -> None:
        self.enrich_calls: list[str] = []

    async def enrich_capture(self, text: str, *, image_bytes=None, preferred_provider=None, allow_web=False):
        self.enrich_calls.append(text)
        return AIEnrichment(
            title="COPX - Copper Miners ETF Investment Idea",
            summary="COPX is the Global X Copper Miners ETF.",
            suggested_folder="4-Incubator",
            suggested_tags=["#investments", "#etf"],
            entities=["COPX", "Global X Copper Miners ETF"],
            note_type="Idea",
            note_status="Incubating",
            parent_moc="Investments MOC",
            moc_category="Investments",
            action_items=["Compare COPX risk with portfolio plan"],
            provider="gemini",
        )

    async def suggest_relations(self, note_text, candidates):
        return []

    async def ask(self, question: str, *, context: str, heavy: bool = True, **kwargs):
        return type("Result", (), {"text": "answer", "provider": "gemini"})()


class SecondBrainServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_test_service_analytics_stays_inside_temp_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = make_test_service(tmp, FakeAI())
            await service.capture_text("Buy knife", telegram_message_id=1)

            self.assertTrue((Path(tmp) / "analytics" / "events" / "2026-05-13.second_brain_capture.jsonl").exists())
            self.assertTrue(str(service.analytics_sink.root_dir).startswith(str(Path(tmp))))

    async def test_capture_indexes_note_and_creates_relation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = make_test_service(tmp, FakeAI())
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
            self.assertEqual(service.ai.relation_calls, 0)

    async def test_retry_pending_ai_enrichments_updates_local_fallback_note(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = make_test_service(tmp, FakeAI())
            fallback = service.vault.create_capture_note(
                CaptureInput(capture_type="text", text="COPX stock idea"),
                enrichment=AIEnrichment(title="COPX stock idea", provider="local_fallback"),
            )
            service._index_note(fallback.note_id)
            retry_ai = RetryAI()
            service.ai = retry_ai

            updated = await service.retry_pending_ai_enrichments(limit=2)

            self.assertEqual(len(updated), 1)
            self.assertEqual(updated[0].note_id, fallback.note_id)
            self.assertEqual(updated[0].provider, "gemini")
            self.assertEqual(len(retry_ai.enrich_calls), 1)
            metadata, body, _ = service.vault.read_note(fallback.note_id)
            self.assertEqual(metadata["ai_retry_status"], "complete")
            self.assertIn("Global X Copper Miners ETF", body)

    async def test_ask_uses_local_retrieval_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ai = FakeAI()
            service = make_test_service(tmp, ai)
            await service.capture_text("Buy knife", telegram_message_id=1)

            answer = await service.ask("What should I buy?")

            self.assertIn("knife", ai.ask_calls[0][1].lower())
            self.assertNotIn("## Raw Capture", ai.ask_calls[0][1])
            self.assertIn("Use the collected", answer)

    async def test_ask_uses_related_notes_from_deep_retrieval(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ai = FakeAI()
            service = make_test_service(tmp, ai)
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
            service = make_test_service(tmp, ai)
            await service.capture_text("Buy knife", telegram_message_id=1)

            actions = service.index.search_actions("what should I do?", limit=5)

            self.assertEqual(len(actions), 1)
            self.assertIn("Compare with wishlist", actions[0].action_text)

    async def test_ask_includes_open_actions_for_task_questions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ai = FakeAI()
            service = make_test_service(tmp, ai)
            await service.capture_text("Buy knife", telegram_message_id=1)

            await service.ask("what are the things I need to do?")

            context = ai.ask_calls[-1][1]
            self.assertIn("Open Actions", context)
            self.assertIn("Compare with wishlist", context)

    async def test_ask_returns_not_found_without_ai_when_no_evidence_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ai = FakeAI()
            service = make_test_service(tmp, ai)

            answer = await service.ask("what do I know about sailing?")

            self.assertIn("could not find", answer.lower())
            self.assertEqual(ai.ask_calls, [])

    async def test_digest_writes_daily_note(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = make_test_service(tmp, FakeAI())
            await service.capture_text("Buy knife", telegram_message_id=1, created_at="2026-05-13T09:00:00Z")

            digest = await service.build_daily_digest(now_iso="2026-05-13T13:00:00Z")

            self.assertIn("Daily Second Brain Digest", digest)
            self.assertTrue((Path(tmp) / "2-Areas" / "Daily Reviews" / "2026-05-13 - Daily Second Brain Digest.md").exists())

    async def test_vault_health_reports_weak_and_orphan_notes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = make_test_service(tmp, FakeAI())
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
            self.assertIn("Overall score:", report)
            self.assertIn("Title score:", report)
            self.assertIn("Link score:", report)
            self.assertIn("Weak titles: 1", report)
            self.assertIn("Missing parent MOC links: 1", report)
            self.assertIn("Non-English metadata: 1", report)

    async def test_consolidate_writes_new_review_note(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ai = FakeAI()
            service = make_test_service(tmp, ai)
            await service.capture_text("Buy knife", telegram_message_id=1, created_at="2026-05-13T09:00:00Z")

            text = await service.consolidate("week", now_iso="2026-05-13T13:00:00Z")

            self.assertIn("Use the collected", text)
            review_path = Path(tmp) / "2-Areas" / "Vault Reviews" / "2026-05-13 - Vault Consolidation.md"
            self.assertTrue(review_path.exists())
            self.assertIn("Vault Consolidation", review_path.read_text(encoding="utf-8"))

    async def test_learn_creates_linked_learning_note_from_note_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ai = FakeAI()
            service = make_test_service(tmp, ai)
            source = await service.capture_text(
                "Representativeness heuristic",
                telegram_message_id=1,
                created_at="2026-05-13T09:00:00Z",
            )

            lesson = await service.learn(source.note_id)

            self.assertIn("Lesson body with examples", lesson.text)
            self.assertIn("recall questions", ai.ask_calls[-1][0].lower())
            self.assertIn("flashcards", ai.ask_calls[-1][0].lower())
            self.assertIn("practice exercise", ai.ask_calls[-1][0].lower())
            self.assertIn("two real-life application examples", ai.ask_calls[-1][0].lower())
            self.assertIn("Learning", lesson.note.title)
            self.assertTrue(lesson.note.path.exists())
            body = lesson.note.path.read_text(encoding="utf-8")
            self.assertIn("Related: [[Knife Brand - Purchase Research Note]]", body)
            self.assertIn("Lesson body with examples, answers, and scored next steps.", body)
            self.assertIsNotNone(service.index.get_note(lesson.note.note_id))

    async def test_list_notes_returns_recent_notes_without_query(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = make_test_service(tmp, FakeAI())
            await service.capture_text("Buy knife", telegram_message_id=1)

            notes = service.list_notes(limit=10)

            self.assertEqual(len(notes), 1)
            self.assertEqual(notes[0].title, "Knife Brand - Purchase Research Note")


if __name__ == "__main__":
    unittest.main()
