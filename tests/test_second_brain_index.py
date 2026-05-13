import tempfile
import unittest
from pathlib import Path

from second_brain_bot.index import SecondBrainIndex
from second_brain_bot.models import NoteRecord, RelationRecord


class SecondBrainIndexTests(unittest.TestCase):
    def test_indexes_and_searches_notes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = SecondBrainIndex(Path(tmp) / "brain.db")
            index.upsert_note(
                NoteRecord(
                    note_id="note-1",
                    title="Buy knife",
                    path="00_Inbox/note.md",
                    tags=["wishlist"],
                    entities=["knife"],
                    body="I need to buy a chef knife later.",
                    status="inbox",
                    created_at="2026-05-13T10:00:00Z",
                    updated_at="2026-05-13T10:00:00Z",
                )
            )

            results = index.search("chef knife", limit=5)

            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].note_id, "note-1")

    def test_relations_are_stored_and_returned(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = SecondBrainIndex(Path(tmp) / "brain.db")
            index.upsert_relation(
                RelationRecord(
                    source_note_id="new",
                    target_note_id="old",
                    target_title="Buy knife",
                    reason="Same knife topic",
                    confidence=0.92,
                )
            )

            relations = index.relations_for("new")

            self.assertEqual(len(relations), 1)
            self.assertEqual(relations[0].target_note_id, "old")
            self.assertEqual(relations[0].confidence, 0.92)


if __name__ == "__main__":
    unittest.main()
