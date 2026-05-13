import tempfile
import unittest
from pathlib import Path

from second_brain_bot.index import SecondBrainIndex
from second_brain_bot.models import ActionRecord, NoteRecord, RelationRecord


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

    def test_deep_search_expands_related_notes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = SecondBrainIndex(Path(tmp) / "brain.db")
            index.upsert_note(
                NoteRecord(
                    note_id="representativeness",
                    title="Representativeness Heuristic",
                    path="3-Resources/Psychology/Representativeness Heuristic.md",
                    tags=["#psychology", "#cognitive-biases"],
                    entities=["Representativeness Heuristic"],
                    body="A note about judging probability by similarity.",
                    status="Reference",
                    created_at="2026-05-13T10:00:00Z",
                    updated_at="2026-05-13T10:00:00Z",
                )
            )
            index.upsert_note(
                NoteRecord(
                    note_id="thinking-fast-slow",
                    title="Thinking Fast and Slow",
                    path="3-Resources/Books/Thinking Fast and Slow.md",
                    tags=["#book", "#psychology"],
                    entities=["Daniel Kahneman"],
                    body="Book note connected to cognitive bias learning.",
                    status="Reference",
                    created_at="2026-05-13T11:00:00Z",
                    updated_at="2026-05-13T11:00:00Z",
                )
            )
            index.upsert_relation(
                RelationRecord(
                    source_note_id="representativeness",
                    target_note_id="thinking-fast-slow",
                    target_title="Thinking Fast and Slow",
                    reason="Primary book source for the concept.",
                    confidence=0.91,
                )
            )

            results = index.deep_search("representativeness", limit=5)

            self.assertEqual([item.note_id for item in results], ["representativeness", "thinking-fast-slow"])

    def test_actions_are_indexed_and_searchable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            index = SecondBrainIndex(Path(tmp) / "brain.db")
            index.upsert_actions_for_note(
                "note-1",
                [
                    ActionRecord(
                        note_id="note-1",
                        action_text="Compare knife brands before buying",
                        source_title="Knife Purchase",
                        source_path="4-Incubator/Purchases/Knife Purchase.md",
                        status="open",
                        priority=70,
                    )
                ],
            )

            actions = index.search_actions("what do I need to buy?", limit=5)

            self.assertEqual(len(actions), 1)
            self.assertEqual(actions[0].action_text, "Compare knife brands before buying")
            self.assertEqual(actions[0].source_title, "Knife Purchase")


if __name__ == "__main__":
    unittest.main()
