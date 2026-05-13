import json
import tempfile
import unittest
from pathlib import Path

from second_brain_bot.ai import AIEnrichment, RelatedNoteSuggestion
from second_brain_bot.vault import CaptureInput, SecondBrainVault, sanitize_filename


class SecondBrainVaultTests(unittest.TestCase):
    def test_sanitize_filename_keeps_obsidian_safe_names(self) -> None:
        self.assertEqual(sanitize_filename("Buy / test: knife?"), "Buy test knife")
        self.assertEqual(sanitize_filename("   "), "untitled")

    def test_create_text_capture_writes_markdown_with_frontmatter(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault = SecondBrainVault(Path(tmp))
            note = vault.create_capture_note(
                CaptureInput(
                    capture_type="text",
                    text="Buy knife later",
                    telegram_message_id=42,
                    created_at="2026-05-13T10:00:00Z",
                ),
                enrichment=AIEnrichment(
                    title="Buy knife later",
                    summary="A purchase reminder about a knife.",
                    suggested_folder="01_Projects",
                    suggested_tags=["wishlist", "knife"],
                    entities=["knife"],
                    action_items=["Compare knife brands"],
                    questions=[],
                ),
                related_notes=[
                    RelatedNoteSuggestion(
                        note_id="note-old",
                        title="Kitchen wishlist",
                        reason="Both mention buying kitchen gear.",
                        confidence=0.91,
                    )
                ],
            )

            self.assertTrue(note.path.exists())
            text = note.path.read_text(encoding="utf-8")
            self.assertIn("status: inbox", text)
            self.assertIn("capture_type: text", text)
            self.assertIn("ai_suggested_title: Buy knife later", text)
            self.assertIn("- wishlist", text)
            self.assertIn("[[Kitchen wishlist]]", text)
            self.assertIn("Buy knife later", text)

    def test_attachment_paths_are_relative_to_vault(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault = SecondBrainVault(Path(tmp))
            path = vault.attachment_path("photo.jpg", created_at="2026-05-13T10:00:00Z")
            self.assertEqual(path.relative_to(Path(tmp)).as_posix(), "Attachments/2026/05/photo.jpg")

    def test_accept_suggestion_moves_note_and_marks_organized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault = SecondBrainVault(Path(tmp))
            note = vault.create_capture_note(
                CaptureInput(
                    capture_type="text",
                    text="A note",
                    telegram_message_id=1,
                    created_at="2026-05-13T10:00:00Z",
                ),
                enrichment=AIEnrichment(
                    title="Useful area note",
                    summary="Summary",
                    suggested_folder="02_Areas",
                    suggested_tags=["area"],
                    entities=[],
                    action_items=[],
                    questions=[],
                ),
            )

            moved = vault.accept_suggestion(note.note_id)

            self.assertTrue(moved.path.exists())
            self.assertIn("02_Areas", moved.path.parts)
            self.assertFalse(note.path.exists())
            text = moved.path.read_text(encoding="utf-8")
            self.assertIn("status: organized", text)
            self.assertIn("- area", text)

    def test_state_file_tracks_note_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault = SecondBrainVault(Path(tmp))
            note = vault.create_capture_note(
                CaptureInput(capture_type="text", text="State note", telegram_message_id=5),
            )
            payload = json.loads((Path(tmp) / ".second_brain_state.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["notes"][note.note_id]["path"], note.path.relative_to(Path(tmp)).as_posix())


if __name__ == "__main__":
    unittest.main()
