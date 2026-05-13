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
                    title="Shure SM7B - Microphone Purchase Plan",
                    summary="A purchase reminder about a knife.",
                    polished_text="Buy knife later\n\n1. Compare knife brands.",
                    suggested_folder="4-Incubator",
                    suggested_tags=["#purchase", "#audio", "#wishlist"],
                    entities=["knife"],
                    aliases=["SM7B", "microphone wishlist"],
                    note_type="Purchase",
                    note_status="Incubating",
                    parent_moc="Purchases MOC",
                    moc_category="Purchases",
                    moc_description="Potential audio gear purchase for future content or recording setup.",
                    related_links=["Things to Buy MOC"],
                    action_items=["Compare knife brands"],
                    questions=[],
                    enrichment_notes=["COPX is the Global X Copper Miners ETF."],
                    scored_suggestions=[
                        {
                            "title": "Spotting Cognitive Distortions",
                            "score": 95,
                            "reason": "Practical CBT method.",
                        }
                    ],
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
            self.assertEqual(note.path.relative_to(Path(tmp)).as_posix(), "4-Incubator/Purchases/Shure SM7B - Microphone Purchase Plan.md")
            text = note.path.read_text(encoding="utf-8")
            self.assertIn("aliases:", text)
            self.assertIn("- SM7B", text)
            self.assertIn("tags:", text)
            self.assertIn("- \"#purchase\"", text)
            self.assertIn("type: Purchase", text)
            self.assertIn("status: Incubating", text)
            self.assertIn("date_created: 2026-05-13", text)
            self.assertIn("Parent: [[Purchases MOC]]", text)
            self.assertIn("[[Things to Buy MOC]]", text)
            self.assertIn("## Polished Capture", text)
            self.assertIn("1. Compare knife brands.", text)
            self.assertIn("### Useful Context", text)
            self.assertIn("COPX is the Global X Copper Miners ETF.", text)
            self.assertIn("Spotting Cognitive Distortions (Score: 95/100)", text)
            self.assertIn("[[Kitchen wishlist]]", text)
            moc = Path(tmp) / "4-Incubator" / "Purchases" / "Purchases MOC.md"
            self.assertTrue(moc.exists())
            moc_text = moc.read_text(encoding="utf-8")
            self.assertIn("type: MOC", moc_text)
            self.assertIn("[[Shure SM7B - Microphone Purchase Plan]]", moc_text)
            self.assertIn("Potential audio gear purchase", moc_text)

    def test_attachment_paths_are_relative_to_vault(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault = SecondBrainVault(Path(tmp))
            path = vault.attachment_path("photo.jpg", created_at="2026-05-13T10:00:00Z")
            self.assertEqual(path.relative_to(Path(tmp)).as_posix(), "Attachments/2026/05/photo.jpg")

    def test_accept_suggestion_keeps_cataloged_note_and_marks_active(self) -> None:
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
                    title="Health Maintenance - Useful Area Note",
                    summary="Summary",
                    suggested_folder="2-Areas",
                    suggested_tags=["#health"],
                    entities=[],
                    note_type="Concept",
                    note_status="Reference",
                    parent_moc="Health MOC",
                    moc_category="Health",
                    action_items=[],
                    questions=[],
                ),
            )

            moved = vault.accept_suggestion(note.note_id)

            self.assertTrue(moved.path.exists())
            self.assertEqual(moved.path, note.path)
            text = moved.path.read_text(encoding="utf-8")
            self.assertIn("status: Active", text)
            self.assertIn("- \"#health\"", text)

    def test_state_file_tracks_note_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault = SecondBrainVault(Path(tmp))
            note = vault.create_capture_note(
                CaptureInput(capture_type="text", text="State note", telegram_message_id=5),
            )
            payload = json.loads((Path(tmp) / ".second_brain_state.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["notes"][note.note_id]["path"], note.path.relative_to(Path(tmp)).as_posix())

    def test_migrate_legacy_inbox_notes_to_cataloged_structure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            legacy_dir = root / "00_Inbox"
            legacy_dir.mkdir(parents=True)
            legacy_path = legacy_dir / "2026-05-13 Another potential earning money strategy 2.md"
            legacy_path.write_text(
                "---\n"
                "id: old-note\n"
                "created_at: 2026-05-13T10:00:00Z\n"
                "status: inbox\n"
                "tags:\n"
                "  - earning\n"
                "---\n"
                "# Another potential earning money strategy\n\n"
                "Amazon FBA + Wyoming LLC idea.\n",
                encoding="utf-8",
            )
            (root / ".second_brain_state.json").write_text(
                json.dumps(
                    {
                        "notes": {
                            "old-note": {
                                "path": "00_Inbox/2026-05-13 Another potential earning money strategy 2.md",
                                "title": "Another potential earning money strategy",
                                "status": "inbox",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            vault = SecondBrainVault(root)

            migrated = vault.migrate_legacy_vault()

            self.assertEqual(migrated, 1)
            expected = root / "4-Incubator" / "Business Ideas" / "Amazon FBA and Wyoming LLC - Business Idea.md"
            self.assertTrue(expected.exists())
            self.assertFalse(legacy_path.exists())
            self.assertFalse(legacy_dir.exists())
            self.assertTrue((root / "4-Incubator" / "Business Ideas" / "Business Ideas MOC.md").exists())


if __name__ == "__main__":
    unittest.main()
