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

    def test_create_capture_preserves_dots_inside_descriptive_titles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault = SecondBrainVault(Path(tmp))
            note = vault.create_capture_note(
                CaptureInput(capture_type="text", text="Micro SaaS acquisition on Acquire.com"),
                enrichment=AIEnrichment(
                    title="Micro SaaS Acquisition via Acquire.com - Business Idea",
                    suggested_folder="4-Incubator",
                    suggested_tags=["#business", "#idea"],
                    note_type="Idea",
                    note_status="Incubating",
                    parent_moc="Business Ideas MOC",
                    moc_category="Business Ideas",
                ),
            )

            self.assertTrue(note.path.name.endswith("Acquire.com - Business Idea.md"))

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
                    provider="gemini",
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
            self.assertIn("ai_provider: gemini", text)
            self.assertIn("ai_retry_status: complete", text)
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

    def test_clear_todo_capture_overrides_purchase_incubator_catalog(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault = SecondBrainVault(Path(tmp))
            note = vault.create_capture_note(
                CaptureInput(
                    capture_type="text",
                    text="Треба пошукати нормальну бутилку для води",
                    created_at="2026-05-13T20:14:52Z",
                ),
                enrichment=AIEnrichment(
                    title="Selection of a Quality Water Bottle",
                    summary="Need to find a good water bottle.",
                    suggested_folder="4-Incubator",
                    suggested_tags=["#purchase", "#wishlist"],
                    note_type="Purchase",
                    note_status="Incubating",
                    parent_moc="Purchases MOC",
                    moc_category="Purchases",
                    action_items=["Research durable water bottle options"],
                    estimated_completion_time="20-30 minutes",
                    provider="gemini",
                ),
            )

            self.assertEqual(
                note.path.relative_to(Path(tmp)).as_posix(),
                "5-Todo List/Purchase Tasks/Selection of a Quality Water Bottle.md",
            )
            text = note.path.read_text(encoding="utf-8")
            self.assertIn("type: Plan", text)
            self.assertIn("status: Active", text)
            self.assertIn("Parent: [[Purchase Tasks MOC]]", text)
            self.assertIn("### Action Items", text)
            self.assertIn("### Estimated Completion Time", text)
            self.assertIn("20-30 minutes", text)
            moc = Path(tmp) / "5-Todo List" / "Purchase Tasks" / "Purchase Tasks MOC.md"
            self.assertTrue(moc.exists())

    def test_local_fallback_capture_is_marked_pending_ai_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault = SecondBrainVault(Path(tmp))
            note = vault.create_capture_note(
                CaptureInput(capture_type="text", text="A concept to enrich later"),
                enrichment=AIEnrichment(
                    title="A concept to enrich later",
                    summary="Fallback summary",
                    provider="local_fallback",
                ),
            )

            metadata, body, _ = vault.read_note(note.note_id)
            candidates = vault.pending_ai_retry_notes(limit=5)

            self.assertEqual(metadata["ai_provider"], "local_fallback")
            self.assertEqual(metadata["ai_retry_status"], "pending")
            self.assertEqual(candidates[0][0], note.note_id)
            self.assertIn("A concept to enrich later", candidates[0][2])

    def test_rewrite_capture_note_updates_existing_note_after_ai_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            vault = SecondBrainVault(Path(tmp))
            note = vault.create_capture_note(
                CaptureInput(capture_type="text", text="COPX stock idea"),
                enrichment=AIEnrichment(title="COPX stock idea", provider="local_fallback"),
            )

            updated = vault.rewrite_capture_note(
                note.note_id,
                capture=CaptureInput(capture_type="text", text="COPX stock idea"),
                enrichment=AIEnrichment(
                    title="COPX - Copper Miners ETF Investment Idea",
                    summary="COPX is a copper miners ETF.",
                    suggested_folder="4-Incubator",
                    suggested_tags=["#investments", "#etf"],
                    note_type="Idea",
                    note_status="Incubating",
                    parent_moc="Investments MOC",
                    moc_category="Investments",
                    provider="gemini",
                ),
                related_notes=[],
            )

            self.assertEqual(updated.note_id, note.note_id)
            self.assertFalse(note.path.exists())
            self.assertTrue(updated.path.exists())
            metadata, body, _ = vault.read_note(note.note_id)
            self.assertEqual(metadata["ai_provider"], "gemini")
            self.assertEqual(metadata["ai_retry_status"], "complete")
            self.assertIn("COPX is a copper miners ETF.", body)

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

    def test_migration_normalizes_date_prefixed_cataloged_notes_and_mocs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_dir = root / "4-Incubator" / "Purchases"
            old_dir.mkdir(parents=True)
            old_path = old_dir / "2026-05-13 Herman Miller Gaming Embody Office Chair Evaluation.md"
            old_path.write_text(
                "---\n"
                "aliases:\n"
                "  - old chair note\n"
                "tags:\n"
                "  - \"#purchases\"\n"
                "type: Purchase\n"
                "status: Incubating\n"
                "date_created: 2026-05-13\n"
                "---\n"
                "# 2026-05-13 Herman Miller Gaming Embody Office Chair Evaluation\n\n"
                "Parent: [[Purchases MOC]]\n\n"
                "## Source Capture\n"
                "Herman Miller Gaming Embody is the best office chair. It's expensive and hard to find.\n",
                encoding="utf-8",
            )
            moc = old_dir / "Purchases MOC.md"
            moc.write_text(
                "---\naliases: [Purchases]\ntags: [\"#moc\"]\ntype: MOC\nstatus: Active\ndate_created: 2026-05-13\n---\n"
                "# Purchases MOC\n\n## Notes\n"
                "- [[2026-05-13 Herman Miller Gaming Embody Office Chair Evaluation]] - old title.\n",
                encoding="utf-8",
            )
            (root / ".second_brain_state.json").write_text(
                json.dumps(
                    {
                        "notes": {
                            "chair-note": {
                                "path": "4-Incubator/Purchases/2026-05-13 Herman Miller Gaming Embody Office Chair Evaluation.md",
                                "title": "2026-05-13 Herman Miller Gaming Embody Office Chair Evaluation",
                                "status": "Incubating",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            vault = SecondBrainVault(root)
            stale_dir = root / "3-Resources" / "Knowledge"
            stale_dir.mkdir(parents=True)
            stale_moc = stale_dir / "Knowledge MOC.md"
            stale_moc.write_text(
                "---\naliases: [Knowledge]\ntags: [\"#moc\"]\ntype: MOC\nstatus: Active\ndate_created: 2026-05-13\n---\n"
                "# Knowledge MOC\n\n## Notes\n",
                encoding="utf-8",
            )

            migrated = vault.migrate_legacy_vault()

            new_path = root / "4-Incubator" / "Purchases" / "Herman Miller Gaming Embody - Office Chair Purchase Evaluation.md"
            self.assertEqual(migrated, 1)
            self.assertTrue(new_path.exists())
            self.assertFalse(old_path.exists())
            moc_text = moc.read_text(encoding="utf-8")
            self.assertIn("[[Herman Miller Gaming Embody - Office Chair Purchase Evaluation]]", moc_text)
            self.assertNotIn("[[2026-05-13 Herman Miller Gaming Embody Office Chair Evaluation]]", moc_text)
            self.assertFalse(stale_moc.exists())

    def test_migration_moves_existing_task_notes_to_todo_list(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_dir = root / "4-Incubator" / "Purchases"
            old_dir.mkdir(parents=True)
            old_path = old_dir / "Selection of a Quality Water Bottle.md"
            old_path.write_text(
                "---\n"
                "aliases:\n"
                "  - Selection of a Quality Water Bottle\n"
                "tags:\n"
                "  - \"#purchase\"\n"
                "type: Purchase\n"
                "status: Incubating\n"
                "date_created: 2026-05-13\n"
                "---\n"
                "# Selection of a Quality Water Bottle\n\n"
                "Parent: [[Purchases MOC]]\n\n"
                "## Source Capture\n"
                "Треба пошукати нормальну бутилку для води\n",
                encoding="utf-8",
            )
            (root / ".second_brain_state.json").write_text(
                json.dumps(
                    {
                        "notes": {
                            "water-bottle": {
                                "path": "4-Incubator/Purchases/Selection of a Quality Water Bottle.md",
                                "title": "Selection of a Quality Water Bottle",
                                "status": "Incubating",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            vault = SecondBrainVault(root)

            migrated = vault.migrate_legacy_vault()

            new_path = root / "5-Todo List" / "Purchase Tasks" / "Selection of a Quality Water Bottle.md"
            self.assertEqual(migrated, 1)
            self.assertTrue(new_path.exists())
            self.assertFalse(old_path.exists())
            text = new_path.read_text(encoding="utf-8")
            self.assertIn("type: Plan", text)
            self.assertIn("status: Active", text)
            self.assertIn("Parent: [[Purchase Tasks MOC]]", text)
            self.assertIn("### Action Items", text)
            self.assertIn("### Estimated Completion Time", text)
            self.assertIn("20-40 minutes", text)

    def test_migration_does_not_move_reference_notes_with_generic_should_wording(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            note_dir = root / "4-Incubator" / "Business Ideas"
            note_dir.mkdir(parents=True)
            note_path = note_dir / "Amazon FBA and Wyoming LLC - Business Idea.md"
            note_path.write_text(
                "---\n"
                "aliases: []\n"
                "tags:\n"
                "  - \"#business\"\n"
                "type: Idea\n"
                "status: Incubating\n"
                "date_created: 2026-05-13\n"
                "---\n"
                "# Amazon FBA and Wyoming LLC - Business Idea\n\n"
                "Parent: [[Business Ideas MOC]]\n\n"
                "## Source Capture\n"
                "Amazon FBA strategy. The operator should compare niches and find reliable suppliers.\n",
                encoding="utf-8",
            )
            (root / ".second_brain_state.json").write_text(
                json.dumps(
                    {
                        "notes": {
                            "fba-note": {
                                "path": "4-Incubator/Business Ideas/Amazon FBA and Wyoming LLC - Business Idea.md",
                                "title": "Amazon FBA and Wyoming LLC - Business Idea",
                                "status": "Incubating",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            vault = SecondBrainVault(root)

            migrated = vault.migrate_legacy_vault()

            self.assertEqual(migrated, 0)
            self.assertTrue(note_path.exists())
            self.assertFalse((root / "5-Todo List" / "Tasks" / "Amazon FBA and Wyoming LLC - Business Idea.md").exists())

    def test_migration_does_not_rename_already_correct_dotted_title(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            note_dir = root / "4-Incubator" / "Business Ideas"
            note_dir.mkdir(parents=True)
            note_path = note_dir / "Micro SaaS Acquisition via Acquire.com - Business Idea.md"
            note_path.write_text(
                "---\naliases: []\ntags: [\"#business-ideas\"]\ntype: Idea\nstatus: Incubating\ndate_created: 2026-05-13\n---\n"
                "# Micro SaaS Acquisition via Acquire.com - Business Idea\n\nParent: [[Business Ideas MOC]]\n",
                encoding="utf-8",
            )
            (root / ".second_brain_state.json").write_text(
                json.dumps(
                    {
                        "notes": {
                            "saas-note": {
                                "path": "4-Incubator/Business Ideas/Micro SaaS Acquisition via Acquire.com - Business Idea.md",
                                "title": "Micro SaaS Acquisition via Acquire.com - Business Idea",
                                "status": "Incubating",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            vault = SecondBrainVault(root)

            migrated = vault.migrate_legacy_vault()

            self.assertEqual(migrated, 0)
            self.assertTrue(note_path.exists())
            self.assertFalse((note_dir / "Micro SaaS Acquisition via Acquire.com - Business Idea 2.md").exists())


if __name__ == "__main__":
    unittest.main()
