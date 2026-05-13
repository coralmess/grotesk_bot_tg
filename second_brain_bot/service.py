from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

from helpers.analytics_events import AnalyticsSink
from second_brain_bot.ai import AIOrchestrator, RelatedNoteSuggestion, format_note_context
from second_brain_bot.index import SecondBrainIndex
from second_brain_bot.models import NoteRecord, RelationRecord
from second_brain_bot.vault import CaptureInput, NoteFile, SecondBrainVault, utc_now_iso
from second_brain_bot.web_lookup import fetch_public_page_summary, should_allow_public_lookup


class SecondBrainService:
    def __init__(
        self,
        *,
        vault_dir: Path,
        ai: AIOrchestrator,
        analytics_sink: AnalyticsSink | None = None,
    ) -> None:
        self.vault = SecondBrainVault(vault_dir)
        self.index = SecondBrainIndex(Path(vault_dir) / ".second_brain_index.db")
        self.ai = ai
        self.analytics_sink = analytics_sink or AnalyticsSink()
        if self.vault.migrate_legacy_vault():
            self._reindex_all_notes()

    async def capture_text(
        self,
        text: str,
        *,
        telegram_message_id: int | None = None,
        created_at: str | None = None,
        attachment_relpath: str | None = None,
        capture_type: str = "text",
        allow_web: bool = False,
    ) -> NoteFile:
        text = text or ""
        lookup_notes = ""
        if should_allow_public_lookup(text, explicit=allow_web):
            lookup_notes = await self._public_lookup_notes(text)
        enrichment_text = text + ("\n\nPublic lookup notes:\n" + lookup_notes if lookup_notes else "")
        enrichment = await self.ai.enrich_capture(enrichment_text, allow_web=bool(lookup_notes or allow_web))
        candidates = self._candidate_notes(enrichment_text, enrichment.entities + enrichment.suggested_tags)
        related = await self.ai.suggest_relations(enrichment_text, candidates)
        note = self.vault.create_capture_note(
            CaptureInput(
                capture_type=capture_type,
                text=text,
                telegram_message_id=telegram_message_id,
                created_at=created_at,
                attachment_relpath=attachment_relpath,
            ),
            enrichment=enrichment,
            related_notes=related,
        )
        self._index_note(note.note_id)
        self._store_relations(note, related)
        self._record_capture(capture_type=capture_type, related_count=len(related), provider=enrichment.provider)
        return note

    async def capture_photo(
        self,
        *,
        caption: str,
        photo_bytes: bytes,
        original_name: str,
        telegram_message_id: int | None = None,
        created_at: str | None = None,
    ) -> NoteFile:
        attachment_path = self.vault.attachment_path(original_name, created_at=created_at)
        attachment_path.parent.mkdir(parents=True, exist_ok=True)
        attachment_path.write_bytes(photo_bytes)
        relpath = attachment_path.relative_to(self.vault.root_dir).as_posix()
        return await self.capture_text(
            caption,
            telegram_message_id=telegram_message_id,
            created_at=created_at,
            attachment_relpath=relpath,
            capture_type="photo",
        )

    async def ask(self, question: str) -> str:
        results = self.index.search(question, limit=8)
        context = "\n\n".join(format_note_context(item, max_chars=900) for item in results)
        result = await self.ai.ask(question, context=context, heavy=True)
        self._record_command("ask", provider=result.provider)
        return result.text

    async def distill(self, selector: str) -> str:
        if selector in {"today", "week"}:
            notes = self.index.recent_notes(limit=30)
        else:
            note = self.index.get_note(selector)
            notes = [note] if note else []
        context = "\n\n".join(f"{item.title}: {item.body[:1000]}" for item in notes if item)
        result = await self.ai.ask("Distill these notes into concise insights and next actions.", context=context, heavy=True)
        date_key = datetime.now(timezone.utc).date().isoformat()
        self.vault.write_daily_digest(date_key, "# Distilled Notes\n\n" + result.text.strip() + "\n")
        self._record_command("distill", provider=result.provider)
        return result.text

    async def build_daily_digest(self, *, now_iso: str | None = None) -> str:
        now_iso = now_iso or utc_now_iso()
        date_key = now_iso[:10]
        recent = self.index.recent_notes(limit=20)
        inbox = self.index.stale_inbox(limit=20)
        context = "\n\n".join(f"{item.title}: {item.body[:700]}" for item in recent)
        result = await self.ai.ask(
            "Create a compact daily Second Brain digest with captures, open loops, related links, and 3-7 insights.",
            context=context,
            heavy=True,
        )
        fallback_lines = [
            "# Daily Second Brain Digest",
            "",
            f"Captures indexed: {len(recent)}",
            f"Open inbox items: {len(inbox)}",
            "",
            result.text.strip() or "No AI digest was available.",
        ]
        digest = "\n".join(fallback_lines).rstrip() + "\n"
        self.vault.write_daily_digest(date_key, digest)
        self._record_command("digest", provider=result.provider)
        return digest

    def accept(self, note_id: str) -> NoteFile:
        note = self.vault.accept_suggestion(note_id)
        self._index_note(note_id)
        self._record_command("accept")
        return note

    def skip(self, note_id: str) -> NoteFile:
        note = self.vault.mark_status(note_id, "needs_manual_review")
        self._index_note(note_id)
        self._record_command("skip")
        return note

    def search(self, query: str, *, limit: int = 10):
        self._record_command("search")
        return self.index.search(query, limit=limit)

    def inbox(self, *, limit: int = 10):
        return self.index.recent_notes(limit=limit, status="Incubating")

    def status(self) -> dict[str, object]:
        counts = self.vault.note_counts()
        return {
            "vault_dir": str(self.vault.root_dir),
            "counts": counts,
        }

    def _candidate_notes(self, text: str, terms: list[str]) -> list:
        query = " ".join([*terms, *re.findall(r"[\w'-]{3,}", text.lower())[:8]])
        return self.index.search(query, limit=10)

    def _index_note(self, note_id: str) -> None:
        metadata, body, path = self.vault.read_note(note_id)
        relpath = path.relative_to(self.vault.root_dir).as_posix()
        tags = metadata.get("tags") if isinstance(metadata.get("tags"), list) else []
        entities = metadata.get("entities") if isinstance(metadata.get("entities"), list) else []
        self.index.upsert_note(
            NoteRecord(
                note_id=note_id,
                title=path.stem,
                path=relpath,
                tags=[str(item) for item in tags],
                entities=[str(item) for item in entities],
                body=body,
                status=str(metadata.get("status") or "Reference"),
                created_at=str(metadata.get("date_created") or metadata.get("created_at") or utc_now_iso()),
                updated_at=str(metadata.get("updated_at") or metadata.get("date_created") or utc_now_iso()),
            )
        )

    def _reindex_all_notes(self) -> None:
        state = self.vault._load_state()
        for note_id in state.get("notes", {}):
            try:
                self._index_note(note_id)
            except Exception:
                continue

    def _store_relations(self, note: NoteFile, related: list[RelatedNoteSuggestion]) -> None:
        for item in related:
            self.index.upsert_relation(
                RelationRecord(
                    source_note_id=note.note_id,
                    target_note_id=item.note_id,
                    target_title=item.title,
                    reason=item.reason,
                    confidence=item.confidence,
                )
            )
            if item.confidence >= 0.85:
                self.vault.add_backlink(
                    note.note_id,
                    item.note_id,
                    source_title=note.title,
                    reason=item.reason,
                )

    async def _public_lookup_notes(self, text: str) -> str:
        urls = re.findall(r"https?://[^\s)>\]]+", text)
        summaries: list[str] = []
        for url in urls[:3]:
            result = await fetch_public_page_summary(url)
            if result is None:
                continue
            summaries.append(
                f"- Source: {result.url}\n  Retrieved: {result.retrieved_at}\n  Title: {result.title}\n  Excerpt: {result.excerpt}"
            )
        return "\n".join(summaries)

    def _record_capture(self, *, capture_type: str, related_count: int, provider: str) -> None:
        try:
            self.analytics_sink.append_event(
                "second_brain_capture",
                {"capture_type": capture_type, "related_count": related_count, "provider": provider},
            )
            self.analytics_sink.add_daily_counters(
                "second_brain_captures",
                dimensions={"capture_type": capture_type},
                counters={"captures": 1, "related_notes": related_count},
            )
        except Exception:
            return

    def _record_command(self, command: str, *, provider: str = "") -> None:
        try:
            self.analytics_sink.append_event("second_brain_command", {"command": command, "provider": provider})
            self.analytics_sink.add_daily_counters(
                "second_brain_commands",
                dimensions={"command": command},
                counters={"commands": 1},
            )
        except Exception:
            return
