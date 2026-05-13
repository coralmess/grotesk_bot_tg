from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from helpers.analytics_events import AnalyticsSink
from second_brain_bot.ai import AIOrchestrator, RelatedNoteSuggestion, format_note_context
from second_brain_bot.index import SecondBrainIndex
from second_brain_bot.models import ActionRecord, NoteRecord, RelationRecord
from second_brain_bot.vault import CaptureInput, NoteFile, SecondBrainVault, utc_now_iso
from second_brain_bot.web_lookup import fetch_public_page_summary, should_allow_public_lookup


@dataclass(frozen=True)
class LearningResult:
    note: NoteFile
    text: str
    provider: str


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
        self.vault.migrate_legacy_vault()
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
        self._store_actions(note, enrichment.action_items)
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
        # Deep retrieval gives the model direct matches plus linked context, which
        # makes answers less brittle than plain keyword search.
        results = self.index.deep_search(question, limit=8)
        context = "\n\n".join(format_note_context(item, max_chars=900) for item in results)
        action_context = self._action_context(question)
        if action_context:
            context = (context + "\n\n" + action_context).strip()
        if not context.strip():
            self._record_command("ask", provider="local_no_evidence")
            return "I could not find saved notes or open actions that support an answer yet."
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

    async def consolidate(self, selector: str = "week", *, now_iso: str | None = None) -> str:
        notes = self.index.recent_notes(limit=40 if selector == "week" else 20)
        context = "\n\n".join(format_note_context(item, max_chars=900) for item in notes)
        result = await self.ai.ask(
            "Consolidate these Second Brain notes into durable insights, merged themes, open loops, and suggested next links. "
            "Do not rewrite old notes. Produce a clean review that can be saved as a new Obsidian note.",
            context=context,
            heavy=True,
        )
        date_key = (now_iso or utc_now_iso())[:10]
        self.vault.write_consolidation_note(date_key, result.text)
        self._record_command("consolidate", provider=result.provider)
        return result.text

    async def learn(self, selector: str) -> LearningResult:
        source = self.index.get_note(selector)
        if source is None:
            matches = self.index.search(selector, limit=1)
            source = matches[0] if matches else None
        if source is None:
            raise KeyError(selector)
        prompt = (
            "Create a practical learning session from this note. "
            "Start with a better explanation in simple language, then explain why it matters. "
            "Answer the note's open questions, expand useful suggestions/actions, give concrete examples, "
            "include two real-life application examples showing exactly how the user could apply it, "
            "list common misconceptions, add recall questions, add flashcards as Q: and A: pairs, "
            "add a short practice exercise, and score next steps from 1 to 100. "
            "Keep it readable in Telegram and useful enough to save as a linked Obsidian note."
        )
        result = await self.ai.ask(prompt, context=format_note_context(source, max_chars=5000), heavy=True, task="learn")
        note = self.vault.write_learning_note(
            source_note_id=source.note_id,
            source_title=source.title,
            source_path=source.path,
            lesson_text=result.text,
            provider=result.provider,
        )
        self._index_note(note.note_id)
        self._record_command("learn", provider=result.provider)
        return LearningResult(note=note, text=result.text, provider=result.provider)

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

    async def retry_pending_ai_enrichments(self, *, limit: int = 2) -> list[NoteFile]:
        updated: list[NoteFile] = []
        for note_id, metadata, source_text in self.vault.pending_ai_retry_notes(limit=limit):
            enrichment = await self.ai.enrich_capture(source_text)
            related = await self.ai.suggest_relations(
                source_text,
                self._candidate_notes(source_text, enrichment.entities + enrichment.suggested_tags),
            )
            note = self.vault.rewrite_capture_note(
                note_id,
                capture=CaptureInput(
                    capture_type=str(metadata.get("capture_type") or "text"),
                    text=source_text,
                    created_at=str(metadata.get("date_created") or ""),
                ),
                enrichment=enrichment,
                related_notes=related,
            )
            self._index_note(note.note_id)
            self._store_relations(note, related)
            self._store_actions(note, enrichment.action_items)
            self._record_capture(capture_type="ai_retry", related_count=len(related), provider=enrichment.provider)
            updated.append(note)
        return updated

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

    def list_notes(self, query: str = "", *, limit: int = 30):
        self._record_command("vault")
        query = (query or "").strip()
        return self.index.search(query, limit=limit) if query else self.index.recent_notes(limit=limit)

    def inbox(self, *, limit: int = 10):
        return self.index.recent_notes(limit=limit, status="Incubating")

    def status(self) -> dict[str, object]:
        counts = self.vault.note_counts()
        return {
            "vault_dir": str(self.vault.root_dir),
            "counts": counts,
        }

    def vault_health(self) -> str:
        notes = self.index.recent_notes(limit=1000)
        weak_titles = []
        missing_parent = []
        non_english_metadata = []
        title_counts: dict[str, int] = {}
        for note in notes:
            normalized_title = _normalized_duplicate_title(note.title)
            title_counts[normalized_title] = title_counts.get(normalized_title, 0) + 1
            if _looks_weak_title(note.title):
                weak_titles.append(note)
            if not note.path.endswith(" MOC.md") and "Parent: [[" not in note.body:
                missing_parent.append(note)
            if _has_non_english_metadata(note.tags + note.entities):
                non_english_metadata.append(note)
        duplicate_groups = sum(1 for count in title_counts.values() if count > 1)
        total = max(1, len(notes))
        title_score = _quality_score(len(notes) - len(weak_titles), total)
        link_score = _quality_score(len(notes) - len(missing_parent), total)
        metadata_score = _quality_score(len(notes) - len(non_english_metadata), total)
        duplicate_score = _quality_score(total - duplicate_groups, total)
        structure_score = _quality_score(
            sum(1 for note in notes if "## Executive Summary" in note.body or "## Source Capture" in note.body),
            total,
        )
        overall_score = round((title_score + link_score + metadata_score + duplicate_score + structure_score) / 5)
        lines = [
            "Vault Health",
            f"Notes scanned: {len(notes)}",
            f"Overall score: {overall_score}/100",
            f"Title score: {title_score}/100",
            f"Link score: {link_score}/100",
            f"Metadata score: {metadata_score}/100",
            f"Duplicate score: {duplicate_score}/100",
            f"Structure score: {structure_score}/100",
            f"Weak titles: {len(weak_titles)}",
            f"Missing parent MOC links: {len(missing_parent)}",
            f"Non-English metadata: {len(non_english_metadata)}",
            f"Duplicate-looking title groups: {duplicate_groups}",
        ]
        examples = weak_titles[:3] + missing_parent[:3] + non_english_metadata[:3]
        if examples:
            lines.append("")
            lines.append("Examples")
            seen: set[str] = set()
            for note in examples:
                if note.note_id in seen:
                    continue
                seen.add(note.note_id)
                lines.append(f"- {note.title} ({note.path})")
        self._record_command("review")
        return "\n".join(lines)

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

    def _store_actions(self, note: NoteFile, actions: list[str]) -> None:
        records: list[ActionRecord] = []
        try:
            source_path = note.path.relative_to(self.vault.root_dir).as_posix()
        except ValueError:
            source_path = str(note.path)
        for action in actions or []:
            text = str(action or "").strip()
            if not text:
                continue
            records.append(
                ActionRecord(
                    note_id=note.note_id,
                    action_text=text,
                    source_title=note.title,
                    source_path=source_path,
                    status="open",
                    priority=70,
                )
            )
        self.index.upsert_actions_for_note(note.note_id, records)

    def _action_context(self, question: str) -> str:
        if not _looks_task_question(question):
            return ""
        actions = self.index.search_actions(question, limit=12)
        if not actions:
            return ""
        lines = ["Open Actions"]
        for action in actions:
            lines.append(f"- {action.action_text} ({action.source_title}; {action.source_path})")
        return "\n".join(lines)

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


def _looks_task_question(question: str) -> bool:
    lowered = (question or "").lower()
    return any(
        phrase in lowered
        for phrase in (
            "what should i do",
            "what do i need",
            "things i need",
            "need to do",
            "tasks",
            "actions",
            "to do",
        )
    )


def _looks_weak_title(title: str) -> bool:
    lowered = (title or "").strip().lower()
    if len(lowered) < 6:
        return True
    return lowered.startswith("2026-") or lowered in {"a note", "note", "untitled", "capture"}


def _normalized_duplicate_title(title: str) -> str:
    lowered = re.sub(r"\s+\d+$", "", (title or "").strip().lower())
    return re.sub(r"\s+", " ", lowered)


def _has_non_english_metadata(values: list[str]) -> bool:
    return any(any(ord(ch) > 127 for ch in str(value or "")) for value in values)


def _quality_score(good: int, total: int) -> int:
    total = max(1, total)
    return max(0, min(100, round((good / total) * 100)))
