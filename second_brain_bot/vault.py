from __future__ import annotations

import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from second_brain_bot.ai import AIEnrichment, RelatedNoteSuggestion

PARA_FOLDERS = (
    "1-Projects",
    "2-Areas",
    "3-Resources",
    "4-Incubator",
    "Attachments",
)
LEGACY_PARA_FOLDERS = {"00_Inbox", "01_Projects", "02_Areas", "03_Resources", "04_Daily", "99_Archive"}
ALLOWED_STATUS = {"Active", "Incubating", "Completed", "Reference", "needs_manual_review"}
ALLOWED_TYPES = {"MOC", "Concept", "Plan", "Purchase", "Idea"}


@dataclass(frozen=True)
class CaptureInput:
    capture_type: str
    text: str
    telegram_message_id: int | None = None
    created_at: str | None = None
    attachment_relpath: str | None = None


@dataclass(frozen=True)
class NoteFile:
    note_id: str
    title: str
    path: Path
    status: str
    provider: str = ""


@dataclass(frozen=True)
class CatalogPlan:
    title: str
    folder: str
    category: str
    parent_moc: str
    aliases: list[str]
    tags: list[str]
    note_type: str
    status: str
    moc_description: str
    related_links: list[str]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sanitize_filename(value: str, *, max_length: int = 80) -> str:
    cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', " ", str(value or ""))
    cleaned = re.sub(r"\s+", " ", cleaned).strip().strip(".")
    if not cleaned:
        return "untitled"
    return cleaned[:max_length].rstrip() or "untitled"


def safe_note_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:8]


def _yaml_scalar(value: Any) -> str:
    if value is None:
        return '""'
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text_value = str(value)
    if text_value and not any(ch in text_value for ch in ('"', "#", "[", "]", "{", "}", "\n", "\r")):
        return text_value
    text = text_value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{text}"'


def _yaml_list(values: list[str]) -> str:
    if not values:
        return "[]"
    return "\n" + "\n".join(f"  - {_yaml_scalar(item)}" for item in values)


def _extract_frontmatter(text: str) -> tuple[dict[str, Any], str]:
    if not text.startswith("---\n"):
        return {}, text
    end = text.find("\n---\n", 4)
    if end < 0:
        return {}, text
    raw = text[4:end].splitlines()
    body = text[end + 5 :]
    data: dict[str, Any] = {}
    current_key: str | None = None
    for line in raw:
        if not line.strip():
            continue
        if line.startswith("  - ") and current_key:
            data.setdefault(current_key, []).append(line[4:].strip().strip('"'))
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        current_key = key.strip()
        value = value.strip()
        if value == "[]" or value == "":
            data[current_key] = []
        else:
            data[current_key] = value.strip('"')
    return data, body


class SecondBrainVault:
    def __init__(self, root_dir: Path) -> None:
        self.root_dir = Path(root_dir)
        self.state_file = self.root_dir / ".second_brain_state.json"
        self.ensure_layout()

    def ensure_layout(self) -> None:
        for folder in PARA_FOLDERS:
            (self.root_dir / folder).mkdir(parents=True, exist_ok=True)

    def attachment_path(self, original_name: str, *, created_at: str | None = None) -> Path:
        ts = _parse_iso(created_at) or datetime.now(timezone.utc)
        safe_name = sanitize_filename(Path(original_name or "photo.jpg").name)
        if "." not in safe_name:
            safe_name = f"{safe_name}.jpg"
        return self.root_dir / "Attachments" / f"{ts.year:04d}" / f"{ts.month:02d}" / safe_name

    def create_capture_note(
        self,
        capture: CaptureInput,
        *,
        enrichment: AIEnrichment | None = None,
        related_notes: list[RelatedNoteSuggestion] | None = None,
    ) -> NoteFile:
        created_at = capture.created_at or utc_now_iso()
        note_id = safe_note_id()
        catalog = _catalog_plan(capture, enrichment)
        path = self._unique_note_path(catalog.folder, catalog.category, f"{catalog.title}.md")
        metadata = {
            "aliases": catalog.aliases,
            "tags": catalog.tags,
            "type": catalog.note_type,
            "status": catalog.status,
            "date_created": created_at[:10],
        }
        body = self._render_capture_body(
            capture,
            enrichment=enrichment,
            related_notes=related_notes or [],
            catalog=catalog,
        )
        self._write_note(path, metadata, body)
        self._ensure_moc(catalog, note_title=catalog.title)
        self._record_state(note_id, path, title=catalog.title, status=catalog.status)
        return NoteFile(
            note_id=note_id,
            title=catalog.title,
            path=path,
            status=catalog.status,
            provider=(enrichment.provider if enrichment else ""),
        )

    def read_note(self, note_id: str) -> tuple[dict[str, Any], str, Path]:
        state = self._load_state()
        info = state.get("notes", {}).get(note_id)
        if not info:
            raise KeyError(note_id)
        path = self.root_dir / info["path"]
        data, body = _extract_frontmatter(path.read_text(encoding="utf-8"))
        return data, body, path

    def accept_suggestion(self, note_id: str) -> NoteFile:
        metadata, body, path = self.read_note(note_id)
        title = sanitize_filename(path.stem)
        metadata["status"] = "Active"
        self._write_note(path, metadata, body)
        self._record_state(note_id, path, title=title, status="Active")
        return NoteFile(note_id=note_id, title=title, path=path, status="Active")

    def mark_status(self, note_id: str, status: str) -> NoteFile:
        if status not in ALLOWED_STATUS:
            raise ValueError(f"unsupported note status: {status}")
        metadata, body, path = self.read_note(note_id)
        metadata["status"] = status
        self._write_note(path, metadata, body)
        title = sanitize_filename(path.stem)
        self._record_state(note_id, path, title=title, status=status)
        return NoteFile(note_id=note_id, title=title, path=path, status=status)

    def append_related_notes(self, note_id: str, related_notes: list[RelatedNoteSuggestion]) -> None:
        if not related_notes:
            return
        metadata, body, path = self.read_note(note_id)
        block = _render_related_notes(related_notes)
        if "## Related Notes" in body:
            body = re.sub(r"\n## Related Notes\n.*?(?=\n## |\Z)", "\n" + block.rstrip() + "\n", body, flags=re.S)
        else:
            body = body.rstrip() + "\n\n" + block
        self._write_note(path, metadata, body)

    def add_backlink(self, source_note_id: str, target_note_id: str, *, source_title: str, reason: str) -> None:
        metadata, body, path = self.read_note(target_note_id)
        line = f"- [[{source_title}]] - {reason}"
        if line in body:
            return
        section = "## Backlinks"
        if section in body:
            body = body.rstrip() + "\n" + line + "\n"
        else:
            body = body.rstrip() + f"\n\n{section}\n{line}\n"
        self._write_note(path, metadata, body)

    def write_daily_digest(self, date_key: str, content: str) -> Path:
        path = self.root_dir / "2-Areas" / "Daily Reviews" / f"{date_key} - Daily Second Brain Digest.md"
        metadata = {
            "aliases": [f"Daily digest {date_key}"],
            "tags": ["#daily", "#digest", "#review"],
            "type": "Concept",
            "status": "Reference",
            "date_created": date_key,
        }
        self._write_note(path, metadata, content)
        return path

    def write_consolidation_note(self, date_key: str, content: str) -> Path:
        path = self.root_dir / "2-Areas" / "Vault Reviews" / f"{date_key} - Vault Consolidation.md"
        metadata = {
            "aliases": [f"Vault consolidation {date_key}"],
            "tags": ["#vault-review", "#consolidation", "#second-brain"],
            "type": "Concept",
            "status": "Reference",
            "date_created": date_key,
        }
        body = "# Vault Consolidation\n\n" + content.strip() + "\n"
        self._write_note(path, metadata, body)
        return path

    def write_learning_note(
        self,
        *,
        source_note_id: str,
        source_title: str,
        source_path: str,
        lesson_text: str,
        provider: str = "",
    ) -> NoteFile:
        note_id = safe_note_id()
        title = sanitize_filename(f"Learning - {source_title}", max_length=96)
        catalog = CatalogPlan(
            title=title,
            folder="3-Resources",
            category="Learning",
            parent_moc="Learning MOC",
            aliases=[title, f"Learn {source_title}"],
            tags=["#learning", "#study", "#second-brain"],
            note_type="Concept",
            status="Reference",
            moc_description="Indexes saved learning sessions generated from existing vault notes.",
            related_links=[source_title],
        )
        path = self._unique_note_path(catalog.folder, catalog.category, f"{catalog.title}.md")
        metadata = {
            "aliases": catalog.aliases,
            "tags": catalog.tags,
            "type": catalog.note_type,
            "status": catalog.status,
            "date_created": utc_now_iso()[:10],
        }
        body = "\n".join(
            [
                f"# {catalog.title}",
                "",
                "Parent: [[Learning MOC]]",
                f"Related: [[{source_title}]]",
                "",
                "## Source Note",
                f"- ID: {source_note_id}",
                f"- Path: {source_path}",
                "",
                "## Learning Session",
                lesson_text.strip() or "No lesson text was generated.",
                "",
            ]
        )
        self._write_note(path, metadata, body)
        self._ensure_moc(catalog, note_title=catalog.title)
        self._record_state(note_id, path, title=catalog.title, status=catalog.status)
        return NoteFile(note_id=note_id, title=catalog.title, path=path, status=catalog.status, provider=provider)

    def note_counts(self) -> dict[str, int]:
        state = self._load_state()
        counts = {"total": 0, "Active": 0, "Incubating": 0, "Completed": 0, "Reference": 0, "needs_manual_review": 0}
        for info in state.get("notes", {}).values():
            status = info.get("status", "Reference")
            counts["total"] += 1
            counts[status] = counts.get(status, 0) + 1
        return counts

    def recent_notes(self, limit: int = 10) -> list[NoteFile]:
        state = self._load_state()
        items = list(state.get("notes", {}).items())[-limit:]
        return [
            NoteFile(
                note_id=note_id,
                title=str(info.get("title") or note_id),
                path=self.root_dir / info["path"],
                status=str(info.get("status") or "inbox"),
            )
            for note_id, info in reversed(items)
        ]

    def migrate_legacy_vault(self) -> int:
        state = self._load_state()
        migrated = 0
        for note_id, info in list(state.get("notes", {}).items()):
            relpath = str(info.get("path") or "")
            if not relpath:
                continue
            old_path = self.root_dir / relpath
            if not old_path.exists() or old_path.suffix.lower() != ".md":
                continue
            metadata, body = _extract_frontmatter(old_path.read_text(encoding="utf-8"))
            is_legacy_path = relpath.split("/", 1)[0] in LEGACY_PARA_FOLDERS
            old_title = old_path.stem
            if not is_legacy_path and not _needs_catalog_normalization(old_title):
                continue
            created_at = str(metadata.get("created_at") or metadata.get("date_created") or utc_now_iso())
            text_for_catalog = _clean_legacy_body(body) or old_path.stem
            legacy_enrichment = _legacy_enrichment_from_text(old_path.stem, text_for_catalog)
            capture = CaptureInput(capture_type="text", text=text_for_catalog.strip(), created_at=created_at)
            catalog = _catalog_plan(capture, legacy_enrichment)
            new_path = self._unique_note_path(catalog.folder, catalog.category, f"{catalog.title}.md")
            if new_path == old_path:
                continue
            new_metadata = {
                "aliases": catalog.aliases,
                "tags": catalog.tags,
                "type": catalog.note_type,
                "status": catalog.status,
                "date_created": created_at[:10],
            }
            new_body = self._render_capture_body(capture, enrichment=legacy_enrichment, related_notes=[], catalog=catalog)
            self._write_note(new_path, new_metadata, new_body)
            self._remove_moc_link(old_title)
            self._ensure_moc(catalog, note_title=catalog.title)
            old_path.unlink()
            self._record_state(note_id, new_path, title=catalog.title, status=catalog.status)
            migrated += 1
        for legacy_folder in LEGACY_PARA_FOLDERS:
            legacy_path = self.root_dir / legacy_folder
            if legacy_path.exists():
                try:
                    legacy_path.rmdir()
                except OSError:
                    pass
        self._cleanup_empty_mocs_and_dirs()
        return migrated

    def _render_capture_body(
        self,
        capture: CaptureInput,
        *,
        enrichment: AIEnrichment | None,
        related_notes: list[RelatedNoteSuggestion],
        catalog: CatalogPlan,
    ) -> str:
        parts = ["# " + catalog.title, "", f"Parent: [[{catalog.parent_moc}]]", ""]
        link_targets = [link for link in catalog.related_links if link and link != catalog.parent_moc]
        if enrichment and enrichment.action_items and "Plans to Do MOC" not in link_targets:
            link_targets.append("Plans to Do MOC")
        if link_targets:
            parts.extend(["Related: " + ", ".join(f"[[{link}]]" for link in link_targets), ""])
        summary = (enrichment.summary if enrichment and enrichment.summary else capture.text.strip()[:280]).strip()
        if summary:
            parts.extend(["## Executive Summary", summary, ""])
        if capture.attachment_relpath:
            parts.extend(["## Attachment", f"![[{capture.attachment_relpath}]]", ""])
        if enrichment and _should_render_polished_text(enrichment.polished_text, capture.text):
            parts.extend(["## Polished Capture", enrichment.polished_text.strip(), ""])
        parts.extend(["## Source Capture", capture.text.strip() or "_No text caption._", ""])
        if enrichment:
            parts.extend(
                [
                    "## Catalog",
                    f"- Type: {catalog.note_type}",
                    f"- Status: {catalog.status}",
                    f"- MOC: [[{catalog.parent_moc}]]",
                    "- Tags: " + ", ".join(catalog.tags or []),
                    "- Entities: " + ", ".join(enrichment.entities or []),
                    "",
                ]
            )
            if enrichment.action_items:
                parts.extend(["### Action Items", *[f"- {item}" for item in enrichment.action_items], ""])
            if enrichment.questions:
                parts.extend(["### Questions", *[f"- {item}" for item in enrichment.questions], ""])
            if enrichment.enrichment_notes:
                parts.extend(["### Useful Context", *[f"- {item}" for item in enrichment.enrichment_notes], ""])
            if enrichment.scored_suggestions:
                parts.append("### Scored Suggestions")
                for item in enrichment.scored_suggestions:
                    title = item.get("title", "")
                    score = item.get("score", "")
                    reason = item.get("reason", "")
                    line = f"- {title} (Score: {score}/100)"
                    if reason:
                        line += f" - {reason}"
                    parts.append(line)
                parts.append("")
        if related_notes:
            parts.append(_render_related_notes(related_notes).rstrip())
        return "\n".join(parts).rstrip() + "\n"

    def _ensure_moc(self, catalog: CatalogPlan, *, note_title: str) -> None:
        moc_path = self.root_dir / catalog.folder / sanitize_filename(catalog.category) / f"{sanitize_filename(catalog.parent_moc)}.md"
        metadata = {
            "aliases": [catalog.category],
            "tags": _normalize_tags([*catalog.tags[:2], "#moc"]),
            "type": "MOC",
            "status": "Active",
            "date_created": utc_now_iso()[:10],
        }
        if moc_path.exists():
            existing_metadata, body = _extract_frontmatter(moc_path.read_text(encoding="utf-8"))
            if isinstance(existing_metadata, dict) and existing_metadata:
                metadata = {**metadata, **{key: existing_metadata.get(key, value) for key, value in metadata.items()}}
        else:
            body = "\n".join(
                [
                    f"# {catalog.parent_moc}",
                    "",
                    f"Purpose: {catalog.moc_description}",
                    "",
                    "## Notes",
                    "",
                ]
            )
        link = f"[[{note_title}]]"
        if link not in body:
            description = catalog.moc_description.rstrip(".")
            body = body.rstrip() + f"\n- {link} - {description}.\n"
        self._write_note(moc_path, metadata, body)

    def _remove_moc_link(self, note_title: str) -> None:
        if not note_title:
            return
        escaped = re.escape(f"[[{note_title}]]")
        for moc_path in self.root_dir.rglob("* MOC.md"):
            metadata, body = _extract_frontmatter(moc_path.read_text(encoding="utf-8"))
            updated = re.sub(rf"^- .*{escaped}.*(?:\n|$)", "", body, flags=re.M)
            if updated != body:
                self._write_note(moc_path, metadata, updated)

    def _cleanup_empty_mocs_and_dirs(self) -> None:
        for moc_path in sorted(self.root_dir.rglob("* MOC.md")):
            _metadata, body = _extract_frontmatter(moc_path.read_text(encoding="utf-8"))
            if "- [[" in body:
                continue
            moc_path.unlink()
        for directory in sorted([path for path in self.root_dir.rglob("*") if path.is_dir()], key=lambda item: len(item.parts), reverse=True):
            if directory.name == "Attachments":
                continue
            try:
                directory.rmdir()
            except OSError:
                pass

    def _write_note(self, path: Path, metadata: dict[str, Any], body: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = _render_frontmatter(metadata) + "\n" + body
        tmp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
        tmp_path.write_text(payload, encoding="utf-8")
        os.replace(tmp_path, path)

    def _unique_note_path(self, folder: str, category: str, filename: str) -> Path:
        root = _safe_para_folder(folder)
        category_name = sanitize_filename(category or "General")
        raw_name = str(filename)
        stem = raw_name[:-3] if raw_name.lower().endswith(".md") else raw_name
        base = self.root_dir / root / category_name / sanitize_filename(stem)
        suffix = Path(filename).suffix or ".md"
        candidate = base.parent / f"{base.name}{suffix}"
        counter = 2
        while candidate.exists():
            candidate = base.parent / f"{base.name} {counter}{suffix}"
            counter += 1
        return candidate

    def _load_state(self) -> dict[str, Any]:
        if not self.state_file.exists():
            return {"notes": {}}
        try:
            data = json.loads(self.state_file.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                data.setdefault("notes", {})
                return data
        except Exception:
            return {"notes": {}}
        return {"notes": {}}

    def _record_state(self, note_id: str, path: Path, *, title: str, status: str) -> None:
        state = self._load_state()
        state.setdefault("notes", {})[note_id] = {
            "path": path.relative_to(self.root_dir).as_posix(),
            "title": title,
            "status": status,
            "updated_at": utc_now_iso(),
        }
        tmp_path = self.state_file.with_name(f"{self.state_file.name}.{os.getpid()}.tmp")
        tmp_path.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(tmp_path, self.state_file)


def _render_frontmatter(metadata: dict[str, Any]) -> str:
    lines = ["---"]
    for key, value in metadata.items():
        if isinstance(value, list):
            lines.append(f"{key}: {_yaml_list([str(item) for item in value])}")
        else:
            lines.append(f"{key}: {_yaml_scalar(value)}")
    lines.append("---")
    return "\n".join(lines)


def _render_related_notes(related_notes: list[RelatedNoteSuggestion]) -> str:
    lines = ["## Related Notes"]
    for item in related_notes:
        lines.append(f"- [[{item.title}]] - {item.reason} ({item.confidence:.2f})")
    return "\n".join(lines) + "\n"


def _catalog_plan(capture: CaptureInput, enrichment: AIEnrichment | None) -> CatalogPlan:
    raw_text = capture.text or ""
    title = sanitize_filename((enrichment.title if enrichment else "") or _title_from_text(raw_text), max_length=96)
    if _looks_generic_title(title):
        title = _descriptive_title_from_text(raw_text, fallback=title)
    folder = _safe_para_folder(enrichment.suggested_folder if enrichment else _infer_folder(raw_text))
    category = sanitize_filename((enrichment.moc_category if enrichment else "") or _moc_category_from_text(raw_text), max_length=64)
    parent_moc = sanitize_filename((enrichment.parent_moc if enrichment else "") or f"{category} MOC", max_length=80)
    if not parent_moc.endswith(" MOC"):
        parent_moc += " MOC"
    note_type = _safe_note_type(enrichment.note_type if enrichment else _infer_note_type(raw_text))
    status = _safe_note_status(enrichment.note_status if enrichment else _default_status(note_type))
    tags = _normalize_tags(enrichment.suggested_tags if enrichment and enrichment.suggested_tags else _tags_from_text(raw_text))
    aliases = _dedupe_strings([*(enrichment.aliases if enrichment else []), *([title] if title else [])])[:6]
    moc_description = (
        (enrichment.moc_description if enrichment and enrichment.moc_description else "").strip()
        or _moc_description(parent_moc)
    )
    related_links = _dedupe_strings(enrichment.related_links if enrichment else [])
    return CatalogPlan(
        title=title,
        folder=folder,
        category=category,
        parent_moc=parent_moc,
        aliases=aliases,
        tags=tags,
        note_type=note_type,
        status=status,
        moc_description=moc_description,
        related_links=related_links,
    )


def _safe_para_folder(value: str | None) -> str:
    value = str(value or "").strip()
    legacy_map = {
        "01_Projects": "1-Projects",
        "02_Areas": "2-Areas",
        "03_Resources": "3-Resources",
        "00_Inbox": "4-Incubator",
        "04_Daily": "2-Areas",
        "99_Archive": "3-Resources",
    }
    value = legacy_map.get(value, value)
    return value if value in PARA_FOLDERS and value != "Attachments" else "3-Resources"


def _safe_note_type(value: str | None) -> str:
    value = str(value or "").strip().title()
    return value if value in ALLOWED_TYPES and value != "MOC" else "Concept"


def _safe_note_status(value: str | None) -> str:
    value = str(value or "").strip().replace("organized", "Active").replace("inbox", "Incubating")
    value = value[:1].upper() + value[1:] if value else value
    return value if value in ALLOWED_STATUS and value != "needs_manual_review" else "Reference"


def _normalize_tags(values: list[str] | tuple[str, ...] | None) -> list[str]:
    result: list[str] = []
    for value in values or []:
        tag = str(value or "").strip().lower().replace(" ", "-")
        if not tag:
            continue
        if not tag.startswith("#"):
            tag = "#" + tag
        if tag not in result:
            result.append(tag)
    if not result:
        result = ["#knowledge", "#reference"]
    return result[:4]


def _dedupe_strings(values: list[str] | tuple[str, ...] | None) -> list[str]:
    result: list[str] = []
    for value in values or []:
        text = str(value or "").strip()
        if text and text not in result:
            result.append(text)
    return result


def _looks_generic_title(title: str) -> bool:
    lowered = title.lower()
    return _needs_catalog_normalization(title)


def _needs_catalog_normalization(title: str) -> bool:
    lowered = title.lower()
    return lowered.startswith("2026-") or lowered.endswith(" 2") or any(
        token in lowered
        for token in (
            "another potential",
            "potential earning",
            "micro saas acquisition via acquire",
            "a note",
            "state note",
            "untitled",
        )
    ) and "acquire.com - business idea" not in lowered


def _descriptive_title_from_text(text: str, *, fallback: str) -> str:
    lowered = (text or "").lower()
    if "amazon fba" in lowered and "wyoming" in lowered:
        return "Amazon FBA and Wyoming LLC - Business Idea"
    if "мікро-придбання" in lowered or "acquire.com" in lowered or "micro-acquisition" in lowered:
        return "Micro SaaS Acquisition via Acquire.com - Business Idea"
    if "дистрес" in lowered or "євробонди" in lowered or "eurobond" in lowered:
        return "Ukrainian Eurobonds Distressed Assets - Investment Idea"
    if "земл" in lowered and ("кордон" in lowered or "логісти" in lowered):
        return "EU Border Land Purchase for Logistics Hub"
    if "herman miller" in lowered and "embody" in lowered:
        return "Herman Miller Gaming Embody - Office Chair Purchase Evaluation"
    if "invincible" in lowered and "issue 79" in lowered:
        return "Invincible Comics - Start Reading at Issue 79"
    if "api key" in lowered and ("glm" in lowered or "groq" in lowered or "cerebras" in lowered):
        return "AI API Key Sources - GLM Cerebras and Groq"
    if "city" in lowered and ("live" in lowered or "living" in lowered):
        return "Potential Cities for Living - Research Plan"
    if "toloka" in lowered:
        return "Toloka - Tech Stock Investment Idea"
    if "scarf" in lowered or "шарф" in lowered:
        return "Head Scarf Under Coat - Purchase Idea"
    if "knife" in lowered:
        return "Knife Purchase Research and Wishlist Note"
    return sanitize_filename(fallback or _title_from_text(text), max_length=96)


def _infer_folder(text: str) -> str:
    lowered = (text or "").lower()
    if any(word in lowered for word in ("buy", "purchase", "wishlist", "idea", "strategy", "someday", "chair", "scarf", "knife")):
        return "4-Incubator"
    if any(word in lowered for word in ("plan", "deadline", "project")):
        return "1-Projects"
    if any(word in lowered for word in ("health", "investment", "wealth", "home")):
        return "2-Areas"
    return "3-Resources"


def _infer_note_type(text: str) -> str:
    lowered = (text or "").lower()
    if any(word in lowered for word in ("buy", "purchase", "wishlist", "scarf", "knife", "chair")):
        return "Purchase"
    if any(word in lowered for word in ("plan", "need to", "todo")):
        return "Plan"
    if any(word in lowered for word in ("idea", "strategy", "investment", "invest")):
        return "Idea"
    return "Concept"


def _default_status(note_type: str) -> str:
    return "Incubating" if note_type in {"Idea", "Purchase"} else "Reference"


def _moc_category_from_text(text: str) -> str:
    lowered = (text or "").lower()
    if any(word in lowered for word in ("api key", "glm", "groq", "cerebras", "modal")):
        return "AI Tools"
    if "invincible" in lowered or "comics" in lowered:
        return "Comics and Media"
    if any(word in lowered for word in ("acquire.com", "мікро-придбання", "fba", "логісти", "логістич", "business")):
        return "Business Ideas"
    if any(word in lowered for word in ("stock", "investment", "invest", "toloka", "etf")):
        return "Investments"
    if any(word in lowered for word in ("buy", "purchase", "wishlist", "scarf", "knife", "chair", "microphone")):
        return "Purchases"
    if any(word in lowered for word in ("earn", "business", "fba", "strategy", "money")):
        return "Business Ideas"
    if any(word in lowered for word in ("city", "living", "move")):
        return "Life Planning"
    if any(word in lowered for word in ("health", "mental", "workout")):
        return "Health"
    return "Knowledge"


def _tags_from_text(text: str) -> list[str]:
    category = _moc_category_from_text(text).lower().replace(" ", "-")
    note_type = _infer_note_type(text).lower()
    return [f"#{category}", f"#{note_type}"]


def _moc_description(parent_moc: str) -> str:
    descriptions = {
        "AI Tools MOC": "Tracks AI provider portals, model access, API keys, and setup references.",
        "Comics and Media MOC": "Tracks reading order, media references, and entertainment knowledge.",
        "Investments MOC": "Tracks investment ideas, risks, theses, and research notes.",
        "Purchases MOC": "Tracks potential purchases, buying criteria, comparisons, and follow-up decisions.",
        "Business Ideas MOC": "Tracks business and earning ideas that may become plans later.",
        "Life Planning MOC": "Tracks life-management decisions, location planning, and personal direction.",
        "Health MOC": "Tracks health, wellbeing, routines, and personal performance notes.",
    }
    return descriptions.get(parent_moc, "Indexes reference notes and reusable knowledge.")


def _legacy_enrichment_from_text(title: str, body: str) -> AIEnrichment:
    text = body or title
    catalog_title = _descriptive_title_from_text(text, fallback=title)
    category = _moc_category_from_text(text)
    note_type = _infer_note_type(text)
    return AIEnrichment(
        title=catalog_title,
        summary=re.sub(r"\s+", " ", text).strip()[:320],
        polished_text=re.sub(r"\s+", " ", text).strip(),
        suggested_folder=_infer_folder(text),
        suggested_tags=_tags_from_text(text),
        aliases=[title],
        note_type=note_type,
        note_status=_default_status(note_type),
        parent_moc=f"{category} MOC",
        moc_category=category,
        moc_description=_moc_description(f"{category} MOC"),
    )


def _clean_legacy_body(body: str) -> str:
    text = body or ""
    raw_match = re.search(r"## Raw Capture\n(?P<raw>.*?)(?=\n## AI Suggestions|\n## Catalog|\Z)", text, flags=re.S)
    if raw_match:
        text = raw_match.group("raw")
    source_match = re.search(r"## Source Capture\n(?P<raw>.*?)(?=\n## AI Suggestions|\n## Catalog|\Z)", text, flags=re.S)
    if source_match:
        text = source_match.group("raw")
    text = re.sub(r"^# .*$", "", text, flags=re.M)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _should_render_polished_text(polished_text: str, raw_text: str) -> bool:
    polished = (polished_text or "").strip()
    raw = (raw_text or "").strip()
    if not polished:
        return False
    return re.sub(r"\s+", " ", polished) != re.sub(r"\s+", " ", raw)


def _title_from_text(text: str) -> str:
    first_line = (text or "").strip().splitlines()[0] if (text or "").strip() else "Capture"
    return first_line[:80]


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
