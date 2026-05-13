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
    "00_Inbox",
    "01_Projects",
    "02_Areas",
    "03_Resources",
    "04_Daily",
    "99_Archive",
    "Attachments",
)
ALLOWED_STATUS = {"inbox", "organized", "needs_manual_review"}


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
        title = sanitize_filename((enrichment.title if enrichment else "") or _title_from_text(capture.text))
        path = self._unique_note_path("00_Inbox", f"{created_at[:10]} {title}.md")
        metadata = {
            "id": note_id,
            "created_at": created_at,
            "updated_at": created_at,
            "status": "inbox",
            "capture_type": capture.capture_type,
            "source": "telegram",
            "telegram_message_id": capture.telegram_message_id or "",
            "tags": ["inbox", "capture", *(enrichment.suggested_tags if enrichment else [])],
            "entities": enrichment.entities if enrichment else [],
            "ai_suggested_title": enrichment.title if enrichment else "",
            "ai_suggested_folder": enrichment.suggested_folder if enrichment else "",
            "ai_suggested_tags": enrichment.suggested_tags if enrichment else [],
            "ai_summary": enrichment.summary if enrichment else "",
            "ai_enrichment_notes": enrichment.enrichment_notes if enrichment else [],
            "related_notes": [item.note_id for item in related_notes or []],
        }
        body = self._render_capture_body(capture, enrichment=enrichment, related_notes=related_notes or [])
        self._write_note(path, metadata, body)
        self._record_state(note_id, path, title=title, status="inbox")
        return NoteFile(note_id=note_id, title=title, path=path, status="inbox")

    def read_note(self, note_id: str) -> tuple[dict[str, Any], str, Path]:
        state = self._load_state()
        info = state.get("notes", {}).get(note_id)
        if not info:
            raise KeyError(note_id)
        path = self.root_dir / info["path"]
        data, body = _extract_frontmatter(path.read_text(encoding="utf-8"))
        return data, body, path

    def accept_suggestion(self, note_id: str) -> NoteFile:
        metadata, body, old_path = self.read_note(note_id)
        title = sanitize_filename(metadata.get("ai_suggested_title") or metadata.get("id") or note_id)
        folder = str(metadata.get("ai_suggested_folder") or "03_Resources")
        if folder not in PARA_FOLDERS or folder == "Attachments":
            folder = "03_Resources"
        tags = metadata.get("tags")
        if not isinstance(tags, list):
            tags = []
        for tag in metadata.get("ai_suggested_tags") or []:
            if tag not in tags:
                tags.append(tag)
        metadata["tags"] = tags
        metadata["status"] = "organized"
        metadata["updated_at"] = utc_now_iso()
        new_path = self._unique_note_path(folder, f"{title}.md")
        self._write_note(new_path, metadata, body)
        if old_path != new_path and old_path.exists():
            old_path.unlink()
        self._record_state(note_id, new_path, title=title, status="organized")
        return NoteFile(note_id=note_id, title=title, path=new_path, status="organized")

    def mark_status(self, note_id: str, status: str) -> NoteFile:
        if status not in ALLOWED_STATUS:
            raise ValueError(f"unsupported note status: {status}")
        metadata, body, path = self.read_note(note_id)
        metadata["status"] = status
        metadata["updated_at"] = utc_now_iso()
        self._write_note(path, metadata, body)
        title = sanitize_filename(metadata.get("ai_suggested_title") or path.stem)
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
        metadata["related_notes"] = [item.note_id for item in related_notes]
        metadata["updated_at"] = utc_now_iso()
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
        metadata["updated_at"] = utc_now_iso()
        self._write_note(path, metadata, body)

    def write_daily_digest(self, date_key: str, content: str) -> Path:
        path = self.root_dir / "04_Daily" / f"{date_key}.md"
        metadata = {
            "id": f"daily-{date_key}",
            "created_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
            "status": "organized",
            "capture_type": "digest",
            "source": "second_brain",
            "tags": ["daily", "digest"],
            "entities": [],
            "related_notes": [],
        }
        self._write_note(path, metadata, content)
        return path

    def note_counts(self) -> dict[str, int]:
        state = self._load_state()
        counts = {"total": 0, "inbox": 0, "organized": 0, "needs_manual_review": 0}
        for info in state.get("notes", {}).values():
            status = info.get("status", "inbox")
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

    def _render_capture_body(
        self,
        capture: CaptureInput,
        *,
        enrichment: AIEnrichment | None,
        related_notes: list[RelatedNoteSuggestion],
    ) -> str:
        parts = ["# " + sanitize_filename((enrichment.title if enrichment else "") or _title_from_text(capture.text)), ""]
        if capture.attachment_relpath:
            parts.extend(["## Attachment", f"![[{capture.attachment_relpath}]]", ""])
        parts.extend(["## Raw Capture", capture.text.strip() or "_No text caption._", ""])
        if enrichment:
            parts.extend(
                [
                    "## AI Suggestions",
                    f"Summary: {enrichment.summary}",
                    f"Suggested folder: `{enrichment.suggested_folder}`",
                    "Suggested tags: " + ", ".join(enrichment.suggested_tags or []),
                    "Entities: " + ", ".join(enrichment.entities or []),
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

    def _write_note(self, path: Path, metadata: dict[str, Any], body: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = _render_frontmatter(metadata) + "\n" + body
        tmp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
        tmp_path.write_text(payload, encoding="utf-8")
        os.replace(tmp_path, path)

    def _unique_note_path(self, folder: str, filename: str) -> Path:
        base = self.root_dir / folder / sanitize_filename(Path(filename).stem)
        suffix = Path(filename).suffix or ".md"
        candidate = base.with_suffix(suffix)
        counter = 2
        while candidate.exists():
            candidate = candidate.with_name(f"{base.name} {counter}{suffix}")
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
