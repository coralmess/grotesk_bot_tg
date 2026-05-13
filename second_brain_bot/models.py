from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NoteRecord:
    note_id: str
    title: str
    path: str
    tags: list[str]
    entities: list[str]
    body: str
    status: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class SearchResult:
    note_id: str
    title: str
    path: str
    tags: list[str]
    entities: list[str]
    body: str
    status: str


@dataclass(frozen=True)
class RelationRecord:
    source_note_id: str
    target_note_id: str
    target_title: str
    reason: str
    confidence: float


@dataclass(frozen=True)
class ActionRecord:
    note_id: str
    action_text: str
    source_title: str
    source_path: str
    status: str = "open"
    priority: int = 50
