from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from helpers.sqlite_runtime import apply_runtime_pragmas
from second_brain_bot.models import NoteRecord, RelationRecord, SearchResult


class SecondBrainIndex:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        apply_runtime_pragmas(conn)
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS notes (
                    note_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    path TEXT NOT NULL,
                    tags_json TEXT NOT NULL,
                    entities_json TEXT NOT NULL,
                    body TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS notes_fts
                USING fts5(note_id UNINDEXED, title, tags, entities, body)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS relations (
                    source_note_id TEXT NOT NULL,
                    target_note_id TEXT NOT NULL,
                    target_title TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    PRIMARY KEY (source_note_id, target_note_id)
                )
                """
            )

    def upsert_note(self, note: NoteRecord) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO notes (
                    note_id, title, path, tags_json, entities_json, body, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(note_id) DO UPDATE SET
                    title=excluded.title,
                    path=excluded.path,
                    tags_json=excluded.tags_json,
                    entities_json=excluded.entities_json,
                    body=excluded.body,
                    status=excluded.status,
                    updated_at=excluded.updated_at
                """,
                (
                    note.note_id,
                    note.title,
                    note.path,
                    json.dumps(note.tags, ensure_ascii=False),
                    json.dumps(note.entities, ensure_ascii=False),
                    note.body,
                    note.status,
                    note.created_at,
                    note.updated_at,
                ),
            )
            # Keep FTS as an explicit mirror so note rewrites and moves cannot leave stale
            # searchable text behind.
            conn.execute("DELETE FROM notes_fts WHERE note_id = ?", (note.note_id,))
            conn.execute(
                "INSERT INTO notes_fts(note_id, title, tags, entities, body) VALUES (?, ?, ?, ?, ?)",
                (note.note_id, note.title, " ".join(note.tags), " ".join(note.entities), note.body),
            )

    def get_note(self, note_id: str) -> SearchResult | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM notes WHERE note_id = ?", (note_id,)).fetchone()
        if row is None:
            return None
        return self._row_to_result(row)

    def search(self, query: str, *, limit: int = 10, exclude_note_id: str | None = None) -> list[SearchResult]:
        query = (query or "").strip()
        if not query:
            return []
        fts_query = _fts_query(query)
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT notes.*
                FROM notes_fts
                JOIN notes ON notes.note_id = notes_fts.note_id
                WHERE notes_fts MATCH ?
                  AND (? IS NULL OR notes.note_id != ?)
                ORDER BY bm25(notes_fts)
                LIMIT ?
                """,
                (fts_query, exclude_note_id, exclude_note_id, int(limit)),
            ).fetchall()
        return [self._row_to_result(row) for row in rows]

    def recent_notes(self, *, limit: int = 10, status: str | None = None) -> list[SearchResult]:
        with self._connect() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM notes WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                    (status, int(limit)),
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM notes ORDER BY created_at DESC LIMIT ?", (int(limit),)).fetchall()
        return [self._row_to_result(row) for row in rows]

    def stale_inbox(self, *, limit: int = 20) -> list[SearchResult]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM notes WHERE status IN ('Incubating', 'needs_manual_review') ORDER BY created_at ASC LIMIT ?",
                (int(limit),),
            ).fetchall()
        return [self._row_to_result(row) for row in rows]

    def upsert_relation(self, relation: RelationRecord) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO relations(source_note_id, target_note_id, target_title, reason, confidence)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(source_note_id, target_note_id) DO UPDATE SET
                    target_title=excluded.target_title,
                    reason=excluded.reason,
                    confidence=excluded.confidence
                """,
                (
                    relation.source_note_id,
                    relation.target_note_id,
                    relation.target_title,
                    relation.reason,
                    float(relation.confidence),
                ),
            )

    def relations_for(self, note_id: str) -> list[RelationRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM relations WHERE source_note_id = ? ORDER BY confidence DESC",
                (note_id,),
            ).fetchall()
        return [
            RelationRecord(
                source_note_id=row["source_note_id"],
                target_note_id=row["target_note_id"],
                target_title=row["target_title"],
                reason=row["reason"],
                confidence=float(row["confidence"]),
            )
            for row in rows
        ]

    @staticmethod
    def _row_to_result(row: sqlite3.Row) -> SearchResult:
        return SearchResult(
            note_id=row["note_id"],
            title=row["title"],
            path=row["path"],
            tags=json.loads(row["tags_json"] or "[]"),
            entities=json.loads(row["entities_json"] or "[]"),
            body=row["body"],
            status=row["status"],
        )


def _fts_query(query: str) -> str:
    parts = [part.replace('"', "") for part in query.split() if part.strip()]
    if not parts:
        return '""'
    return " OR ".join(f'"{part}"' for part in parts[:8])
