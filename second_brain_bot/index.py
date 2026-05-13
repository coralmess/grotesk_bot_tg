from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from helpers.sqlite_runtime import apply_runtime_pragmas
from second_brain_bot.models import ActionRecord, NoteRecord, RelationRecord, SearchResult


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
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS actions (
                    note_id TEXT NOT NULL,
                    action_text TEXT NOT NULL,
                    source_title TEXT NOT NULL,
                    source_path TEXT NOT NULL,
                    status TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    PRIMARY KEY (note_id, action_text)
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

    def deep_search(self, query: str, *, limit: int = 10, exclude_note_id: str | None = None) -> list[SearchResult]:
        query = (query or "").strip()
        if not query:
            return []
        ranked: dict[str, tuple[int, SearchResult]] = {}

        def add(result: SearchResult, score: int) -> None:
            if exclude_note_id and result.note_id == exclude_note_id:
                return
            current = ranked.get(result.note_id)
            if current is None or score > current[0]:
                ranked[result.note_id] = (score, result)

        for item in self._exact_search(query, limit=max(limit, 10), exclude_note_id=exclude_note_id):
            add(item, 300)
        for item in self.search(query, limit=max(limit, 10), exclude_note_id=exclude_note_id):
            add(item, 200)

        seed_ids = [item.note_id for _score, item in sorted(ranked.values(), key=lambda pair: pair[0], reverse=True)[:limit]]
        if seed_ids:
            for item in self._related_notes(seed_ids, limit=max(limit, 10), exclude_note_id=exclude_note_id):
                add(item, 100)

        ordered = [item for _score, item in sorted(ranked.values(), key=lambda pair: pair[0], reverse=True)]
        return ordered[: int(limit)]

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

    def upsert_actions_for_note(self, note_id: str, actions: list[ActionRecord]) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM actions WHERE note_id = ?", (note_id,))
            for action in actions:
                conn.execute(
                    """
                    INSERT INTO actions(note_id, action_text, source_title, source_path, status, priority)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(note_id, action_text) DO UPDATE SET
                        source_title=excluded.source_title,
                        source_path=excluded.source_path,
                        status=excluded.status,
                        priority=excluded.priority
                    """,
                    (
                        action.note_id,
                        action.action_text,
                        action.source_title,
                        action.source_path,
                        action.status,
                        int(action.priority),
                    ),
                )

    def search_actions(self, query: str = "", *, limit: int = 10, status: str = "open") -> list[ActionRecord]:
        terms = [part.strip().lower() for part in (query or "").split() if len(part.strip()) >= 3][:8]
        with self._connect() as conn:
            if terms and not _looks_generic_task_query(query):
                clauses = []
                params: list[object] = []
                for term in terms:
                    like = f"%{term}%"
                    clauses.append("(lower(action_text) LIKE ? OR lower(source_title) LIKE ? OR lower(source_path) LIKE ?)")
                    params.extend([like, like, like])
                params.extend([status, int(limit)])
                rows = conn.execute(
                    f"""
                    SELECT *
                    FROM actions
                    WHERE ({' OR '.join(clauses)})
                      AND status = ?
                    ORDER BY priority DESC, source_title ASC
                    LIMIT ?
                    """,
                    tuple(params),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM actions
                    WHERE status = ?
                    ORDER BY priority DESC, source_title ASC
                    LIMIT ?
                    """,
                    (status, int(limit)),
                ).fetchall()
        return [
            ActionRecord(
                note_id=row["note_id"],
                action_text=row["action_text"],
                source_title=row["source_title"],
                source_path=row["source_path"],
                status=row["status"],
                priority=int(row["priority"]),
            )
            for row in rows
        ]

    def _exact_search(self, query: str, *, limit: int, exclude_note_id: str | None) -> list[SearchResult]:
        terms = [part.strip().lower() for part in query.split() if part.strip()][:8]
        if not terms:
            return []
        clauses = []
        params: list[object] = []
        for term in terms:
            like = f"%{term}%"
            clauses.append(
                "(lower(title) LIKE ? OR lower(path) LIKE ? OR lower(tags_json) LIKE ? OR lower(entities_json) LIKE ?)"
            )
            params.extend([like, like, like, like])
        params.extend([exclude_note_id, exclude_note_id, int(limit)])
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM notes
                WHERE {' OR '.join(clauses)}
                  AND (? IS NULL OR note_id != ?)
                ORDER BY created_at DESC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
        return [self._row_to_result(row) for row in rows]

    def _related_notes(self, note_ids: list[str], *, limit: int, exclude_note_id: str | None) -> list[SearchResult]:
        note_ids = [note_id for note_id in note_ids if note_id]
        if not note_ids:
            return []
        placeholders = ",".join("?" for _ in note_ids)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT DISTINCT notes.*
                FROM relations
                JOIN notes
                  ON notes.note_id = relations.target_note_id
                  OR notes.note_id = relations.source_note_id
                WHERE (relations.source_note_id IN ({placeholders}) OR relations.target_note_id IN ({placeholders}))
                  AND notes.note_id NOT IN ({placeholders})
                  AND (? IS NULL OR notes.note_id != ?)
                ORDER BY relations.confidence DESC
                LIMIT ?
                """,
                tuple([*note_ids, *note_ids, *note_ids, exclude_note_id, exclude_note_id, int(limit)]),
            ).fetchall()
        return [self._row_to_result(row) for row in rows]

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


def _looks_generic_task_query(query: str) -> bool:
    lowered = (query or "").lower()
    return any(
        phrase in lowered
        for phrase in ("what should i do", "what do i need", "need to do", "things i need", "tasks", "actions")
    )
