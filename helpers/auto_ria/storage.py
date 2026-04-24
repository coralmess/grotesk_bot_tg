from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AutoRiaSentItem:
    car_id: str
    title: str
    url: str
    price_usd: int
    message_id: int | None
    message_kind: str
    caption: str


class AutoRiaStorage:
    def __init__(self, db_path: Path) -> None:
        self._db_path = Path(db_path)

    def create_tables(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS auto_ria_items (
                    car_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    url TEXT NOT NULL,
                    price_usd INTEGER NOT NULL,
                    message_id INTEGER,
                    message_kind TEXT NOT NULL DEFAULT 'photo',
                    caption TEXT NOT NULL DEFAULT '',
                    sold_at TEXT,
                    sent_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self._ensure_column(conn, "message_id", "INTEGER")
            self._ensure_column(conn, "message_kind", "TEXT NOT NULL DEFAULT 'photo'")
            self._ensure_column(conn, "caption", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(conn, "sold_at", "TEXT")
            conn.commit()

    def _ensure_column(self, conn: sqlite3.Connection, column_name: str, ddl: str) -> None:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(auto_ria_items)").fetchall()}
        if column_name not in columns:
            conn.execute(f"ALTER TABLE auto_ria_items ADD COLUMN {column_name} {ddl}")

    def fetch_seen_ids(self, car_ids: list[str]) -> set[str]:
        if not car_ids:
            return set()
        placeholders = ",".join("?" for _ in car_ids)
        with sqlite3.connect(self._db_path) as conn:
            rows = conn.execute(
                f"SELECT car_id FROM auto_ria_items WHERE car_id IN ({placeholders})",
                car_ids,
            ).fetchall()
        return {row[0] for row in rows}

    def mark_sent(
        self,
        *,
        car_id: str,
        title: str,
        url: str,
        price_usd: int,
        message_id: int | None = None,
        message_kind: str = "photo",
        caption: str = "",
    ) -> None:
        # The bot only sends first-seen cars for now, so persisting the sent car id is the
        # minimal state needed to prevent repeats while message_id/caption enable sold edits.
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO auto_ria_items
                    (car_id, title, url, price_usd, message_id, message_kind, caption, sold_at, sent_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, NULL, CURRENT_TIMESTAMP)
                """,
                (car_id, title, url, price_usd, message_id, message_kind or "photo", caption or ""),
            )
            conn.commit()

    def fetch_active_sent_items(self) -> list[AutoRiaSentItem]:
        with sqlite3.connect(self._db_path) as conn:
            rows = conn.execute(
                """
                SELECT car_id, title, url, price_usd, message_id, message_kind, caption
                FROM auto_ria_items
                WHERE sold_at IS NULL
                """
            ).fetchall()
        return [
            AutoRiaSentItem(
                car_id=row[0],
                title=row[1],
                url=row[2],
                price_usd=row[3],
                message_id=row[4],
                message_kind=row[5] or "photo",
                caption=row[6] or "",
            )
            for row in rows
        ]

    def mark_sold(self, *, car_id: str) -> None:
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                "UPDATE auto_ria_items SET sold_at = CURRENT_TIMESTAMP WHERE car_id = ?",
                (car_id,),
            )
            conn.commit()
