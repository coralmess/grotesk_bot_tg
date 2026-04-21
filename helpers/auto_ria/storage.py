from __future__ import annotations

import sqlite3
from pathlib import Path


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
                    sent_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.commit()

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

    def mark_sent(self, *, car_id: str, title: str, url: str, price_usd: int) -> None:
        # The bot only sends first-seen cars for now, so persisting the sent car id is the
        # minimal state needed to prevent repeats across future polling cycles and restarts.
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO auto_ria_items (car_id, title, url, price_usd, sent_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (car_id, title, url, price_usd),
            )
            conn.commit()
