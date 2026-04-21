from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path
from typing import Any

import aiosqlite

from helpers.sqlite_runtime import RUNTIME_DB_PRAGMA_STATEMENTS


class LystStorage:
    """Runtime storage adapter for the Lyst scraper state."""

    def __init__(self, *, db_name: str, shoe_data_file: Path, logger) -> None:
        # Lyst keeps a small amount of local state across runs, so storage needs one
        # cohesive adapter instead of ad-hoc sqlite helpers spread across the monolith.
        self.db_name = db_name
        self.shoe_data_file = shoe_data_file
        self.logger = logger
        self.db_semaphore = asyncio.Semaphore(1)
        self._pragma_statements = ["PRAGMA foreign_keys = ON", *RUNTIME_DB_PRAGMA_STATEMENTS]

    def connect_db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_name, timeout=30.0)
        for stmt in self._pragma_statements:
            conn.execute(stmt)
        return conn

    def create_tables(self) -> None:
        conn = self.connect_db()
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS shoes (
                key TEXT PRIMARY KEY, name TEXT, unique_id TEXT,
                original_price TEXT, sale_price TEXT, image_url TEXT,
                store TEXT, country TEXT, shoe_link TEXT,
                lowest_price TEXT, lowest_price_uah REAL,
                uah_price REAL, active INTEGER);
            CREATE TABLE IF NOT EXISTS processed_shoes (
                key TEXT PRIMARY KEY, active INTEGER DEFAULT 1);
            CREATE INDEX IF NOT EXISTS idx_processed_shoes_active
                ON processed_shoes(key) WHERE active = 1;
            CREATE INDEX IF NOT EXISTS idx_shoe_active ON shoes (active, country, uah_price);
            """
        )
        conn.commit()
        conn.close()

    async def db_operation_with_retry(self, operation_func, max_retries: int = 3):
        async with self.db_semaphore:
            for attempt in range(max_retries):
                try:
                    async with aiosqlite.connect(self.db_name, timeout=30.0) as conn:
                        for stmt in self._pragma_statements:
                            await conn.execute(stmt)
                        return await operation_func(conn)
                except Exception as exc:
                    if "database is locked" in str(exc).lower() and attempt < max_retries - 1:
                        self.logger.warning(
                            "Database locked, retrying in %s seconds (attempt %s/%s)",
                            2**attempt,
                            attempt + 1,
                            max_retries,
                        )
                        await asyncio.sleep(2**attempt)
                        continue
                    raise

    async def is_shoe_processed(self, key: str) -> bool:
        async def _operation(conn):
            async with conn.execute("SELECT 1 FROM processed_shoes WHERE key = ?", (key,)) as cursor:
                return await cursor.fetchone() is not None

        return await self.db_operation_with_retry(_operation)

    async def mark_shoe_processed(self, key: str) -> None:
        async def _operation(conn):
            await conn.execute("INSERT OR IGNORE INTO processed_shoes(key, active) VALUES (?, 1)", (key,))
            await conn.commit()

        await self.db_operation_with_retry(_operation)

    def load_shoe_data_from_db(self) -> dict[str, dict[str, Any]]:
        conn = self.connect_db()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM shoes")
        data = {
            row[0]: {
                "name": row[1],
                "unique_id": row[2],
                "original_price": row[3],
                "sale_price": row[4],
                "image_url": row[5],
                "store": row[6],
                "country": row[7],
                "shoe_link": row[8],
                "lowest_price": row[9],
                "lowest_price_uah": row[10],
                "uah_price": row[11],
                "active": bool(row[12]),
            }
            for row in cursor.fetchall()
        }
        conn.close()
        return data

    def load_shoe_data_from_json(self) -> dict[str, Any]:
        try:
            with self.shoe_data_file.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    async def save_shoe_data_bulk(self, shoes: list[dict[str, Any]]) -> None:
        async def _operation(conn):
            data = [
                (
                    s["key"],
                    s["name"],
                    s["unique_id"],
                    s["original_price"],
                    s["sale_price"],
                    s["image_url"],
                    s["store"],
                    s["country"],
                    s.get("shoe_link", ""),
                    s.get("lowest_price", ""),
                    s.get("lowest_price_uah", 0.0),
                    s.get("uah_price", 0.0),
                    1 if s.get("active", True) else 0,
                )
                for s in shoes
            ]
            await conn.executemany(
                """
                INSERT OR REPLACE INTO shoes (
                    key, name, unique_id, original_price, sale_price,
                    image_url, store, country, shoe_link, lowest_price,
                    lowest_price_uah, uah_price, active
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                data,
            )
            await conn.commit()

        await self.db_operation_with_retry(_operation)

    async def async_save_shoe_data(self, shoe_data: dict[str, dict[str, Any]]) -> None:
        shoes = [dict(shoe, key=key) for key, shoe in shoe_data.items()]
        await self.save_shoe_data_bulk(shoes)

    async def migrate_json_to_sqlite(self) -> None:
        async def _operation(conn):
            async with conn.execute("SELECT COUNT(*) FROM shoes") as cursor:
                return (await cursor.fetchone())[0]

        if await self.db_operation_with_retry(_operation) == 0:
            data = self.load_shoe_data_from_json()
            if data:
                await self.async_save_shoe_data(data)

    async def load_shoe_data(self) -> dict[str, dict[str, Any]]:
        self.create_tables()
        await self.migrate_json_to_sqlite()
        # Keep sync sqlite reads off the event loop because Lyst already has enough
        # blocking pressure from external sites and browser automation.
        return await asyncio.to_thread(self.load_shoe_data_from_db)

    async def save_shoe_data(self, data: dict[str, dict[str, Any]]) -> None:
        await self.async_save_shoe_data(data)
