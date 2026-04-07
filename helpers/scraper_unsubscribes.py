from __future__ import annotations

import asyncio
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import urlsplit

from helpers.runtime_paths import RUNTIME_DB_DIR, runtime_file

UNSUBSCRIBE_DB_FILE = runtime_file(RUNTIME_DB_DIR, "scraper_unsubscribes.db")
OLX_HOST_PART = "olx.ua"
SHAFA_HOST_PART = "shafa.ua"
ITEM_ID_RE = re.compile(r"(\d+)")
SHAFA_ITEM_SLUG_RE = re.compile(r"^\d{6,}(?:-[a-z0-9-]+)?$", re.IGNORECASE)
URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)


@dataclass(frozen=True)
class ReplyItemIdentity:
    source: str
    item_id: str
    link: str
    name: str


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(UNSUBSCRIBE_DB_FILE)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
    except Exception:
        pass
    return conn


def _init_sync() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS unsubscribed_items (
                source TEXT NOT NULL,
                item_id TEXT NOT NULL,
                link TEXT NOT NULL,
                name TEXT NOT NULL,
                unsubscribed_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (source, item_id)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_unsubscribed_items_source ON unsubscribed_items(source);"
        )
        conn.commit()


async def init_unsubscribe_db() -> None:
    await asyncio.to_thread(_init_sync)


def detect_source_from_link(link: str) -> Optional[str]:
    host = (urlsplit(link).netloc or "").lower()
    if OLX_HOST_PART in host:
        return "olx"
    if SHAFA_HOST_PART in host:
        return "shafa"
    return None


def extract_item_id(source: str, link: str) -> str:
    slug = link.rstrip("/").split("/")[-1].split("?", 1)[0]
    if source == "olx":
        return slug[:-5] if slug.endswith(".html") else slug
    if source == "shafa":
        if SHAFA_ITEM_SLUG_RE.match(slug) and (match := ITEM_ID_RE.match(slug)):
            return match.group(1)
        return slug
    return slug


def _iter_message_entities(message: Any) -> Iterable[Any]:
    for attr in ("caption_entities", "entities"):
        entities = getattr(message, attr, None) or []
        for entity in entities:
            yield entity


def _extract_link_from_message(message: Any) -> Optional[str]:
    text = getattr(message, "caption", None) or getattr(message, "text", None) or ""
    for entity in _iter_message_entities(message):
        entity_type = getattr(entity, "type", None)
        if entity_type == "text_link" and getattr(entity, "url", None):
            return str(entity.url).strip()
        if entity_type == "url":
            try:
                start = int(getattr(entity, "offset", 0))
                length = int(getattr(entity, "length", 0))
                candidate = text[start : start + length].strip()
                if candidate:
                    return candidate
            except Exception:
                continue
    if match := URL_RE.search(text):
        return match.group(0).strip()
    return None


def _extract_name_from_message_text(text: str) -> str:
    line = next((part.strip() for part in (text or "").splitlines() if part.strip()), "")
    if not line:
        return ""
    prefixes = ("✨", "OLX Price changed:", "OLX:", "🔁 Зміна ціни:", "SHAFA:")
    if line.startswith("✨") and line.endswith("✨") and len(line) > 2:
        line = line.strip("✨").strip()
    else:
        for prefix in prefixes:
            if line.startswith(prefix):
                line = line[len(prefix) :].strip(" ✨")
                break
    return " ".join(line.split())


def parse_reply_item_identity(reply_message: Any) -> Optional[ReplyItemIdentity]:
    if reply_message is None:
        return None
    link = _extract_link_from_message(reply_message)
    if not link:
        return None
    source = detect_source_from_link(link)
    if not source:
        return None
    item_id = extract_item_id(source, link)
    if not item_id:
        return None
    text = getattr(reply_message, "caption", None) or getattr(reply_message, "text", None) or ""
    name = _extract_name_from_message_text(text) or item_id
    return ReplyItemIdentity(source=source, item_id=item_id, link=link, name=name)


def _upsert_unsubscribed_item_sync(identity: ReplyItemIdentity) -> None:
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO unsubscribed_items (source, item_id, link, name, unsubscribed_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT(source, item_id) DO UPDATE SET
                link=excluded.link,
                name=excluded.name,
                unsubscribed_at=datetime('now')
            """,
            (identity.source, identity.item_id, identity.link, identity.name),
        )
        conn.commit()


async def unsubscribe_from_reply_message(message: Any) -> tuple[bool, str]:
    await init_unsubscribe_db()
    reply = getattr(message, "reply_to_message", None)
    identity = parse_reply_item_identity(reply)
    if identity is None:
        return False, "Reply to an OLX or Shafa item message with /unsubscribe."
    await asyncio.to_thread(_upsert_unsubscribed_item_sync, identity)
    source_label = identity.source.upper()
    return True, f"🔕 Unsubscribed from {source_label} item updates forever:\n{identity.name}"


def _fetch_unsubscribed_ids_sync(source: str, item_ids: list[str]) -> set[str]:
    if not item_ids:
        return set()
    with _connect() as conn:
        placeholders = ",".join("?" * len(item_ids))
        rows = conn.execute(
            f"SELECT item_id FROM unsubscribed_items WHERE source = ? AND item_id IN ({placeholders})",
            [source, *item_ids],
        ).fetchall()
        return {str(row["item_id"]) for row in rows}


async def fetch_unsubscribed_ids(source: str, item_ids: list[str]) -> set[str]:
    await init_unsubscribe_db()
    return await asyncio.to_thread(_fetch_unsubscribed_ids_sync, source, item_ids)
