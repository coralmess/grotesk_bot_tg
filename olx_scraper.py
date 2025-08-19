from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from bs4 import BeautifulSoup
from telegram import Bot
from telegram.error import RetryAfter, TimedOut
from telegram.constants import ParseMode
from PIL import Image
import io
import asyncio
import re
import sqlite3
import aiohttp
import random
from html import escape

from config import OLX_URLS, TELEGRAM_OLX_BOT_TOKEN, DANYLO_DEFAULT_CHAT_ID

BASE_OLX = "https://www.olx.ua"
STORE_FILE = Path(__file__).with_name("olx_items.json")

# Concurrency controls
_HTTP_SEMAPHORE = asyncio.Semaphore(10)
_SEND_SEMAPHORE = asyncio.Semaphore(3)

# aiohttp session (shared)
_http_session: Optional[aiohttp.ClientSession] = None


def _get_http_session() -> aiohttp.ClientSession:
    global _http_session
    if _http_session is None or _http_session.closed:
        timeout = aiohttp.ClientTimeout(total=25)
        connector = aiohttp.TCPConnector(limit=20)
        _http_session = aiohttp.ClientSession(timeout=timeout, connector=connector)
    return _http_session


def _clean_token(value: Optional[str]) -> str:
    return (value or "").strip().strip("'\"")


@dataclass
class OlxItem:
    id: str
    name: str
    link: str
    price_text: str
    price_int: int
    state: str


def normalize_price(text: str) -> Tuple[str, int]:
    # Keep only digits for integer comparison and format back as "<int> грн"
    digits = re.sub(r"[^\d]", "", text or "")
    price_int = int(digits) if digits else 0
    return (f"{price_int} грн" if price_int else (text or "").strip()), price_int


def extract_id_from_link(link: str) -> str:
    # e.g. https://www.olx.ua/d/uk/obyavlenie/tufli-lodochki-firmy-agnona-IDP0w0I.html
    slug = link.rstrip("/").split("/")[-1]
    if slug.endswith(".html"):
        slug = slug[:-5]
    # strip query params if any
    slug = slug.split("?")[0]
    return slug


def parse_card(card) -> Optional[OlxItem]:
    try:
        a = card.find("a", href=True)
        href = a["href"] if a else None
        if not href:
            return None
        link = href if href.startswith("http") else f"{BASE_OLX}{href}"
        name_el = card.find(["h4", "h3"]) or card.find("img", alt=True)
        name = name_el.get_text(strip=True) if hasattr(name_el, "get_text") else (name_el["alt"].strip() if name_el else "")
        price_el = card.find("p", attrs={"data-testid": "ad-price"})
        price_text_raw = price_el.get_text(" ", strip=True) if price_el else ""
        price_text, price_int = normalize_price(price_text_raw)
        # state
        state = ""
        st = card.find("span", attrs={"title": True})
        if st and st.get("title"):
            state = str(st.get("title")).strip()
        elif st:
            state = st.get_text(strip=True)
        item_id = extract_id_from_link(link)
        if not (name and link and item_id):
            return None
        return OlxItem(id=item_id, name=name, link=link, price_text=price_text, price_int=price_int, state=state)
    except Exception:
        return None


def collect_cards_with_stop(soup: BeautifulSoup) -> List:
    cards: List = []
    for el in soup.find_all(True, recursive=True):
        classes = el.get("class") or []
        if isinstance(classes, str):
            classes = [classes]
        if "css-wsrviy" in classes:
            break
        if el.name == "div" and (el.get("data-cy") == "l-card" or el.get("data-testid") == "l-card"):
            cards.append(el)
    return cards


async def fetch_html(url: str, max_retries: int = 3) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
        "Accept-Language": "uk,ru;q=0.9,en;q=0.8",
    }

    last_error: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            async with _HTTP_SEMAPHORE:
                session = _get_http_session()
                async with session.get(url, headers=headers) as r:
                    r.raise_for_status()
                    return await r.text()
        except Exception as e:
            last_error = e
            await asyncio.sleep(1 + attempt + random.random())
    # If all retries failed, raise last error
    if last_error:
        raise last_error
    return ""


async def scrape_olx_url(url: str) -> List[OlxItem]:
    html = await fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    items: List[OlxItem] = []
    for card in collect_cards_with_stop(soup):
        item = parse_card(card)
        if item:
            items.append(item)
    return items


async def send_message(bot: Bot, chat_id: str, text: str, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            async with _SEND_SEMAPHORE:
                await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=False)
            return True
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after)
        except TimedOut:
            await asyncio.sleep(2 * (attempt + 1))
        except Exception:
            if attempt == max_retries - 1:
                return False
            await asyncio.sleep(2 * (attempt + 1))
    return False


def build_message(item: OlxItem, prev: Optional[Dict], source_name: str) -> str:
    safe_name = escape(item.name, quote=True)
    safe_state = escape(item.state or "-", quote=True)
    safe_source = escape(source_name or "OLX", quote=True)
    safe_link = escape(item.link, quote=True)
    open_link = f'<a href="{safe_link}">Відкрити</a>'
    if not prev:
        return (
            f"✨{safe_name}✨ \n\n"
            f"💰 Ціна: {item.price_text}\n"
            f"🥪 Стан: {safe_state}\n"
            f"🍘 Лінка: {safe_source}\n"
            f"🔗 {open_link}"
        )
    if prev and prev.get("price_int") != item.price_int:
        was = prev.get("price_int") or 0
        return (
            f"OLX Price changed: {safe_name}\n"
            f"💰 Ціна: {item.price_text} (було {was} грн)\n"
            f"🥪 Стан: {safe_state}\n"
            f"🍘 Лінка: {safe_source}\n"
            f"🔗 {open_link}"
        )
    return (
        f"OLX: {safe_name}\n"
        f"💰 Ціна: {item.price_text}\n"
        f"🧩 Стан: {safe_state}\n"
        f"🍘 Лінка: {safe_source}\n"
        f"🔗 {open_link}"
    )


def _parse_highest_from_srcset(srcset: str) -> Optional[str]:
    if not srcset:
        return None
    best = None
    best_w = -1
    for part in srcset.split(','):
        m = re.search(r"\s*(\S+)\s+(\d+)w\s*", part)
        if not m:
            continue
        url, w = m.group(1), int(m.group(2))
        if w > best_w:
            best_w = w
            best = url
    return best


async def fetch_item_images(item_url: str, max_images: int = 3) -> List[str]:
    try:
        html = await fetch_html(item_url)
        soup = BeautifulSoup(html, "html.parser")
        wrapper = soup.find("div", class_="swiper-wrapper")
        if not wrapper:
            return []
        imgs: List[str] = []
        for slide in wrapper.find_all(["div", "img"], recursive=True):
            if slide.name == "img":
                img = slide
            else:
                img = slide.find("img")
            if not img:
                continue
            srcset = img.get("srcset")
            src = img.get("src")
            best = _parse_highest_from_srcset(srcset) if srcset else src
            if best and best not in imgs:
                imgs.append(best)
            if len(imgs) >= max_images:
                break
        return imgs[:max_images]
    except Exception:
        return []


async def fetch_first_image_best(item_url: str) -> Optional[str]:
    try:
        html = await fetch_html(item_url)
        soup = BeautifulSoup(html, "html.parser")
        wrapper = soup.find("div", class_="swiper-wrapper")
        if not wrapper:
            return None
        # find the first slide image by DOM order
        img_tag = None
        for slide in wrapper.find_all(["div", "img"], recursive=True):
            if slide.name == "img":
                img_tag = slide
                break
            else:
                img_tag = slide.find("img")
                if img_tag:
                    break
        if not img_tag:
            return None
        srcset = img_tag.get("srcset")
        src = img_tag.get("src")
        return _parse_highest_from_srcset(srcset) if srcset else src
    except Exception:
        return None


def _upscale_image_bytes(img_bytes: bytes, scale: float = 2.0, max_dim: int = 2048) -> Optional[bytes]:
    try:
        im = Image.open(io.BytesIO(img_bytes))
        # Convert to RGB to avoid Telegram issues with palette/alpha
        if im.mode not in ("RGB", "L"):
            im = im.convert("RGB")
        w, h = im.size
        new_w, new_h = int(w * scale), int(h * scale)
        # cap by max_dim on longer side
        longer = max(new_w, new_h)
        if longer > max_dim:
            ratio = max_dim / float(longer)
            new_w = int(new_w * ratio)
            new_h = int(new_h * ratio)
        if new_w <= 0 or new_h <= 0:
            new_w, new_h = w, h
        im_up = im.resize((new_w, new_h), resample=Image.Resampling.LANCZOS)
        out = io.BytesIO()
        im_up.save(out, format="JPEG", quality=92, optimize=True)
        out.seek(0)
        return out.getvalue()
    except Exception:
        return None


async def _download_bytes(url: str, timeout_s: int = 30, max_retries: int = 3) -> Optional[bytes]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    }
    last_error: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            async with _HTTP_SEMAPHORE:
                session = _get_http_session()
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout_s)) as r:
                    r.raise_for_status()
                    return await r.read()
        except Exception as e:
            last_error = e
            await asyncio.sleep(1 + attempt + random.random())
    return None


async def send_photo_with_upscale(bot: Bot, chat_id: str, caption: str, image_url: Optional[str], max_retries: int = 3) -> bool:
    if not image_url:
        return await send_message(bot, chat_id, caption)
    raw = await _download_bytes(image_url)
    data = _upscale_image_bytes(raw) if raw else None
    photo_bytes = data or raw
    if not photo_bytes:
        # Fallback: try sending by URL directly
        for attempt in range(max_retries):
            try:
                async with _SEND_SEMAPHORE:
                    await bot.send_photo(chat_id=chat_id, photo=image_url, caption=caption, parse_mode=ParseMode.HTML)
                return True
            except RetryAfter as e:
                await asyncio.sleep(e.retry_after)
            except TimedOut:
                await asyncio.sleep(2 * (attempt + 1))
            except Exception:
                if attempt == max_retries - 1:
                    return False
                await asyncio.sleep(2 * (attempt + 1))
        return False
    for attempt in range(max_retries):
        try:
            async with _SEND_SEMAPHORE:
                await bot.send_photo(chat_id=chat_id, photo=io.BytesIO(photo_bytes), caption=caption, parse_mode=ParseMode.HTML)
            return True
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after)
        except TimedOut:
            await asyncio.sleep(2 * (attempt + 1))
        except Exception:
            if attempt == max_retries - 1:
                # Final fallback: try sending by URL
                try:
                    async with _SEND_SEMAPHORE:
                        await bot.send_photo(chat_id=chat_id, photo=image_url, caption=caption, parse_mode=ParseMode.HTML)
                    return True
                except Exception:
                    return False
            await asyncio.sleep(2 * (attempt + 1))
    return False


async def send_olx_message(bot: Bot, chat_id: str, text: str, image_urls: List[str], max_retries: int = 3) -> bool:
    # Deprecated multi-image sender; route to single-photo sender using the first image only
    first = image_urls[0] if image_urls else None
    return await send_photo_with_upscale(bot, chat_id, text, first)


# --- New SQLite storage (replaces JSON) ---

DB_FILE = Path(__file__).with_name("olx_items.db")


def _apply_pragmas(conn: sqlite3.Connection):
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
    except Exception:
        pass


def _db_connect():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn)
    return conn


def _db_init_sync():
    with _db_connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS olx_items (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                link TEXT NOT NULL,
                price_text TEXT NOT NULL,
                price_int INTEGER NOT NULL,
                state TEXT,
                source TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                last_sent_at TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_olx_items_source ON olx_items(source);")
        conn.commit()


async def db_init():
    await asyncio.to_thread(_db_init_sync)


def _db_get_item_sync(item_id: str) -> Optional[Dict]:
    with _db_connect() as conn:
        cur = conn.execute(
            "SELECT id, name, link, price_text, price_int, state, source, created_at, updated_at, last_sent_at FROM olx_items WHERE id = ?",
            (item_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


async def db_get_item(item_id: str) -> Optional[Dict]:
    return await asyncio.to_thread(_db_get_item_sync, item_id)


def _db_upsert_item_sync(item: OlxItem, source_name: str, touch_last_sent: bool):
    with _db_connect() as conn:
        # Upsert item, update metadata always; update last_sent_at only when sending
        conn.execute(
            """
            INSERT INTO olx_items (id, name, link, price_text, price_int, state, source, created_at, updated_at, last_sent_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'), CASE WHEN ? THEN datetime('now') ELSE NULL END)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                link=excluded.link,
                price_text=excluded.price_text,
                price_int=excluded.price_int,
                state=excluded.state,
                source=excluded.source,
                updated_at=datetime('now'),
                last_sent_at=CASE WHEN ? THEN datetime('now') ELSE last_sent_at END
            """,
            (item.id, item.name, item.link, item.price_text, item.price_int, item.state, source_name, 1 if touch_last_sent else 0, 1 if touch_last_sent else 0),
        )
        conn.commit()


async def db_upsert_item(item: OlxItem, source_name: str, touch_last_sent: bool):
    await asyncio.to_thread(_db_upsert_item_sync, item, source_name, touch_last_sent)


async def run_olx_scraper():
    token = _clean_token(TELEGRAM_OLX_BOT_TOKEN)
    default_chat = _clean_token(DANYLO_DEFAULT_CHAT_ID)
    if not token:
        return
    # initialize database on first run
    await db_init()
    bot = Bot(token=token)

    async def _process_entry(entry: Dict):
        url = entry.get("url")
        chat_id = _clean_token(entry.get("telegram_chat_id") or default_chat)
        source_name = entry.get("url_name") or "OLX"
        if not url or not chat_id:
            return
        try:
            items = await scrape_olx_url(url)
        except Exception:
            return
        # Only send if new or price changed; always persist latest state
        for it in items:
            try:
                prev = await db_get_item(it.id)
                need_send = (prev is None) or ((prev or {}).get("price_int") != it.price_int)
                if need_send:
                    text = build_message(it, prev, source_name)
                    # fetch only the first image in best resolution
                    first_img = await fetch_first_image_best(it.link)
                    sent = await send_photo_with_upscale(bot, chat_id, text, first_img)
                    # persist and mark sent when successful
                    await db_upsert_item(it, source_name, touch_last_sent=bool(sent))
                else:
                    # Update record without touching last_sent_at
                    await db_upsert_item(it, source_name, touch_last_sent=False)
                await asyncio.sleep(0.2)
            except Exception:
                # best-effort per item
                continue

    # Process multiple OLX sources with limited concurrency
    sem = asyncio.Semaphore(3)

    async def _guarded_process(entry):
        async with sem:
            await _process_entry(entry)

    tasks = [_guarded_process(entry) for entry in OLX_URLS or []]
    if tasks:
        await asyncio.gather(*tasks)

    # Close session optionally (keep alive across runs if module persists)
    # await _http_session.close()  # intentionally not closing to reuse across cycles if importer persists
