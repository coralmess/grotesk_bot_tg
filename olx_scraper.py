from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar
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
import logging
from html import escape
from functools import wraps

from config import OLX_URLS, TELEGRAM_OLX_BOT_TOKEN, DANYLO_DEFAULT_CHAT_ID

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_OLX = "https://www.olx.ua"

# Concurrency controls
_HTTP_SEMAPHORE = asyncio.Semaphore(10)
_SEND_SEMAPHORE = asyncio.Semaphore(3)

T = TypeVar('T')

# aiohttp session (shared)
_http_session: Optional[aiohttp.ClientSession] = None


def _get_http_session() -> aiohttp.ClientSession:
    global _http_session
    if (_http_session is None) or _http_session.closed:
        timeout = aiohttp.ClientTimeout(total=25)
        connector = aiohttp.TCPConnector(limit=20)
        _http_session = aiohttp.ClientSession(timeout=timeout, connector=connector)
    return _http_session


def _clean_token(value: Optional[str]) -> str:
    return (value or "").strip().strip("'\"")


# Generic retry decorator for async functions (Improvement #6)
def async_retry(max_retries: int = 3, backoff_base: float = 1.0):
    """Decorator that retries async functions with exponential backoff."""
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_error: Optional[Exception] = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except RetryAfter as e:
                    await asyncio.sleep(e.retry_after)
                    last_error = e
                except TimedOut as e:
                    await asyncio.sleep(backoff_base * (attempt + 1))
                    last_error = e
                except Exception as e:
                    last_error = e
                    if attempt < max_retries - 1:
                        await asyncio.sleep(backoff_base * (attempt + 1) + random.random())
                    else:
                        logger.error(f"Function {func.__name__} failed after {max_retries} retries: {e}")
            return None
        return wrapper
    return decorator


@dataclass
class OlxItem:
    id: str
    name: str
    link: str
    price_text: str
    price_int: int
    state: Optional[str] = None
    size: Optional[str] = None
    first_image_url: Optional[str] = None  # Improvement #2: Include image in dataclass


def normalize_price(text: str) -> Tuple[str, int]:
    # Keep only digits for integer comparison and format back as "<int> грн"
    digits = re.sub(r"[^\d]", "", text or "")
    price_int = int(digits) if digits else 0
    return (f"{price_int} грн" if price_int else (text or "").strip()), price_int


def extract_id_from_link(link: str) -> str:
    # e.g. https://www.olx.ua/d/uk/obyavlenie/tufli-lodochki-firmy-agnona-IDP0w0I.html?reason=seller_profile
    slug = link.rstrip("/").split("/")[-1]
    # strip query params first, then optional .html suffix
    slug = slug.split("?", 1)[0]
    if slug.endswith(".html"):
        slug = slug[:-5]
    return slug


def _extract_name_from_card(card, title_anchor) -> str:
    """Extract item name from card element (Improvement #8)."""
    if title_anchor:
        name = title_anchor.get_text(strip=True)
        if name:
            return name
    
    name_el = card.find(["h4", "h3"]) or card.find("img", alt=True)
    if hasattr(name_el, "get_text"):
        return name_el.get_text(strip=True)
    elif name_el and hasattr(name_el, "get"):
        return name_el.get("alt", "").strip()
    return ""


def _extract_state_from_card(card) -> Optional[str]:
    """Extract item state/condition from card element (Improvement #8)."""
    st = card.find("span", attrs={"title": True})
    if st and st.get("title"):
        return str(st.get("title")).strip()
    elif st:
        return st.get_text(strip=True)
    return None


def _extract_size_from_card(card) -> Optional[str]:
    """Extract item size from card element (Improvement #8)."""
    size_el = card.find(class_="css-rkfuwj")
    if size_el:
        return size_el.get_text(" ", strip=True)
    return None


def _is_valid_image_url(url: Optional[str]) -> bool:
    """Check if URL is a valid image URL (not a placeholder)."""
    if not url:
        return False
    url = url.strip()
    # Reject relative paths and placeholder images
    if not url.startswith(("http://", "https://")):
        return False
    # Reject common placeholder patterns
    if any(placeholder in url.lower() for placeholder in ["no_thumbnail", "placeholder", "no-image", "noimage"]):
        return False
    return True


def _extract_first_image_from_card(card) -> Optional[str]:
    """Extract first image URL from card element (Improvement #2)."""
    img = card.find("img")
    if not img:
        return None
    
    # Try srcset first for best quality
    srcset = img.get("srcset")
    if srcset:
        best = _parse_highest_from_srcset(srcset)
        if best and _is_valid_image_url(best):
            return best
    
    # Fallback to src
    src = img.get("src")
    return src if _is_valid_image_url(src) else None


def parse_card(card) -> Optional[OlxItem]:
    """Parse OLX card element into OlxItem."""
    try:
        # Prefer anchor with visible title text, fallback to first anchor
        anchors = card.find_all("a", href=True)
        title_anchor = next((a for a in anchors if a.get_text(strip=True)), None)
        a = title_anchor or (anchors[0] if anchors else None)
        href = a["href"] if a else None
        if not href:
            return None
        link = href if href.startswith("http") else f"{BASE_OLX}{href}"

        # Extract fields using helper functions (Improvement #8)
        name = _extract_name_from_card(card, title_anchor)

        # Price: support span or p with data-testid="ad-price"
        price_el = card.find(attrs={"data-testid": "ad-price"})
        price_text_raw = price_el.get_text(" ", strip=True) if price_el else ""
        price_text, price_int = normalize_price(price_text_raw)

        state = _extract_state_from_card(card)
        size = _extract_size_from_card(card)
        first_image_url = _extract_first_image_from_card(card)

        item_id = extract_id_from_link(link)
        if not (name and link and item_id):
            return None
        return OlxItem(
            id=item_id,
            name=name,
            link=link,
            price_text=price_text,
            price_int=price_int,
            state=state,
            size=size,
            first_image_url=first_image_url
        )
    except Exception as e:
        logger.debug(f"Failed to parse card: {e}")
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


@async_retry(max_retries=3, backoff_base=1.0)
async def fetch_html(url: str) -> str:
    """Fetch HTML content from URL with retry logic (Improvement #6)."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
        "Accept-Language": "uk,ru;q=0.9,en;q=0.8",
    }

    async with _HTTP_SEMAPHORE:
        session = _get_http_session()
        async with session.get(url, headers=headers) as r:
            r.raise_for_status()
            return await r.text()


async def scrape_olx_url(url: str) -> List[OlxItem]:
    """Scrape OLX URL and return list of items with images included (Improvement #2)."""
    html = await fetch_html(url)
    if not html:
        logger.warning(f"No HTML content received from {url}")
        return []
    
    soup = BeautifulSoup(html, "html.parser")
    # Skip pages that explicitly state there are zero listings
    try:
        page_text = soup.get_text(" ", strip=True)
        if "Ми знайшли 0 оголошень" in page_text:
            logger.info(f"No listings found at {url}")
            return []
    except Exception as e:
        logger.debug(f"Error checking for zero listings: {e}")
    
    items: List[OlxItem] = []
    for card in collect_cards_with_stop(soup):
        item = parse_card(card)
        if item:
            items.append(item)
    return items


@async_retry(max_retries=3, backoff_base=2.0)
async def send_message(bot: Bot, chat_id: str, text: str) -> bool:
    """Send text message via Telegram bot (Improvement #6)."""
    async with _SEND_SEMAPHORE:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=False)
    return True


def _escape_html_dict(data: Dict[str, Optional[str]]) -> Dict[str, str]:
    """Helper to escape all HTML values in dict (Improvement #15)."""
    return {key: escape(val or "", quote=True) for key, val in data.items()}


def build_message(item: OlxItem, prev: Optional[Dict[str, Any]], source_name: str) -> str:
    """Build Telegram message from OlxItem (Improvement #15: cleaner escaping)."""
    # Escape all fields at once
    safe = _escape_html_dict({
        "name": item.name,
        "state": item.state,
        "size": item.size,
        "source": source_name or "OLX",
        "link": item.link,
    })
    
    open_link = f'<a href="{safe["link"]}">Відкрити</a>'

    # Compose optional lines
    state_line = f"\n🥪 Стан: {safe['state']}" if safe["state"] else ""
    size_line = f"\n📏 Розмір: {safe['size']}" if safe["size"] else ""

    if not prev:
        return (
            f"✨{safe['name']}✨ \n\n"
            f"💰 Ціна: {item.price_text}" 
            f"{state_line}"
            f"{size_line}\n"
            f"🍘 Лінка: {safe['source']}\n"
            f"🔗 {open_link}"
        )
    if prev and prev.get("price_int") != item.price_int:
        was = prev.get("price_int") or 0
        return (
            f"OLX Price changed: {safe['name']}\n\n"
            f"💰 Ціна: {item.price_text} (було {was} грн)"
            f"{state_line}"
            f"{size_line}\n"
            f"🍘 Лінка: {safe['source']}\n"
            f"🔗 {open_link}"
        )
    return (
        f"OLX: {safe['name']}\n\n"
        f"💰 Ціна: {item.price_text}"
        f"{state_line}"
        f"{size_line}\n"
        f"🍘 Лінка: {safe['source']}\n"
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
    """
    Fetch multiple images from item detail page.
    Note: This is now rarely needed since images are extracted during scraping (Improvement #2).
    """
    try:
        html = await fetch_html(item_url)
        if not html:
            return []
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
    except Exception as e:
        logger.debug(f"Failed to fetch images from {item_url}: {e}")
        return []


async def fetch_first_image_best(item_url: str) -> Optional[str]:
    """
    Fetch first image from item detail page.
    Note: This is now rarely needed since images are extracted during scraping (Improvement #2).
    """
    try:
        html = await fetch_html(item_url)
        if not html:
            return None
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
    except Exception as e:
        logger.debug(f"Failed to fetch first image from {item_url}: {e}")
        return None


def _upscale_image_bytes_sync(img_bytes: bytes, scale: float = 2.0, max_dim: int = 2048) -> Optional[bytes]:
    """Synchronous image upscaling (called via thread pool)."""
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
    except Exception as e:
        logger.debug(f"Image upscaling failed: {e}")
        return None


async def _upscale_image_bytes(img_bytes: bytes, scale: float = 2.0, max_dim: int = 2048) -> Optional[bytes]:
    """Async wrapper for image upscaling (Improvement #5: offload to thread pool)."""
    return await asyncio.to_thread(_upscale_image_bytes_sync, img_bytes, scale, max_dim)


@async_retry(max_retries=3, backoff_base=1.0)
async def _download_bytes(url: str, timeout_s: int = 30) -> Optional[bytes]:
    """Download bytes from URL with retry logic (Improvement #6)."""
    # Validate URL before attempting download
    if not _is_valid_image_url(url):
        logger.debug(f"Skipping invalid/placeholder image URL: {url}")
        return None
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    }
    async with _HTTP_SEMAPHORE:
        session = _get_http_session()
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout_s)) as r:
            r.raise_for_status()
            return await r.read()


@async_retry(max_retries=3, backoff_base=2.0)
async def _send_photo_by_url(bot: Bot, chat_id: str, photo_url: str, caption: str) -> bool:
    """Send photo by URL (Improvement #6)."""
    async with _SEND_SEMAPHORE:
        await bot.send_photo(chat_id=chat_id, photo=photo_url, caption=caption, parse_mode=ParseMode.HTML)
    return True


@async_retry(max_retries=3, backoff_base=2.0)
async def _send_photo_by_bytes(bot: Bot, chat_id: str, photo_bytes: bytes, caption: str) -> bool:
    """Send photo by bytes (Improvement #6)."""
    async with _SEND_SEMAPHORE:
        await bot.send_photo(chat_id=chat_id, photo=io.BytesIO(photo_bytes), caption=caption, parse_mode=ParseMode.HTML)
    return True


async def send_photo_with_upscale(bot: Bot, chat_id: str, caption: str, image_url: Optional[str]) -> bool:
    """Send photo with upscaling (Improvement #6: unified retry logic)."""
    # Validate image URL first
    if not image_url or not _is_valid_image_url(image_url):
        logger.debug(f"No valid image URL, sending text-only message")
        result = await send_message(bot, chat_id, caption)
        return result if result is not None else False
    
    # Try to download and upscale image
    raw = await _download_bytes(image_url)
    if not raw:
        logger.debug(f"Failed to download image, falling back to text-only message")
        result = await send_message(bot, chat_id, caption)
        return result if result is not None else False
    
    data = await _upscale_image_bytes(raw)
    photo_bytes = data or raw
    
    # Try sending upscaled image
    result = await _send_photo_by_bytes(bot, chat_id, photo_bytes, caption)
    if result is not None:
        return result
    
    # Final fallback: send text-only message
    logger.debug(f"Failed to send photo, falling back to text-only message")
    result = await send_message(bot, chat_id, caption)
    return result if result is not None else False


# --- New SQLite storage (replaces JSON) ---

DB_FILE = Path(__file__).with_name("olx_items.db")


def _apply_pragmas(conn: sqlite3.Connection):
    """Apply SQLite pragmas for better performance."""
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
    except Exception as e:
        logger.debug(f"Failed to apply pragmas: {e}")


def _db_connect() -> sqlite3.Connection:
    """Create database connection with optimizations."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn)
    return conn


def _db_init_sync():
    with _db_connect() as conn:
        # Create table with all columns
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS olx_items (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                link TEXT NOT NULL,
                price_text TEXT NOT NULL,
                price_int INTEGER NOT NULL,
                state TEXT,
                size TEXT,
                source TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                last_sent_at TEXT
            )
            """
        )
        
        # Migration: Add size column if it doesn't exist
        try:
            cursor = conn.execute("PRAGMA table_info(olx_items)")
            columns = [col[1] for col in cursor.fetchall()]
            if 'size' not in columns:
                conn.execute("ALTER TABLE olx_items ADD COLUMN size TEXT")
                print("Added missing 'size' column to database")
        except Exception as e:
            print(f"Migration error (can be ignored if table is new): {e}")
        
        conn.execute("CREATE INDEX IF NOT EXISTS idx_olx_items_source ON olx_items(source);")
        conn.commit()


async def db_init():
    await asyncio.to_thread(_db_init_sync)


def _db_get_item_sync(item_id: str, conn: Optional[sqlite3.Connection] = None) -> Optional[Dict[str, Any]]:
    """Get item from database (Improvement #1: support reusing connection)."""
    close_conn = False
    if conn is None:
        conn = _db_connect()
        close_conn = True
    
    try:
        cur = conn.execute(
            "SELECT id, name, link, price_text, price_int, state, size, source, created_at, updated_at, last_sent_at FROM olx_items WHERE id = ?",
            (item_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        if close_conn:
            conn.close()


async def db_get_item(item_id: str) -> Optional[Dict[str, Any]]:
    """Async wrapper for getting item from database."""
    return await asyncio.to_thread(_db_get_item_sync, item_id, None)


def _db_upsert_item_sync(item: OlxItem, source_name: str, touch_last_sent: bool, conn: Optional[sqlite3.Connection] = None):
    """Upsert item to database (Improvement #1: support reusing connection)."""
    close_conn = False
    if conn is None:
        conn = _db_connect()
        close_conn = True
    
    try:
        # Upsert item, update metadata always; update last_sent_at only when sending
        conn.execute(
            """
            INSERT INTO olx_items (id, name, link, price_text, price_int, state, size, source, created_at, updated_at, last_sent_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'), CASE WHEN ? THEN datetime('now') ELSE NULL END)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                link=excluded.link,
                price_text=excluded.price_text,
                price_int=excluded.price_int,
                state=excluded.state,
                size=excluded.size,
                source=excluded.source,
                updated_at=datetime('now'),
                last_sent_at=CASE WHEN ? THEN datetime('now') ELSE last_sent_at END
            """,
            (item.id, item.name, item.link, item.price_text, item.price_int, item.state, item.size, source_name, 1 if touch_last_sent else 0, 1 if touch_last_sent else 0),
        )
        conn.commit()
    finally:
        if close_conn:
            conn.close()


async def db_upsert_item(item: OlxItem, source_name: str, touch_last_sent: bool):
    """Async wrapper for upserting item to database."""
    await asyncio.to_thread(_db_upsert_item_sync, item, source_name, touch_last_sent, None)


def _db_batch_operations_sync(items: List[Tuple[OlxItem, str, bool]]) -> List[Optional[Dict[str, Any]]]:
    """Batch database operations with single connection (Improvement #1: reduce connections)."""
    conn = _db_connect()
    try:
        results = []
        for item, source_name, touch_last_sent in items:
            # Get previous state
            prev = _db_get_item_sync(item.id, conn)
            results.append(prev)
            # Upsert item
            _db_upsert_item_sync(item, source_name, touch_last_sent, conn)
        return results
    finally:
        conn.close()


async def db_batch_operations(items: List[Tuple[OlxItem, str, bool]]) -> List[Optional[Dict[str, Any]]]:
    """Async wrapper for batch database operations (Improvement #1)."""
    return await asyncio.to_thread(_db_batch_operations_sync, items)


async def run_olx_scraper():
    """Main scraper function (Improvements #1, #2: batch DB ops, no redundant fetches)."""
    token = _clean_token(TELEGRAM_OLX_BOT_TOKEN)
    default_chat = _clean_token(DANYLO_DEFAULT_CHAT_ID)
    if not token:
        logger.warning("No Telegram bot token configured")
        return
    
    # initialize database on first run
    await db_init()
    bot = Bot(token=token)

    async def _process_entry(entry: Dict[str, Any]):
        url = entry.get("url")
        chat_id = _clean_token(entry.get("telegram_chat_id") or default_chat)
        source_name = entry.get("url_name") or "OLX"
        if not url or not chat_id:
            return
        
        try:
            # Scrape items (images now included in OlxItem - Improvement #2)
            items = await scrape_olx_url(url)
            if not items:
                return
            
            # Batch database operations (Improvement #1)
            batch_data = [(item, source_name, False) for item in items]
            prev_items = await db_batch_operations(batch_data)
            
            # Process items with controlled concurrency (Improvement #1)
            send_tasks = []
            for idx, it in enumerate(items):
                prev = prev_items[idx]
                need_send = (prev is None) or ((prev or {}).get("price_int") != it.price_int)
                
                if need_send:
                    text = build_message(it, prev, source_name)
                    # Use image from OlxItem (no redundant fetch - Improvement #2)
                    send_tasks.append(_send_item_message(bot, chat_id, text, it, source_name))
            
            # Send messages with controlled concurrency
            if send_tasks:
                await asyncio.gather(*send_tasks, return_exceptions=True)
                
        except Exception as e:
            logger.error(f"Failed to process entry {url}: {e}")

    async def _send_item_message(bot: Bot, chat_id: str, text: str, item: OlxItem, source_name: str):
        """Send message for a single item."""
        try:
            sent = await send_photo_with_upscale(bot, chat_id, text, item.first_image_url)
            # Update only the sent status
            await db_upsert_item(item, source_name, touch_last_sent=sent)
            await asyncio.sleep(0.2)
        except Exception as e:
            logger.error(f"Failed to send message for item {item.id}: {e}")

    # Process multiple OLX sources with limited concurrency
    sem = asyncio.Semaphore(3)

    async def _guarded_process(entry: Dict[str, Any]):
        async with sem:
            await _process_entry(entry)

    tasks = [_guarded_process(entry) for entry in OLX_URLS or []]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    
    logger.info("OLX scraper completed")
    # Close session optionally (keep alive across runs if module persists)
    # await _http_session.close()  # intentionally not closing to reuse across cycles if importer persists
