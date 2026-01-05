from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from bs4 import BeautifulSoup
from telegram import Bot
from telegram.error import RetryAfter, TimedOut
from telegram.constants import ParseMode
from PIL import Image
import io, asyncio, re, sqlite3, aiohttp, random, logging, os, time, json, tracemalloc
from html import escape
from functools import wraps
from config import SHAFA_URLS, TELEGRAM_OLX_BOT_TOKEN, DANYLO_DEFAULT_CHAT_ID

try:
    from playwright.async_api import async_playwright, Browser, BrowserContext
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
try:
    import lxml  # noqa: F401
    _LXML_AVAILABLE = True
except ImportError:
    _LXML_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("telegram.request").setLevel(logging.WARNING)

if not PLAYWRIGHT_AVAILABLE:
    logger.warning("⚠️  Playwright not installed. Run: pip install playwright && playwright install chromium")

BASE_SHAFA = "https://shafa.ua"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
ACCEPT_LANGUAGE = "uk,ru;q=0.9,en;q=0.8"
_HTTP_SEMAPHORE = asyncio.Semaphore(10)
_SEND_SEMAPHORE = asyncio.Semaphore(3)
_PLAYWRIGHT_SEMAPHORE = asyncio.Semaphore(2)  # Limit concurrent browser instances
_http_session: Optional[aiohttp.ClientSession] = None
_playwright_browser: Optional[Browser] = None
_playwright_context: Optional[BrowserContext] = None
_playwright_instance = None
MIN_PRICE_DIFF = 50
MIN_PRICE_DIFF_PERCENT = 12.0
_BENCH_ENABLED = os.getenv("SHAFA_BENCH") == "1"
_BENCH_SKIP_SEND = os.getenv("SHAFA_BENCH_SKIP_SEND") == "1"
_BENCH_SKIP_MEDIA = os.getenv("SHAFA_BENCH_SKIP_MEDIA") == "1"
_OPT_REUSE_CONTEXT = os.getenv("SHAFA_OPT_REUSE_CONTEXT", "1") == "1"
_OPT_BATCH_DB = os.getenv("SHAFA_OPT_BATCH_DB", "1") == "1"
_OPT_LXML = os.getenv("SHAFA_OPT_LXML", "1") == "1"
_OPT_SQLITE_CACHE = os.getenv("SHAFA_OPT_SQLITE_CACHE", "1") == "1"
_OPT_NO_SEND_DELAY = os.getenv("SHAFA_OPT_NO_SEND_DELAY", "1") == "1"
_PARSER = "lxml" if _OPT_LXML and _LXML_AVAILABLE else "html.parser"
_METRICS = {
    "bytes_html": 0,
    "bytes_images": 0,
    "errors": 0,
    "urls_processed": 0,
    "urls_skipped": 0,
}

if _OPT_LXML and not _LXML_AVAILABLE:
    logger.warning("lxml not available; falling back to html.parser")


def _get_http_session() -> aiohttp.ClientSession:
    global _http_session
    if (_http_session is None) or _http_session.closed:
        _http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25), connector=aiohttp.TCPConnector(limit=20))
    return _http_session

def _clean_token(value: Optional[str]) -> str:
    return (value or "").strip().strip("'\"")

def async_retry(max_retries: int = 3, backoff_base: float = 1.0):
    """Decorator that retries async functions with exponential backoff."""
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except RetryAfter as e:
                    logger.warning(f"⏳ Rate limited, waiting {e.retry_after}s...")
                    await asyncio.sleep(e.retry_after)
                except TimedOut as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"⏱️  Timeout in {func.__name__}, retrying... ({attempt + 1}/{max_retries})")
                    await asyncio.sleep(backoff_base * (attempt + 1))
                except Exception as e:
                    if attempt < max_retries - 1:
                        await asyncio.sleep(backoff_base * (attempt + 1) + random.random())
                    else:
                        logger.error(f"❌ {func.__name__} failed after {max_retries} retries: {e}")
            return None
        return wrapper
    return decorator


@dataclass
class ShafaItem:
    id: str
    name: str
    link: str
    price_text: str
    price_int: int
    brand: Optional[str] = None
    size: Optional[str] = None
    first_image_url: Optional[str] = None

def normalize_price(text: str) -> Tuple[str, int]:
    """Extract price from text like '89 грн' or '80 грн з 02 січ'."""
    digits = re.sub(r"[^\d]", "", text or "")
    price_int = int(digits) if digits else 0
    return (f"{price_int} грн" if price_int else (text or "").strip()), price_int

def extract_id_from_link(link: str) -> str:
    """Extract item ID from Shafa URL like '/uk/men/nizhnee-bele/trusy/197214351-brendovi-bokseri-sunspel'."""
    slug = link.rstrip("/").split("/")[-1].split("?", 1)[0]
    # Extract number at the start of the slug
    if match := re.match(r"(\d+)", slug):
        return match.group(1)
    return slug

def _is_valid_image_url(url: Optional[str]) -> bool:
    if not url or not (url := url.strip()).startswith(("http://", "https://")):
        return False
    return not any(p in url.lower() for p in ["no_thumbnail", "placeholder", "no-image", "noimage"]) and not url.startswith("data:")


def _extract_image_from_card(card) -> Optional[str]:
    """Extract first image URL from card element."""
    if not (img := card.find("img", class_="wD1fsK")):
        return None
    
    if (src := img.get("src")) and _is_valid_image_url(src):
        return src
    
    for attr in ["data-src", "data-lazy-src"]:
        if (url := img.get(attr)) and _is_valid_image_url(url):
            return url
    
    return None


def parse_card(card) -> Optional[ShafaItem]:
    """Parse Shafa card element into ShafaItem."""
    try:
        # Find the main product link
        a = card.find("a", class_="p1SYwW")
        if not a or not (href := a.get("href")):
            return None
        
        link = href if href.startswith("http") else f"{BASE_SHAFA}{href}"
        item_id = extract_id_from_link(link)
        
        # Extract name from the link text
        name_el = card.find("a", class_="CnMTkD")
        name = name_el.get_text(strip=True) if name_el else ""
        
        # Extract price - look for the main price (not the discount price)
        price_el = card.find("div", class_="D8o9s7")
        if price_el:
            # Get first <p> tag which contains the main price
            main_price = price_el.find("p")
            price_text, price_int = normalize_price(main_price.get_text(" ", strip=True) if main_price else "")
        else:
            price_text, price_int = "", 0
        
        # Extract brand
        brand_el = card.find("p", class_="i7zcRu")
        brand = brand_el.get_text(strip=True) if brand_el else None
        
        # Extract size
        size_el = card.find("p", class_="NyHfpp")
        size = size_el.get_text(strip=True) if size_el else None
        
        # Extract image
        first_image_url = _extract_image_from_card(card)
        
        if not (name and link and item_id):
            return None
        
        return ShafaItem(
            id=item_id,
            name=name,
            link=link,
            price_text=price_text,
            price_int=price_int,
            brand=brand,
            size=size,
            first_image_url=first_image_url
        )
    except Exception as e:
        logger.debug(f"Failed to parse card: {e}")
        return None


def collect_cards(soup: BeautifulSoup) -> List:
    """Collect all product cards from the page."""
    # Try multiple strategies to find product cards
    
    # Strategy 1: Look for divs with "dqgIPe" class
    cards = soup.find_all("div", class_=lambda x: x and "dqgIPe" in x)
    if cards:
        return cards
    
    # Strategy 2: Look for links with class "p1SYwW" (product links) and get their parent containers
    product_links = soup.find_all("a", class_="p1SYwW")
    if product_links:
        cards = []
        for link in product_links:
            # Navigate up to find the product card container
            parent = link.parent
            while parent and parent.name != 'body':
                classes = parent.get("class", [])
                if isinstance(classes, str):
                    classes = [classes]
                # Look for a container that seems like a product card
                if any(cls for cls in classes if cls):
                    cards.append(parent)
                    break
                parent = parent.parent
        if cards:
            return cards
    
    # Strategy 3: Look for any div containing product info elements
    cards = []
    for div in soup.find_all("div"):
        # Check if this div contains the key elements of a product card
        if (div.find("a", class_="p1SYwW") and 
            div.find("a", class_="CnMTkD") and
            div.find("img", class_="wD1fsK")):
            cards.append(div)
    
    return cards

@async_retry(max_retries=3, backoff_base=1.0)
async def fetch_html(url: str) -> str:
    """Fetch HTML content from URL with retry logic and delay for lazy-loaded images."""
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": ACCEPT_LANGUAGE,
    }
    async with _HTTP_SEMAPHORE:
        session = _get_http_session()
        async with session.get(url, headers=headers) as r:
            r.raise_for_status()
            html = await r.text()
            if _BENCH_ENABLED:
                _METRICS["bytes_html"] += r.content_length or len(html.encode("utf-8", errors="ignore"))
            await asyncio.sleep(2)
            return html


async def fetch_html_with_playwright(url: str) -> Optional[str]:
    """Fetch HTML content using Playwright for JavaScript-rendered pages."""
    if not PLAYWRIGHT_AVAILABLE:
        logger.error("❌ Playwright not available")
        return None
    
    global _playwright_browser
    
    async with _PLAYWRIGHT_SEMAPHORE:
        try:
            if _playwright_browser is None:
                logger.error("Playwright browser not initialized")
                return None
            
            # Create a new page
            if _playwright_context is not None:
                page = await _playwright_context.new_page()
            else:
                page = await _playwright_browser.new_page(user_agent=USER_AGENT)
            
            try:
                # Navigate to the page
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                
                # Wait for product cards to appear
                try:
                    await page.wait_for_selector("div[class*='dqgIPe'], a.p1SYwW", timeout=10000)
                except Exception:
                    pass
                
                # Additional wait for images to load
                await asyncio.sleep(2)
                
                # Get the page content
                html = await page.content()
                if _BENCH_ENABLED:
                    _METRICS["bytes_html"] += len(html.encode("utf-8", errors="ignore"))
                return html
                
            finally:
                await page.close()
                
        except Exception as e:
            logger.error(f"❌ Failed to fetch page: {e}")
            return None


def _parse_items_from_html(html: str) -> Tuple[List[ShafaItem], bool]:
    soup = BeautifulSoup(html, _PARSER)
    cards = collect_cards(soup)
    if not cards:
        return [], False
    items = [item for card in cards if (item := parse_card(card))]
    return items, True


async def scrape_shafa_url(url: str) -> Optional[List[ShafaItem]]:
    """Scrape Shafa URL and return list of items with images included. Returns None on error."""
    # Use Playwright for JavaScript-rendered content
    html = await fetch_html_with_playwright(url)
    
    if not html:
        logger.warning(f"⚠️  No content received from {url}")
        return None
    
    items, had_cards = _parse_items_from_html(html)
    if not had_cards:
        logger.info("No products found")
    return items

@async_retry(max_retries=3, backoff_base=2.0)
async def send_message(bot: Optional[Bot], chat_id: str, text: str) -> bool:
    """Send text message via Telegram bot."""
    if _BENCH_SKIP_SEND or bot is None:
        return True
    async with _SEND_SEMAPHORE:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=False)
    return True

def _escape_html_dict(data: Dict[str, Optional[str]]) -> Dict[str, str]:
    """Helper to escape all HTML values in dict."""
    return {key: escape(val or "", quote=True) for key, val in data.items()}


def build_message(item: ShafaItem, prev: Optional[Dict[str, Any]], source_name: str) -> str:
    """Build Telegram message from ShafaItem."""
    safe = _escape_html_dict({
        "name": item.name,
        "brand": item.brand,
        "size": item.size,
        "source": source_name or "Shafa",
        "link": item.link
    })
    open_link = f'<a href="{safe["link"]}">Відкрити</a>'
    brand_line = f"\n🏷️ Бренд: {safe['brand']}" if safe["brand"] else ""
    size_line = f"\n📏 Розмір: {safe['size']}" if safe["size"] else ""
    
    if not prev:
        return f"✨{safe['name']}✨ \n\n💰 Ціна: {item.price_text}{brand_line}{size_line}\n🍘 Джерело: {safe['source']}\n🔗 {open_link}"
    if prev and prev.get("price_int") != item.price_int:
        was = prev.get("price_int") or 0
        return f"Shafa Price changed: {safe['name']}\n\n💰 Ціна: {item.price_text} (було {was} грн){brand_line}{size_line}\n🍘 Джерело: {safe['source']}\n🔗 {open_link}"
    return f"Shafa: {safe['name']}\n\n💰 Ціна: {item.price_text}{brand_line}{size_line}\n🍘 Джерело: {safe['source']}\n🔗 {open_link}"


def _upscale_image_bytes_sync(img_bytes: bytes, scale: float = 2.0, max_dim: int = 2048) -> Optional[bytes]:
    """Synchronous image upscaling (called via thread pool)."""
    try:
        im = Image.open(io.BytesIO(img_bytes))
        if im.mode not in ("RGB", "L"):
            im = im.convert("RGB")
        w, h = im.size
        new_w, new_h = int(w * scale), int(h * scale)
        longer = max(new_w, new_h)
        if longer > max_dim:
            ratio = max_dim / float(longer)
            new_w, new_h = int(new_w * ratio), int(new_h * ratio)
        if new_w <= 0 or new_h <= 0:
            new_w, new_h = w, h
        im_up = im.resize((new_w, new_h), resample=Image.Resampling.LANCZOS)
        out = io.BytesIO()
        im_up.save(out, format="JPEG", quality=92, optimize=True)
        out.seek(0)
        return out.getvalue()
    except Exception as e:
        logger.error(f"🖼️  Image upscaling failed: {e}")
        return None

async def _upscale_image_bytes(img_bytes: bytes, scale: float = 2.0, max_dim: int = 2048) -> Optional[bytes]:
    """Async wrapper for image upscaling."""
    return await asyncio.to_thread(_upscale_image_bytes_sync, img_bytes, scale, max_dim)


@async_retry(max_retries=3, backoff_base=1.0)
async def _download_bytes(url: str, timeout_s: int = 30) -> Optional[bytes]:
    """Download bytes from URL with retry logic."""
    if not _is_valid_image_url(url):
        return None
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    }
    async with _HTTP_SEMAPHORE:
        session = _get_http_session()
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout_s)) as r:
            r.raise_for_status()
            data = await r.read()
            if _BENCH_ENABLED:
                _METRICS["bytes_images"] += len(data)
            return data

@async_retry(max_retries=3, backoff_base=2.0)
async def _send_photo_by_bytes(bot: Optional[Bot], chat_id: str, photo_bytes: bytes, caption: str) -> bool:
    """Send photo by bytes."""
    if _BENCH_SKIP_SEND or bot is None:
        return True
    async with _SEND_SEMAPHORE:
        await bot.send_photo(chat_id=chat_id, photo=io.BytesIO(photo_bytes), caption=caption, parse_mode=ParseMode.HTML)
    return True

@async_retry(max_retries=3, backoff_base=2.0)
async def _send_photo_by_url(bot: Optional[Bot], chat_id: str, photo_url: str, caption: str) -> bool:
    """Send photo by URL."""
    if _BENCH_SKIP_SEND or bot is None:
        return True
    async with _SEND_SEMAPHORE:
        await bot.send_photo(chat_id=chat_id, photo=photo_url, caption=caption, parse_mode=ParseMode.HTML)
    return True


async def send_photo_with_upscale(bot: Optional[Bot], chat_id: str, caption: str, image_url: Optional[str]) -> bool:
    """Send photo with upscaling."""
    if _BENCH_SKIP_SEND or bot is None:
        if _BENCH_SKIP_MEDIA:
            return True
        if not image_url or not _is_valid_image_url(image_url):
            return True
        if not (raw := await _download_bytes(image_url)):
            return True
        _ = (await _upscale_image_bytes(raw)) or raw
        return True
    if not image_url or not _is_valid_image_url(image_url):
        logger.info(f"📝 Sending text-only message (no image)")
        result = await send_message(bot, chat_id, caption)
        return result if result is not None else False
    
    if (result := await _send_photo_by_url(bot, chat_id, image_url, caption)) is not None:
        return result

    if not (raw := await _download_bytes(image_url)):
        logger.warning(f"⚠️  Image download failed, sending text-only")
        result = await send_message(bot, chat_id, caption)
        return result if result is not None else False
    
    photo_bytes = (await _upscale_image_bytes(raw)) or raw
    
    if (result := await _send_photo_by_bytes(bot, chat_id, photo_bytes, caption)) is not None:
        return result
    
    logger.warning(f"⚠️  Photo send failed, falling back to text")
    result = await send_message(bot, chat_id, caption)
    return result if result is not None else False

DB_FILE = Path(__file__).with_name("shafa_items.db")

def _apply_pragmas(conn: sqlite3.Connection):
    """Apply SQLite pragmas for better performance."""
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        if _OPT_SQLITE_CACHE:
            conn.execute("PRAGMA cache_size=-20000;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            conn.execute("PRAGMA mmap_size=268435456;")
    except Exception:
        pass

def _db_connect() -> sqlite3.Connection:
    """Create database connection with optimizations."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn)
    return conn


def _db_init_sync():
    with _db_connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS shafa_items (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, link TEXT NOT NULL,
                price_text TEXT NOT NULL, price_int INTEGER NOT NULL,
                brand TEXT, size TEXT, source TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                last_sent_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS shafa_sources (
                url TEXT PRIMARY KEY,
                no_items_streak INTEGER DEFAULT 0,
                cycle_count INTEGER DEFAULT 0,
                last_checked_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_shafa_items_source ON shafa_items(source);")
        conn.commit()

async def db_init():
    await asyncio.to_thread(_db_init_sync)


def _db_get_item_sync(item_id: str, conn: Optional[sqlite3.Connection] = None) -> Optional[Dict[str, Any]]:
    """Get item from database."""
    close_conn = conn is None
    if close_conn:
        conn = _db_connect()
    try:
        cur = conn.execute("SELECT id, name, link, price_text, price_int, brand, size, source, created_at, updated_at, last_sent_at FROM shafa_items WHERE id = ?", (item_id,))
        return dict(row) if (row := cur.fetchone()) else None
    finally:
        if close_conn:
            conn.close()

async def db_get_item(item_id: str) -> Optional[Dict[str, Any]]:
    """Async wrapper for getting item from database."""
    return await asyncio.to_thread(_db_get_item_sync, item_id, None)

def _db_upsert_item_sync(item: ShafaItem, source_name: str, touch_last_sent: bool, conn: Optional[sqlite3.Connection] = None):
    """Upsert item to database."""
    close_conn = conn is None
    if close_conn:
        conn = _db_connect()
    try:
        conn.execute("""
            INSERT INTO shafa_items (id, name, link, price_text, price_int, brand, size, source, created_at, updated_at, last_sent_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'), CASE WHEN ? THEN datetime('now') ELSE NULL END)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name, link=excluded.link, price_text=excluded.price_text, price_int=excluded.price_int,
                brand=excluded.brand, size=excluded.size, source=excluded.source, updated_at=datetime('now'),
                last_sent_at=CASE WHEN ? THEN datetime('now') ELSE last_sent_at END
            """, (item.id, item.name, item.link, item.price_text, item.price_int, item.brand, item.size, source_name, 1 if touch_last_sent else 0, 1 if touch_last_sent else 0))
        conn.commit()
    finally:
        if close_conn:
            conn.close()

async def db_upsert_item(item: ShafaItem, source_name: str, touch_last_sent: bool):
    """Async wrapper for upserting item to database."""
    await asyncio.to_thread(_db_upsert_item_sync, item, source_name, touch_last_sent, None)


def _db_upsert_items_sync(items: List[Tuple[ShafaItem, bool]], source_name: str):
    """Batch upsert items to database."""
    if not items:
        return
    with _db_connect() as conn:
        conn.executemany("""
            INSERT INTO shafa_items (id, name, link, price_text, price_int, brand, size, source, created_at, updated_at, last_sent_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'), CASE WHEN ? THEN datetime('now') ELSE NULL END)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name, link=excluded.link, price_text=excluded.price_text, price_int=excluded.price_int,
                brand=excluded.brand, size=excluded.size, source=excluded.source, updated_at=datetime('now'),
                last_sent_at=CASE WHEN ? THEN datetime('now') ELSE last_sent_at END
            """, [
                (item.id, item.name, item.link, item.price_text, item.price_int, item.brand, item.size, source_name,
                 1 if touch_last_sent else 0, 1 if touch_last_sent else 0)
                for item, touch_last_sent in items
            ])
        conn.commit()

async def db_upsert_items(items: List[Tuple[ShafaItem, bool]], source_name: str):
    """Async wrapper for batch upserting items to database."""
    await asyncio.to_thread(_db_upsert_items_sync, items, source_name)


def _db_fetch_existing_sync(item_ids: List[str]) -> List[Optional[Dict[str, Any]]]:
    """Fetch existing items using a single shared connection with batch query."""
    if not item_ids:
        return []
    
    conn = _db_connect()
    try:
        # Batch query using IN clause - much faster than N individual queries
        placeholders = ','.join('?' * len(item_ids))
        query = f"SELECT id, name, link, price_text, price_int, brand, size, source, created_at, updated_at, last_sent_at FROM shafa_items WHERE id IN ({placeholders})"
        rows = conn.execute(query, item_ids).fetchall()
        
        # Build lookup dict for O(1) access
        items_dict = {row['id']: dict(row) for row in rows}
        
        # Return results in same order as input item_ids (preserving None for missing items)
        return [items_dict.get(item_id) for item_id in item_ids]
    finally:
        conn.close()

async def db_fetch_existing(item_ids: List[str]) -> List[Optional[Dict[str, Any]]]:
    """Async wrapper for fetching existing items from the database."""
    return await asyncio.to_thread(_db_fetch_existing_sync, item_ids)


def _db_get_source_stats_sync(url: str) -> Dict[str, int]:
    with _db_connect() as conn:
        cur = conn.execute("SELECT no_items_streak, cycle_count FROM shafa_sources WHERE url = ?", (url,))
        row = cur.fetchone()
        if row:
            return {"streak": row[0], "cycle_count": row[1]}
        return {"streak": 0, "cycle_count": 0}

async def db_get_source_stats(url: str) -> Dict[str, int]:
    return await asyncio.to_thread(_db_get_source_stats_sync, url)

def _db_update_source_stats_sync(url: str, streak: int, cycle_count: int):
    with _db_connect() as conn:
        conn.execute("""
            INSERT INTO shafa_sources (url, no_items_streak, cycle_count, last_checked_at)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(url) DO UPDATE SET
                no_items_streak=excluded.no_items_streak,
                cycle_count=excluded.cycle_count,
                last_checked_at=datetime('now')
        """, (url, streak, cycle_count))
        conn.commit()

async def db_update_source_stats(url: str, streak: int, cycle_count: int):
    await asyncio.to_thread(_db_update_source_stats_sync, url, streak, cycle_count)


async def run_shafa_scraper():
    """Main scraper function."""
    bench_start = time.perf_counter() if _BENCH_ENABLED else None
    cpu_start = time.process_time() if _BENCH_ENABLED else None
    if _BENCH_ENABLED:
        tracemalloc.start()
    early_error = None
    total_scraped = 0
    total_sent = 0
    total_without_images = 0
    total_new = 0
    logger.info("=" * 60)
    logger.info("�️  SHAFA.UA SCRAPER STARTED")
    logger.info("=" * 60)
    
    if not PLAYWRIGHT_AVAILABLE:
        early_error = "playwright_missing"
        logger.error("❌ Playwright not installed. Run: pip install playwright && playwright install chromium")
        return
    
    token, default_chat = _clean_token(TELEGRAM_OLX_BOT_TOKEN), _clean_token(DANYLO_DEFAULT_CHAT_ID)
    if not token and not _BENCH_SKIP_SEND:
        early_error = "missing_token"
        logger.error("❌ No Telegram bot token configured")
        return
    
    bot: Optional[Bot] = None
    
    try:
        await db_init()
        logger.info("✅ Database initialized")
    except Exception as e:
        early_error = "db_init_failed"
        logger.error(f"❌ Database initialization failed: {e}")
        return

    global _playwright_browser, _playwright_instance, _playwright_context, _http_session
    try:
        _playwright_instance = await async_playwright().start()
        _playwright_browser = await _playwright_instance.chromium.launch(headless=True)
        if _OPT_REUSE_CONTEXT:
            _playwright_context = await _playwright_browser.new_context(user_agent=USER_AGENT)
        logger.info("Playwright browser initialized")
    except Exception as e:
        early_error = "playwright_init_failed"
        logger.error(f"Playwright init failed: {e}")
        return
    
    if not _BENCH_SKIP_SEND and token:
        bot = Bot(token=token)
    
    async def _send_item_message(bot: Optional[Bot], chat_id: str, text: str, item: ShafaItem, source_name: str, update_db: bool = True) -> bool:
        """Send message for a single item."""
        nonlocal total_without_images, total_sent
        try:
            image_url = item.first_image_url
            if not image_url:
                total_without_images += 1
            
            sent = await send_photo_with_upscale(bot, chat_id, text, image_url)
            if update_db:
                await db_upsert_item(item, source_name, touch_last_sent=sent)
            
            if sent:
                total_sent += 1
            
            if not _OPT_NO_SEND_DELAY:
                await asyncio.sleep(0.2)
            return bool(sent)
        except RetryAfter as e:
            logger.warning(f"⏳ Rate limited, waiting {e.retry_after}s")
        except TimedOut:
            logger.warning(f"⏱️  Timeout sending item")
        except Exception as e:
            logger.error(f"❌ Send failed: {e}")
        return False
    
    async def _process_entry(entry: Dict[str, Any]):
        nonlocal total_scraped, total_new
        url = entry.get("url")
        chat_id = default_chat or "bench"
        source_name = entry.get("url_name") or "Shafa"
        if not url:
            return
        if not chat_id and not _BENCH_SKIP_SEND:
            return
        
        logger.info("-" * 60)
        logger.info(f"🔍 Processing: {source_name}")
        logger.info(f"🔗 URL: {url}")
        
        stats = await db_get_source_stats(url)
        streak = stats["streak"]
        cycle_count = stats["cycle_count"] + 1
        
        level = min(streak // 365, 23)
        divisor = level + 1
        
        if cycle_count % divisor != 0:
            logger.info(f"⏭️  Skipped (Cycle {cycle_count}/{divisor}, Streak: {streak})")
            await db_update_source_stats(url, streak, cycle_count)
            if _BENCH_ENABLED:
                _METRICS["urls_skipped"] += 1
            return
        
        try:
            if _BENCH_ENABLED:
                _METRICS["urls_processed"] += 1
            items = await scrape_shafa_url(url)
            if items is None:
                return
            
            if items:
                new_streak = 0
                new_cycle = 0
            else:
                new_streak = streak + 1
                new_cycle = 0
            
            await db_update_source_stats(url, new_streak, new_cycle)
            
            if not items:
                return
            
            total_scraped += len(items)
            prev_items = await db_fetch_existing([item.id for item in items])
            
            new_count = 0
            updated_count = 0
            
            send_tasks = []
            items_to_send = []
            for idx, it in enumerate(items):
                prev = prev_items[idx]
                if prev is None:
                    new_count += 1
                    items_to_send.append(it)
                    send_tasks.append(_send_item_message(bot, chat_id, build_message(it, prev, source_name), it, source_name, update_db=not _OPT_BATCH_DB))
                    continue

                previous_price = prev.get("price_int") or 0
                price_diff = abs(it.price_int - previous_price)
                percent_change = (price_diff / previous_price * 100.0) if previous_price > 0 else None

                if price_diff < MIN_PRICE_DIFF or (percent_change is not None and percent_change < MIN_PRICE_DIFF_PERCENT):
                    continue

                updated_count += 1
                items_to_send.append(it)
                send_tasks.append(_send_item_message(bot, chat_id, build_message(it, prev, source_name), it, source_name, update_db=not _OPT_BATCH_DB))
            
            if send_tasks:
                results = await asyncio.gather(*send_tasks, return_exceptions=True)
                if _OPT_BATCH_DB:
                    updates = []
                    for it, res in zip(items_to_send, results):
                        sent = False if isinstance(res, Exception) else bool(res)
                        updates.append((it, sent))
                    await db_upsert_items(updates, source_name)
            
            total_new += new_count
                
        except aiohttp.ClientError as e:
            logger.error(f"🌐 Network error: {e}")
        except Exception as e:
            logger.error(f"❌ Processing failed: {e}")
            if _BENCH_ENABLED:
                _METRICS["errors"] += 1
    
    try:
        sem = asyncio.Semaphore(3)
        async def _guarded_process(entry: Dict[str, Any]):
            async with sem:
                await _process_entry(entry)
        
        if tasks := [_guarded_process(entry) for entry in SHAFA_URLS or []]:
            logger.info(f"📊 Processing {len(tasks)} source(s)")
            await asyncio.gather(*tasks, return_exceptions=True)
        else:
            logger.warning("⚠️  No URLs configured")
        
        logger.info("=" * 60)
        logger.info(f"✅ SCRAPER COMPLETED")
        logger.info(f"📦 New: {total_new} | Sent: {total_sent}")
        if total_scraped > 0:
            logger.info(f"📈 Success Rate: {(total_sent/total_scraped*100):.1f}%")
        logger.info("=" * 60)
    
    finally:
        # Cleanup browser and HTTP session
        if _playwright_context:
            try:
                await _playwright_context.close()
                _playwright_context = None
            except Exception:
                pass

        if _playwright_browser:
            try:
                await _playwright_browser.close()
                _playwright_browser = None
            except Exception:
                pass
        
        if _playwright_instance:
            try:
                await _playwright_instance.stop()
                _playwright_instance = None
            except Exception:
                pass
        
        if _http_session and not _http_session.closed:
            try:
                await _http_session.close()
                _http_session = None
            except Exception:
                pass

        if bot is not None:
            try:
                await bot.shutdown()
            except Exception:
                pass
            try:
                await bot.close()
            except Exception:
                pass
        
        logger.info("🔒 Resources cleaned up")
        if _BENCH_ENABLED:
            wall_s = time.perf_counter() - bench_start if bench_start is not None else 0.0
            cpu_s = time.process_time() - cpu_start if cpu_start is not None else 0.0
            current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            summary = {
                "wall_s": round(wall_s, 4),
                "cpu_s": round(cpu_s, 4),
                "peak_py_mb": round(peak / (1024 * 1024), 3),
                "bytes_html": _METRICS["bytes_html"],
                "bytes_images": _METRICS["bytes_images"],
                "errors": _METRICS["errors"],
                "urls_processed": _METRICS["urls_processed"],
                "urls_skipped": _METRICS["urls_skipped"],
                "total_scraped": total_scraped,
                "total_sent": total_sent,
                "total_new": total_new,
                "total_without_images": total_without_images,
                "urls_configured": len(SHAFA_URLS or []),
                "early_error": early_error,
                "lxml_available": _LXML_AVAILABLE,
                "parser": _PARSER,
                "bench_flags": {
                    "skip_send": _BENCH_SKIP_SEND,
                    "skip_media": _BENCH_SKIP_MEDIA,
                "opt_reuse_context": _OPT_REUSE_CONTEXT,
                "opt_batch_db": _OPT_BATCH_DB,
                "opt_lxml": _OPT_LXML,
                "opt_sqlite_cache": _OPT_SQLITE_CACHE,
                "opt_no_send_delay": _OPT_NO_SEND_DELAY,
            },
        }
            print("SHAFA_BENCH_SUMMARY " + json.dumps(summary, ensure_ascii=True))

        # Give time for async cleanup to complete
        await asyncio.sleep(0.5)


if __name__ == "__main__":
    asyncio.run(run_shafa_scraper())
