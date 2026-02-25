from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from bs4 import BeautifulSoup
from telegram import Bot
from telegram.error import RetryAfter, TimedOut
from telegram.constants import ParseMode
from PIL import Image
import io, asyncio, re, sqlite3, aiohttp, random, logging
from html import escape
from functools import wraps
from urllib.parse import urljoin, urlsplit, urlunsplit
from config import TELEGRAM_OLX_BOT_TOKEN, DANYLO_DEFAULT_CHAT_ID, SHAFA_REQUEST_JITTER_SEC, RUN_USER_AGENT, RUN_ACCEPT_LANGUAGE, SHAFA_TASK_CONCURRENCY, SHAFA_HTTP_CONCURRENCY, SHAFA_SEND_CONCURRENCY, SHAFA_UPSCALE_CONCURRENCY, SHAFA_PLAYWRIGHT_CONCURRENCY, SHAFA_HTTP_CONNECTOR_LIMIT
from config_shafa_urls import SHAFA_URLS
from dynamic_sources import load_dynamic_urls, merge_sources

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
    logger.warning("Playwright is not installed. Run: pip install playwright && playwright install chromium")

BASE_SHAFA = "https://shafa.ua"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
_HTTP_SEMAPHORE = asyncio.Semaphore(SHAFA_HTTP_CONCURRENCY)
_SEND_SEMAPHORE = asyncio.Semaphore(SHAFA_SEND_CONCURRENCY)
_UPSCALE_SEMAPHORE = asyncio.Semaphore(SHAFA_UPSCALE_CONCURRENCY)
_PLAYWRIGHT_SEMAPHORE = asyncio.Semaphore(SHAFA_PLAYWRIGHT_CONCURRENCY)  # Limit concurrent browser instances
_http_session: Optional[aiohttp.ClientSession] = None
_playwright_browser: Optional[Browser] = None
_playwright_context: Optional[BrowserContext] = None
_playwright_instance = None
MIN_PRICE_DIFF = 50
MIN_PRICE_DIFF_PERCENT = 25.0
_PARSER = "lxml" if _LXML_AVAILABLE else "html.parser"

if not _LXML_AVAILABLE:
    logger.warning("lxml not found; using html.parser")

def _get_http_session() -> aiohttp.ClientSession:
    global _http_session
    if (_http_session is None) or _http_session.closed:
        _http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25), connector=aiohttp.TCPConnector(limit=SHAFA_HTTP_CONNECTOR_LIMIT))
    return _http_session

def async_retry(max_retries: int = 3, backoff_base: float = 1.0):
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except RetryAfter as e:
                    logger.warning(f"Telegram rate limit hit; waiting {e.retry_after}s...")
                    await asyncio.sleep(e.retry_after)
                except TimedOut:
                    if attempt == max_retries - 1:
                        logger.warning(f"Timeout in {func.__name__} after {max_retries} attempts")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(backoff_base * (attempt + 1))
                except Exception as e:
                    if "Wrong type of the page content" in str(e):
                        logger.warning(f"{func.__name__} got non-image content, falling back to bytes")
                        return None
                    if attempt < max_retries - 1:
                        await asyncio.sleep(backoff_base * (attempt + 1) + random.random())
                    else:
                        logger.error(f"{func.__name__} failed after {max_retries} attempts: {e}")
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
    digits = re.sub(r"[^\d]", "", text or "")
    price_int = int(digits) if digits else 0
    return (f"{price_int} грн" if price_int else (text or "").strip()), price_int


def _has_numeric_price(price_text: Optional[str], price_int: int) -> bool:
    if price_int <= 0:
        return False
    return bool(re.search(r"\d", price_text or ""))


def _looks_like_item_href(href: Optional[str]) -> bool:
    if not href:
        return False
    href = href.strip()
    if href.startswith(("javascript:", "#", "mailto:", "tel:")):
        return False
    parsed = urlsplit(href)
    path = (parsed.path or "").rstrip("/")
    low = path.lower()
    if not low.startswith("/uk/"):
        return False
    if low in ("/uk", "/uk/"):
        return False
    for bad in ("/my/", "/msg/", "/member/", "/api/", "/social/", "/login"):
        if bad in low:
            return False
    parts = [p for p in low.split("/") if p]
    if len(parts) < 4:
        return False
    return bool(re.search(r"\d", parts[-1]))


def _normalize_item_url(href: str) -> str:
    absolute = urljoin(BASE_SHAFA, (href or "").strip())
    parsed = urlsplit(absolute)
    path = (parsed.path or "").rstrip("/")
    if not path:
        path = "/"
    return urlunsplit((parsed.scheme, parsed.netloc, path, parsed.query, ""))


def _extract_anchor(card):
    # Prefer known product-link class, fallback to href heuristics.
    a = card.find("a", class_="p1SYwW")
    if a and _looks_like_item_href(a.get("href")):
        return a
    for cand in card.find_all("a", href=True):
        if _looks_like_item_href(cand.get("href")):
            return cand
    return None


def _extract_price_from_node_text(text: str) -> Tuple[str, int]:
    if not text:
        return "", 0
    normalized = " ".join((text or "").split())
    return normalize_price(normalized)


def _extract_price_from_card(card) -> Tuple[str, int]:
    # Current (active) price selectors first.
    current_selectors = [
        "p.D8o9s7",
        "div.D8o9s7 p",
        "div[class*='D8o9s7'] p",
    ]
    for selector in current_selectors:
        for node in card.select(selector):
            price_text, price_int = _extract_price_from_node_text(node.get_text(" ", strip=True))
            if price_int > 0:
                return price_text, price_int

    # Fallback for cards that show sale/new + old price in different nodes.
    # Keep this class-agnostic: scan short text nodes with explicit currency marks.
    footer = card.find("footer") or card
    candidates: List[Tuple[int, str, int]] = []
    idx = 0
    for node in footer.find_all(["p", "span", "div"], limit=200):
        idx += 1
        text = node.get_text(" ", strip=True)
        if not text:
            continue
        text_norm = " ".join(text.split())
        if len(text_norm) > 40:
            continue
        low = text_norm.lower()
        if not re.search(r"(грн|uah|₴)", low):
            continue
        price_text, price_int = _extract_price_from_node_text(text_norm)
        if price_int > 0:
            candidates.append((idx, price_text, price_int))
    if candidates:
        # Deduplicate by amount while preserving first appearance order.
        seen_amounts = set()
        ordered: List[Tuple[int, str]] = []
        for _, price_text, price_int in candidates:
            if price_int in seen_amounts:
                continue
            seen_amounts.add(price_int)
            ordered.append((price_int, price_text))
        if len(ordered) == 1:
            amount, text = ordered[0]
            return text, amount
        # Sale cards often contain [new_price, old_price] or [old_price, new_price].
        # Pick the lower amount from the first two meaningful values.
        first_two = ordered[:2]
        amount, text = min(first_two, key=lambda x: x[0])
        return text, amount
    return "", 0

def extract_id_from_link(link: str) -> str:
    slug = link.rstrip("/").split("/")[-1].split("?", 1)[0]
    if match := re.match(r"(\d+)", slug):
        return match.group(1)
    return slug

def _is_valid_image_url(url: Optional[str]) -> bool:
    if not url or not (url := url.strip()).startswith(("http://", "https://")):
        return False
    return not any(p in url.lower() for p in ["no_thumbnail", "placeholder", "no-image", "noimage"]) and not url.startswith("data:")

def _strip_image_url(url: Optional[str]) -> Optional[str]:
    if not _is_valid_image_url(url):
        return None
    return url.split("_", 1)[0] if "_" in url else url


def _extract_from_srcset(srcset: Optional[str]) -> Optional[str]:
    if not srcset:
        return None
    parts = [p.strip() for p in srcset.split(",") if p.strip()]
    if not parts:
        return None
    # Use the last candidate (usually highest resolution in srcset order).
    candidate = parts[-1].split()[0]
    return _strip_image_url(candidate)


def _extract_image_from_anchor(anchor) -> Optional[str]:
    if not anchor:
        return None
    img = anchor.find("img", class_="wD1fsK") or anchor.find("img")
    if not img:
        return None
    if src := _strip_image_url(img.get("src")):
        return src
    for attr in ["data-src", "data-lazy-src", "data-original"]:
        if url := _strip_image_url(img.get(attr)):
            return url
    for attr in ["srcset", "data-srcset"]:
        if url := _extract_from_srcset(img.get(attr)):
            return url
    return None


def _extract_image_from_card(card, anchor, link_url: str) -> Optional[str]:
    # Strict binding: image should belong to the same item anchor.
    if url := _extract_image_from_anchor(anchor):
        return url
    for a in card.find_all("a", href=True):
        if not _looks_like_item_href(a.get("href")):
            continue
        if _normalize_item_url(a.get("href", "")) != link_url:
            continue
        if url := _extract_image_from_anchor(a):
            return url
    # Better no image than a wrong image from another card.
    return None

def parse_card(card) -> Optional[ShafaItem]:
    try:
        a = _extract_anchor(card)
        if not a or not (href := a.get("href")):
            return None
        link = _normalize_item_url(href)
        if not _looks_like_item_href(link):
            return None
        item_id = extract_id_from_link(link)
        name_el = card.find("a", class_="CnMTkD")
        if name_el and _looks_like_item_href(name_el.get("href")):
            name = name_el.get_text(strip=True)
        else:
            # Class-agnostic name fallback tied to the same item URL.
            name_candidates: List[str] = []
            for anchor in card.find_all("a", href=True):
                if not _looks_like_item_href(anchor.get("href")):
                    continue
                if _normalize_item_url(anchor.get("href", "")) != link:
                    continue
                txt = " ".join(anchor.get_text(" ", strip=True).split())
                if len(txt) < 3:
                    continue
                if re.search(r"(грн|uah|₴|\d+\s*%?)", txt.lower()):
                    continue
                name_candidates.append(txt)
            name = max(name_candidates, key=len) if name_candidates else ""
            if not name and (img := card.find("img")):
                name = (img.get("alt") or "").strip()
        price_text, price_int = _extract_price_from_card(card)
        brand_el = card.find("p", class_="i7zcRu")
        brand = brand_el.get_text(strip=True) if brand_el else None
        size_el = card.find("p", class_="NyHfpp")
        size = size_el.get_text(strip=True) if size_el else None
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
            first_image_url=_extract_image_from_card(card, a, link)
        )
    except Exception as e:
        logger.debug(f"Failed to parse card: {e}")
        return None

def collect_cards(soup: BeautifulSoup) -> List:
    cards = soup.find_all("div", class_=lambda x: x and "dqgIPe" in x)
    if cards:
        return cards
    product_links = soup.find_all("a", href=lambda h: _looks_like_item_href(h))
    if product_links:
        cards = []
        seen = set()
        for link in product_links:
            parent = link.parent
            best = None
            while parent and parent.name != 'body':
                if parent.name == "div":
                    anchors = parent.find_all("a", href=lambda h: _looks_like_item_href(h))
                    anchor_count = len(anchors)
                    if anchor_count == 0 or anchor_count > 3:
                        parent = parent.parent
                        continue
                    if not parent.find("img"):
                        parent = parent.parent
                        continue
                    best = parent
                    if anchor_count == 1:
                        break
                parent = parent.parent
            if best is not None:
                ident = id(best)
                if ident not in seen:
                    seen.add(ident)
                    cards.append(best)
        if cards:
            return cards
    cards = []
    for div in soup.find_all("div"):
        if (div.find("a", href=lambda h: _looks_like_item_href(h)) and
            div.find("img")):
            cards.append(div)
    return cards

async def fetch_html_with_playwright(url: str) -> Optional[str]:
    if not PLAYWRIGHT_AVAILABLE:
        logger.error("Playwright is unavailable")
        return None
    global _playwright_browser
    async with _PLAYWRIGHT_SEMAPHORE:
        try:
            if _playwright_browser is None:
                logger.error("Playwright is not initialized")
                return None
            if _playwright_context is not None:
                page = await _playwright_context.new_page()
            else:
                page = await _playwright_browser.new_page(user_agent=USER_AGENT)
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                try:
                    await page.wait_for_selector("div[class*='dqgIPe'], a.p1SYwW", timeout=10000)
                except Exception:
                    pass
                await asyncio.sleep(2)
                html = await page.content()
                return html
            finally:
                await page.close()
        except Exception as e:
            logger.error(f"Failed to load page: {e}")
            return None

def _parse_items_from_html(html: str) -> Tuple[List[ShafaItem], bool]:
    soup = BeautifulSoup(html, _PARSER)
    cards = collect_cards(soup)
    if not cards:
        return [], False
    items = [item for card in cards if (item := parse_card(card))]
    return items, True

async def scrape_shafa_url(url: str) -> Optional[List[ShafaItem]]:
    html = await fetch_html_with_playwright(url)
    if not html:
        logger.warning(f"Empty response from {url}")
        return None
    items, _ = _parse_items_from_html(html)
    return items

@async_retry(max_retries=3, backoff_base=2.0)
async def send_message(bot: Bot, chat_id: str, text: str) -> bool:
    async with _SEND_SEMAPHORE:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=False)
    return True

def build_message(item: ShafaItem, prev: Optional[Dict[str, Any]], source_name: str) -> str:
    safe = {key: escape(val or "", quote=True) for key, val in {
        "name": item.name,
        "brand": item.brand,
        "size": item.size,
        "source": source_name or "SHAFA",
        "link": item.link
    }.items()}
    open_link = f'<a href="{safe["link"]}">Відкрити</a>'
    brand_line = f"\n🏷 Бренд: {safe['brand']}" if safe["brand"] else ""
    size_line = f"\n📏 Розмір: {safe['size']}" if safe["size"] else ""
    if not prev:
        return (
            f"✨{safe['name']}✨\n\n"
            f"💰 Ціна: {item.price_text}{brand_line}{size_line}\n"
            f"🏠 Джерело: {safe['source']}\n"
            f"🔗 {open_link}"
        )
    if prev and prev.get("price_int") != item.price_int:
        was = prev.get("price_int") or 0
        return (
            f"🔁 Зміна ціни: {safe['name']}\n\n"
            f"💰 Ціна: {item.price_text} (було {was} грн){brand_line}{size_line}\n"
            f"🏠 Джерело: {safe['source']}\n"
            f"🔗 {open_link}"
        )
    return (
        f"SHAFA: {safe['name']}\n\n"
        f"💰 Ціна: {item.price_text}{brand_line}{size_line}\n"
        f"🏠 Джерело: {safe['source']}\n"
        f"🔗 {open_link}"
    )

def _upscale_image_bytes_sync(
    img_bytes: bytes,
    scale: float = 2.0,
    max_dim: int = 5000,
    min_upscale_dim: int = 1280,
) -> Optional[bytes]:
    try:
        im = Image.open(io.BytesIO(img_bytes))
        if im.mode not in ("RGB", "L"):
            im = im.convert("RGB")
        w, h = im.size
        # Upscale when the smaller side is below threshold.
        if min(w, h) >= min_upscale_dim:
            return None
        def _fits_telegram(width: int, height: int) -> bool:
            if width <= 0 or height <= 0:
                return False
            if width + height > 10000:
                return False
            ratio = max(width / float(height), height / float(width))
            return ratio <= 20.0

        def _encode_for_telegram(image: Image.Image) -> Optional[bytes]:
            max_bytes = 10 * 1024 * 1024
            for quality in (98, 95, 92, 88, 84, 80):
                out = io.BytesIO()
                image.save(out, format="JPEG", quality=quality, subsampling=0, optimize=False)
                data = out.getvalue()
                if len(data) <= max_bytes:
                    return data
            return None

        for factor in (3.0, 2.5, 2.0):
            new_w, new_h = int(w * factor), int(h * factor)
            longer = max(new_w, new_h)
            if longer > max_dim:
                ratio = max_dim / float(longer)
                new_w, new_h = int(new_w * ratio), int(new_h * ratio)
            if not _fits_telegram(new_w, new_h):
                continue

            im_up = im.resize((new_w, new_h), resample=Image.Resampling.LANCZOS)
            data = _encode_for_telegram(im_up)
            if data is not None:
                return data

            # If quality alone is not enough for size limits, step dimensions down slightly.
            trial = im_up
            for _ in range(6):
                dw = max(1, int(trial.width * 0.9))
                dh = max(1, int(trial.height * 0.9))
                if (dw, dh) == trial.size:
                    break
                trial = trial.resize((dw, dh), resample=Image.Resampling.LANCZOS)
                if not _fits_telegram(dw, dh):
                    continue
                data = _encode_for_telegram(trial)
                if data is not None:
                    return data
        return None
    except Exception as e:
        logger.error(f"Image upscale failed: {e}")
        return None

async def _upscale_image_bytes(
    img_bytes: bytes,
    scale: float = 2.0,
    max_dim: int = 5000,
    min_upscale_dim: int = 1280,
) -> Optional[bytes]:
    async with _UPSCALE_SEMAPHORE:
        return await asyncio.to_thread(_upscale_image_bytes_sync, img_bytes, scale, max_dim, min_upscale_dim)

@async_retry(max_retries=3, backoff_base=1.0)
async def _download_bytes(url: str, timeout_s: int = 30) -> Optional[bytes]:
    if not _is_valid_image_url(url):
        return None
    headers = {
        "User-Agent": RUN_USER_AGENT,
        "Accept-Language": RUN_ACCEPT_LANGUAGE,
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    }
    async with _HTTP_SEMAPHORE:
        session = _get_http_session()
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout_s)) as r:
            if r.status == 429:
                retry_after = r.headers.get("Retry-After")
                wait_s = int(retry_after) if retry_after and retry_after.isdigit() else 15
                logger.warning(f"Rate limited (429). Sleeping {wait_s}s before retry.")
                await asyncio.sleep(wait_s)
                return None
            if r.status == 403:
                logger.warning("Forbidden (403). Backing off for 30s.")
                await asyncio.sleep(30)
                return None
            r.raise_for_status()
            return await r.read()

@async_retry(max_retries=3, backoff_base=2.0)
async def _send_photo_by_bytes(bot: Bot, chat_id: str, photo_bytes: bytes, caption: str) -> bool:
    async with _SEND_SEMAPHORE:
        await bot.send_photo(chat_id=chat_id, photo=io.BytesIO(photo_bytes), caption=caption, parse_mode=ParseMode.HTML)
    return True

async def send_photo_with_upscale(bot: Bot, chat_id: str, caption: str, image_url: Optional[str]) -> bool:
    if not image_url or not _is_valid_image_url(image_url):
        logger.info("Text only (no photo)")
        result = await send_message(bot, chat_id, caption)
        return result if result is not None else False
    if not (raw := await _download_bytes(image_url)):
        logger.warning("Photo not downloaded; sending text")
        result = await send_message(bot, chat_id, caption)
        return result if result is not None else False
    photo_bytes = (await _upscale_image_bytes(raw)) or raw
    if (result := await _send_photo_by_bytes(bot, chat_id, photo_bytes, caption)) is not None:
        return result
    logger.warning("Photo not sent; sending text")
    result = await send_message(bot, chat_id, caption)
    return result if result is not None else False

DB_FILE = Path(__file__).with_name("shafa_items.db")

def _apply_pragmas(conn: sqlite3.Connection):
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute("PRAGMA cache_size=-20000;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA mmap_size=268435456;")
    except Exception:
        pass

def _db_connect() -> sqlite3.Connection:
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
                brand TEXT, size TEXT, source TEXT, first_image_url TEXT,
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
        try:
            cols = [row[1] for row in conn.execute("PRAGMA table_info(shafa_items)").fetchall()]
            if "first_image_url" not in cols:
                conn.execute("ALTER TABLE shafa_items ADD COLUMN first_image_url TEXT")
        except Exception:
            pass
        conn.execute("CREATE INDEX IF NOT EXISTS idx_shafa_items_source ON shafa_items(source);")
        conn.commit()

def _db_upsert_items_sync(items: List[Tuple[ShafaItem, bool]], source_name: str):
    if not items:
        return
    with _db_connect() as conn:
        conn.executemany("""
            INSERT INTO shafa_items (id, name, link, price_text, price_int, brand, size, source, first_image_url, created_at, updated_at, last_sent_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'), CASE WHEN ? THEN datetime('now') ELSE NULL END)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name, link=excluded.link, price_text=excluded.price_text, price_int=excluded.price_int,
                brand=excluded.brand, size=excluded.size, source=excluded.source, updated_at=datetime('now'),
                first_image_url=CASE
                    WHEN excluded.first_image_url IS NOT NULL AND excluded.first_image_url <> '' THEN excluded.first_image_url
                    ELSE shafa_items.first_image_url
                END,
                last_sent_at=CASE WHEN ? THEN datetime('now') ELSE last_sent_at END
            """, [
                (item.id, item.name, item.link, item.price_text, item.price_int, item.brand, item.size, source_name, item.first_image_url,
                 1 if touch_last_sent else 0, 1 if touch_last_sent else 0)
                for item, touch_last_sent in items
            ])
        conn.commit()

def _db_fetch_existing_sync(item_ids: List[str]) -> List[Optional[Dict[str, Any]]]:
    if not item_ids:
        return []
    conn = _db_connect()
    try:
        # Batch query using IN clause - much faster than N individual queries
        placeholders = ','.join('?' * len(item_ids))
        query = f"SELECT id, name, link, price_text, price_int, brand, size, source, first_image_url, created_at, updated_at, last_sent_at FROM shafa_items WHERE id IN ({placeholders})"
        rows = conn.execute(query, item_ids).fetchall()
        # Build lookup dict for O(1) access
        items_dict = {row['id']: dict(row) for row in rows}
        # Return results in same order as input item_ids (preserving None for missing items)
        return [items_dict.get(item_id) for item_id in item_ids]
    finally:
        conn.close()

def _db_get_source_stats_sync(url: str) -> Dict[str, int]:
    with _db_connect() as conn:
        cur = conn.execute("SELECT no_items_streak, cycle_count FROM shafa_sources WHERE url = ?", (url,))
        row = cur.fetchone()
        if row:
            return {"streak": row[0], "cycle_count": row[1]}
        return {"streak": 0, "cycle_count": 0}

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

async def run_shafa_scraper():
    total_scraped = 0
    total_sent = 0
    total_new = 0
    errors = []
    def _add_error(msg: str):
        if msg:
            errors.append(str(msg)[:200])
    logger.info("SHAFA.UA start")
    if not PLAYWRIGHT_AVAILABLE:
        logger.error("Playwright is not installed. Run: pip install playwright && playwright install chromium")
        _add_error("Playwright not installed")
        return "; ".join(dict.fromkeys(errors))
    token = (TELEGRAM_OLX_BOT_TOKEN or "").strip().strip("'\"")
    default_chat = (DANYLO_DEFAULT_CHAT_ID or "").strip().strip("'\"")
    if not token:
        logger.error("Telegram token not set")
        _add_error("Telegram token not set")
        return "; ".join(dict.fromkeys(errors))
    bot: Optional[Bot] = None
    summary = ""
    try:
        await asyncio.to_thread(_db_init_sync)
        logger.info("Database ready")
    except Exception as e:
        logger.error(f"Database init failed: {e}")
        _add_error(f"DB init failed: {e}")
        return "; ".join(dict.fromkeys(errors))
    global _playwright_browser, _playwright_instance, _playwright_context, _http_session
    try:
        _playwright_instance = await async_playwright().start()
        _playwright_browser = await _playwright_instance.chromium.launch(headless=True)
        _playwright_context = await _playwright_browser.new_context(user_agent=USER_AGENT)
        logger.info("Playwright started")
    except Exception as e:
        logger.error(f"Playwright error: {e}")
        _add_error(f"Playwright error: {e}")
        return "; ".join(dict.fromkeys(errors))
    bot = Bot(token=token)
    async def _send_item_message(bot: Bot, chat_id: str, text: str, item: ShafaItem) -> bool:
        nonlocal total_sent
        try:
            image_url = item.first_image_url
            sent = await send_photo_with_upscale(bot, chat_id, text, image_url)
            if sent:
                total_sent += 1
            return bool(sent)
        except RetryAfter as e:
            logger.warning(f"Telegram rate limit hit; waiting {e.retry_after}s")
        except TimedOut:
            logger.warning("Timeout while sending")
            _add_error("Telegram send timeout")
        except Exception as e:
            logger.error(f"Send failed: {e}")
            _add_error(f"Send failed: {e}")
        return False
    async def _process_entry(entry: Dict[str, Any]):
        nonlocal total_scraped, total_new
        url, chat_id, source_name = entry.get("url"), default_chat, entry.get("url_name") or "SHAFA"
        if not url or not chat_id:
            return
        if SHAFA_REQUEST_JITTER_SEC > 0:
            await asyncio.sleep(random.uniform(0, SHAFA_REQUEST_JITTER_SEC))
        stats = await asyncio.to_thread(_db_get_source_stats_sync, url)
        streak = stats["streak"]
        cycle_count = stats["cycle_count"] + 1
        level = min(streak // 365, 23)
        divisor = level + 1
        if cycle_count % divisor != 0:
            logger.info(f"Skipping (cycle {cycle_count}/{divisor}, streak {streak})")
            await asyncio.to_thread(_db_update_source_stats_sync, url, streak, cycle_count)
            return
        try:
            items = await scrape_shafa_url(url)
            if items is None:
                return
            if items:
                new_streak = 0
                new_cycle = 0
            else:
                new_streak = streak + 1
                new_cycle = 0
            await asyncio.to_thread(_db_update_source_stats_sync, url, new_streak, new_cycle)
            if not items:
                return
            total_scraped += len(items)
            prev_items = await asyncio.to_thread(_db_fetch_existing_sync, [item.id for item in items])
            new_count = 0
            send_tasks = []
            items_to_send = []
            updates = []
            for idx, it in enumerate(items):
                prev = prev_items[idx]
                if prev and not it.first_image_url and prev.get("first_image_url"):
                    it.first_image_url = prev.get("first_image_url")
                if prev is None:
                    if not _has_numeric_price(it.price_text, it.price_int):
                        logger.warning(f"Skipping new item with empty/invalid price: {it.id}")
                        continue
                    new_count += 1
                    items_to_send.append(it)
                    send_tasks.append(_send_item_message(bot, chat_id, build_message(it, prev, source_name), it))
                    continue
                # Do not apply price updates when parser produced empty/invalid price.
                if not _has_numeric_price(it.price_text, it.price_int):
                    logger.warning(f"Skipping update with empty/invalid price for item {it.id}")
                    continue
                previous_price = prev.get("price_int") or 0
                if previous_price <= 0 and it.price_int > 0:
                    # Heal DB after previous bad/empty price without sending noisy updates.
                    updates.append((it, False))
                    continue
                if previous_price > 0 and it.price_int > previous_price:
                    # Price increased: update DB but do not notify.
                    updates.append((it, False))
                    continue
                price_diff = abs(it.price_int - previous_price)
                percent_change = (price_diff / previous_price * 100.0) if previous_price > 0 else None
                if price_diff < MIN_PRICE_DIFF or (percent_change is not None and percent_change < MIN_PRICE_DIFF_PERCENT):
                    continue
                items_to_send.append(it)
                send_tasks.append(_send_item_message(bot, chat_id, build_message(it, prev, source_name), it))
            if send_tasks:
                results = await asyncio.gather(*send_tasks, return_exceptions=True)
                for it, res in zip(items_to_send, results):
                    sent = False if isinstance(res, Exception) else bool(res)
                    updates.append((it, sent))
            if updates:
                await asyncio.to_thread(_db_upsert_items_sync, updates, source_name)
            total_new += new_count
        except aiohttp.ClientError as e:
            logger.error(f"Network error: {e}")
            _add_error("Network error")
        except Exception as e:
            logger.error(f"Processing error: {e}")
            _add_error(f"Processing error: {e}")
    try:
        sem = asyncio.Semaphore(SHAFA_TASK_CONCURRENCY)
        async def _guarded_process(entry: Dict[str, Any]):
            async with sem:
                await _process_entry(entry)
        sources = merge_sources(SHAFA_URLS or [], load_dynamic_urls("shafa"))
        if tasks := [_guarded_process(entry) for entry in sources]:
            logger.info(f"Sources: {len(tasks)}")
            await asyncio.gather(*tasks, return_exceptions=True)
        else:
            logger.warning("No URLs configured")
        logger.info("Completed")
        logger.info(f"New: {total_new} | Sent: {total_sent}")
        if total_scraped > 0:
            logger.info(f"Success rate: {(total_sent/total_scraped*100):.1f}%")
    finally:
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
        logger.info("Resources cleaned up")
        # Give time for async cleanup to complete
        await asyncio.sleep(0.5)
    if errors:
        uniq = []
        for item in errors:
            if item not in uniq:
                uniq.append(item)
            if len(uniq) >= 3:
                break
        summary = "; ".join(uniq)
        logger.warning(f"SHAFA run errors: {summary}")
    return summary

if __name__ == "__main__":
    asyncio.run(run_shafa_scraper())

