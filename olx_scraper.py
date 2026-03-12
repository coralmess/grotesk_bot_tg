from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from bs4 import BeautifulSoup
from telegram import Bot
from telegram.error import RetryAfter, TimedOut
from telegram.constants import ParseMode
from PIL import Image
import io, asyncio, re, sqlite3, aiohttp, random, logging
import aiosqlite
from html import escape
from functools import wraps
from config import TELEGRAM_OLX_BOT_TOKEN, DANYLO_DEFAULT_CHAT_ID, OLX_REQUEST_JITTER_SEC, RUN_USER_AGENT, RUN_ACCEPT_LANGUAGE, OLX_TASK_CONCURRENCY, OLX_HTTP_HTML_CONCURRENCY, OLX_HTTP_IMAGE_CONCURRENCY, OLX_UPSCALE_CONCURRENCY, OLX_SEND_CONCURRENCY, OLX_HTTP_CONNECTOR_LIMIT
from config_olx_urls import OLX_URLS
from helpers.dynamic_sources import load_dynamic_urls, merge_sources
from helpers.runtime_paths import OLX_ITEMS_DB_FILE
try:
    import lxml  # noqa: F401
    _LXML_AVAILABLE = True
except ImportError:
    _LXML_AVAILABLE = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
BASE_OLX = "https://www.olx.ua"
_HTTP_HTML_SEMAPHORE = asyncio.Semaphore(OLX_HTTP_HTML_CONCURRENCY)
_HTTP_IMAGE_SEMAPHORE = asyncio.Semaphore(OLX_HTTP_IMAGE_CONCURRENCY)
_SEND_SEMAPHORE = asyncio.Semaphore(OLX_SEND_CONCURRENCY)
_UPSCALE_SEMAPHORE = asyncio.Semaphore(OLX_UPSCALE_CONCURRENCY)
_http_session: Optional[aiohttp.ClientSession] = None
MIN_PRICE_DIFF = 50
MIN_PRICE_DIFF_PERCENT = 20.0
# Precompiled patterns reduce overhead in tight parsing loops.
PRICE_FRAGMENT_RE = re.compile(r"(\d[\d\s.,]*)")
SRCSET_PART_RE = re.compile(r"^\s*(\S+)(?:\s+([\d.]+[wx]))?\s*$", re.IGNORECASE)
NO_LISTINGS_UA_TEXT = "\u041c\u0438 \u0437\u043d\u0430\u0439\u0448\u043b\u0438 0 \u043e\u0433\u043e\u043b\u043e\u0448\u0435\u043d\u044c"
NO_LISTINGS_PATTERNS = (
    NO_LISTINGS_UA_TEXT.lower(),
    "we found 0 listings",
    "we found 0 ads",
    "0 listings found",
)
_PARSER = "lxml" if _LXML_AVAILABLE else "html.parser"
NO_LISTINGS_TEXT = NO_LISTINGS_UA_TEXT

class RetryableHttpStatus(Exception):
    def __init__(self, status: int, wait_s: float = 0.0, context: str = ""):
        self.status = status
        self.wait_s = max(0.0, float(wait_s or 0.0))
        self.context = context or "http"
        super().__init__(f"{self.context} status={self.status}")


def _get_http_session() -> aiohttp.ClientSession:
    global _http_session
    if (_http_session is None) or _http_session.closed:
        _http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25), connector=aiohttp.TCPConnector(limit=OLX_HTTP_CONNECTOR_LIMIT))
    return _http_session

def _clean_token(value: Optional[str]) -> str:
    return (value or "").strip().strip("'\"")


def _normalize_search_text(text: str) -> str:
    value = " ".join((text or "").replace("\xa0", " ").split())
    if not value:
        return value
    normalized_values = [value]
    # Recover common UTF-8 -> latin1 mojibake page text so "0 listings" is not missed.
    try:
        repaired = value.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
        if repaired:
            normalized_values.append(repaired)
    except Exception:
        pass
    return " ".join(normalized_values).lower()


def _contains_no_listings(text: str) -> bool:
    normalized = _normalize_search_text(text)
    return any(pattern in normalized for pattern in NO_LISTINGS_PATTERNS)

def async_retry(max_retries: int = 3, backoff_base: float = 1.0):
    """Decorator that retries async functions with exponential backoff."""
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except RetryAfter as e:
                    logger.warning(f"Rate limited, waiting {e.retry_after}s...")
                    await asyncio.sleep(e.retry_after)
                except TimedOut:
                    if attempt < max_retries - 1:
                        logger.warning(f"Timeout in {func.__name__}, retrying... ({attempt + 1}/{max_retries})")
                    await asyncio.sleep(backoff_base * (attempt + 1))
                except RetryableHttpStatus as e:
                    if attempt < max_retries - 1:
                        wait_s = e.wait_s if e.wait_s > 0 else (backoff_base * (attempt + 1))
                        logger.warning(f"Retryable HTTP {e.status} in {func.__name__}, waiting {wait_s:.1f}s")
                        await asyncio.sleep(wait_s)
                    else:
                        logger.warning(f"{func.__name__} exhausted retries for HTTP {e.status}")
                except Exception as e:
                    if attempt < max_retries - 1:
                        await asyncio.sleep(backoff_base * (attempt + 1) + random.random())
                    else:
                        logger.error(f"{func.__name__} failed after {max_retries} retries: {e}")
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
    first_image_url: Optional[str] = None

def normalize_price(text: str) -> Tuple[str, int]:
    raw = (text or "").replace(" ", " ").strip()
    if not raw:
        return "", 0
    match = PRICE_FRAGMENT_RE.search(raw)
    if not match:
        return raw, 0
    num = match.group(1).replace(" ", "").replace("\u00a0", "")
    if not num:
        return raw, 0
    if "." in num and "," in num:
        # Mixed separators: infer decimal by the rightmost separator.
        decimal_sep = "," if num.rfind(",") > num.rfind(".") else "."
        thousands_sep = "." if decimal_sep == "," else ","
        num = num.replace(thousands_sep, "")
        if decimal_sep == ",":
            num = num.replace(",", ".")
    elif "," in num:
        parts = num.split(",")
        # Treat as thousands separators only when each group after the first has 3 digits.
        if len(parts) > 1 and all(len(p) == 3 for p in parts[1:]):
            num = "".join(parts)
        else:
            num = ".".join(parts)
    elif "." in num:
        parts = num.split(".")
        if len(parts) > 1 and all(len(p) == 3 for p in parts[1:]):
            num = "".join(parts)
    try:
        value = float(num)
    except ValueError:
        return raw, 0
    price_int = int(round(value))
    price_text = f"{value:.2f} грн" if "." in num else f"{price_int} грн"
    return price_text, price_int

def extract_id_from_link(link: str) -> str:
    slug = link.rstrip("/").split("/")[-1].split("?", 1)[0]
    return slug[:-5] if slug.endswith(".html") else slug

def _extract_name_from_card(card, title_anchor) -> str:
    if title_anchor and (name := title_anchor.get_text(strip=True)):
        return name
    name_el = card.find(["h4", "h3"]) or card.find("img", alt=True)
    return name_el.get_text(strip=True) if hasattr(name_el, "get_text") else (name_el.get("alt", "").strip() if name_el else "")

def _extract_state_from_card(card) -> Optional[str]:
    st = card.find("span", attrs={"title": True})
    return str(st.get("title")).strip() if st and st.get("title") else (st.get_text(strip=True) if st else None)

def _extract_size_from_card(card) -> Optional[str]:
    size_el = card.find(class_="css-rkfuwj")
    return size_el.get_text(" ", strip=True) if size_el else None

def _is_valid_image_url(url: Optional[str]) -> bool:
    if not url or not (url := url.strip()).startswith(("http://", "https://")):
        return False
    return not any(p in url.lower() for p in ["no_thumbnail", "placeholder", "no-image", "noimage", ".svg"]) and not url.startswith("data:")


def _extract_first_image_from_card(card) -> Optional[str]:
    """Extract first image URL from card element - prioritizes highest quality from srcset."""
    if not (img := card.find("img")):
        logger.debug("No img tag found in card")
        return None
    
    for attr in ["data-src", "data-lazy-src"]:
        if (url := img.get(attr)) and _is_valid_image_url(url):
            logger.debug(f"Extracted image from {attr}: {url[:80]}...")
            return url
    
    if (srcset := img.get("srcset")) and (best := _parse_highest_from_srcset(srcset)):
        if _is_valid_image_url(best):
            logger.debug(f"Extracted image from srcset: {best[:80]}...")
            return best
        logger.debug(f"Invalid image URL from srcset: {best[:100]}")
    
    if (src := img.get("src")):
        if _is_valid_image_url(src):
            logger.debug(f"Extracted image from src: {src[:80]}...")
            return src
        logger.debug(f"Invalid image URL from src: {src[:100]}")
    
    logger.debug("No valid image URL found in card, will fetch from detail page")
    return None


def parse_card(card) -> Optional[OlxItem]:
    """Parse OLX card element into OlxItem."""
    try:
        anchors = card.find_all("a", href=True)
        title_anchor = next((a for a in anchors if a.get_text(strip=True)), None)
        a = title_anchor or (anchors[0] if anchors else None)
        if not (href := a["href"] if a else None):
            return None
        link = href if href.startswith("http") else f"{BASE_OLX}{href}"
        name = _extract_name_from_card(card, title_anchor)
        price_el = card.find(attrs={"data-testid": "ad-price"})
        price_text, price_int = normalize_price(price_el.get_text(" ", strip=True) if price_el else "")
        item_id = extract_id_from_link(link)
        if not (name and link and item_id):
            return None
        return OlxItem(
            id=item_id, name=name, link=link, price_text=price_text, price_int=price_int,
            state=_extract_state_from_card(card), size=_extract_size_from_card(card),
            first_image_url=_extract_first_image_from_card(card)
        )
    except Exception as e:
        logger.debug(f"Failed to parse card: {e}")
        return None


def collect_cards_with_stop(soup: BeautifulSoup) -> List:
    cards = []
    for el in soup.find_all(True, recursive=True):
        classes = el.get("class") or []
        classes = [classes] if isinstance(classes, str) else classes
        if "css-wsrviy" in classes:
            break
        if el.name == "div" and (el.get("data-cy") == "l-card" or el.get("data-testid") == "l-card"):
            cards.append(el)
    return cards

@async_retry(max_retries=3, backoff_base=1.0)
async def fetch_html(url: str) -> str:
    """Fetch HTML content from URL with retry logic and delay for lazy-loaded images."""
    headers = {
        "User-Agent": RUN_USER_AGENT,
        "Accept-Language": RUN_ACCEPT_LANGUAGE,
    }
    async with _HTTP_HTML_SEMAPHORE:
        session = _get_http_session()
        async with session.get(url, headers=headers) as r:
            if r.status == 429:
                retry_after = r.headers.get("Retry-After")
                wait_s = int(retry_after) if retry_after and retry_after.isdigit() else 15
                logger.warning(f"⏳ OLX rate limited (429). Sleeping {wait_s}s before retry.")
                await asyncio.sleep(wait_s)
                raise aiohttp.ClientResponseError(r.request_info, r.history, status=r.status)
            if r.status == 403:
                logger.warning("⛔ OLX forbidden (403). Backing off for 60s.")
                await asyncio.sleep(60)
                raise aiohttp.ClientResponseError(r.request_info, r.history, status=r.status)
            r.raise_for_status()
            return await r.text()


async def scrape_olx_url(url: str) -> Optional[List[OlxItem]]:
    """Scrape OLX URL and return list of items with images included. Returns None on error."""
    if not (html := await fetch_html(url)):
        logger.warning(f"⚠️  No HTML content received from {url}")
        return None
    
    if _contains_no_listings(html):
        logger.info(f"No listings found at {url}")
        return []

    soup = BeautifulSoup(html, _PARSER)
    try:
        if _contains_no_listings(soup.get_text(" ", strip=True)):
            logger.info(f"No listings found at {url}")
            return []
    except Exception as e:
        logger.debug(f"Error checking for zero listings: {e}")

    cards = collect_cards_with_stop(soup)
    items = [item for card in cards if (item := parse_card(card))]
    return items

@async_retry(max_retries=3, backoff_base=2.0)
async def send_message(bot: Bot, chat_id: str, text: str) -> bool:
    """Send text message via Telegram bot."""
    async with _SEND_SEMAPHORE:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=False)
    return True

def _escape_html_dict(data: Dict[str, Optional[str]]) -> Dict[str, str]:
    """Helper to escape all HTML values in dict."""
    return {key: escape(val or "", quote=True) for key, val in data.items()}


def build_message(item: OlxItem, prev: Optional[Dict[str, Any]], source_name: str) -> str:
    """Build Telegram message from OlxItem."""
    safe = _escape_html_dict({"name": item.name, "state": item.state, "size": item.size, "source": source_name or "OLX", "link": item.link})
    open_link = f'<a href="{safe["link"]}">Відкрити</a>'
    state_line = f"\n🥪 Стан: {safe['state']}" if safe["state"] else ""
    size_line = f"\n📏 Розмір: {safe['size']}" if safe["size"] else ""
    
    if not prev:
        return f"✨{safe['name']}✨ \n\n💰 Ціна: {item.price_text}{state_line}{size_line}\n🍘 Лінка: {safe['source']}\n🔗 {open_link}"
    if prev and prev.get("price_int") != item.price_int:
        was = prev.get("price_int") or 0
        return f"OLX Price changed: {safe['name']}\n\n💰 Ціна: {item.price_text} (було {was} грн){state_line}{size_line}\n🍘 Лінка: {safe['source']}\n🔗 {open_link}"
    return f"OLX: {safe['name']}\n\n💰 Ціна: {item.price_text}{state_line}{size_line}\n🍘 Лінка: {safe['source']}\n🔗 {open_link}"

def _parse_highest_from_srcset(srcset: str) -> Optional[str]:
    if not srcset:
        return None
    best_url = None
    best_score = -1.0
    # Supports both width descriptors (320w) and density descriptors (2x).
    for part in srcset.split(","):
        part = part.strip()
        if not part:
            continue
        m = SRCSET_PART_RE.match(part)
        if m:
            url = m.group(1)
            descriptor = (m.group(2) or "").lower()
        else:
            tokens = part.split()
            if not tokens:
                continue
            url = tokens[0]
            descriptor = (tokens[1].lower() if len(tokens) > 1 else "")

        score = 1.0
        if descriptor.endswith("w"):
            try:
                score = float(descriptor[:-1])
            except Exception:
                score = 1.0
        elif descriptor.endswith("x"):
            try:
                score = float(descriptor[:-1]) * 1000.0
            except Exception:
                score = 1.0

        if score >= best_score:
            best_score = score
            best_url = url
    return best_url


async def fetch_item_images(item_url: str, max_images: int = 3) -> List[str]:
    """Fetch multiple images from item detail page."""
    try:
        if not (html := await fetch_html(item_url)):
            return []
        soup = BeautifulSoup(html, _PARSER)
        if not (wrapper := soup.find("div", class_="swiper-wrapper")):
            return []
        imgs = []
        for slide in wrapper.find_all(["div", "img"], recursive=True):
            img = slide if slide.name == "img" else slide.find("img")
            if img and (best := (_parse_highest_from_srcset(img.get("srcset")) if img.get("srcset") else img.get("src"))) and best not in imgs:
                imgs.append(best)
                if len(imgs) >= max_images:
                    break
        return imgs[:max_images]
    except Exception as e:
        logger.debug(f"Failed to fetch images from {item_url}: {e}")
        return []

async def fetch_first_image_best(item_url: str) -> Optional[str]:
    """Fetch first image from item detail page."""
    try:
        if not (html := await fetch_html(item_url)):
            return None
        soup = BeautifulSoup(html, _PARSER)
        if not (wrapper := soup.find("div", class_="swiper-wrapper")):
            return None
        img_tag = None
        for slide in wrapper.find_all(["div", "img"], recursive=True):
            if slide.name == "img":
                img_tag = slide
                break
            elif img_tag := slide.find("img"):
                break
        if not img_tag:
            return None
        return _parse_highest_from_srcset(img_tag.get("srcset")) if img_tag.get("srcset") else img_tag.get("src")
    except Exception as e:
        logger.debug(f"Failed to fetch first image from {item_url}: {e}")
        return None


def _upscale_image_bytes_sync(
    img_bytes: bytes,
    scale: float = 2.0,
    max_dim: int = 5000,
    min_upscale_dim: int = 1280,
) -> Optional[bytes]:
    """Synchronous image upscaling (called via thread pool)."""
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
        logger.error(f"🖼️  Image upscaling failed: {e}")
        return None

async def _upscale_image_bytes(
    img_bytes: bytes,
    scale: float = 2.0,
    max_dim: int = 5000,
    min_upscale_dim: int = 1280,
) -> Optional[bytes]:
    """Async wrapper for image upscaling."""
    async with _UPSCALE_SEMAPHORE:
        return await asyncio.to_thread(_upscale_image_bytes_sync, img_bytes, scale, max_dim, min_upscale_dim)


@async_retry(max_retries=3, backoff_base=1.0)
async def _download_bytes(url: str, timeout_s: int = 30) -> Optional[bytes]:
    """Download bytes from URL with retry logic."""
    if not _is_valid_image_url(url):
        logger.debug(f"Skipping invalid/placeholder image URL: {url}")
        return None

    headers = {
        "User-Agent": RUN_USER_AGENT,
        "Accept-Language": RUN_ACCEPT_LANGUAGE,
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    }
    async with _HTTP_IMAGE_SEMAPHORE:
        session = _get_http_session()
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout_s)) as r:
            if r.status == 429:
                retry_after = r.headers.get("Retry-After")
                wait_s = int(retry_after) if retry_after and retry_after.isdigit() else 15
                raise RetryableHttpStatus(429, wait_s=wait_s, context="image download")
            if r.status == 403:
                # Keep 403 conservative: avoid long retry loops and fall back to text-only send.
                logger.warning("Image forbidden (403). Falling back to text-only send.")
                return None
            r.raise_for_status()
            return await r.read()

@async_retry(max_retries=3, backoff_base=2.0)
async def _send_photo_by_url(bot: Bot, chat_id: str, photo_url: str, caption: str) -> bool:
    """Send photo by URL."""
    async with _SEND_SEMAPHORE:
        await bot.send_photo(chat_id=chat_id, photo=photo_url, caption=caption, parse_mode=ParseMode.HTML)
    return True

@async_retry(max_retries=3, backoff_base=2.0)
async def _send_photo_by_bytes(bot: Bot, chat_id: str, photo_bytes: bytes, caption: str) -> bool:
    """Send photo by bytes."""
    async with _SEND_SEMAPHORE:
        await bot.send_photo(chat_id=chat_id, photo=io.BytesIO(photo_bytes), caption=caption, parse_mode=ParseMode.HTML)
    return True


async def send_photo_with_upscale(bot: Bot, chat_id: str, caption: str, image_url: Optional[str]) -> bool:
    """Send photo with upscaling."""
    if not image_url or not _is_valid_image_url(image_url):
        logger.warning(f"⚠️  {'No' if not image_url else 'Invalid'} image URL{': ' + image_url[:100] if image_url else ''}, sending text-only message")
        result = await send_message(bot, chat_id, caption)
        return result if result is not None else False
    
    if not (raw := await _download_bytes(image_url)):
        logger.warning(f"⚠️  Failed to download image from {image_url[:100]}, falling back to text-only message")
        result = await send_message(bot, chat_id, caption)
        return result if result is not None else False
    
    photo_bytes = (await _upscale_image_bytes(raw)) or raw
    
    if (result := await _send_photo_by_bytes(bot, chat_id, photo_bytes, caption)) is not None:
        return result
    
    logger.warning(f"⚠️  Failed to send photo after retries, falling back to text-only message")
    result = await send_message(bot, chat_id, caption)
    return result if result is not None else False

DB_FILE = OLX_ITEMS_DB_FILE

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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS olx_items (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, link TEXT NOT NULL,
                price_text TEXT NOT NULL, price_int INTEGER NOT NULL,
                state TEXT, size TEXT, source TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                last_sent_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS olx_sources (
                url TEXT PRIMARY KEY,
                no_items_streak INTEGER DEFAULT 0,
                cycle_count INTEGER DEFAULT 0,
                last_checked_at TEXT DEFAULT (datetime('now'))
            )
        """)
        try:
            cursor = conn.execute("PRAGMA table_info(olx_items)")
            cols = [col[1] for col in cursor.fetchall()]
            if 'size' not in cols:
                conn.execute("ALTER TABLE olx_items ADD COLUMN size TEXT")
                logger.info("Added missing 'size' column to database")
        except Exception as e:
            logger.error(f"❌ Migration error: {e}")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_olx_items_source ON olx_items(source);")
        conn.commit()

async def db_init():
    await asyncio.to_thread(_db_init_sync)


def _db_get_item_sync(item_id: str, conn: Optional[sqlite3.Connection] = None) -> Optional[Dict[str, Any]]:
    """Get item from database."""
    close_conn = conn is None
    if close_conn:
        conn = _db_connect()
    try:
        cur = conn.execute("SELECT id, name, link, price_text, price_int, state, size, source, created_at, updated_at, last_sent_at FROM olx_items WHERE id = ?", (item_id,))
        return dict(row) if (row := cur.fetchone()) else None
    finally:
        if close_conn:
            conn.close()

async def db_get_item(item_id: str) -> Optional[Dict[str, Any]]:
    """Async wrapper for getting item from database."""
    return await asyncio.to_thread(_db_get_item_sync, item_id, None)

def _db_upsert_item_sync(item: OlxItem, source_name: str, touch_last_sent: bool, conn: Optional[sqlite3.Connection] = None):
    """Upsert item to database."""
    close_conn = conn is None
    if close_conn:
        conn = _db_connect()
    try:
        conn.execute("""
            INSERT INTO olx_items (id, name, link, price_text, price_int, state, size, source, created_at, updated_at, last_sent_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'), CASE WHEN ? THEN datetime('now') ELSE NULL END)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name, link=excluded.link, price_text=excluded.price_text, price_int=excluded.price_int,
                state=excluded.state, size=excluded.size, source=excluded.source, updated_at=datetime('now'),
                last_sent_at=CASE WHEN ? THEN datetime('now') ELSE last_sent_at END
            """, (item.id, item.name, item.link, item.price_text, item.price_int, item.state, item.size, source_name, 1 if touch_last_sent else 0, 1 if touch_last_sent else 0))
        conn.commit()
    finally:
        if close_conn:
            conn.close()

async def db_upsert_item(item: OlxItem, source_name: str, touch_last_sent: bool):
    """Async wrapper for upserting item to database."""
    await asyncio.to_thread(_db_upsert_item_sync, item, source_name, touch_last_sent, None)


def _db_fetch_existing_sync(item_ids: List[str]) -> List[Optional[Dict[str, Any]]]:
    """Fetch existing items using a single shared connection with batch query."""
    if not item_ids:
        return []
    
    conn = _db_connect()
    try:
        # Batch query using IN clause - much faster than N individual queries
        placeholders = ','.join('?' * len(item_ids))
        query = f"SELECT id, name, link, price_text, price_int, state, size, source, created_at, updated_at, last_sent_at FROM olx_items WHERE id IN ({placeholders})"
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
        cur = conn.execute("SELECT no_items_streak, cycle_count FROM olx_sources WHERE url = ?", (url,))
        row = cur.fetchone()
        if row:
            return {"streak": row[0], "cycle_count": row[1]}
        return {"streak": 0, "cycle_count": 0}

async def db_get_source_stats(url: str) -> Dict[str, int]:
    return await asyncio.to_thread(_db_get_source_stats_sync, url)

def _db_update_source_stats_sync(url: str, streak: int, cycle_count: int):
    with _db_connect() as conn:
        conn.execute("""
            INSERT INTO olx_sources (url, no_items_streak, cycle_count, last_checked_at)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(url) DO UPDATE SET
                no_items_streak=excluded.no_items_streak,
                cycle_count=excluded.cycle_count,
                last_checked_at=datetime('now')
        """, (url, streak, cycle_count))
        conn.commit()

async def db_update_source_stats(url: str, streak: int, cycle_count: int):
    await asyncio.to_thread(_db_update_source_stats_sync, url, streak, cycle_count)


async def run_olx_scraper():
    """Main scraper function."""
    logger.info("OLX Scraper started")
    errors = []

    def _add_error(msg: str):
        if msg:
            errors.append(str(msg)[:200])

    token, default_chat = _clean_token(TELEGRAM_OLX_BOT_TOKEN), _clean_token(DANYLO_DEFAULT_CHAT_ID)
    if not token:
        logger.warning("No Telegram bot token configured")
        _add_error("No Telegram bot token configured")
        return "; ".join(dict.fromkeys(errors))

    try:
        await db_init()
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        _add_error(f"DB init failed: {e}")
        return "; ".join(dict.fromkeys(errors))

    bot = Bot(token=token)
    db_conn = None
    db_lock = asyncio.Lock()

    # Statistics tracking
    total_scraped = 0
    total_without_images = 0

    UPSERT_SQL = """
        INSERT INTO olx_items (id, name, link, price_text, price_int, state, size, source, created_at, updated_at, last_sent_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'), CASE WHEN ? THEN datetime('now') ELSE NULL END)
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name, link=excluded.link, price_text=excluded.price_text, price_int=excluded.price_int,
            state=excluded.state, size=excluded.size, source=excluded.source, updated_at=datetime('now'),
            last_sent_at=CASE WHEN ? THEN datetime('now') ELSE last_sent_at END
    """

    async def _open_writer_conn() -> aiosqlite.Connection:
        conn = await aiosqlite.connect(DB_FILE)
        for pragma in ("PRAGMA journal_mode=WAL;", "PRAGMA synchronous=NORMAL;", "PRAGMA busy_timeout=5000;"):
            await conn.execute(pragma)
        await conn.commit()
        return conn

    async def _persist_item_state(item: OlxItem, source_name: str, touch_last_sent: bool):
        # Persist immediately after each send attempt to preserve crash-durability semantics.
        async with db_lock:
            await db_conn.execute(
                UPSERT_SQL,
                (
                    item.id,
                    item.name,
                    item.link,
                    item.price_text,
                    item.price_int,
                    item.state,
                    item.size,
                    source_name,
                    1 if touch_last_sent else 0,
                    1 if touch_last_sent else 0,
                ),
            )
            await db_conn.commit()

    async def _send_item_message(bot: Bot, chat_id: str, text: str, item: OlxItem, source_name: str):
        """Send message for a single item."""
        nonlocal total_without_images
        try:
            image_url = item.first_image_url
            if not image_url:
                image_url = await fetch_first_image_best(item.link)
                if not image_url:
                    logger.warning(f"No image available for item {item.id}")
                    total_without_images += 1
            sent = await send_photo_with_upscale(bot, chat_id, text, image_url)
            await _persist_item_state(item, source_name, bool(sent))
            await asyncio.sleep(0.2)
        except RetryAfter as e:
            logger.warning(f"Rate limited for item {item.id}, waiting {e.retry_after}s")
        except TimedOut:
            logger.warning(f"Timeout sending item {item.id}")
            _add_error("Telegram send timeout")
        except Exception as e:
            logger.error(f"Failed to send item {item.id}: {e}")
            _add_error(f"Send item error: {e}")

    async def _process_entry(entry: Dict[str, Any]):
        nonlocal total_scraped
        # Always send to the single default chat id; ignore any per-entry chat override.
        url, chat_id, source_name = entry.get("url"), default_chat, entry.get("url_name") or "OLX"
        if not url or not chat_id:
            return
        if OLX_REQUEST_JITTER_SEC > 0:
            await asyncio.sleep(random.uniform(0, OLX_REQUEST_JITTER_SEC))

        stats = await db_get_source_stats(url)
        streak = stats["streak"]
        cycle_count = stats["cycle_count"] + 1

        level = min(streak // 365, 23)
        divisor = level + 1

        if cycle_count % divisor != 0:
            logger.info(f"Skipping {source_name} (Streak: {streak}, Level: {level}, Cycle: {cycle_count}/{divisor})")
            await db_update_source_stats(url, streak, cycle_count)
            return

        try:
            items = await scrape_olx_url(url)
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

            send_tasks = []
            for idx, it in enumerate(items):
                prev = prev_items[idx]
                if prev is None:
                    send_tasks.append(_send_item_message(bot, chat_id, build_message(it, prev, source_name), it, source_name))
                    continue

                previous_price = prev.get("price_int") or 0
                price_diff = abs(it.price_int - previous_price)
                percent_change = (price_diff / previous_price * 100.0) if previous_price > 0 else None

                # Skip notifications when price delta does not meet configured thresholds.
                if price_diff < MIN_PRICE_DIFF or (percent_change is not None and percent_change < MIN_PRICE_DIFF_PERCENT):
                    pct_display = f"{percent_change:.2f}%" if percent_change is not None else "N/A"
                    logger.debug(
                        "Skipping item %s due to minor price change (diff=%d UAH, %s)",
                        it.id,
                        price_diff,
                        pct_display,
                    )
                    continue

                send_tasks.append(_send_item_message(bot, chat_id, build_message(it, prev, source_name), it, source_name))

            if send_tasks:
                await asyncio.gather(*send_tasks, return_exceptions=True)

        except aiohttp.ClientError as e:
            logger.error(f"Network error processing {source_name}: {e}")
            _add_error(f"{source_name}: network error")
        except Exception as e:
            logger.error(f"Failed to process {source_name}: {e}")
            _add_error(f"{source_name}: {e}")

    try:
        db_conn = await _open_writer_conn()

        sem = asyncio.Semaphore(OLX_TASK_CONCURRENCY)

        async def _guarded_process(entry: Dict[str, Any]):
            async with sem:
                await _process_entry(entry)

        sources = merge_sources(OLX_URLS or [], load_dynamic_urls("olx"))
        if tasks := [_guarded_process(entry) for entry in sources]:
            logger.info(f"Processing {len(tasks)} OLX source(s)...")
            await asyncio.gather(*tasks, return_exceptions=True)
        else:
            logger.warning("No OLX URLs configured")

        logger.info("OLX scraper completed successfully")
        logger.info(
            f"TOTAL SCRAPED: {total_scraped} items | WITHOUT IMAGES: {total_without_images} items "
            f"({(total_without_images / total_scraped * 100) if total_scraped > 0 else 0:.1f}%)"
        )
        if errors:
            uniq = []
            for item in errors:
                if item not in uniq:
                    uniq.append(item)
                if len(uniq) >= 3:
                    break
            summary = "; ".join(uniq)
            logger.warning(f"OLX run errors: {summary}")
            return summary
        return ""
    finally:
        if db_conn is not None:
            try:
                await db_conn.close()
            except Exception:
                pass
        global _http_session
        if _http_session is not None and not _http_session.closed:
            try:
                await _http_session.close()
            except Exception:
                pass
            _http_session = None
        try:
            await bot.shutdown()
        except Exception:
            pass
        try:
            await bot.close()
        except Exception:
            pass



