from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from bs4 import BeautifulSoup
from telegram import Bot
from telegram.error import RetryAfter, TimedOut
import asyncio, re, sqlite3, aiohttp, random, logging
import aiosqlite
from html import escape
from config import (
    TELEGRAM_OLX_BOT_TOKEN,
    DANYLO_DEFAULT_CHAT_ID,
    OLX_REQUEST_JITTER_SEC,
    RUN_USER_AGENT,
    RUN_ACCEPT_LANGUAGE,
    OLX_TASK_CONCURRENCY,
    OLX_HTTP_HTML_CONCURRENCY,
    OLX_HTTP_IMAGE_CONCURRENCY,
    OLX_UPSCALE_CONCURRENCY,
    OLX_SEND_CONCURRENCY,
    OLX_HTTP_CONNECTOR_LIMIT,
    OLX_SOURCE_CHUNK_SIZE,
    OLX_SOURCE_CHUNK_PAUSE_MIN_SEC,
    OLX_SOURCE_CHUNK_PAUSE_MAX_SEC,
    MARKET_IMAGE_UPSCALE_MIN_DIM,
    MARKET_IMAGE_UPSCALE_MAX_DIM,
    MARKET_IMAGE_UPSCALE_FACTORS,
)
from config_olx_urls import OLX_URLS
from helpers.dynamic_sources import load_dynamic_urls, merge_sources
from helpers.marketplace_core import (
    MarketplaceItem,
    SourceStats,
    duplicate_key,
    finished_source_decision,
    make_source_decision,
    notification_storage_key,
)
from helpers.marketplace_pipeline import (
    ItemDecision,
    ItemUpdate,
    MarketplaceRepository,
    PipelineStats,
    RunDuplicateTracker,
    process_marketplace_items,
)
from helpers.marketplace_sender import (
    RetryableHttpStatus,
    async_retry,
    build_image_downloader,
    build_media_sender,
    build_message_sender,
    build_photo_sender,
)
from helpers.process_pool import run_cpu_bound
from helpers.scraper_unsubscribes import fetch_unsubscribed_ids
from helpers.runtime_paths import OLX_ITEMS_DB_FILE, SCRAPER_RUNS_JSONL_FILE
from helpers.scraper_stats import RunStatsCollector, utc_now_iso
from helpers.sqlite_runtime import RUNTIME_DB_PRAGMA_STATEMENTS, apply_runtime_pragmas
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
NOTIFICATION_CLAIM_STALE_MINUTES = 120
# Precompiled patterns reduce overhead in tight parsing loops.
PRICE_FRAGMENT_RE = re.compile(r"(\d[\d\s.,]*)")
SRCSET_PART_RE = re.compile(r"^\s*(\S+)(?:\s+([\d.]+[wx]))?\s*$", re.IGNORECASE)
OLX_IMAGE_SIZE_RE = re.compile(r"[?;&]s=(\d+)x(\d+)", re.IGNORECASE)
NO_LISTINGS_UA_TEXT = "\u041c\u0438 \u0437\u043d\u0430\u0439\u0448\u043b\u0438 0 \u043e\u0433\u043e\u043b\u043e\u0448\u0435\u043d\u044c"
NO_LISTINGS_PATTERNS = (
    NO_LISTINGS_UA_TEXT.lower(),
    "we found 0 listings",
    "we found 0 ads",
    "0 listings found",
)
_PARSER = "lxml" if _LXML_AVAILABLE else "html.parser"
NO_LISTINGS_TEXT = NO_LISTINGS_UA_TEXT
OLX_RESULT_BOUNDARY_PHRASES = (
    "Більше результатів",
    "Ми знайшли результати для схожих запитів",
    "Ми нічого не знайшли",
    "Немає оголошень",
    "Показано результати для",
    "More results",
    "No listings",
    "Showing results for",
)
OLX_RESULT_BOUNDARY_CLASSES = {
    "css-wsrviy",  # OLX separator class used before April 2026.
    "css-133tiyu",  # OLX separator class observed after recommendation layout update.
}


def _source_chunks(sources: List[Dict[str, Any]], chunk_size: int) -> List[List[Dict[str, Any]]]:
    size = max(1, int(chunk_size or 1))
    return [sources[index : index + size] for index in range(0, len(sources), size)]


def _next_chunk_pause(min_sec: float, max_sec: float) -> float:
    low = max(0.0, float(min_sec or 0.0))
    high = max(low, float(max_sec or 0.0))
    return random.uniform(low, high) if high > 0 else 0.0

def _get_http_session() -> aiohttp.ClientSession:
    global _http_session
    if (_http_session is None) or _http_session.closed:
        _http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25), connector=aiohttp.TCPConnector(limit=OLX_HTTP_CONNECTOR_LIMIT))
    return _http_session

def _clean_token(value: Optional[str]) -> str:
    return (value or "").strip().strip("'\"")


def _normalize_duplicate_name(name: str) -> str:
    return " ".join((name or "").split()).casefold()


def _duplicate_key(name: str, price_int: int) -> Optional[Tuple[str, int]]:
    return duplicate_key(name, price_int)


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

@dataclass
class OlxItem(MarketplaceItem):
    # OLX keeps only source-specific fields here; shared notification/dedupe behavior now
    # lives in the marketplace core so the two scrapers cannot drift again.
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


def _image_url_score(url: str, descriptor: str = "", order: int = 0) -> Tuple[float, float, int]:
    # OLX often puts the highest quality image in `src` while `srcset` contains a
    # slightly smaller crop. Score the explicit `;s=WIDTHxHEIGHT` transform first
    # so the scraper picks the largest real image, not just the largest srcset label.
    if match := OLX_IMAGE_SIZE_RE.search(url or ""):
        width, height = int(match.group(1)), int(match.group(2))
        return float(width * height), float(max(width, height)), order

    descriptor = (descriptor or "").lower()
    try:
        if descriptor.endswith("w"):
            width = float(descriptor[:-1])
            return width * width, width, order
        if descriptor.endswith("x"):
            density = float(descriptor[:-1])
            return density * 1_000_000.0, density, order
    except ValueError:
        pass
    return 1.0, 1.0, order


def _iter_srcset_candidates(srcset: str, start_order: int = 0) -> List[Tuple[str, str, int]]:
    candidates: List[Tuple[str, str, int]] = []
    for offset, part in enumerate((srcset or "").split(",")):
        part = part.strip()
        if not part:
            continue
        m = SRCSET_PART_RE.match(part)
        if m:
            url = m.group(1)
            descriptor = m.group(2) or ""
        else:
            tokens = part.split()
            if not tokens:
                continue
            url = tokens[0]
            descriptor = tokens[1] if len(tokens) > 1 else ""
        candidates.append((url, descriptor, start_order + offset))
    return candidates


def _select_best_image_url(candidates: List[Tuple[str, str, int]]) -> Optional[str]:
    best: Optional[Tuple[Tuple[float, float, int], str]] = None
    for url, descriptor, order in candidates:
        if not _is_valid_image_url(url):
            continue
        score = _image_url_score(url, descriptor, order)
        if best is None or score >= best[0]:
            best = (score, url)
    return best[1] if best else None


def _extract_best_image_from_img_tag(img) -> Optional[str]:
    candidates: List[Tuple[str, str, int]] = []
    order = 0
    for attr in ("src", "data-src", "data-lazy-src"):
        if url := img.get(attr):
            candidates.append((url, "", order))
            order += 1
    candidates.extend(_iter_srcset_candidates(img.get("srcset") or "", order))
    return _select_best_image_url(candidates)


def _extract_first_image_from_card(card) -> Optional[str]:
    """Extract first image URL from card element, preferring the largest OLX image transform."""
    if not (img := card.find("img")):
        logger.debug("No img tag found in card")
        return None

    if best := _extract_best_image_from_img_tag(img):
        logger.debug(f"Extracted best OLX image: {best[:80]}...")
        return best

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


def _is_olx_results_boundary(el) -> bool:
    classes = el.get("class") or []
    classes = [classes] if isinstance(classes, str) else classes
    if any(class_name in OLX_RESULT_BOUNDARY_CLASSES for class_name in classes):
        return True

    # OLX class names are generated and changed from css-wsrviy to css-133tiyu.
    # The stable part is the small separator text rendered before recommendation
    # cards, so use it as a fallback but only on compact elements to avoid
    # matching the whole page container before the real listings are visited.
    if el.name not in {"div", "p", "section"}:
        return False
    text = el.get_text(" ", strip=True)
    if not text or len(text) > 300:
        return False
    return any(phrase in text for phrase in OLX_RESULT_BOUNDARY_PHRASES)


def collect_cards_with_stop(soup: BeautifulSoup) -> List:
    cards = []
    for el in soup.find_all(True, recursive=True):
        if _is_olx_results_boundary(el):
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
        logger.debug(f"No listings found at {url}")
        return []

    soup = BeautifulSoup(html, _PARSER)
    try:
        if _contains_no_listings(soup.get_text(" ", strip=True)):
            logger.debug(f"No listings found at {url}")
            return []
    except Exception as e:
        logger.debug(f"Error checking for zero listings: {e}")

    cards = collect_cards_with_stop(soup)
    items = [item for card in cards if (item := parse_card(card))]
    return items

# Route OLX sends through the shared sender so timeout handling stays identical to SHAFA.
send_message = build_message_sender(send_semaphore=_SEND_SEMAPHORE)

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
    return _select_best_image_url(_iter_srcset_candidates(srcset))


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
            if img and (best := _extract_best_image_from_img_tag(img)) and best not in imgs:
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
        return _extract_best_image_from_img_tag(img_tag)
    except Exception as e:
        logger.debug(f"Failed to fetch first image from {item_url}: {e}")
        return None


_download_bytes = build_image_downloader(
    http_semaphore=_HTTP_IMAGE_SEMAPHORE,
    get_http_session=_get_http_session,
    user_agent=RUN_USER_AGENT,
    accept_language=RUN_ACCEPT_LANGUAGE,
    logger=logger,
)
_send_photo_by_bytes = build_photo_sender(send_semaphore=_SEND_SEMAPHORE)
# Keep image fallback policy centralized so transport fixes apply to both marketplace feeds.
send_photo_with_upscale = build_media_sender(
    is_valid_image_url=_is_valid_image_url,
    download_bytes=_download_bytes,
    send_message=send_message,
    send_photo_by_bytes=_send_photo_by_bytes,
    run_cpu_bound_fn=run_cpu_bound,
    logger=logger,
    min_upscale_dim=MARKET_IMAGE_UPSCALE_MIN_DIM,
    max_dim=MARKET_IMAGE_UPSCALE_MAX_DIM,
    upscale_factors=MARKET_IMAGE_UPSCALE_FACTORS,
)

DB_FILE = OLX_ITEMS_DB_FILE

def _apply_pragmas(conn: sqlite3.Connection):
    """Apply SQLite pragmas for better performance."""
    try:
        apply_runtime_pragmas(conn)
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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS olx_notifications (
                notification_key TEXT PRIMARY KEY,
                item_id TEXT NOT NULL,
                name TEXT NOT NULL,
                price_int INTEGER NOT NULL,
                source TEXT,
                state TEXT NOT NULL,
                claimed_at TEXT,
                sent_at TEXT,
                updated_at TEXT DEFAULT (datetime('now'))
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
        conn.execute("CREATE INDEX IF NOT EXISTS idx_olx_items_price_name ON olx_items(price_int, name);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_olx_notifications_price_state ON olx_notifications(price_int, state);")
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


def _notification_storage_key(key: Tuple[str, int]) -> str:
    return notification_storage_key(key)


def _db_claim_notification_key_sync(item: OlxItem, source_name: str) -> bool:
    key = _duplicate_key(item.name, item.price_int)
    if key is None:
        return True

    storage_key = _notification_storage_key(key)
    conn = _db_connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT state,
                   CASE
                       WHEN claimed_at IS NULL OR claimed_at <= datetime('now', ?)
                       THEN 1 ELSE 0
                   END AS is_stale
            FROM olx_notifications
            WHERE notification_key = ?
            """,
            (f"-{NOTIFICATION_CLAIM_STALE_MINUTES} minutes", storage_key),
        ).fetchone()
        if row is not None:
            if row["state"] == "sent":
                conn.commit()
                return False
            if row["state"] == "pending" and not bool(row["is_stale"]):
                conn.commit()
                return False
        conn.execute(
            """
            INSERT INTO olx_notifications (
                notification_key, item_id, name, price_int, source, state, claimed_at, sent_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, 'pending', datetime('now'), NULL, datetime('now'))
            ON CONFLICT(notification_key) DO UPDATE SET
                item_id=excluded.item_id,
                name=excluded.name,
                price_int=excluded.price_int,
                source=excluded.source,
                state='pending',
                claimed_at=datetime('now'),
                sent_at=NULL,
                updated_at=datetime('now')
            """,
            (storage_key, item.id, item.name, item.price_int, source_name),
        )
        conn.commit()
        return True
    finally:
        conn.close()


async def db_claim_notification_key(item: OlxItem, source_name: str) -> bool:
    return await asyncio.to_thread(_db_claim_notification_key_sync, item, source_name)


def _db_mark_notification_sent_sync(item: OlxItem, source_name: str) -> None:
    key = _duplicate_key(item.name, item.price_int)
    if key is None:
        return
    storage_key = _notification_storage_key(key)
    with _db_connect() as conn:
        conn.execute(
            """
            INSERT INTO olx_notifications (
                notification_key, item_id, name, price_int, source, state, claimed_at, sent_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, 'sent', datetime('now'), datetime('now'), datetime('now'))
            ON CONFLICT(notification_key) DO UPDATE SET
                item_id=excluded.item_id,
                name=excluded.name,
                price_int=excluded.price_int,
                source=excluded.source,
                state='sent',
                sent_at=datetime('now'),
                updated_at=datetime('now')
            """,
            (storage_key, item.id, item.name, item.price_int, source_name),
        )
        conn.commit()


async def db_mark_notification_sent(item: OlxItem, source_name: str) -> None:
    await asyncio.to_thread(_db_mark_notification_sent_sync, item, source_name)


def _db_release_notification_claim_sync(item: OlxItem, source_name: str) -> None:
    key = _duplicate_key(item.name, item.price_int)
    if key is None:
        return
    storage_key = _notification_storage_key(key)
    with _db_connect() as conn:
        conn.execute(
            """
            UPDATE olx_notifications
            SET item_id = ?,
                name = ?,
                price_int = ?,
                source = ?,
                state = 'failed',
                updated_at = datetime('now')
            WHERE notification_key = ?
            """,
            (item.id, item.name, item.price_int, source_name, storage_key),
        )
        conn.commit()


async def db_release_notification_claim(item: OlxItem, source_name: str) -> None:
    await asyncio.to_thread(_db_release_notification_claim_sync, item, source_name)


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


def _db_fetch_duplicate_keys_sync(items: List[OlxItem]) -> set[Tuple[str, int]]:
    candidate_map: Dict[Tuple[str, int], set[str]] = {}
    for item in items:
        if key := _duplicate_key(item.name, item.price_int):
            candidate_map.setdefault(key, set()).add(item.id)
    if not candidate_map:
        return set()

    conn = _db_connect()
    try:
        prices = sorted({price for _, price in candidate_map})
        placeholders = ",".join("?" * len(prices))
        query = f"SELECT id, name, price_int FROM olx_items WHERE price_int IN ({placeholders})"
        rows = conn.execute(query, prices).fetchall()
        notification_query = f"""
            SELECT notification_key, name, price_int
            FROM olx_notifications
            WHERE price_int IN ({placeholders}) AND state IN ('pending', 'sent')
        """
        notification_rows = conn.execute(notification_query, prices).fetchall()
        duplicates: set[Tuple[str, int]] = set()
        for row in rows:
            row_key = _duplicate_key(str(row["name"] or ""), int(row["price_int"] or 0))
            if row_key is None or row_key not in candidate_map:
                continue
            if str(row["id"]) not in candidate_map[row_key]:
                duplicates.add(row_key)
        for row in notification_rows:
            row_key = _duplicate_key(str(row["name"] or ""), int(row["price_int"] or 0))
            if row_key is not None and row_key in candidate_map:
                duplicates.add(row_key)
        return duplicates
    finally:
        conn.close()


async def db_fetch_duplicate_keys(items: List[OlxItem]) -> set[Tuple[str, int]]:
    return await asyncio.to_thread(_db_fetch_duplicate_keys_sync, items)


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


class OlxRepository(MarketplaceRepository[OlxItem]):
    # The repository contract keeps OLX storage separate while letting the shared pipeline
    # drive persistence and idempotency in the same order as SHAFA.
    async def fetch_existing(self, item_ids: list[str]) -> list[Optional[Dict[str, Any]]]:
        return await db_fetch_existing(item_ids)

    async def fetch_duplicate_keys(self, items: list[OlxItem]) -> set[Tuple[str, int]]:
        return await db_fetch_duplicate_keys(items)

    async def claim_notification_key(self, item: OlxItem, source_name: str) -> bool:
        return await db_claim_notification_key(item, source_name)

    async def mark_notification_sent(self, item: OlxItem, source_name: str) -> None:
        await db_mark_notification_sent(item, source_name)

    async def release_notification_claim(self, item: OlxItem, source_name: str) -> None:
        await db_release_notification_claim(item, source_name)

    async def persist_items(self, updates: list[ItemUpdate[OlxItem]], source_name: str) -> None:
        for update in updates:
            await db_upsert_item(update.item, source_name, update.touch_last_sent)

    async def get_source_stats(self, url: str) -> SourceStats:
        stats = await db_get_source_stats(url)
        return SourceStats(streak=stats["streak"], cycle_count=stats["cycle_count"])

    async def update_source_stats(self, url: str, streak: int, cycle_count: int) -> None:
        await db_update_source_stats(url, streak, cycle_count)


def _decide_olx_item(item: OlxItem, previous: Optional[Dict[str, Any]]) -> ItemDecision:
    if previous is None:
        return ItemDecision(send_notification=True, is_new_item=True)

    previous_price = previous.get("price_int") or 0
    price_diff = abs(item.price_int - previous_price)
    percent_change = (price_diff / previous_price * 100.0) if previous_price > 0 else None
    if price_diff < MIN_PRICE_DIFF or (percent_change is not None and percent_change < MIN_PRICE_DIFF_PERCENT):
        return ItemDecision()
    return ItemDecision(send_notification=True)


async def _legacy_run_olx_scraper():
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
    run_seen_duplicate_keys: set[Tuple[str, int]] = set()
    run_seen_lock = asyncio.Lock()

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
        for pragma in RUNTIME_DB_PRAGMA_STATEMENTS:
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

    async def _claim_duplicate_key_for_run(item: OlxItem) -> bool:
        key = _duplicate_key(item.name, item.price_int)
        if key is None:
            return True
        async with run_seen_lock:
            if key in run_seen_duplicate_keys:
                return False
            run_seen_duplicate_keys.add(key)
            return True

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
            if sent:
                await db_mark_notification_sent(item, source_name)
            else:
                await db_release_notification_claim(item, source_name)
            await _persist_item_state(item, source_name, bool(sent))
            await asyncio.sleep(0.2)
        except RetryAfter as e:
            logger.warning(f"Rate limited for item {item.id}, waiting {e.retry_after}s")
        except TimedOut:
            await db_mark_notification_sent(item, source_name)
            logger.warning(f"Timeout sending item {item.id}")
            _add_error("Telegram send timeout")
        except Exception as e:
            await db_release_notification_claim(item, source_name)
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
            logger.debug(f"Skipping {source_name} (Streak: {streak}, Level: {level}, Cycle: {cycle_count}/{divisor})")
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
            duplicate_keys_in_db = await db_fetch_duplicate_keys(items)
            unsubscribed_item_ids = await fetch_unsubscribed_ids("olx", [item.id for item in items])

            send_tasks = []
            for idx, it in enumerate(items):
                prev = prev_items[idx]
                if it.id in unsubscribed_item_ids:
                    logger.debug("Skipping unsubscribed OLX item: %s", it.id)
                    continue
                duplicate_key = _duplicate_key(it.name, it.price_int)
                if duplicate_key is not None and duplicate_key in duplicate_keys_in_db:
                    logger.debug("Skipping OLX duplicate already in DB: %s | %s грн", it.name, it.price_int)
                    continue
                if not await _claim_duplicate_key_for_run(it):
                    logger.debug("Skipping OLX duplicate in current run: %s | %s грн", it.name, it.price_int)
                    continue
                if prev is None:
                    if not await db_claim_notification_key(it, source_name):
                        logger.debug("Skipping OLX duplicate already claimed/sent: %s | %s грн", it.name, it.price_int)
                        continue
                    send_tasks.append(_send_item_message(bot, chat_id, build_message(it, prev, source_name), it, source_name))
                    continue

                previous_price = prev.get("price_int") or 0
                price_diff = abs(it.price_int - previous_price)
                percent_change = (price_diff / previous_price * 100.0) if previous_price > 0 else None

                # Intentionally keep the stored baseline unchanged for minor deltas.
                # We only care about alerting against the last significant price, not
                # every small oscillation, so sub-threshold changes must not update DB.
                if price_diff < MIN_PRICE_DIFF or (percent_change is not None and percent_change < MIN_PRICE_DIFF_PERCENT):
                    pct_display = f"{percent_change:.2f}%" if percent_change is not None else "N/A"
                    logger.debug(
                        "Skipping item %s due to minor price change (diff=%d UAH, %s)",
                        it.id,
                        price_diff,
                        pct_display,
                    )
                    continue

                if not await db_claim_notification_key(it, source_name):
                    logger.debug("Skipping OLX duplicate already claimed/sent: %s | %s грн", it.name, it.price_int)
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


async def run_olx_scraper():
    logger.info("OLX Scraper started")
    errors: list[str] = []
    run_stats = RunStatsCollector("olx")
    run_stats.set_deploy_metadata()

    def _add_error(msg: str) -> None:
        if msg:
            errors.append(str(msg)[:200])
            run_stats.record_error("runtime", message=str(msg)[:200])

    token = _clean_token(TELEGRAM_OLX_BOT_TOKEN)
    default_chat = _clean_token(DANYLO_DEFAULT_CHAT_ID)
    if not token:
        logger.warning("No Telegram bot token configured")
        _add_error("No Telegram bot token configured")
        return "; ".join(dict.fromkeys(errors))

    try:
        await db_init()
    except Exception as exc:
        logger.error("Database initialization failed: %s", exc)
        _add_error(f"DB init failed: {exc}")
        return "; ".join(dict.fromkeys(errors))

    bot = Bot(token=token)
    # The adapter still owns OLX fetch/parse quirks, but post-parse decisions now flow
    # through the shared marketplace pipeline for parity with SHAFA.
    repository = OlxRepository()
    duplicate_tracker = RunDuplicateTracker[OlxItem]()
    total_scraped = 0
    total_without_images = 0
    pipeline_totals = PipelineStats()

    async def _send_item(item: OlxItem, text: str, source_name: str) -> bool:
        nonlocal total_without_images
        try:
            image_url = item.first_image_url
            if not image_url:
                image_url = await fetch_first_image_best(item.link)
                if image_url:
                    item.first_image_url = image_url
            if not image_url:
                logger.warning("No image available for item %s", item.id)
                total_without_images += 1
            sent = await send_photo_with_upscale(bot, default_chat, text, image_url)
            await asyncio.sleep(0.2)
            return bool(sent)
        except RetryAfter as exc:
            logger.warning("Rate limited for item %s, waiting %ss", item.id, exc.retry_after)
            _add_error("Telegram rate limited")
        except TimedOut:
            logger.warning("Timeout sending item %s", item.id)
            _add_error("Telegram send timeout")
            return True
        except Exception as exc:
            logger.error("Failed to send item %s: %s", item.id, exc)
            _add_error(f"Send item error: {exc}")
        return False

    async def _process_entry(entry: Dict[str, Any]) -> None:
        nonlocal total_scraped
        url = entry.get("url")
        source_name = entry.get("url_name") or "OLX"
        if not url or not default_chat:
            run_stats.inc("sources_invalid")
            return
        if OLX_REQUEST_JITTER_SEC > 0:
            await asyncio.sleep(random.uniform(0, OLX_REQUEST_JITTER_SEC))

        source_stats = await repository.get_source_stats(url)
        source_decision = make_source_decision(source_stats)
        if not source_decision.should_process:
            logger.debug(
                "Skipping %s (Streak: %s, Level: %s, Cycle: %s/%s)",
                source_name,
                source_stats.streak,
                source_decision.level,
                source_decision.next_cycle_count,
                source_decision.divisor,
            )
            await repository.update_source_stats(url, source_decision.next_streak, source_decision.next_cycle_count)
            run_stats.inc("sources_skipped_by_backoff")
            run_stats.record_source(source_name, status="skipped_by_backoff", url=url)
            return

        try:
            run_stats.inc("sources_attempted")
            items = await scrape_olx_url(url)
            if items is None:
                run_stats.inc("sources_failed")
                run_stats.record_source(source_name, status="scrape_failed", url=url)
                run_stats.record_error("scrape_failed", source=source_name)
                return

            next_streak, next_cycle = finished_source_decision(source_stats.streak, len(items))
            await repository.update_source_stats(url, next_streak, next_cycle)
            if not items:
                run_stats.inc("sources_empty")
                run_stats.record_source(source_name, status="empty", url=url, items_scraped=0)
                return

            total_scraped += len(items)
            run_stats.inc("items_scraped", len(items))
            pipeline_stats = await process_marketplace_items(
                source_kind="olx",
                source_name=source_name,
                items=items,
                repository=repository,
                duplicate_tracker=duplicate_tracker,
                decide_item=_decide_olx_item,
                build_message=build_message,
                send_item=_send_item,
                logger=logger,
            )
            pipeline_totals.add(pipeline_stats)
            run_stats.inc("sources_with_items")
            run_stats.record_source(
                source_name,
                status="ok",
                url=url,
                items_scraped=len(items),
                new_items=pipeline_stats.total_new,
                sent_items=pipeline_stats.total_sent,
                skipped_items=(
                    pipeline_stats.total_unsubscribed
                    + pipeline_stats.total_duplicate_db
                    + pipeline_stats.total_duplicate_run
                    + pipeline_stats.total_notification_claim_skipped
                ),
            )
        except aiohttp.ClientError as exc:
            logger.error("Network error processing %s: %s", source_name, exc)
            _add_error(f"{source_name}: network error")
            run_stats.inc("sources_failed")
            run_stats.record_source(source_name, status="network_error", url=url)
            run_stats.record_error("network", source=source_name, message=str(exc)[:200])
        except Exception as exc:
            logger.error("Failed to process %s: %s", source_name, exc)
            _add_error(f"{source_name}: {exc}")
            run_stats.inc("sources_failed")
            run_stats.record_source(source_name, status="error", url=url, error=str(exc)[:120])
            run_stats.record_error(type(exc).__name__, source=source_name, message=str(exc)[:200])

    try:
        sem = asyncio.Semaphore(OLX_TASK_CONCURRENCY)

        async def _guarded_process(entry: Dict[str, Any]) -> None:
            async with sem:
                await _process_entry(entry)

        sources = merge_sources(OLX_URLS or [], load_dynamic_urls("olx"))
        run_stats.set_field("sources_total", len(sources))
        source_chunks = _source_chunks(sources, OLX_SOURCE_CHUNK_SIZE)
        run_stats.set_field("source_chunk_size", max(1, int(OLX_SOURCE_CHUNK_SIZE or 1)))
        run_stats.set_field("source_chunks_total", len(source_chunks))
        if source_chunks:
            logger.info(
                "Processing %s OLX source(s) in %s chunk(s) of up to %s...",
                len(sources),
                len(source_chunks),
                max(1, int(OLX_SOURCE_CHUNK_SIZE or 1)),
            )
            for chunk_index, source_chunk in enumerate(source_chunks, start=1):
                # Chunking deliberately spreads OLX load over time. The scraper still
                # keeps per-source concurrency inside a chunk, but avoids one huge burst
                # competing with SHAFA, Telegram image work, and site-side rate limits.
                logger.info(
                    "Processing OLX chunk %s/%s (%s source(s))",
                    chunk_index,
                    len(source_chunks),
                    len(source_chunk),
                )
                await asyncio.gather(*[_guarded_process(entry) for entry in source_chunk], return_exceptions=True)
                if chunk_index < len(source_chunks):
                    pause_s = _next_chunk_pause(OLX_SOURCE_CHUNK_PAUSE_MIN_SEC, OLX_SOURCE_CHUNK_PAUSE_MAX_SEC)
                    run_stats.inc("source_chunk_pauses")
                    logger.info("OLX chunk %s/%s complete; sleeping %.1fs", chunk_index, len(source_chunks), pause_s)
                    if pause_s > 0:
                        await asyncio.sleep(pause_s)
        else:
            logger.warning("No OLX URLs configured")

        logger.info("OLX scraper completed successfully")
        logger.info(
            "TOTAL SCRAPED: %s items | WITHOUT IMAGES: %s items (%.1f%%)",
            total_scraped,
            total_without_images,
            (total_without_images / total_scraped * 100) if total_scraped > 0 else 0.0,
        )
        # These counters explain why scraped items did not become Telegram messages:
        # already known, unsubscribed, duplicated, claim-collided, or delivery failed.
        logger.info(
            "OLX pipeline: SEEN=%s | NEW=%s | SENT=%s | PERSIST_ONLY=%s | UNSUB=%s | "
            "DUP_DB=%s | DUP_RUN=%s | CLAIM_SKIP=%s | SEND_FAILED=%s",
            pipeline_totals.total_seen,
            pipeline_totals.total_new,
            pipeline_totals.total_sent,
            pipeline_totals.total_persisted_without_send,
            pipeline_totals.total_unsubscribed,
            pipeline_totals.total_duplicate_db,
            pipeline_totals.total_duplicate_run,
            pipeline_totals.total_notification_claim_skipped,
            pipeline_totals.total_send_failed,
        )
        run_stats.set_field("without_images", total_without_images)
        for field in (
            "total_seen",
            "total_new",
            "total_sent",
            "total_persisted_without_send",
            "total_unsubscribed",
            "total_duplicate_db",
            "total_duplicate_run",
            "total_notification_claim_skipped",
            "total_send_candidates",
            "total_send_failed",
        ):
            run_stats.set_field(field, getattr(pipeline_totals, field))
        run_stats.set_coverage(
            expected=len(sources),
            attempted=run_stats.counters.get("sources_attempted", 0),
            completed=run_stats.counters.get("sources_with_items", 0) + run_stats.counters.get("sources_empty", 0),
            blocked=run_stats.counters.get("sources_failed", 0),
            skipped=run_stats.counters.get("sources_skipped_by_backoff", 0),
        )
        run_stats.set_notification_funnel(
            seen=pipeline_totals.total_seen,
            candidates=pipeline_totals.total_send_candidates,
            new=pipeline_totals.total_new,
            persisted_without_send=pipeline_totals.total_persisted_without_send,
            sent=pipeline_totals.total_sent,
            failed=pipeline_totals.total_send_failed,
            skipped=(
                pipeline_totals.total_unsubscribed
                + pipeline_totals.total_duplicate_db
                + pipeline_totals.total_duplicate_run
                + pipeline_totals.total_notification_claim_skipped
            ),
        )
        run_stats.set_data_freshness(
            latest_source_check_utc=utc_now_iso(),
            sources_with_items=run_stats.counters.get("sources_with_items", 0),
            sources_empty=run_stats.counters.get("sources_empty", 0),
        )
        run_stats.set_resource_snapshot()
        run_stats.write_jsonl(SCRAPER_RUNS_JSONL_FILE, run_stats.finish(outcome="error" if errors else "success"))
        if errors:
            uniq: list[str] = []
            for item in errors:
                if item not in uniq:
                    uniq.append(item)
                if len(uniq) >= 3:
                    break
            summary = "; ".join(uniq)
            logger.warning("OLX run errors: %s", summary)
            return summary
        return ""
    finally:
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



