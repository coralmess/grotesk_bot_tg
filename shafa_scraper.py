from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from bs4 import BeautifulSoup
from telegram import Bot
from telegram.error import RetryAfter, TimedOut
import asyncio, re, sqlite3, aiohttp, random, logging
from html import escape
from functools import lru_cache
from urllib.parse import urljoin, urlsplit, urlunsplit
from config import (
    TELEGRAM_OLX_BOT_TOKEN,
    DANYLO_DEFAULT_CHAT_ID,
    SHAFA_REQUEST_JITTER_SEC,
    RUN_USER_AGENT,
    RUN_ACCEPT_LANGUAGE,
    SHAFA_TASK_CONCURRENCY,
    SHAFA_HTTP_CONCURRENCY,
    SHAFA_SEND_CONCURRENCY,
    SHAFA_UPSCALE_CONCURRENCY,
    SHAFA_PLAYWRIGHT_CONCURRENCY,
    SHAFA_HTTP_CONNECTOR_LIMIT,
    MARKET_IMAGE_UPSCALE_MIN_DIM,
    MARKET_IMAGE_UPSCALE_MAX_DIM,
    MARKET_IMAGE_UPSCALE_FACTORS,
)
from config_shafa_urls import SHAFA_URLS
from helpers.analytics_events import AnalyticsSink
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
from helpers.marketplace_playwright import PlaywrightRuntimeManager
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
from helpers.runtime_paths import SCRAPER_RUNS_JSONL_FILE, SHAFA_ITEMS_DB_FILE
from helpers.scraper_stats import RunStatsCollector, utc_now_iso
from helpers.sqlite_runtime import apply_runtime_pragmas

try:
    from playwright.async_api import async_playwright
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
_ANALYTICS_SINK = AnalyticsSink()
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
_HTTP_SEMAPHORE = asyncio.Semaphore(SHAFA_HTTP_CONCURRENCY)
_SEND_SEMAPHORE = asyncio.Semaphore(SHAFA_SEND_CONCURRENCY)
_UPSCALE_SEMAPHORE = asyncio.Semaphore(SHAFA_UPSCALE_CONCURRENCY)
_PLAYWRIGHT_SEMAPHORE = asyncio.Semaphore(SHAFA_PLAYWRIGHT_CONCURRENCY)  # Limit concurrent browser instances
_http_session: Optional[aiohttp.ClientSession] = None
_playwright_runtime: Optional[PlaywrightRuntimeManager] = None
MIN_PRICE_DIFF = 50
MIN_PRICE_DIFF_PERCENT = 25.0
NOTIFICATION_CLAIM_STALE_MINUTES = 120
_PARSER = "lxml" if _LXML_AVAILABLE else "html.parser"
# Hot-path regexes are precompiled once so repeated card parsing stays cheap.
NON_DIGIT_RE = re.compile(r"[^\d]")
HAS_DIGIT_RE = re.compile(r"\d")
CURRENCY_RE = re.compile(r"(грн|uah|₴)", re.IGNORECASE)
PRICE_NOISE_RE = re.compile(r"(грн|uah|₴|\d+\s*%?)", re.IGNORECASE)
ITEM_ID_RE = re.compile(r"(\d+)")
ITEM_SLUG_RE = re.compile(r"^\d{6,}(?:-[a-z0-9-]+)?$", re.IGNORECASE)
INVALID_SHAFA_PATH_PARTS = ("/my/", "/msg/", "/member/", "/api/", "/social/", "/login")

if not _LXML_AVAILABLE:
    logger.warning("lxml not found; using html.parser")

def _get_http_session() -> aiohttp.ClientSession:
    global _http_session
    if (_http_session is None) or _http_session.closed:
        _http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25), connector=aiohttp.TCPConnector(limit=SHAFA_HTTP_CONNECTOR_LIMIT))
    return _http_session


def _normalize_duplicate_name(name: str) -> str:
    return " ".join((name or "").split()).casefold()


def _duplicate_key(name: str, price_int: int) -> Optional[Tuple[str, int]]:
    return duplicate_key(name, price_int)

@dataclass
class ShafaItem(MarketplaceItem):
    # SHAFA keeps parser-specific metadata here while duplicate suppression, unsubscribe
    # handling, and send ordering are shared with OLX through the marketplace core.
    id: str
    name: str
    link: str
    price_text: str
    price_int: int
    brand: Optional[str] = None
    size: Optional[str] = None
    first_image_url: Optional[str] = None

def normalize_price(text: str) -> Tuple[str, int]:
    digits = NON_DIGIT_RE.sub("", text or "")
    price_int = int(digits) if digits else 0
    return (f"{price_int} грн" if price_int else (text or "").strip()), price_int


def _has_numeric_price(price_text: Optional[str], price_int: int) -> bool:
    if price_int <= 0:
        return False
    return bool(HAS_DIGIT_RE.search(price_text or ""))


def _looks_like_item_href(href: Optional[str]) -> bool:
    if not href:
        return False
    return _looks_like_item_href_cached(href.strip())


@lru_cache(maxsize=8192)
def _looks_like_item_href_cached(href: str) -> bool:
    # Cache avoids re-validating identical hrefs across cards/cycles.
    if not href or href.startswith(("javascript:", "#", "mailto:", "tel:")):
        return False
    parsed = urlsplit(href)
    path = (parsed.path or "").rstrip("/")
    low = path.lower()
    if not low.startswith("/uk/"):
        return False
    if low in ("/uk", "/uk/"):
        return False
    for bad in INVALID_SHAFA_PATH_PARTS:
        if bad in low:
            return False
    # Filter links look like .../if/characteristics=123 and should never be treated as items.
    if "/if/" in low:
        return False
    parts = [p for p in low.split("/") if p]
    if len(parts) < 4:
        return False
    last = parts[-1]
    if "=" in last:
        return False
    return bool(ITEM_SLUG_RE.match(last))


def _normalize_item_url(href: str) -> str:
    return _normalize_item_url_cached((href or "").strip())


@lru_cache(maxsize=8192)
def _normalize_item_url_cached(href: str) -> str:
    # Canonical URL normalization is heavily reused; cache keeps it O(1) after first hit.
    absolute = urljoin(BASE_SHAFA, href)
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
        if not CURRENCY_RE.search(low):
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
    if ITEM_SLUG_RE.match(slug) and (match := ITEM_ID_RE.match(slug)):
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
                if PRICE_NOISE_RE.search(txt):
                    continue
                name_candidates.append(txt)
            name = max(name_candidates, key=len) if name_candidates else ""
            if not name and (img := card.find("img")):
                name = (img.get("alt") or "").strip()
        price_text, price_int = _extract_price_from_card(card)
        if price_int <= 0:
            return None
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
    global _playwright_runtime
    async with _PLAYWRIGHT_SEMAPHORE:
        try:
            if _playwright_runtime is None:
                logger.error("Playwright is not initialized")
                return None
            # Reusing the managed runtime avoids booting a fresh browser every cycle, which
            # used to make SHAFA runs slower and more bursty inside the shared market service.
            page = await _playwright_runtime.new_page()
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
            if _playwright_runtime is not None:
                await _playwright_runtime.reset(str(e))
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

send_message = build_message_sender(send_semaphore=_SEND_SEMAPHORE)

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

_download_bytes = build_image_downloader(
    http_semaphore=_HTTP_SEMAPHORE,
    get_http_session=_get_http_session,
    user_agent=RUN_USER_AGENT,
    accept_language=RUN_ACCEPT_LANGUAGE,
    logger=logger,
)
_send_photo_by_bytes = build_photo_sender(send_semaphore=_SEND_SEMAPHORE)
# Use the shared sender so SHAFA and OLX keep identical image fallback and timeout rules.
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
    source_kind="shafa",
    analytics_sink=_ANALYTICS_SINK,
)

DB_FILE = SHAFA_ITEMS_DB_FILE

def _apply_pragmas(conn: sqlite3.Connection):
    try:
        apply_runtime_pragmas(conn)
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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS shafa_notifications (
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
            cols = [row[1] for row in conn.execute("PRAGMA table_info(shafa_items)").fetchall()]
            if "first_image_url" not in cols:
                conn.execute("ALTER TABLE shafa_items ADD COLUMN first_image_url TEXT")
        except Exception:
            pass
        conn.execute("CREATE INDEX IF NOT EXISTS idx_shafa_items_source ON shafa_items(source);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_shafa_items_price_name ON shafa_items(price_int, name);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_shafa_notifications_price_state ON shafa_notifications(price_int, state);")
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


def _notification_storage_key(key: Tuple[str, int]) -> str:
    return f"{key[0]}\x1f{key[1]}"


def _db_claim_notification_key_sync(item: ShafaItem, source_name: str) -> bool:
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
            FROM shafa_notifications
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
            INSERT INTO shafa_notifications (
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


def _db_mark_notification_sent_sync(item: ShafaItem, source_name: str) -> None:
    key = _duplicate_key(item.name, item.price_int)
    if key is None:
        return
    storage_key = _notification_storage_key(key)
    with _db_connect() as conn:
        conn.execute(
            """
            INSERT INTO shafa_notifications (
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


def _db_release_notification_claim_sync(item: ShafaItem, source_name: str) -> None:
    key = _duplicate_key(item.name, item.price_int)
    if key is None:
        return
    storage_key = _notification_storage_key(key)
    with _db_connect() as conn:
        conn.execute(
            """
            UPDATE shafa_notifications
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


def _db_fetch_duplicate_keys_sync(items: List[ShafaItem]) -> set[Tuple[str, int]]:
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
        query = f"SELECT id, name, price_int FROM shafa_items WHERE price_int IN ({placeholders})"
        rows = conn.execute(query, prices).fetchall()
        notification_query = f"""
            SELECT notification_key, name, price_int
            FROM shafa_notifications
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


async def db_get_source_stats(url: str) -> Dict[str, int]:
    return await asyncio.to_thread(_db_get_source_stats_sync, url)


async def db_update_source_stats(url: str, streak: int, cycle_count: int) -> None:
    await asyncio.to_thread(_db_update_source_stats_sync, url, streak, cycle_count)


class ShafaRepository(MarketplaceRepository[ShafaItem]):
    # SHAFA still uses its own DB, but the shared repository interface forces it to honor
    # the same persistence and notification semantics as the OLX adapter.
    async def fetch_existing(self, item_ids: list[str]) -> list[Optional[Dict[str, Any]]]:
        return await asyncio.to_thread(_db_fetch_existing_sync, item_ids)

    async def fetch_duplicate_keys(self, items: list[ShafaItem]) -> set[Tuple[str, int]]:
        return await asyncio.to_thread(_db_fetch_duplicate_keys_sync, items)

    async def claim_notification_key(self, item: ShafaItem, source_name: str) -> bool:
        return await asyncio.to_thread(_db_claim_notification_key_sync, item, source_name)

    async def mark_notification_sent(self, item: ShafaItem, source_name: str) -> None:
        await asyncio.to_thread(_db_mark_notification_sent_sync, item, source_name)

    async def release_notification_claim(self, item: ShafaItem, source_name: str) -> None:
        await asyncio.to_thread(_db_release_notification_claim_sync, item, source_name)

    async def persist_items(self, updates: list[ItemUpdate[ShafaItem]], source_name: str) -> None:
        if not updates:
            return
        await asyncio.to_thread(
            _db_upsert_items_sync,
            [(update.item, update.touch_last_sent) for update in updates],
            source_name,
        )

    async def get_source_stats(self, url: str) -> SourceStats:
        stats = await db_get_source_stats(url)
        return SourceStats(streak=stats["streak"], cycle_count=stats["cycle_count"])

    async def update_source_stats(self, url: str, streak: int, cycle_count: int) -> None:
        await db_update_source_stats(url, streak, cycle_count)


def _hydrate_shafa_item(item: ShafaItem, previous: Optional[Dict[str, Any]]) -> None:
    if previous and not item.first_image_url and previous.get("first_image_url"):
        item.first_image_url = previous.get("first_image_url")


def _decide_shafa_item(item: ShafaItem, previous: Optional[Dict[str, Any]]) -> ItemDecision:
    if previous is None:
        if not _has_numeric_price(item.price_text, item.price_int):
            return ItemDecision()
        return ItemDecision(send_notification=True, is_new_item=True)

    if not _has_numeric_price(item.price_text, item.price_int):
        return ItemDecision()

    previous_price = previous.get("price_int") or 0
    if previous_price <= 0 and item.price_int > 0:
        return ItemDecision(persist_without_send=True)
    if previous_price > 0 and item.price_int > previous_price:
        return ItemDecision(persist_without_send=True)

    price_diff = abs(item.price_int - previous_price)
    percent_change = (price_diff / previous_price * 100.0) if previous_price > 0 else None
    if price_diff < MIN_PRICE_DIFF or (percent_change is not None and percent_change < MIN_PRICE_DIFF_PERCENT):
        return ItemDecision()
    return ItemDecision(send_notification=True)

async def run_shafa_scraper():
    total_scraped = 0
    total_sent = 0
    total_new = 0
    pipeline_totals = PipelineStats()
    run_stats = RunStatsCollector("shafa")
    run_stats.set_deploy_metadata()
    errors: list[str] = []

    def _add_error(msg: str) -> None:
        if msg:
            errors.append(str(msg)[:200])
            run_stats.record_error("runtime", message=str(msg)[:200])

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

    try:
        await asyncio.to_thread(_db_init_sync)
        logger.info("Database ready")
    except Exception as exc:
        logger.error("Database init failed: %s", exc)
        _add_error(f"DB init failed: {exc}")
        return "; ".join(dict.fromkeys(errors))

    global _playwright_runtime, _http_session
    try:
        # Warm the runtime once per service lifetime so individual SHAFA runs can reuse it
        # and only fall back to a reset when the browser becomes unhealthy.
        _playwright_runtime = PlaywrightRuntimeManager(
            async_playwright_factory=async_playwright,
            user_agent=USER_AGENT,
            logger=logger,
            chromium_launch_kwargs={"headless": True},
        )
        await _playwright_runtime.ensure_started()
    except Exception as exc:
        logger.error("Playwright error: %s", exc)
        _add_error(f"Playwright error: {exc}")
        return "; ".join(dict.fromkeys(errors))

    bot = Bot(token=token)
    repository = ShafaRepository()
    duplicate_tracker = RunDuplicateTracker[ShafaItem]()

    async def _send_item(item: ShafaItem, text: str, source_name: str) -> bool:
        try:
            sent = await send_photo_with_upscale(
                bot,
                default_chat,
                text,
                item.first_image_url,
                source_name=source_name,
            )
            return bool(sent)
        except RetryAfter as exc:
            logger.warning("Telegram rate limit hit; waiting %ss", exc.retry_after)
        except TimedOut:
            logger.warning("Timeout while sending")
            _add_error("Telegram send timeout")
            return True
        except Exception as exc:
            logger.error("Send failed: %s", exc)
            _add_error(f"Send failed: {exc}")
        return False

    async def _process_entry(entry: Dict[str, Any]) -> None:
        nonlocal total_scraped, total_new, total_sent
        url = entry.get("url")
        source_name = entry.get("url_name") or "SHAFA"
        if not url or not default_chat:
            run_stats.inc("sources_invalid")
            return
        if SHAFA_REQUEST_JITTER_SEC > 0:
            await asyncio.sleep(random.uniform(0, SHAFA_REQUEST_JITTER_SEC))

        source_stats = await repository.get_source_stats(url)
        source_decision = make_source_decision(source_stats)
        if not source_decision.should_process:
            logger.debug(
                "Skipping (cycle %s/%s, streak %s)",
                source_decision.next_cycle_count,
                source_decision.divisor,
                source_stats.streak,
            )
            await repository.update_source_stats(url, source_decision.next_streak, source_decision.next_cycle_count)
            run_stats.inc("sources_skipped_by_backoff")
            run_stats.record_source(source_name, status="skipped_by_backoff", url=url)
            return

        try:
            run_stats.inc("sources_attempted")
            items = await scrape_shafa_url(url)
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
                source_kind="shafa",
                source_name=source_name,
                items=items,
                repository=repository,
                duplicate_tracker=duplicate_tracker,
                decide_item=_decide_shafa_item,
                build_message=build_message,
                send_item=_send_item,
                hydrate_from_previous=_hydrate_shafa_item,
                analytics_sink=_ANALYTICS_SINK,
                logger=logger,
            )
            total_new += pipeline_stats.total_new
            total_sent += pipeline_stats.total_sent
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
            logger.error("Network error: %s", exc)
            _add_error("Network error")
            run_stats.inc("sources_failed")
            run_stats.record_source(source_name, status="network_error", url=url)
            run_stats.record_error("network", source=source_name, message=str(exc)[:200])
        except Exception as exc:
            logger.error("Processing error: %s", exc)
            _add_error(f"Processing error: {exc}")
            run_stats.inc("sources_failed")
            run_stats.record_source(source_name, status="error", url=url, error=str(exc)[:120])
            run_stats.record_error(type(exc).__name__, source=source_name, message=str(exc)[:200])

    try:
        sem = asyncio.Semaphore(SHAFA_TASK_CONCURRENCY)

        async def _guarded_process(entry: Dict[str, Any]) -> None:
            async with sem:
                await _process_entry(entry)

        sources = merge_sources(SHAFA_URLS or [], load_dynamic_urls("shafa"))
        run_stats.set_field("sources_total", len(sources))
        if tasks := [_guarded_process(entry) for entry in sources]:
            logger.info("Sources: %s", len(tasks))
            await asyncio.gather(*tasks, return_exceptions=True)
        else:
            logger.warning("No URLs configured")
        logger.info("Completed")
        logger.info("TOTAL SCRAPED: %s items", total_scraped)
        logger.info("New: %s | Sent: %s", total_new, total_sent)
        # SHAFA often has healthy zero-new runs. These counters separate "nothing new"
        # from hidden drops such as duplicate filtering, unsubscribes, or send failures.
        logger.info(
            "SHAFA pipeline: SEEN=%s | NEW=%s | SENT=%s | PERSIST_ONLY=%s | UNSUB=%s | "
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
        if total_scraped > 0:
            logger.info("Success rate: %.1f%%", (total_sent / total_scraped * 100))
    finally:
        if _playwright_runtime is not None:
            try:
                await _playwright_runtime.close()
            except Exception:
                pass
            _playwright_runtime = None
        if _http_session and not _http_session.closed:
            try:
                await _http_session.close()
                _http_session = None
            except Exception:
                pass
        try:
            await bot.shutdown()
        except Exception:
            pass
        try:
            await bot.close()
        except Exception:
            pass
        logger.info("Resources cleaned up")
        await asyncio.sleep(0.5)
    if errors:
        uniq = []
        for item in errors:
            if item not in uniq:
                uniq.append(item)
            if len(uniq) >= 3:
                break
        summary = "; ".join(uniq)
        logger.warning("SHAFA run errors: %s", summary)
        return summary
    return ""

if __name__ == "__main__":
    asyncio.run(run_shafa_scraper())

