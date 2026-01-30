import json, time, asyncio, logging, colorama, subprocess, shutil, traceback, urllib.parse, re, html, io, uuid, requests, sqlite3
from pathlib import Path
from telegram.constants import ParseMode
from collections import defaultdict, namedtuple, deque
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from telegram import Bot
from telegram.error import RetryAfter, TimedOut
from GroteskBotStatus import (
    status_heartbeat,
    load_last_runs_from_file,
    LAST_OLX_RUN_UTC,
    LAST_SHAFA_RUN_UTC,
    begin_lyst_cycle,
    mark_olx_run,
    mark_shafa_run,
    mark_lyst_start,
    mark_lyst_issue,
    finalize_lyst_run,
)
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DANYLO_DEFAULT_CHAT_ID, EXCHANGERATE_API_KEY, IS_RUNNING_LYST, CHECK_INTERVAL_SEC, CHECK_JITTER_SEC, MAINTENANCE_INTERVAL_SEC, DB_VACUUM, OLX_RETENTION_DAYS, SHAFA_RETENTION_DAYS, LYST_MAX_BROWSERS, LYST_SHOE_CONCURRENCY, UPSCALE_IMAGES
from config_lyst import (
    BASE_URLS,
    LYST_COUNTRIES,
    LYST_PAGE_SCRAPE,
    LYST_URL_TIMEOUT_SEC as LYST_URL_TIMEOUT_DEFAULT,
    LYST_STALL_TIMEOUT_SEC as LYST_STALL_TIMEOUT_DEFAULT,
    LYST_PAGE_TIMEOUT_SEC,
    LYST_MAX_SCROLL_ATTEMPTS,
)
from lyst_debug import (
    attach_lyst_debug_listeners,
    dump_lyst_debug_event,
    write_stop_too_early_dump,
)
from scheduler import run_scheduler
from colorama import Fore, Back, Style
from PIL import Image, ImageDraw, ImageFont
from asyncio import Semaphore
import aiosqlite
from olx_scraper import run_olx_scraper
from shafa_scraper import run_shafa_scraper

# Basic anti-bot headers & stealth tweaks (best-effort for Cloudflare)
STEALTH_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
STEALTH_HEADERS = {
    "Accept-Language": "en-US,en;q=0.9",
    "Upgrade-Insecure-Requests": "1",
    "DNT": "1",
}
STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
window.chrome = { runtime: {} };
"""

# Initialize constants and globals
colorama.init(autoreset=True)
BOT_VERSION, DB_NAME = "4.1.0", "shoes.db"
LIVE_MODE, ASK_FOR_LIVE_MODE = False, False
PAGE_SCRAPE = LYST_PAGE_SCRAPE
SHOE_DATA_FILE, EXCHANGE_RATES_FILE = 'shoe_data.json', 'exchange_rates.json'
BOT_LOG_FILE = Path(__file__).with_name("python.log")
SHOES_DB_FILE = Path(__file__).with_name("shoes.db")
OLX_DB_FILE = Path(__file__).with_name("olx_items.db")
SHAFA_DB_FILE = Path(__file__).with_name("shafa_items.db")
LOG_TAIL_LINES = 500
COUNTRIES = LYST_COUNTRIES
BLOCK_RESOURCES = False
RESOLVE_REDIRECTS = False
SKIPPED_ITEMS = set()
KYIV_TZ = ZoneInfo("Europe/Kyiv")
LYST_URL_TIMEOUT_SEC = LYST_URL_TIMEOUT_DEFAULT
LYST_LAST_PROGRESS_TS = 0.0
LYST_STALL_TIMEOUT_SEC = LYST_STALL_TIMEOUT_DEFAULT
OLX_TIMEOUT_SEC = 1800
SHAFA_TIMEOUT_SEC = 1800
LYST_IMAGE_STRATEGY = "adaptive"  # "adaptive" or "settle"
LYST_IMAGE_READY_TARGET = 0.6
LYST_IMAGE_EXTRA_SCROLLS = 4
LYST_IMAGE_SETTLE_PASSES = 2

# Config-driven priorities and thresholds with safe defaults (compact, safe getattr)
COUNTRY_PRIORITY = ["PL", "US", "IT", "GB"]
SALE_EMOJI_ROCKET_THRESHOLD, SALE_EMOJI_UAH_THRESHOLD = 75, 2600
try:
    import config as _conf
    COUNTRY_PRIORITY = getattr(_conf, 'COUNTRY_PRIORITY', COUNTRY_PRIORITY)
    SALE_EMOJI_ROCKET_THRESHOLD = getattr(_conf, 'SALE_EMOJI_ROCKET_THRESHOLD', SALE_EMOJI_ROCKET_THRESHOLD)
    SALE_EMOJI_UAH_THRESHOLD = getattr(_conf, 'SALE_EMOJI_UAH_THRESHOLD', SALE_EMOJI_UAH_THRESHOLD)
    BLOCK_RESOURCES = getattr(_conf, 'BLOCK_RESOURCES', BLOCK_RESOURCES)
    RESOLVE_REDIRECTS = getattr(_conf, 'RESOLVE_REDIRECTS', RESOLVE_REDIRECTS)
except Exception:
    pass

# Database semaphore to prevent concurrent access issues
DB_SEMAPHORE = Semaphore(1)

# Define namedtuples and container classes
ConversionResult = namedtuple('ConversionResult', ['uah_amount', 'exchange_rate', 'currency_symbol'])

# Statistics tracking
max_wait_times = {'url_changes': 0, 'final_url_changes': 0}
link_statistics = {
    'lyst_track_lead': {'success': 0, 'fail': 0, 'fail_links': []}, 'click_here': {'success': 0, 'fail': 0, 'fail_links': []},
    'other_failures': {'count': 0, 'links': []}, 'steps': {
        'Initial URL change': {'count': 0, 'final_url_obtained': 0}, 'After some waiting': {'count': 0, 'final_url_obtained': 0},
        'After Click here': {'count': 0, 'final_url_obtained': 0}, 'Track Lead': {'count': 0, 'final_url_obtained': 0}, 'Unknown': {'count': 0, 'final_url_obtained': 0}
    }
}

class ColoredFormatter(logging.Formatter):
    COLORS = {'DEBUG': Fore.CYAN, 'INFO': Fore.WHITE, 'WARNING': Fore.YELLOW, 'ERROR': Fore.RED, 
              'CRITICAL': Fore.RED + Back.WHITE, 'STAT': Fore.MAGENTA, 'GOOD': Fore.GREEN, 
              'LIGHTBLUE_INFO': Fore.LIGHTBLUE_EX}

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=KYIV_TZ)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat()

    def format(self, record):
        log_color = self.COLORS.get(record.levelname, Fore.WHITE)
        timestamp = Fore.LIGHTBLACK_EX + self.formatTime(record, self.datefmt) + Style.RESET_ALL
        return f"{timestamp}     {log_color}{record.getMessage()}{Style.RESET_ALL}"

class TelegramMessageQueue:
    def __init__(self, bot_token):
        self.queue, self.bot_token, self.pending_messages = asyncio.Queue(), bot_token, {}

    async def add_message(self, chat_id, message, image_url=None, uah_price=None, sale_percentage=None):
        message_id = str(uuid.uuid4())
        self.pending_messages[message_id] = False
        await self.queue.put((message_id, chat_id, message, image_url, uah_price, sale_percentage))
        return message_id

    async def process_queue(self):
        while True:
            message_id, chat_id, message, image_url, uah_price, sale_percentage = await self.queue.get()
            success = await send_telegram_message(self.bot_token, chat_id, message, image_url, uah_price, sale_percentage)
            self.pending_messages[message_id] = success
            if not success:
                await self.queue.put((message_id, chat_id, message, image_url, uah_price, sale_percentage))
            await asyncio.sleep(1)

    def is_message_sent(self, message_id):
        return self.pending_messages.get(message_id, False)

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
if logger.hasHandlers():
    logger.handlers.clear()
console_handler = logging.StreamHandler()
console_handler.setFormatter(ColoredFormatter('%(asctime)s', datefmt='%d.%m %H:%M:%S'))
logger.addHandler(console_handler)

file_handler = logging.FileHandler(BOT_LOG_FILE, encoding="utf-8")
file_handler.setFormatter(ColoredFormatter('%(asctime)s', datefmt='%d.%m %H:%M:%S'))
logger.addHandler(file_handler)

class SpecialLogger:
    @staticmethod
    def stat(message): logger.log(35, message)
    @staticmethod
    def good(message): logger.log(25, message)
    @staticmethod
    def info(message): logger.log(22, message)

special_logger = SpecialLogger()

# Add custom log levels
for level_name, level_num in [("STAT", 35), ("GOOD", 25), ("LIGHTBLUE_INFO", 22)]:
    logging.addLevelName(level_num, level_name)

class BrowserPool:
    def __init__(self, max_browsers=6):
        self.max_browsers, self._semaphore = max_browsers, Semaphore(max_browsers)
        self._playwright, self._browser_type = None, None

    async def init(self):
        if not self._playwright:
            self._playwright = await async_playwright().start()
            self._browser_type = self._playwright.chromium

    async def close(self):
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None

    async def get_browser(self):
        await self.init()
        await self._semaphore.acquire()
        browser = await self._browser_type.launch(
            headless=not LIVE_MODE,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        return BrowserWrapper(browser, self._semaphore)

class BrowserWrapper:
    def __init__(self, browser, semaphore):
        self.browser, self._semaphore = browser, semaphore

    async def __aenter__(self):
        return self.browser

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.browser.close()
        self._semaphore.release()

browser_pool = BrowserPool(max_browsers=LYST_MAX_BROWSERS)

# Helper functions
def clean_link_for_display(link):
    cleaned_link = re.sub(r'^(https?://)?(www\.)?', '', link)
    return (cleaned_link[:22] + '...') if len(cleaned_link) > 25 else cleaned_link

def load_font(font_size, prefer_heavy=False):
    font_dir = Path(__file__).with_name("fonts")
    if prefer_heavy:
        font_candidates = [
            font_dir / "SFPro-Heavy.ttf",
            font_dir / "SFPro-Bold.ttf",
        ]
    else:
        font_candidates = [
            font_dir / "SFPro-Bold.ttf",
            font_dir / "SFPro-Heavy.ttf",
        ]
    font_candidates += [
        "SFPro-Heavy.ttf",
        "SFPro-Bold.ttf",
        "arialbd.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    ]
    for font_file in font_candidates:
        try:
            return ImageFont.truetype(str(font_file), font_size)
        except IOError:
            continue
    return ImageFont.load_default()

def process_image(image_url, uah_price, sale_percentage):
    response = requests.get(image_url)
    try:
        img = Image.open(io.BytesIO(response.content))
        # If upscaling is disabled, downscale large sources to keep file size under Telegram limit
        if not UPSCALE_IMAGES:
            max_edge = 1280
            w, h = img.size
            scale = min(1.0, max_edge / max(w, h)) if max(w, h) else 1.0
            if scale < 1.0:
                new_size = (int(w * scale), int(h * scale))
                img = img.resize(new_size, Image.LANCZOS)
        if UPSCALE_IMAGES:
            width, height = [dim * 2 for dim in img.size]
            img = img.resize((width, height), Image.LANCZOS)
        else:
            width, height = img.size

        price_text, sale_text = f"{uah_price} UAH", f"-{sale_percentage}%"
        padding = max(12, int(width * 0.03))
        text_margin = max(20, int(width * 0.1))
        text_margin = min(text_margin, int(width * 0.14))

        # Choose base font size and adjust if needed to fit width
        base_scale = 0.06 if width > height else 0.055
        font_size = max(24, int(width * base_scale))
        font = load_font(font_size, prefer_heavy=False)

        def _fit_font(font_size):
            while font_size > 12:
                font = load_font(font_size, prefer_heavy=False)
                dummy = Image.new('RGB', (width, width), (255, 255, 255))
                d = ImageDraw.Draw(dummy)
                price_bbox = d.textbbox((0, 0), price_text, font=font)
                sale_bbox = d.textbbox((0, 0), sale_text, font=font)
                price_w = price_bbox[2] - price_bbox[0]
                sale_w = sale_bbox[2] - sale_bbox[0]
                if max(price_w, sale_w) <= (width - (text_margin * 2)):
                    return font
                font_size -= 2
            return load_font(font_size, prefer_heavy=False)

        font = _fit_font(font_size)
        ascent, descent = font.getmetrics()
        text_height = ascent + descent
        line_padding = max(2, int(font_size * 0.15))

        if width > height:
            # Make square by adding white space ABOVE the image (not below the prices)
            top_pad = width - height
            square_img = Image.new('RGB', (width, width), (255, 255, 255))
            square_img.paste(img, (0, top_pad))

            draw = ImageDraw.Draw(square_img)
            bottom_area = text_height + (padding * 2) + line_padding

            new_img = Image.new('RGB', (width, width + bottom_area), (255, 255, 255))
            new_img.paste(square_img, (0, 0))
            draw = ImageDraw.Draw(new_img)
            text_y = width + padding + ascent + (line_padding // 2)
            draw.text((text_margin, text_y), price_text, font=font, fill=(22, 22, 24), anchor="ls")
            draw.text((width - text_margin, text_y), sale_text, font=font, fill=(255, 59, 48), anchor="rs")
        else:
            # Default: add a bottom bar for text
            bottom_area = text_height + (padding * 2) + line_padding
            new_img = Image.new('RGB', (width, height + bottom_area), (255, 255, 255))
            new_img.paste(img, (0, 0))
            draw = ImageDraw.Draw(new_img)
            text_y = height + padding + ascent + (line_padding // 2)
            draw.text((text_margin, text_y), price_text, font=font, fill=(22, 22, 24), anchor="ls")
            draw.text((width - text_margin, text_y), sale_text, font=font, fill=(255, 59, 48), anchor="rs")

        img_byte_arr = io.BytesIO()
        if UPSCALE_IMAGES:
            new_img.save(img_byte_arr, format='PNG', quality=95)
        else:
            # JPEG is smaller and avoids Telegram size limits for large images
            if new_img.mode != 'RGB':
                new_img = new_img.convert('RGB')
            new_img.save(img_byte_arr, format='JPEG', quality=85, optimize=True, subsampling=0)
        img_byte_arr.seek(0)
        return img_byte_arr
    finally:
        response.close()

# Database functions
PRAGMA_STATEMENTS = ['PRAGMA foreign_keys = ON','PRAGMA journal_mode = WAL','PRAGMA synchronous = NORMAL','PRAGMA busy_timeout = 30000']

def connect_db():
    conn = sqlite3.connect(DB_NAME, timeout=30.0)
    for stmt in PRAGMA_STATEMENTS:
        conn.execute(stmt)
    return conn

def create_tables():
    conn = connect_db()
    conn.executescript('''
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
    ''')
    conn.commit(); conn.close()

async def db_operation_with_retry(operation_func, max_retries=3):
    """Helper function to handle database operations with retry logic"""
    async with DB_SEMAPHORE:
        for attempt in range(max_retries):
            try:
                async with aiosqlite.connect(DB_NAME, timeout=30.0) as conn:
                    for stmt in PRAGMA_STATEMENTS:
                        await conn.execute(stmt)
                    return await operation_func(conn)
            except Exception as e:
                if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                    logger.warning(f"Database locked, retrying in {2 ** attempt} seconds (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(2 ** attempt)
                    continue
                raise

async def is_shoe_processed(key):
    async def _operation(conn):
        async with conn.execute("SELECT 1 FROM processed_shoes WHERE key = ?", (key,)) as cursor:
            return await cursor.fetchone() is not None
    return await db_operation_with_retry(_operation)

async def mark_shoe_processed(key):
    async def _operation(conn):
        await conn.execute("INSERT OR IGNORE INTO processed_shoes(key, active) VALUES (?, 1)", (key,))
        await conn.commit()
    await db_operation_with_retry(_operation)

def load_shoe_data_from_db():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM shoes')
    data = {row[0]: {
        'name': row[1], 'unique_id': row[2], 'original_price': row[3],
        'sale_price': row[4], 'image_url': row[5], 'store': row[6],
        'country': row[7], 'shoe_link': row[8], 'lowest_price': row[9],
        'lowest_price_uah': row[10], 'uah_price': row[11], 'active': bool(row[12])
    } for row in cursor.fetchall()}
    conn.close()
    return data

def load_shoe_data_from_json():
    try:
        with open(SHOE_DATA_FILE, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

async def save_shoe_data_bulk(shoes):
    """Save multiple shoes to database in a single transaction."""
    async def _operation(conn):
        data = [(
            s['key'], s['name'], s['unique_id'], s['original_price'], s['sale_price'], s['image_url'],
            s['store'], s['country'], s.get('shoe_link', ''), s.get('lowest_price', ''), s.get('lowest_price_uah', 0.0),
            s.get('uah_price', 0.0), 1 if s.get('active', True) else 0
        ) for s in shoes]
        await conn.executemany('''INSERT OR REPLACE INTO shoes (
            key, name, unique_id, original_price, sale_price,
            image_url, store, country, shoe_link, lowest_price,
            lowest_price_uah, uah_price, active
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''', data)
        await conn.commit()
    
    await db_operation_with_retry(_operation)

async def async_save_shoe_data(shoe_data):
    shoes = [dict(shoe, key=key) for key, shoe in shoe_data.items()]
    await save_shoe_data_bulk(shoes)

async def migrate_json_to_sqlite():
    async def _operation(conn):
        async with conn.execute('SELECT COUNT(*) FROM shoes') as cursor:
            return (await cursor.fetchone())[0]
    if await db_operation_with_retry(_operation) == 0:
        data = load_shoe_data_from_json()
        if data: await async_save_shoe_data(data)

async def load_shoe_data():
    create_tables()
    await migrate_json_to_sqlite()
    return load_shoe_data_from_db()

async def save_shoe_data(data):
    await async_save_shoe_data(data)

# Web scraping and browser functions
async def handle_route(route):
    if route.request.resource_type in ["image", "media", "font", "stylesheet"]:
        await route.abort()
    else:
        await route.continue_()

async def normalize_lazy_images(page):
    # Promote lazy-load attributes into src/srcset so HTML contains image URLs
    await page.evaluate("""
        () => {
            const attrs = [
                {from: 'data-src', to: 'src'},
                {from: 'data-lazy-src', to: 'src'},
                {from: 'data-srcset', to: 'srcset'},
                {from: 'data-lazy-srcset', to: 'srcset'},
            ];
            document.querySelectorAll('img').forEach(img => {
                attrs.forEach(({from, to}) => {
                    const val = img.getAttribute(from);
                    if (val && !img.getAttribute(to)) {
                        img.setAttribute(to, val);
                    }
                });
            });
        }
    """)

async def count_product_images_ready(page):
    return await page.evaluate("""
        () => {
            const cards = document.querySelectorAll('div._693owt3');
            const attrList = ['src','data-src','data-lazy-src','srcset','data-srcset','data-lazy-srcset'];
            const hasUrl = (el) => {
                for (const attr of attrList) {
                    const val = el.getAttribute(attr);
                    if (val && (val.startsWith('http') || val.startsWith('//'))) return true;
                }
                return false;
            };
            let ready = 0;
            cards.forEach(card => {
                const media = card.querySelectorAll('img, source');
                for (const el of media) {
                    if (hasUrl(el)) { ready += 1; break; }
                }
            });
            return { total: cards.length, ready };
        }
    """)

async def settle_lazy_images(page, passes=2, step=600, pause=0.7):
    for _ in range(passes):
        await page.evaluate(f"window.scrollBy(0, {step})")
        await asyncio.sleep(pause)
        await normalize_lazy_images(page)

async def scroll_page(page, max_attempts=None):
    SCROLL_PAUSE_TIME = 1
    SCROLL_STEP = 5000 if BLOCK_RESOURCES else 800
    if max_attempts is None:
        max_attempts = 10 if PAGE_SCRAPE else 300
    last_height = await page.evaluate("document.body.scrollHeight")
    total_scrolled, scroll_attempts = 0, 0

    while scroll_attempts < max_attempts:
        await page.evaluate(f"window.scrollBy(0, {SCROLL_STEP})")
        total_scrolled += SCROLL_STEP
        await asyncio.sleep(SCROLL_PAUSE_TIME)
        new_height = await page.evaluate("document.body.scrollHeight")
        
        if total_scrolled > new_height: break
        # scroll_attempts = 0 if new_height > last_height else scroll_attempts + 1
        scroll_attempts += 1

        last_height = new_height

    # Optional adaptive tail-scrolls if many cards still lack image URLs
    if LYST_IMAGE_STRATEGY == "adaptive":
        for _ in range(LYST_IMAGE_EXTRA_SCROLLS):
            counts = await count_product_images_ready(page)
            total = counts.get("total") or 0
            ready = counts.get("ready") or 0
            if total == 0:
                break
            if (ready / total) >= LYST_IMAGE_READY_TARGET:
                break
            await page.evaluate(f"window.scrollBy(0, {SCROLL_STEP})")
            await asyncio.sleep(SCROLL_PAUSE_TIME)

async def get_page_content(url, country, max_scroll_attempts=None, url_name=None, page_num=None):
    async with (await browser_pool.get_browser()) as browser:
        context = await browser.new_context(
            user_agent=STEALTH_UA,
            locale="en-US",
            timezone_id="Europe/Kyiv",
            extra_http_headers=STEALTH_HEADERS,
        )
        await context.add_init_script(STEALTH_SCRIPT)
        await context.add_cookies([{'name': 'country', 'value': country, 'domain': '.lyst.com', 'path': '/'}])
        page = await context.new_page()
        debug_events = []
        attach_lyst_debug_listeners(page, debug_events)
        if BLOCK_RESOURCES:
            await page.route("**/*", handle_route)
        step = "goto"
        try:
            logger.info(f"LYST step=goto url={url}")
            await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=int(LYST_PAGE_TIMEOUT_SEC * 1000),
            )
            if max_scroll_attempts is not None and max_scroll_attempts <= 0:
                step = "scroll_skip"
                logger.info(f"LYST step=scroll_skip url={url}")
            else:
                step = "scroll"
                logger.info(f"LYST step=scroll url={url}")
                await scroll_page(page, max_scroll_attempts)
            if LYST_IMAGE_STRATEGY == "settle":
                step = "settle_lazy_images"
                logger.info(f"LYST step=settle_lazy_images url={url}")
                await settle_lazy_images(page, passes=LYST_IMAGE_SETTLE_PASSES)
            step = "normalize_lazy_images"
            logger.info(f"LYST step=normalize_lazy_images url={url}")
            await normalize_lazy_images(page)
            step = "wait_selector"
            try:
                logger.info(f"LYST step=wait_selector url={url}")
                await page.wait_for_selector('._693owt3', timeout=20000)
            except Exception:
                # If selector wait fails, still return content for parsing
                pass
            step = "content"
            logger.info(f"LYST step=content url={url}")
            return await page.content()
        except asyncio.CancelledError:
            now_kyiv = datetime.now(KYIV_TZ).strftime('%Y-%m-%d %H:%M:%S')
            log_lines = tail_log_lines(BOT_LOG_FILE, line_count=200)
            final_url = None
            try:
                final_url = page.url
            except Exception:
                final_url = None
            try:
                await asyncio.shield(
                    dump_lyst_debug_event(
                        "lyst_timeout",
                        reason="page timeout",
                        url=url,
                        country=country,
                        url_name=url_name,
                        page_num=page_num,
                        step=step,
                        page=page,
                        now_kyiv=now_kyiv,
                        log_lines=log_lines,
                        extra_lines=debug_events,
                        final_url=final_url,
                    )
                )
            except Exception:
                pass
            raise
        except Exception as exc:
            reason = "target closed" if "Target page, context or browser has been closed" in str(exc) else f"exception: {exc}"
            prefix = "lyst_target_closed" if reason == "target closed" else "lyst_error"
            now_kyiv = datetime.now(KYIV_TZ).strftime('%Y-%m-%d %H:%M:%S')
            log_lines = tail_log_lines(BOT_LOG_FILE, line_count=200)
            final_url = None
            try:
                final_url = page.url
            except Exception:
                final_url = None
            try:
                await dump_lyst_debug_event(
                    prefix,
                    reason=reason,
                    url=url,
                    country=country,
                    url_name=url_name,
                    page_num=page_num,
                    step=step,
                    page=page,
                    now_kyiv=now_kyiv,
                    log_lines=log_lines,
                    extra_lines=debug_events,
                    final_url=final_url,
                )
            except Exception:
                pass
            raise
        finally:
            await context.close()

async def get_soup(url, country, max_retries=3, max_scroll_attempts=None, url_name=None, page_num=None):
    for attempt in range(max_retries):
        try:
            try:
                content = await asyncio.wait_for(
                    get_page_content(url, country, max_scroll_attempts, url_name=url_name, page_num=page_num),
                    timeout=LYST_PAGE_TIMEOUT_SEC,
                )
            except asyncio.TimeoutError:
                suffix = ""
                if url_name or page_num is not None:
                    suffix = f" | url_name={url_name or ''} page={page_num if page_num is not None else ''}"
                logger.error(f"LYST timeout fetching page content for {url}{suffix}")
                mark_lyst_issue("page timeout")
                content = None
            if not content:
                mark_lyst_issue("Failed to get soup")
                if attempt < max_retries - 1:
                    await asyncio.sleep(5)
                    continue
                return None
            try:
                return BeautifulSoup(content, 'lxml')
            except Exception:
                return BeautifulSoup(content, 'html.parser')
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Failed to get soup (attempt {attempt + 1}/{max_retries}). Retrying...")
                await asyncio.sleep(5)
            else:
                logger.error(f"Failed to get soup for {url}")
                mark_lyst_issue("Failed to get soup")
                raise

async def get_soup_and_content(url, country, max_retries=3, max_scroll_attempts=None, url_name=None, page_num=None):
    for attempt in range(max_retries):
        try:
            try:
                content = await asyncio.wait_for(
                    get_page_content(url, country, max_scroll_attempts, url_name=url_name, page_num=page_num),
                    timeout=LYST_PAGE_TIMEOUT_SEC,
                )
            except asyncio.TimeoutError:
                suffix = ""
                if url_name or page_num is not None:
                    suffix = f" | url_name={url_name or ''} page={page_num if page_num is not None else ''}"
                logger.error(f"LYST timeout fetching page content for {url}{suffix}")
                mark_lyst_issue("page timeout")
                content = None
            if not content:
                mark_lyst_issue("Failed to get soup")
                if attempt < max_retries - 1:
                    await asyncio.sleep(5)
                    continue
                return None, None
            try:
                return BeautifulSoup(content, 'lxml'), content
            except Exception:
                return BeautifulSoup(content, 'html.parser'), content
        except Exception:
            if attempt < max_retries - 1:
                logger.warning(f"Failed to get soup (attempt {attempt + 1}/{max_retries}). Retrying...")
                await asyncio.sleep(5)
            else:
                logger.error(f"Failed to get soup for {url}")
                mark_lyst_issue("Failed to get soup")
                return None, None

def is_lyst_domain(url):
    return 'lyst.com' in urllib.parse.urlparse(url).netloc

def extract_embedded_url(url):
    parsed = urllib.parse.urlparse(url); qs = urllib.parse.parse_qs(parsed.query)
    for p in ('URL','murl','destination','url'):
        v = qs.get(p)
        if v: return urllib.parse.unquote(v[0])
    return url

def _touch_lyst_progress():
    global LYST_LAST_PROGRESS_TS
    LYST_LAST_PROGRESS_TS = time.time()

async def get_final_clear_link(initial_url, semaphore, item_name, country, current_item, total_items):
    logger.info(f"Processing final link for {item_name} | Country: {country} | Progress: {current_item}/{total_items}")
    async with (await browser_pool.get_browser()) as browser:
        context = await browser.new_context()
        page = await context.new_page()
        steps_info = {'steps_taken': [], 'final_step': None, 'initial_url': initial_url, 'final_url': None}
        
        try:
            if BLOCK_RESOURCES:
                await page.route("**/*", handle_route)
            await page.goto(initial_url)
            # Step 1: Initial URL change
            start_time = time.time()
            await page.wait_for_url(lambda url: url != initial_url, timeout=20000)
            wait_time = time.time() - start_time
            max_wait_times['url_changes'] = max(max_wait_times['url_changes'], wait_time)
            current_step = 'Initial URL change'
            steps_info['steps_taken'].append(current_step)
            link_statistics['steps'][current_step]['count'] += 1

            await asyncio.sleep(5)
            current_url = extract_embedded_url(page.url)

            if not is_lyst_domain(current_url):
                steps_info['final_step'] = current_step
                steps_info['final_url'] = current_url
                link_statistics['steps'][current_step]['final_url_obtained'] += 1
            elif "lyst.com" in current_url and "return" in current_url:
                await page.goto(current_url)
                await page.wait_for_load_state('networkidle')
                current_step = 'After some waiting'
                
                if not is_lyst_domain(current_url):
                    steps_info['final_step'] = current_step
                    steps_info['final_url'] = current_url
                    link_statistics['steps'][current_step]['final_url_obtained'] += 1
            
            # Set default if not already set            
            if steps_info['final_url'] is None:
                steps_info['final_url'] = current_url
                steps_info['final_step'] = 'Unknown'
                current_step = 'Unknown'
                link_statistics['steps'][current_step]['count'] += 1

            final_url = urllib.parse.unquote(steps_info['final_url'])
            logger.info(f"Final link obtained for: {item_name}")
            return final_url
        except Exception:
            link_statistics['other_failures']['count'] += 1
            link_statistics['other_failures']['links'].append(initial_url)
            return initial_url
        finally:
            await context.close()

# Data extraction and processing
PRICE_TOKEN_RE = re.compile(r'([\d.,]+\s*[^\d\s]+|[^\d\s]+\s*[\d.,]+)')

def extract_price(price_str):
    price_num = re.sub(r'[^\d.]', '', price_str)
    try: return float(price_num)
    except ValueError: return 0

def extract_price_tokens(text):
    if not text:
        return []
    tokens = []
    for m in PRICE_TOKEN_RE.finditer(text.replace('\xa0', ' ')):
        token = m.group(0).replace(' ', '')
        # Normalize trailing currency (e.g. "215€") to leading ("€215")
        if token and token[-1] not in '0123456789' and (token[0].isdigit() or token[0] == '.'):
            token = token[-1] + token[:-1]
        tokens.append(token)
    return tokens

def _pick_src_from_srcset(srcset_value):
    if not srcset_value:
        return None
    candidates = []
    for part in srcset_value.split(','):
        part = part.strip()
        if not part:
            continue
        url = part.split(' ')[0].strip()
        if url:
            candidates.append(url)
    for url in candidates:
        if url.startswith(("http://", "https://")):
            return url
    return None

def _extract_image_url_from_tag(tag):
    if not tag:
        return None
    candidates = [
        tag.get('src'),
        tag.get('data-src'),
        tag.get('data-lazy-src'),
        _pick_src_from_srcset(tag.get('srcset')),
        _pick_src_from_srcset(tag.get('data-srcset')),
        _pick_src_from_srcset(tag.get('data-lazy-srcset')),
    ]
    for url in candidates:
        if url and url.startswith(("http://", "https://")):
            return url
    return None

def _dedupe_preserve(items):
    seen = set()
    deduped = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped

def find_price_strings(root):
    if not root:
        return None, None
    # Prefer explicit strike-through price for original
    del_el = root.find(['del', 's', 'strike'])
    del_tokens = extract_price_tokens(del_el.get_text(" ", strip=True)) if del_el else []
    tokens = _dedupe_preserve(extract_price_tokens(root.get_text(" ", strip=True)))
    if not tokens:
        return None, None
    if del_tokens:
        original = del_tokens[0]
        others = [t for t in tokens if t != original]
        sale = min(others, key=extract_price) if others else original
        return original, sale
    if len(tokens) >= 2:
        original = max(tokens, key=extract_price)
        sale = min(tokens, key=extract_price)
        return original, sale
    return tokens[0], tokens[0]

def extract_ldjson_image_map(soup):
    if not soup:
        return {}
    image_map = {}
    for script in soup.find_all('script', type='application/ld+json'):
        text = script.string or script.get_text(strip=True)
        if not text or 'ItemList' not in text:
            continue
        try:
            data = json.loads(text)
        except Exception:
            continue
        if data.get('@type') != 'ItemList':
            continue
        for item in data.get('itemListElement', []):
            product = item.get('item', {}) if isinstance(item, dict) else {}
            url = product.get('url')
            images = product.get('image') or []
            if isinstance(images, str):
                images = [images]
            image_url = images[0] if images else None
            if url and image_url:
                image_map[url] = image_url
                if url.startswith("https://www.lyst.com"):
                    image_map[url.replace("https://www.lyst.com", "")] = image_url
    return image_map

def extract_shoe_data(card, country, image_fallback_map=None):
    if not card:
        logger.warning("Received None card in extract_shoe_data")
        return None
        
    try:
        # Extract name via a few fallback strategies
        finders = [
            lambda: card.find_all('span', class_=lambda x: x and 'vjlibs5' in x),
            lambda: card.find_all('span', class_=lambda x: x and 'vjlibs5' in x and 'vjlibs2' in x),
            lambda: card.find_all('span', class_=re.compile(r'.*vjlibs5.*')),
            lambda: card.find_all('span', class_=lambda x: x and ('_1b08vvh31' in x and 'vjlibs' in x)),
        ]
        name_elements = []
        for fn in finders:
            name_elements = fn()
            if name_elements: break
        if not name_elements:
            # Fallback to image alt or link text
            img_alt = None
            img_tag = card.find('img', alt=True)
            if img_tag:
                img_alt = (img_tag.get('alt') or '').strip()
            link_text = None
            link_tag = card.find('a', href=True)
            if link_tag:
                link_text = link_tag.get_text(" ", strip=True)
            full_name = (img_alt or link_text or "").strip()
            if not full_name:
                logger.warning(f"No name elements found. Card HTML structure:")
                debug_spans = card.find_all('span', class_=re.compile(r'.*vjlibs.*'))
                for i, span in enumerate(debug_spans[:5]):
                    logger.warning(f"  Debug span {i}: class='{span.get('class')}', text='{span.text.strip()[:50]}'")
                return None
        else:
            full_name = ' '.join(e.text.strip() for e in name_elements if e and e.text)
        if 'Giuseppe Zanotti' in full_name: return None
        
        # Extract price elements (prefer data-testid, fallback to class heuristics)
        price_container = card.find('div', attrs={'data-testid': 'product-price'}) or card.find('div', class_='ducdwf0')
        if not price_container:
            # Fallback: try extracting prices from the whole card text
            tokens = extract_price_tokens(card.get_text(" ", strip=True))
            if len(tokens) >= 2:
                original_price = max(tokens, key=extract_price)
                sale_price = min(tokens, key=extract_price)
            elif len(tokens) == 1:
                original_price = sale_price = tokens[0]
            else:
                logger.warning("Price container not found")
                return None
        else:
            original_price, sale_price = find_price_strings(price_container)
        if not original_price or not sale_price:
            # Legacy class-based fallbacks
            price_div = card.find('div', class_='ducdwf0') or price_container
            strategies = [
                lambda: (
                    price_div.find('div', class_=lambda x: x and '_1b08vvhr6' in x and 'vjlibs1' in x),
                    price_div.find('div', class_=lambda x: x and '_1b08vvh36' in x and 'vjlibs2' in x)
                ),
                lambda: (
                    price_div.find('div', class_=lambda x: x and ('_1b08vvhos' in x and 'vjlibs1' in x)),
                    price_div.find('div', class_=lambda x: x and ('_1b08vvh1w' in x and 'vjlibs2' in x))
                ),
                lambda: (
                    price_div.find('div', class_=lambda x: x and 'vjlibs1' in x and 'vjlibs2' in x and '_1b08vvhq2' in x and '_1b08vvh36' not in x),
                    price_div.find('div', class_=lambda x: x and 'vjlibs2' in x and '_1b08vvh36' in x)
                ),
                lambda: (
                    price_div.find('div', class_=lambda x: x and 'vjlibs1' in x and '_1b08vvhnk' in x and '_1b08vvh1q' not in x),
                    price_div.find('div', class_=lambda x: x and 'vjlibs2' in x and '_1b08vvh1q' in x) or
                    price_div.find('div', class_=lambda x: x and '_1b08vvh1w' in x)
                ),
            ]
            for strat in strategies:
                o, s = strat()
                if o and s and o != s:
                    o_tokens = extract_price_tokens(o.get_text(" ", strip=True))
                    s_tokens = extract_price_tokens(s.get_text(" ", strip=True))
                    if o_tokens and s_tokens:
                        original_price, sale_price = o_tokens[0], s_tokens[0]
                    break
            if not original_price or not sale_price:
                logger.warning("Price elements not found")
                return None
        if extract_price(original_price) < 80:
            logger.info(f"Skipping item '{full_name}' with original price {original_price}")
            return None
        
        # Extract unique ID
        product_card_div = card.find('div', attrs={'data-testid': 'product-card'}) or card.find('div', class_=lambda x: x and 'kah5ce0' in x and 'kah5ce2' in x)
        unique_id = product_card_div['id'] if product_card_div and 'id' in product_card_div.attrs else None

        # Extract store
        store = "Unknown Store"
        retailer_name = card.find('span', attrs={'data-testid': 'retailer-name'})
        if retailer_name:
            store_span = retailer_name.find('span', class_='_1fcx6l24')
            store_text = store_span.get_text(" ", strip=True) if store_span else retailer_name.get_text(" ", strip=True)
            store = store_text if store_text else store
        else:
            store_elem = card.find('div', attrs={'data-testid': 'retailer'}) or card.find('span', class_='_1fcx6l24')
            if store_elem:
                store_text = store_elem.get_text(" ", strip=True)
                store = store_text if store_text else store
        
        # Extract link
        link_elem = None
        for a in card.find_all('a', href=True):
            href = a.get('href') or ''
            if '/track/' in href:
                continue
            if any(p in href for p in ['/clothing/', '/shoes/', '/accessories/', '/bags/', '/jewelry/']):
                link_elem = a
                break
        if not link_elem:
            link_elem = card.find('a', href=True)
        href = link_elem['href'] if link_elem and 'href' in link_elem.attrs else None
        full_url = f"https://www.lyst.com{href}" if href and href.startswith('/') else href if href and href.startswith('http') else None
        if not unique_id and full_url:
            unique_id = str(uuid.uuid5(uuid.NAMESPACE_URL, full_url))

        # Extract image
        img_elem = (
            card.find('img', src=True)
            or card.find('img', attrs={'data-src': True})
            or card.find('img', attrs={'data-lazy-src': True})
            or card.find('img', attrs={'data-srcset': True})
            or card.find('img', attrs={'data-lazy-srcset': True})
            or card.find('img', srcset=True)
        )
        image_url = _extract_image_url_from_tag(img_elem)
        if not image_url:
            source_elem = card.find('source', srcset=True) or card.find('source', attrs={'data-srcset': True})
            image_url = _extract_image_url_from_tag(source_elem)
        # Fallback to JSON-LD image map if lazy image isn't in DOM
        if (not image_url or not image_url.startswith(("http://", "https://"))) and image_fallback_map:
            if full_url and full_url in image_fallback_map:
                image_url = image_fallback_map.get(full_url)
            elif href and href in image_fallback_map:
                image_url = image_fallback_map.get(href)
        # Ignore inline data URLs or non-external image sources
        if not image_url or not image_url.startswith(("http://", "https://")):
            if unique_id:
                SKIPPED_ITEMS.add(unique_id)
            return None
        
        # Validate required fields
        required_fields = {
            'name': full_name, 'original_price': original_price, 'sale_price': sale_price,
            'image_url': image_url, 'store': store, 'shoe_link': full_url, 'unique_id': unique_id
        }
        if any(not v for v in required_fields.values()):
            missing_fields = [f for f, v in required_fields.items() if not v]
            logger.warning(f"Missing required fields: {', '.join(missing_fields)}")
            return None
        
        return {
            'name': full_name, 'original_price': original_price, 'sale_price': sale_price,
            'image_url': image_url, 'store': store, 'country': country,
            'shoe_link': full_url, 'unique_id': unique_id
        }
    except Exception as e:
        logger.error(f"Error extracting shoe data: {e}")
        return None

async def scrape_page(url, country, max_scroll_attempts=None, url_name=None, page_num=None):
    soup, content = await get_soup_and_content(
        url,
        country,
        max_scroll_attempts=max_scroll_attempts,
        url_name=url_name,
        page_num=page_num,
    )
    if not soup:
        mark_lyst_issue("Failed to get soup")
        return [], content
    
    shoe_cards = soup.find_all('div', class_='_693owt3')
    image_fallback_map = extract_ldjson_image_map(soup)
    return [data for card in shoe_cards if (data := extract_shoe_data(card, country, image_fallback_map))], content

async def scrape_all_pages(base_url, country, use_pagination=None):
    if use_pagination is None:
        use_pagination = PAGE_SCRAPE
    
    max_scroll_attempts = LYST_MAX_SCROLL_ATTEMPTS
    all_shoes, page = [], 1
    
    while True:
        _touch_lyst_progress()
        if use_pagination:
            url = base_url['url'] if page == 1 else f"{base_url['url']}&page={page}"
            logger.info(f"Scraping page {page} for country {country} - {base_url['url_name']}")
        else:
            url = base_url['url']
            logger.info(f"Scraping single page for country {country} - {base_url['url_name']}")

        shoes, content = await scrape_page(
            url,
            country,
            max_scroll_attempts=max_scroll_attempts,
            url_name=base_url["url_name"],
            page_num=page if use_pagination else None,
        )
        if not shoes:
            if use_pagination and page < 3:
                logger.error(f"{base_url['url_name']} for {country} Stopped too early. Please check for errors")
                mark_lyst_issue("Stopped too early")
                now_kyiv = datetime.now(KYIV_TZ).strftime('%Y-%m-%d %H:%M:%S')
                log_lines = tail_log_lines(BOT_LOG_FILE, line_count=200)
                write_stop_too_early_dump(
                    reason="Stopped too early",
                    url=url,
                    country=country,
                    url_name=base_url['url_name'],
                    page_num=page,
                    content=content,
                    now_kyiv=now_kyiv,
                    log_lines=log_lines,
                )
                if use_pagination == PAGE_SCRAPE:
                    logger.info(f"Retrying {base_url['url_name']} for {country} with PAGE_SCRAPE={not use_pagination}")
                    return await scrape_all_pages(base_url, country, use_pagination=not use_pagination)
            
            logger.info(f"Total for {country} {base_url['url_name']}: {len(all_shoes)}. Stopped on page {page}")
            break
        all_shoes.extend(shoes)
        
        if not use_pagination:
            break
            
        page += 1
        await asyncio.sleep(1) 
    return all_shoes

# Price and currency conversions
def calculate_sale_percentage(original_price, sale_price, country):
    def parse(p):
        symbol = '€' if country in ('PL', 'IT') else '£' if country == 'GB' else '$'
        p = p.replace(symbol, '').strip()
        p = p.replace(',', '.') if symbol == '€' and (',' in p and '.' not in p) else p.replace(',', '')
        return float(re.sub(r'[^\d.]', '', p) or 0)
    try:
        original, sale = parse(original_price), parse(sale_price)
        return int((1 - sale / original) * 100) if original > 0 else 0
    except Exception:
        return 0

def load_exchange_rates():
    try:
        with open(EXCHANGE_RATES_FILE, 'r') as f:
            data = json.load(f)
        is_fresh = (datetime.now() - datetime.fromisoformat(data['last_update'])).days < 1
        return data['rates'] if is_fresh else update_exchange_rates()
    except (FileNotFoundError, json.JSONDecodeError, KeyError, ValueError):
        return update_exchange_rates()


def update_exchange_rates():
    try:
        resp = requests.get(f"https://v6.exchangerate-api.com/v6/{EXCHANGERATE_API_KEY}/latest/UAH").json()
        rates = {k: resp['conversion_rates'][k] for k in ('EUR', 'USD', 'GBP')}
        with open(EXCHANGE_RATES_FILE, 'w') as f:
            json.dump({'last_update': datetime.now().isoformat(), 'rates': rates}, f)
        return rates
    except Exception as e:
        logger.error(f"Error updating exchange rates: {e}")
        return {'EUR': 1, 'USD': 1, 'GBP': 1}

def convert_to_uah(price, country, exchange_rates, name):
    try:
        currency_map = {
            '€': ('EUR', lambda p: float(p.replace('€','').replace(',', '.').strip())),
            '£': ('GBP', lambda p: float(p.replace('£','').replace(',', '').strip())),
            '$': ('USD', lambda p: float(p.replace('$','').replace(',', '').strip()))
        }
        
        for symbol, (code, parse_fn) in currency_map.items():
            if symbol in price:
                currency, currency_symbol = code, symbol
                amount = parse_fn(price)
                break
        else:
            logger.error(f"Unrecognized currency symbol in price '{price}' for '{name}' country '{country}'")
            return ConversionResult(0, 0, '')

        rate = exchange_rates.get(currency)
        if not rate:
            logger.error(f"Exchange rate not found for currency '{currency}' (country: {country})")
            return ConversionResult(0, 0, '')

        uah_amount = amount / rate
        return ConversionResult(round(uah_amount / 10) * 10, round(1 / rate, 2), currency_symbol)
    except (ValueError, KeyError) as e:
        logger.error(f"Error converting price '{price}' for '{name}' country '{country}': {e}")
        return ConversionResult(0, 0, '')

# Message formatting and sending
def get_sale_emoji(sale_percentage, uah_sale):
    if sale_percentage >= SALE_EMOJI_ROCKET_THRESHOLD: return "🚀🚀🚀"
    if uah_sale < SALE_EMOJI_UAH_THRESHOLD: return "🐚🐚🐚"
    return "🍄🍄🍄"

def build_shoe_message(shoe, sale_percentage, uah_sale, kurs, kurs_symbol, old_sale_price=None, status=None):
    if status is None:  # New item
        sale_emoji = get_sale_emoji(sale_percentage, uah_sale)
        return (
            f"{sale_emoji}  New item  {sale_emoji}\n{shoe['name']}\n\n"
            f"💀 Prices : <s>{shoe['original_price']}</s>  <b>{shoe['sale_price']}</b>  <i>(Sale: <b>{sale_percentage}%</b>)</i>\n"
            f"🤑 Grivniki : <b>{uah_sale} UAH </b>\n"
            f"🧊 Kurs : {kurs_symbol} {kurs} \n"
            f"🔗 Store : <a href='{shoe['shoe_link']}'>{shoe['store']}</a>\n"
            f"🌍 Country : {shoe['country']}"
        )
    return (
        f"💎💎💎 {status} 💎💎💎 \n{shoe['name']}:\n\n"
        f"💀 Prices : <s>{shoe['original_price']}</s>  <s>{old_sale_price}</s>  <b>{shoe['sale_price']}</b>  <i>(Sale: <b>{sale_percentage}%</b>)</i> \n"
        f"🤑 Grivniki : {uah_sale} UAH\n"
        f"📉 Lowest price : {shoe['lowest_price']} ({shoe['lowest_price_uah']} UAH)\n"
        f"🧊 Kurs : {kurs_symbol} {kurs} \n"
        f"🔗 Store : <a href='{shoe['shoe_link ']}'>{shoe['store']}</a>\n"
        f"🌍 Country : {shoe['country']}"
    )

async def send_telegram_message(bot_token, chat_id, message, image_url=None, uah_price=None, sale_percentage=None, max_retries=3):
    bot = Bot(token=bot_token)
    for attempt in range(max_retries):
        try:
            if image_url and image_url.startswith(('http://', 'https://')):
                if uah_price is not None and sale_percentage is not None:
                    img_byte_arr = process_image(image_url, uah_price, sale_percentage)
                    await bot.send_photo(chat_id=chat_id, photo=img_byte_arr, caption=message, parse_mode='HTML')
                else:
                    await bot.send_photo(chat_id=chat_id, photo=image_url, caption=message, parse_mode='HTML')
            else:
                await bot.send_message(chat_id=chat_id, text=message, parse_mode='HTML')
            return True
        except RetryAfter as e:
            logger.warning(f"Rate limited. Sleeping for {e.retry_after} seconds")
            await asyncio.sleep(e.retry_after)
        except TimedOut:
            logger.warning(f"Request timed out on attempt {attempt + 1}")
            await asyncio.sleep(3 * (attempt + 1))
        except Exception as e:
            logger.error(f"Error sending Telegram message (attempt {attempt + 1}): {e}")
            if attempt == max_retries - 1:
                logger.error(f"Failed to send Telegram message after {max_retries} attempts")
                return False
            await asyncio.sleep(2 * (attempt + 1))
    return False

def get_allowed_chat_ids():
    allowed = set()
    for raw in (DANYLO_DEFAULT_CHAT_ID, TELEGRAM_CHAT_ID):
        if raw is None:
            continue
        try:
            allowed.add(int(raw))
        except (TypeError, ValueError):
            continue
    return allowed

def tail_log_lines(path, line_count=LOG_TAIL_LINES):
    if not path.exists():
        return []
    lines = deque(maxlen=line_count)
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                lines.append(line.rstrip("\n"))
    except Exception as exc:
        logger.error(f"Failed to read log file: {exc}")
        return []
    return list(lines)

async def send_log_tail(bot, chat_id, log_path, line_count=LOG_TAIL_LINES):
    lines = tail_log_lines(log_path, line_count)
    if not lines:
        await bot.send_message(chat_id=chat_id, text="Log file is empty or missing.")
        return
    payload = "\n".join(lines) + "\n"
    log_bytes = payload.encode("utf-8", errors="replace")
    bio = io.BytesIO(log_bytes)
    bio.name = f"python_last_{line_count}.log"
    await bot.send_document(
        chat_id=chat_id,
        document=bio,
        caption=f"Last {line_count} lines from {log_path.name}"
    )

async def command_listener(bot_token, allowed_chat_ids, log_path):
    if not bot_token:
        logger.warning("Command listener disabled: TELEGRAM_BOT_TOKEN is not set.")
        return
    if not allowed_chat_ids:
        logger.warning("Command listener disabled: no allowed chat IDs configured.")
        return

    bot = Bot(token=bot_token)
    offset = None
    logger.info("Command listener started.")

    while True:
        try:
            updates = await bot.get_updates(
                offset=offset,
                timeout=20,
                allowed_updates=["message"]
            )
            for update in updates:
                offset = update.update_id + 1
                message = update.message
                if not message or not message.text:
                    continue
                chat_id = message.chat_id
                if chat_id not in allowed_chat_ids:
                    continue
                raw_text = message.text.strip()
                if not raw_text:
                    continue
                command = raw_text.split()[0].split("@")[0].lower()
                if command in ("/log", "/logs", "/log500"):
                    await send_log_tail(bot, chat_id, log_path, LOG_TAIL_LINES)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(f"Command listener error: {exc}")
            await asyncio.sleep(5)

async def _run_olx_and_mark():
    await run_olx_scraper()
    mark_olx_run()

async def _run_shafa_and_mark():
    await run_shafa_scraper()
    mark_shafa_run()

# Processing functions
def filter_duplicates(shoes, exchange_rates):
    filtered_shoes, grouped_shoes = [], defaultdict(list)
    for shoe in shoes:
        grouped_shoes[f"{shoe['name']}_{shoe['unique_id']}"] .append(shoe)

    for group in grouped_shoes.values():
        # Deduplicate within same country: prefer item with valid image_url
        country_map = {}
        for shoe in group:
            country = shoe['country']
            if country not in country_map:
                country_map[country] = shoe
            else:
                existing = country_map[country]
                existing_img = existing.get('image_url')
                new_img = shoe.get('image_url')
                
                existing_has_img = existing_img and existing_img.startswith(('http', 'https'))
                new_has_img = new_img and new_img.startswith(('http', 'https'))
                
                if not existing_has_img and new_has_img:
                    country_map[country] = shoe
        
        group = list(country_map.values())

        if len(group) == 1:
            filtered_shoes.append(group[0])
            continue
        group.sort(key=lambda x: COUNTRY_PRIORITY.index(x['country']) if x['country'] in COUNTRY_PRIORITY else len(COUNTRY_PRIORITY))
        for shoe in group:
            shoe['uah_price'] = convert_to_uah(shoe['sale_price'], shoe['country'], exchange_rates, shoe['name']).uah_amount
        base = group[0]
        replacement = next((s for s in group[1:] if base['uah_price'] - s['uah_price'] >= 200), None)
        filtered_shoes.append(replacement or base)
    return filtered_shoes

async def process_shoe(shoe, old_data, message_queue, exchange_rates):
    key = f"{shoe['name']}_{shoe['unique_id']}"
    if await is_shoe_processed(key): return

    # Calculate sale details
    sale_percentage = calculate_sale_percentage(shoe['original_price'], shoe['sale_price'], shoe['country'])
    sale_exchange_data = convert_to_uah(shoe['sale_price'], shoe['country'], exchange_rates, shoe['name'])
    kurs, uah_sale, kurs_symbol = sale_exchange_data.exchange_rate, sale_exchange_data.uah_amount, sale_exchange_data.currency_symbol

    # Handle new shoe
    if key not in old_data:
        shoe.update({
            'lowest_price': shoe['sale_price'],
            'lowest_price_uah': uah_sale,
            'uah_price': uah_sale,
            'active': True
        })
        message = build_shoe_message(shoe, sale_percentage, uah_sale, kurs, kurs_symbol)
        message_id = await message_queue.add_message(shoe['base_url']['telegram_chat_id'], message, shoe['image_url'], uah_sale, sale_percentage)
        while not message_queue.is_message_sent(message_id):
            await asyncio.sleep(1)
        await mark_shoe_processed(key)
        old_data[key] = shoe
        # Save individual shoe instead of entire dataset
        await save_shoe_data_bulk([dict(shoe, key=key)])
    else:
        # Update existing shoe
        old_shoe = old_data[key]
        old_sale_price = old_shoe['sale_price']
        old_sale_country = old_shoe['country']
        old_uah = old_shoe.get('uah_price') or convert_to_uah(old_sale_price, old_sale_country, exchange_rates, shoe['name']).uah_amount
        shoe['uah_price'] = uah_sale
        lowest_price_uah = old_shoe.get('lowest_price_uah') or old_uah

        # Update lowest price if needed
        if uah_sale < lowest_price_uah:
            shoe['lowest_price'], shoe['lowest_price_uah'] = shoe['sale_price'], uah_sale
        else:
            shoe['lowest_price'], shoe['lowest_price_uah'] = old_shoe['lowest_price'], lowest_price_uah
        
        shoe['active'] = True
        old_data[key] = shoe
        # Save individual shoe instead of entire dataset
        await save_shoe_data_bulk([dict(shoe, key=key)])

async def process_all_shoes(all_shoes, old_data, message_queue, exchange_rates):
    new_shoe_count = 0
    semaphore = asyncio.Semaphore(LYST_SHOE_CONCURRENCY)  # Reduce concurrency to prevent database locks
    total_items = len(all_shoes)

    async def process_single_shoe(i, shoe):
        nonlocal new_shoe_count
        async with semaphore:  # Limit concurrency
            try:
                country, name, unique_id = shoe['country'], shoe['name'], shoe['unique_id']
                key = f"{name}_{unique_id}"
                sale_percentage = calculate_sale_percentage(shoe['original_price'], shoe['sale_price'], country)
                
                if sale_percentage < shoe['base_url']['min_sale']: return

                # Get final link or use existing one
                if key not in old_data:
                    if RESOLVE_REDIRECTS:
                        shoe['shoe_link'] = await get_final_clear_link(shoe['shoe_link'], semaphore, name, country, i, total_items)
                    # else: shoe['shoe_link'] remains the initial URL
                    new_shoe_count += 1
                else:
                    shoe['shoe_link'] = old_data[key]['shoe_link']
                
                await process_shoe(shoe, old_data, message_queue, exchange_rates)
            except Exception as e:
                logger.error(f"Error processing shoe {shoe.get('name', 'unknown')}: {e}")
                logger.error(traceback.format_exc())

    # Process shoes in smaller batches to reduce database contention
    batch_size = 10
    for i in range(0, len(all_shoes), batch_size):
        batch = all_shoes[i:i + batch_size]
        await asyncio.gather(*[process_single_shoe(i + j, shoe) for j, shoe in enumerate(batch)])
        # Small delay between batches to prevent overwhelming the database
        await asyncio.sleep(0.1)
    
    logger.info(f"Processed {new_shoe_count} new shoes in total")

    # Handle removed shoes in batches
    current_shoes = {f"{shoe['name']}_{shoe['unique_id']}" for shoe in all_shoes}
    removed_shoes = [dict(shoe, key=k, active=False) for k, shoe in old_data.items() if k not in current_shoes and shoe.get('active', True)]
    for s in removed_shoes:
        old_data[s['key']]['active'] = False
    if removed_shoes:
        await save_shoe_data_bulk(removed_shoes)

async def process_url(base_url, countries, exchange_rates):
    _touch_lyst_progress()
    mark_lyst_start()
    all_shoes = []
    country_results = await asyncio.gather(*(scrape_all_pages(base_url, c) for c in countries))
    for country, result in zip(countries, country_results):
        for shoe in result:
            if isinstance(shoe, dict):
                shoe['base_url'] = base_url
                all_shoes.append(shoe)
            else:
                logger.error(f"Unexpected item data type for {country}: {type(shoe)}")
        special_logger.info(f"Found {len(result)} items for {country} - {base_url['url_name']}")
    return all_shoes

# Utility functions
def print_statistics():
    special_logger.stat(f"Max wait time for initial URL change: {max_wait_times['url_changes']:.2f} seconds")
    special_logger.stat(f"Max wait time for final URL change: {max_wait_times['final_url_changes']:.2f} seconds")
        
def print_link_statistics():
    if 'steps' in link_statistics:
        special_logger.stat("Final URL obtained at the following steps:")
        total_final_urls = sum(info['final_url_obtained'] for info in link_statistics['steps'].values())

        for step_name, info in link_statistics['steps'].items():
            count, final_url_count = info['count'], info['final_url_obtained']
            success_rate = (final_url_count / count) * 100 if count > 0 else 0
            percentage_of_total = (final_url_count / total_final_urls) * 100 if total_final_urls > 0 else 0
            special_logger.stat(f"{step_name}: {final_url_count}/{count} final URLs obtained ({success_rate:.2f}% success rate), {percentage_of_total:.2f}% of total final URLs")
                    
def center_text(text, width, fill_char=' '): return text.center(width, fill_char)

async def run_lyst_cycle_impl(message_queue):
    global LYST_LAST_PROGRESS_TS
    SKIPPED_ITEMS.clear()
    begin_lyst_cycle()
    _touch_lyst_progress()
    try:
        old_data = await load_shoe_data()
        exchange_rates = load_exchange_rates()
        url_tasks = [
            asyncio.wait_for(process_url(base_url, COUNTRIES, exchange_rates), timeout=LYST_URL_TIMEOUT_SEC)
            for base_url in BASE_URLS
        ]
        url_results = await asyncio.gather(*url_tasks, return_exceptions=True)

        all_shoes = []
        for result in url_results:
            if isinstance(result, Exception):
                logger.error(f"Lyst task failed: {result}")
                continue
            all_shoes.extend(result)
        if not all_shoes:
            mark_lyst_issue("0 items scraped")

        collected_ids = {shoe['unique_id'] for shoe in all_shoes}
        recovered_count = sum(1 for uid in SKIPPED_ITEMS if uid in collected_ids)
        special_logger.stat(f"Items skipped due to image but present in final list: {recovered_count}/{len(SKIPPED_ITEMS)}")

        unfiltered_len = len(all_shoes)
        all_shoes = filter_duplicates(all_shoes, exchange_rates)
        special_logger.stat(f"Removed {unfiltered_len - len(all_shoes)} duplicates")

        await process_all_shoes(all_shoes, old_data, message_queue, exchange_rates)
        finalize_lyst_run()

        print_statistics()
        print_link_statistics()
    except asyncio.CancelledError:
        mark_lyst_issue("stalled")
        finalize_lyst_run()
        raise
    except Exception as exc:
        logger.error(f"Lyst run failed: {exc}")
        mark_lyst_issue("failed")
        finalize_lyst_run()
        raise



def _db_maintenance_sync():
    db_files = [SHOES_DB_FILE, OLX_DB_FILE, SHAFA_DB_FILE]
    for db_path in db_files:
        if not db_path.exists():
            continue
        try:
            conn = sqlite3.connect(db_path)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA busy_timeout=5000;")
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            conn.execute("PRAGMA optimize;")
            conn.execute("ANALYZE;")
            if db_path == OLX_DB_FILE and OLX_RETENTION_DAYS > 0:
                conn.execute(
                    "DELETE FROM olx_items WHERE updated_at < datetime('now', ?)",
                    (f"-{OLX_RETENTION_DAYS} days",),
                )
                conn.commit()
            if db_path == SHAFA_DB_FILE and SHAFA_RETENTION_DAYS > 0:
                conn.execute(
                    "DELETE FROM shafa_items WHERE updated_at < datetime('now', ?)",
                    (f"-{SHAFA_RETENTION_DAYS} days",),
                )
                conn.commit()
            if DB_VACUUM:
                conn.execute("VACUUM;")
            conn.commit()
            conn.close()
        except Exception as exc:
            logger.warning(f"DB maintenance failed for {db_path.name}: {exc}")

async def maintenance_loop(interval_s: int):
    if interval_s <= 0:
        return
    while True:
        await asyncio.to_thread(_db_maintenance_sync)
        await asyncio.sleep(interval_s)

# Main application
async def main():
    global LIVE_MODE
    load_last_runs_from_file()
    # Initialize and start message queue
    message_queue = TelegramMessageQueue(TELEGRAM_BOT_TOKEN)
    asyncio.create_task(message_queue.process_queue())
    asyncio.create_task(command_listener(TELEGRAM_BOT_TOKEN, get_allowed_chat_ids(), BOT_LOG_FILE))
    try:
        chat_id = int((DANYLO_DEFAULT_CHAT_ID or "").strip())
    except Exception:
        chat_id = None
    if chat_id:
        asyncio.create_task(status_heartbeat(TELEGRAM_BOT_TOKEN, chat_id, interval_s=600))
    if MAINTENANCE_INTERVAL_SEC > 0:
        asyncio.create_task(maintenance_loop(MAINTENANCE_INTERVAL_SEC))
    terminal_width = shutil.get_terminal_size().columns
    bot_version = f"Grotesk bot v.{BOT_VERSION}"
    print(
        Fore.GREEN + '-' * terminal_width + Style.RESET_ALL + '\n' +
        Fore.CYAN + Style.BRIGHT + bot_version.center(terminal_width) + Style.RESET_ALL + '\n' +
        Fore.GREEN + '-' * terminal_width + Style.RESET_ALL
    )

    if ASK_FOR_LIVE_MODE:
        LIVE_MODE = input("Enter 'live' to enable live mode, or press Enter to continue in headless mode: ").strip().lower() == 'live'
    if LIVE_MODE:
        special_logger.good("Live mode enabled")

    async def run_lyst_cycle():
        global LYST_LAST_PROGRESS_TS
        begin_lyst_cycle()
        _touch_lyst_progress()
        try:
            old_data = await load_shoe_data()
            exchange_rates = load_exchange_rates()
            url_tasks = [
                asyncio.wait_for(process_url(base_url, COUNTRIES, exchange_rates), timeout=LYST_URL_TIMEOUT_SEC)
                for base_url in BASE_URLS
            ]
            url_results = await asyncio.gather(*url_tasks, return_exceptions=True)

            all_shoes = []
            for result in url_results:
                if isinstance(result, Exception):
                    logger.error(f"Lyst task failed: {result}")
                    continue
                all_shoes.extend(result)
            if not all_shoes:
                mark_lyst_issue("0 items scraped")

            collected_ids = {shoe['unique_id'] for shoe in all_shoes}
            recovered_count = sum(1 for uid in SKIPPED_ITEMS if uid in collected_ids)
            special_logger.stat(f"Items skipped due to image but present in final list: {recovered_count}/{len(SKIPPED_ITEMS)}")

            unfiltered_len = len(all_shoes)
            all_shoes = filter_duplicates(all_shoes, exchange_rates)
            special_logger.stat(f"Removed {unfiltered_len - len(all_shoes)} duplicates")

            await process_all_shoes(all_shoes, old_data, message_queue, exchange_rates)
            finalize_lyst_run()

            print_statistics()
            print_link_statistics()
        except asyncio.CancelledError:
            mark_lyst_issue("stalled")
            finalize_lyst_run()
            raise
        except Exception as exc:
            logger.error(f"Lyst run failed: {exc}")
            mark_lyst_issue("failed")
            finalize_lyst_run()
            raise

    try:
        await run_scheduler(
            run_olx=_run_olx_and_mark,
            run_shafa=_run_shafa_and_mark,
            run_lyst=lambda: run_lyst_cycle_impl(message_queue),
            is_running_lyst=lambda: IS_RUNNING_LYST,
            get_lyst_progress_ts=lambda: LYST_LAST_PROGRESS_TS,
            check_interval_sec=CHECK_INTERVAL_SEC,
            check_jitter_sec=CHECK_JITTER_SEC,
            logger=logger,
            last_olx_run_exists=LAST_OLX_RUN_UTC is not None,
            last_shafa_run_exists=LAST_SHAFA_RUN_UTC is not None,
            olx_timeout_sec=OLX_TIMEOUT_SEC,
            shafa_timeout_sec=SHAFA_TIMEOUT_SEC,
            lyst_stall_timeout_sec=LYST_STALL_TIMEOUT_SEC,
        )
    finally:
        pass  # Removed application.stop() as we're no longer using telegram.ext.Application

if __name__ == "__main__":
    if IS_RUNNING_LYST:
        create_tables()  # Create tables at startup instead of creating them just before using
    asyncio.run(main())
