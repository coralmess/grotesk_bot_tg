import json, time, asyncio, logging, colorama, subprocess, shutil, traceback, urllib.parse, re, html, io, uuid, requests, sqlite3, threading, hashlib, os, random
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
    begin_lyst_cycle,
    mark_lyst_start,
    mark_lyst_issue,
    finalize_lyst_run,
)
from helpers.dynamic_sources import add_dynamic_url, detect_source
from helpers import image_pipeline as image_pipeline_helpers
from helpers import lyst_identity as lyst_identity_helpers
from helpers import telegram_runtime as telegram_runtime_helpers
from helpers import lyst_state as lyst_state_helpers
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DANYLO_DEFAULT_CHAT_ID, EXCHANGERATE_API_KEY, IS_RUNNING_LYST, CHECK_INTERVAL_SEC, CHECK_JITTER_SEC, MAINTENANCE_INTERVAL_SEC, DB_VACUUM, OLX_RETENTION_DAYS, SHAFA_RETENTION_DAYS, LYST_MAX_BROWSERS, LYST_SHOE_CONCURRENCY, LYST_COUNTRY_CONCURRENCY, UPSCALE_IMAGES, UPSCALE_METHOD, LYST_HTTP_ONLY, LYST_HTTP_TIMEOUT_SEC, LYST_HTTP_CONCURRENCY, LYST_HTTP_REQUEST_JITTER_SEC, LYST_CLOUDFLARE_RETRY_COUNT, LYST_CLOUDFLARE_RETRY_DELAY_SEC
from config_lyst import (
    BASE_URLS,
    LYST_COUNTRIES,
    LYST_PAGE_SCRAPE,
    LYST_URL_TIMEOUT_SEC as LYST_URL_TIMEOUT_DEFAULT,
    LYST_STALL_TIMEOUT_SEC as LYST_STALL_TIMEOUT_DEFAULT,
    LYST_PAGE_TIMEOUT_SEC,
    LYST_MAX_SCROLL_ATTEMPTS,
)
from helpers.lyst_debug import (
    attach_lyst_debug_listeners,
    dump_lyst_debug_event,
    write_stop_too_early_dump,
)
from helpers.logging_utils import configure_third_party_loggers, install_secret_redaction
from helpers.scheduler import run_lyst_scheduler
from colorama import Fore, Back, Style
from PIL import Image, ImageDraw, ImageFont
from asyncio import Semaphore
import aiosqlite
from helpers.runtime_paths import (
    PYTHON_LOG_FILE,
    SHOES_DB_FILE as RUNTIME_SHOES_DB_FILE,
    OLX_ITEMS_DB_FILE as RUNTIME_OLX_DB_FILE,
    SHAFA_ITEMS_DB_FILE as RUNTIME_SHAFA_DB_FILE,
    LYST_RESUME_JSON_FILE,
    SHOE_DATA_JSON_FILE,
    EXCHANGE_RATES_JSON_FILE,
)
from helpers.sqlite_runtime import RUNTIME_DB_PRAGMA_STATEMENTS, run_runtime_db_maintenance
try:
    import cv2
except Exception:
    cv2 = None
try:
    import numpy as np
except Exception:
    np = None

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
BROWSER_LAUNCH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
] + lyst_identity_helpers.browser_launch_args()

# Initialize constants and globals
colorama.init(autoreset=True)
BOT_VERSION, DB_NAME = "4.1.0", str(RUNTIME_SHOES_DB_FILE)
LIVE_MODE, ASK_FOR_LIVE_MODE = False, False
PAGE_SCRAPE = LYST_PAGE_SCRAPE
SHOE_DATA_FILE = SHOE_DATA_JSON_FILE
EXCHANGE_RATES_FILE = EXCHANGE_RATES_JSON_FILE
BOT_LOG_FILE = PYTHON_LOG_FILE
SHOES_DB_FILE = RUNTIME_SHOES_DB_FILE
OLX_DB_FILE = RUNTIME_OLX_DB_FILE
SHAFA_DB_FILE = RUNTIME_SHAFA_DB_FILE
LYST_RESUME_FILE = LYST_RESUME_JSON_FILE
LOG_TAIL_LINES = 500
COUNTRIES = LYST_COUNTRIES
BLOCK_RESOURCES = False
RESOLVE_REDIRECTS = False
SKIPPED_ITEMS = set()
KYIV_TZ = ZoneInfo("Europe/Kyiv")
LYST_URL_TIMEOUT_SEC = LYST_URL_TIMEOUT_DEFAULT
LYST_LAST_PROGRESS_TS = 0.0
LYST_LAST_STEP_INFO = {}
LYST_ACTIVE_TASK = None
LYST_STALL_TIMEOUT_SEC = LYST_STALL_TIMEOUT_DEFAULT
OLX_TIMEOUT_SEC = 1800
SHAFA_TIMEOUT_SEC = 1800
LYST_IMAGE_STRATEGY = "adaptive"  # "adaptive" or "settle"
LYST_IMAGE_READY_TARGET = 0.6
LYST_IMAGE_EXTRA_SCROLLS = 4
LYST_IMAGE_SETTLE_PASSES = 2

EDSR_MODEL_URL = "https://github.com/Saafke/EDSR_Tensorflow/raw/master/models/EDSR_x2.pb"
EDSR_MODEL_PATH = Path(__file__).with_name("upscale_weights") / "EDSR_x2.pb"
_EDSR_LOCK = threading.Lock()
_EDSR_SUPERRES = None

class LystCloudflareChallenge(Exception):
    pass


class LystRunAborted(Exception):
    pass

LYST_RESUME_STATE = {"resume_active": False, "entries": {}}
LYST_RESUME_LOCK = asyncio.Lock()
LYST_ABORT_EVENT = asyncio.Event()
LYST_RUN_FAILED = False
LYST_RUN_PROGRESS = {}
LYST_CYCLE_STARTED_IN_RESUME = False
LYST_RESUME_ENTRY_OUTCOMES = {}
LYST_HTTP_ONLY_ENABLED = LYST_HTTP_ONLY
LYST_HTTP_ONLY_DISABLE_REASON = ""

def build_lyst_context_lines(*, attempt=None, max_retries=None, max_scroll_attempts=None, use_pagination=None):
    # Keep this wrapper for backward compatibility; implementation lives in helpers/lyst_state.py.
    return lyst_state_helpers.build_context_lines(
        attempt=attempt,
        max_retries=max_retries,
        max_scroll_attempts=max_scroll_attempts,
        use_pagination=use_pagination,
        block_resources=BLOCK_RESOURCES,
        page_scrape=PAGE_SCRAPE,
        image_strategy=LYST_IMAGE_STRATEGY,
        image_ready_target=LYST_IMAGE_READY_TARGET,
        image_extra_scrolls=LYST_IMAGE_EXTRA_SCROLLS,
        image_settle_passes=LYST_IMAGE_SETTLE_PASSES,
        lyst_http_only=LYST_HTTP_ONLY,
        lyst_http_only_enabled=LYST_HTTP_ONLY_ENABLED,
        lyst_http_only_disabled_reason=LYST_HTTP_ONLY_DISABLE_REASON,
        lyst_http_timeout_sec=LYST_HTTP_TIMEOUT_SEC,
        lyst_page_timeout_sec=LYST_PAGE_TIMEOUT_SEC,
        lyst_url_timeout_sec=LYST_URL_TIMEOUT_SEC,
        lyst_stall_timeout_sec=LYST_STALL_TIMEOUT_SEC,
        lyst_max_browsers=LYST_MAX_BROWSERS,
        lyst_shoe_concurrency=LYST_SHOE_CONCURRENCY,
        lyst_country_concurrency=LYST_COUNTRY_CONCURRENCY,
        live_mode=LIVE_MODE,
    )

def reset_lyst_http_only_state():
    global LYST_HTTP_ONLY_ENABLED, LYST_HTTP_ONLY_DISABLE_REASON
    LYST_HTTP_ONLY_ENABLED, LYST_HTTP_ONLY_DISABLE_REASON = lyst_state_helpers.reset_http_only_state(
        lyst_http_only_default=LYST_HTTP_ONLY
    )

def disable_lyst_http_only(reason: str):
    global LYST_HTTP_ONLY_ENABLED, LYST_HTTP_ONLY_DISABLE_REASON
    # Preserve previous reason when already disabled to avoid noisy state churn.
    if not LYST_HTTP_ONLY_ENABLED:
        return
    LYST_HTTP_ONLY_ENABLED, LYST_HTTP_ONLY_DISABLE_REASON = lyst_state_helpers.disable_http_only(
        currently_enabled=LYST_HTTP_ONLY_ENABLED,
        reason=reason,
        logger=logger,
    )

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
LYST_HTTP_SEMAPHORE = asyncio.Semaphore(LYST_HTTP_CONCURRENCY)

# Define namedtuples and container classes
ConversionResult = namedtuple('ConversionResult', ['uah_amount', 'exchange_rate', 'currency_symbol'])
EMPTY_CONVERSION_RESULT = ConversionResult(0, 0, '')
CURRENCY_CODE_BY_SYMBOL = {
    '\u20ac': 'EUR',
    '\u00e2\u201a\u00ac': 'EUR',
    '\u00a3': 'GBP',
    '\u00c2\u00a3': 'GBP',
    '$': 'USD',
}
CURRENCY_DISPLAY_SYMBOL_BY_SYMBOL = {
    '\u00e2\u201a\u00ac': '\u20ac',
    '\u00c2\u00a3': '\u00a3',
}

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

class TelegramMessageQueue(telegram_runtime_helpers.TelegramMessageQueue):
    # Thin compatibility layer so existing call sites keep the same constructor/signature.
    def __init__(self, bot_token):
        super().__init__(bot_token, send_func=send_telegram_message)

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
configure_third_party_loggers()
install_secret_redaction(logger)

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
        browser = await _launch_browser(self._browser_type)
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

class LystContextPool:
    def __init__(self):
        self._playwright = None
        self._browser = None
        self._contexts = {}
        self._context_init_locks = {}
        self._country_semaphores = {}
        self._init_lock = asyncio.Lock()

    async def init(self):
        async with self._init_lock:
            if self._playwright:
                return
            self._playwright = await async_playwright().start()
            self._browser = await _launch_browser(self._playwright.chromium)

    def get_country_semaphore(self, country):
        sem = self._country_semaphores.get(country)
        if sem is None:
            sem = asyncio.Semaphore(LYST_COUNTRY_CONCURRENCY)
            self._country_semaphores[country] = sem
        return sem

    async def get_context(self, country):
        await self.init()
        lock = self._context_init_locks.get(country)
        if lock is None:
            lock = asyncio.Lock()
            self._context_init_locks[country] = lock
        async with lock:
            ctx = self._contexts.get(country)
            if ctx is not None:
                return ctx, True
            ctx = await _create_lyst_country_context(self._browser, country)
            self._contexts[country] = ctx
            return ctx, False

    async def reset_context(self, country):
        ctx = self._contexts.pop(country, None)
        if ctx is None:
            return
        try:
            await ctx.close()
        except Exception:
            pass

    async def reset_browser(self):
        try:
            for ctx in list(self._contexts.values()):
                try:
                    await ctx.close()
                except Exception:
                    pass
            self._contexts.clear()
        except Exception:
            pass
        try:
            if self._browser:
                await self._browser.close()
        except Exception:
            pass
        self._browser = await _launch_browser(self._playwright.chromium)

lyst_context_pool = LystContextPool()

# Helper functions
async def _launch_browser(browser_type):
    return await browser_type.launch(
        headless=not LIVE_MODE,
        args=BROWSER_LAUNCH_ARGS,
    )

async def _create_lyst_country_context(browser, country):
    storage_state_path = lyst_identity_helpers.country_storage_state_path(country)
    context_kwargs = {
        "user_agent": STEALTH_UA,
        "locale": "en-US",
        "timezone_id": "Europe/Kyiv",
        "extra_http_headers": STEALTH_HEADERS,
    }
    if storage_state_path.exists():
        context_kwargs["storage_state"] = str(storage_state_path)
    ctx = await browser.new_context(
        **context_kwargs,
    )
    await ctx.add_init_script(STEALTH_SCRIPT)
    await ctx.add_cookies([{'name': 'country', 'value': country, 'domain': '.lyst.com', 'path': '/'}])
    await lyst_identity_helpers.persist_context_storage_state(country, ctx, logger)
    return ctx

# Compiled once because link cleanup runs for many outgoing messages.
DISPLAY_LINK_PREFIX_RE = re.compile(r'^(https?://)?(www\.)?', re.IGNORECASE)

def clean_link_for_display(link):
    cleaned_link = DISPLAY_LINK_PREFIX_RE.sub('', link or "")
    return (cleaned_link[:22] + '...') if len(cleaned_link) > 25 else cleaned_link

def _resume_key(base_url, country):
    return f"{base_url['url_name']}|{country}"

def _now_kyiv_str():
    return datetime.now(KYIV_TZ).strftime('%Y-%m-%d %H:%M:%S')

def _write_json_atomic(path: Path, payload):
    # Delegated to shared helper to keep one atomic-write implementation across bot modules.
    lyst_state_helpers._write_json_atomic(path, payload)

def load_lyst_resume_state():
    return lyst_state_helpers.load_resume_state(resume_file=LYST_RESUME_FILE, logger=logger)

def save_lyst_resume_state(state):
    lyst_state_helpers.save_resume_state(resume_file=LYST_RESUME_FILE, state=state, logger=logger)

async def update_lyst_resume_entry(key, **fields):
    await lyst_state_helpers.update_resume_entry(
        resume_lock=LYST_RESUME_LOCK,
        resume_state=LYST_RESUME_STATE,
        key=key,
        fields=fields,
        now_kyiv_str_fn=_now_kyiv_str,
        save_state_fn=save_lyst_resume_state,
    )

def init_lyst_resume_state():
    global LYST_RESUME_STATE, LYST_RUN_FAILED, LYST_RUN_PROGRESS, LYST_CYCLE_STARTED_IN_RESUME, LYST_RESUME_ENTRY_OUTCOMES
    # Wrapper keeps old API while shared helper owns the state-reset rules.
    loaded_state = load_lyst_resume_state()
    LYST_RESUME_STATE, LYST_RUN_FAILED, LYST_RUN_PROGRESS = lyst_state_helpers.init_resume_state(
        loaded_state=loaded_state,
        abort_event=LYST_ABORT_EVENT,
    )
    LYST_CYCLE_STARTED_IN_RESUME = bool(LYST_RESUME_STATE.get("resume_active", False))
    LYST_RESUME_ENTRY_OUTCOMES = {}

async def mark_lyst_run_failed(reason: str):
    global LYST_RUN_FAILED
    LYST_RUN_FAILED = await lyst_state_helpers.mark_run_failed(
        reason=reason,
        resume_lock=LYST_RESUME_LOCK,
        resume_state=LYST_RESUME_STATE,
        run_progress=LYST_RUN_PROGRESS,
        now_kyiv_str_fn=_now_kyiv_str,
        save_state_fn=save_lyst_resume_state,
        abort_event=LYST_ABORT_EVENT,
    )

def log_lyst_run_progress_summary():
    lyst_state_helpers.log_run_progress_summary(run_progress=LYST_RUN_PROGRESS, logger=logger)

async def finalize_lyst_resume_after_processing():
    await lyst_state_helpers.finalize_resume_after_processing(
        resume_lock=LYST_RESUME_LOCK,
        resume_state=LYST_RESUME_STATE,
        run_failed=LYST_RUN_FAILED,
        save_state_fn=save_lyst_resume_state,
    )

def load_font(font_size, prefer_heavy=False):
    return image_pipeline_helpers.load_font(
        font_size,
        fonts_dir=Path(__file__).with_name("fonts"),
        prefer_heavy=prefer_heavy,
    )

def _ensure_edsr_weights():
    return image_pipeline_helpers._ensure_edsr_weights(
        model_path=EDSR_MODEL_PATH,
        model_url=EDSR_MODEL_URL,
        logger=logger,
    )

def _get_edsr_superres():
    return image_pipeline_helpers._get_edsr_superres(
        cv2_module=cv2,
        model_path=EDSR_MODEL_PATH,
        model_url=EDSR_MODEL_URL,
        logger=logger,
    )

def _upscale_with_edsr(pil_img):
    return image_pipeline_helpers._upscale_with_edsr(
        pil_img,
        cv2_module=cv2,
        np_module=np,
        model_path=EDSR_MODEL_PATH,
        model_url=EDSR_MODEL_URL,
        logger=logger,
    )

def _fetch_image_bytes(image_url: str) -> bytes:
    return image_pipeline_helpers._fetch_image_bytes(
        image_url,
        image_url_candidates_fn=_image_url_candidates,
    )

def process_image(image_url, uah_price, sale_percentage):
    # Shared image rendering/compression pipeline extracted to helper for easier testing.
    return image_pipeline_helpers.process_image(
        image_url,
        uah_price,
        sale_percentage,
        upscale_images=UPSCALE_IMAGES,
        upscale_method=UPSCALE_METHOD,
        image_url_candidates_fn=_image_url_candidates,
        logger=logger,
        fonts_dir=Path(__file__).with_name("fonts"),
        cv2_module=cv2,
        np_module=np,
        edsr_model_path=EDSR_MODEL_PATH,
        edsr_model_url=EDSR_MODEL_URL,
    )

# Database functions
PRAGMA_STATEMENTS = ['PRAGMA foreign_keys = ON', *RUNTIME_DB_PRAGMA_STATEMENTS]

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
    # load_shoe_data_from_db uses sync sqlite3; run it in a thread so the event loop stays responsive.
    return await asyncio.to_thread(load_shoe_data_from_db)

async def save_shoe_data(data):
    await async_save_shoe_data(data)

# Web scraping and browser functions
BLOCKED_RESOURCE_TYPES = {"media", "font", "stylesheet"}
BLOCKED_URL_PARTS = (
    "googletagmanager.com",
    "google-analytics.com",
    "doubleclick.net",
    "facebook.net",
    "facebook.com/tr",
    "hotjar.com",
    "segment.io",
    "mixpanel.com",
    "optimizely.com",
    "clarity.ms",
    "sentry.io",
    "newrelic.com",
)

async def handle_route(route):
    url = route.request.url
    resource_type = route.request.resource_type
    if resource_type in BLOCKED_RESOURCE_TYPES or any(part in url for part in BLOCKED_URL_PARTS):
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

def is_cloudflare_challenge(content: str) -> bool:
    if not content:
        return False
    lowered = content.lower()
    if "cloudflare" in lowered and ("just a moment" in lowered or "checking your browser" in lowered):
        return True
    if "cf-challenge" in lowered or "cf_challenge" in lowered or "cf-turnstile" in lowered:
        return True
    if "<title>just a moment" in lowered or "<title>attention required" in lowered:
        return True
    return False

class LystHttpTerminalPage(Exception):
    def __init__(self, status_code: int, content: str = ""):
        super().__init__(f"http_only_terminal_status_{status_code}")
        self.status_code = status_code
        self.content = content or ""


def _lyst_http_base_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    filtered_query = [(key, value) for key, value in query if key.lower() != "page"]
    return urllib.parse.urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, urllib.parse.urlencode(filtered_query, doseq=True), "")
    )


def _lyst_http_content_has_product_cards(content: str) -> bool:
    if not content:
        return False
    lowered = content.lower()
    return 'data-testid="product-card"' in lowered or "data-testid='product-card'" in lowered or "_693owt3" in content


def _fetch_lyst_http_content(url: str, country: str, variant: str = "direct"):
    # Instance tests showed that the direct HTTP request already returns the correct
    # ordered item list. The best fallback was a warmed HTTP session with browser-like
    # navigation headers, so we keep exactly that as the final HTTP attempt.
    headers = {
        "User-Agent": STEALTH_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": STEALTH_HEADERS.get("Accept-Language", "en-US,en;q=0.9"),
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    with requests.Session() as session:
        session.headers.update(headers)
        try:
            session.cookies.set("country", country, domain=".lyst.com", path="/")
        except Exception:
            session.cookies.set("country", country)
        if variant == "home_warm":
            session.headers.update(
                {
                    "Sec-Fetch-Site": "same-origin",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-User": "?1",
                    "Sec-Fetch-Dest": "document",
                    "sec-ch-ua": '"Chromium";v="124", "Not.A/Brand";v="99", "Google Chrome";v="124"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                }
            )
            session.get("https://www.lyst.com/", timeout=LYST_HTTP_TIMEOUT_SEC)
            session.headers["Referer"] = _lyst_http_base_url(url)
        resp = session.get(url, timeout=LYST_HTTP_TIMEOUT_SEC)
        return resp.status_code, resp.text

async def get_page_content_http(
    url,
    country,
    *,
    max_scroll_attempts=None,
    url_name=None,
    page_num=None,
    attempt=None,
    max_retries=None,
    use_pagination=None,
):
    context_lines = build_lyst_context_lines(
        attempt=attempt,
        max_retries=max_retries,
        max_scroll_attempts=max_scroll_attempts,
        use_pagination=use_pagination,
    )
    context_lines.append("http_only: true")
    http_attempt = 0
    cloudflare_retries = 0
    last_exc = None
    variants = ("direct", "home_warm")
    for variant_index, variant in enumerate(variants, start=1):
        for variant_attempt in range(1, 3):
            if LYST_ABORT_EVENT.is_set():
                raise LystRunAborted("lyst_run_aborted")
            http_attempt += 1
            _touch_lyst_progress(
                "http_attempt",
                url=url,
                country=country,
                url_name=url_name,
                page_num=page_num,
                attempt=http_attempt,
            )
            context_lines.append(f"http_variant: {variant}")
            context_lines.append(f"http_attempt: {http_attempt}")
            context_lines.append(f"http_variant_attempt: {variant_attempt}")
            try:
                # The HTTP path is what triggers the instance-wide Lyst challenge, so we
                # serialize it with a dedicated semaphore and add a small jitter. This
                # keeps the rest of the scraper concurrent while forcing the risky part
                # to stay low-rate and predictable.
                async with LYST_HTTP_SEMAPHORE:
                    # Re-check after waiting for the semaphore. Many country/url tasks
                    # queue here at startup, so a challenge from one task must stop the
                    # queued requests before they make another HTTP hit on the same IP.
                    if LYST_ABORT_EVENT.is_set():
                        raise LystRunAborted("lyst_run_aborted")
                    if LYST_HTTP_REQUEST_JITTER_SEC > 0:
                        await asyncio.sleep(random.uniform(0, LYST_HTTP_REQUEST_JITTER_SEC))
                    if LYST_ABORT_EVENT.is_set():
                        raise LystRunAborted("lyst_run_aborted")
                    status_code, content = await asyncio.wait_for(
                        asyncio.to_thread(_fetch_lyst_http_content, url, country, variant),
                        timeout=LYST_HTTP_TIMEOUT_SEC + 5,
                    )
                    while (
                        (is_cloudflare_challenge(content) or status_code in (403, 429))
                        and cloudflare_retries < LYST_CLOUDFLARE_RETRY_COUNT
                    ):
                        cloudflare_retries += 1
                        # Keep the semaphore during the cooldown+retry. If we released it
                        # here, queued tasks could all hit the same challenge and create a
                        # second wave of doomed requests before the abort signal propagates.
                        logger.warning(
                            f"LYST Cloudflare retry {cloudflare_retries}/{LYST_CLOUDFLARE_RETRY_COUNT} "
                            f"for {url_name or url} [{country}] after {LYST_CLOUDFLARE_RETRY_DELAY_SEC:.0f}s"
                        )
                        await asyncio.sleep(LYST_CLOUDFLARE_RETRY_DELAY_SEC)
                        if LYST_ABORT_EVENT.is_set():
                            raise LystRunAborted("lyst_run_aborted")
                        status_code, content = await asyncio.wait_for(
                            asyncio.to_thread(_fetch_lyst_http_content, url, country, variant),
                            timeout=LYST_HTTP_TIMEOUT_SEC + 5,
                        )
            except asyncio.TimeoutError as exc:
                status_code, content = None, ""
                last_exc = exc
            except Exception as exc:
                status_code, content = None, ""
                last_exc = exc
            context_lines.append(f"http_status: {status_code if status_code is not None else 'error'}")
            if status_code is None:
                if variant_attempt < 2:
                    await asyncio.sleep(2)
                    continue
                break
            if status_code == 410:
                # Lyst returns 410 when pagination runs past the real last page. Treat
                # that as a clean terminal page instead of a scrape failure.
                raise LystHttpTerminalPage(status_code, content)
            if is_cloudflare_challenge(content) or status_code in (403, 429):
                now_kyiv = datetime.now(KYIV_TZ).strftime('%Y-%m-%d %H:%M:%S')
                log_lines = tail_log_lines(BOT_LOG_FILE, line_count=200)
                try:
                    await dump_lyst_debug_event(
                        "lyst_cloudflare",
                        reason=f"Cloudflare challenge ({status_code})" if status_code is not None else "Cloudflare challenge",
                        url=url,
                        country=country,
                        url_name=url_name,
                        page_num=page_num,
                        step=f"http_{variant}",
                        content=content,
                        now_kyiv=now_kyiv,
                        log_lines=log_lines,
                        context_lines=context_lines,
                    )
                except Exception:
                    pass
                # Instance tests showed that once HTTP starts returning Cloudflare/429,
                # the challenge is effectively IP-wide for the rest of the run and the
                # Playwright fallback also fails. Bubble this up so the caller can abort
                # the run quickly and resume later instead of escalating load.
                raise LystCloudflareChallenge()
            if not content.strip():
                if variant_attempt < 2:
                    await asyncio.sleep(2)
                    continue
                break
            if status_code >= 400:
                if status_code in (408, 500, 502, 503, 504) and variant_attempt < 2:
                    await asyncio.sleep(2)
                    continue
                if variant_index < len(variants):
                    await asyncio.sleep(2)
                    break
                raise RuntimeError(f"http_only_status_{status_code}")
            if _lyst_http_content_has_product_cards(content):
                return content
            if variant_index < len(variants):
                # A 200 without product cards is still unusable. Before escalating to
                # Playwright, retry once with the warmed HTTP variant proven on instance.
                break
            raise RuntimeError("http_only_missing_product_cards")
    raise RuntimeError(f"http_only_exception: {last_exc}" if last_exc else "http_only_unusable_response")

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

def is_target_closed_error(exc: Exception) -> bool:
    try:
        if exc.__class__.__name__ == "TargetClosedError":
            return True
    except Exception:
        pass
    msg = str(exc)
    return "Target page, context or browser has been closed" in msg or "TargetClosedError" in msg

def is_pipe_closed_error(exc: Exception) -> bool:
    msg = str(exc)
    if not msg:
        return False
    lowered = msg.lower()
    return "pipe closed" in lowered or "os.write(pipe, data)" in lowered or "epipe" in lowered

def _lyst_url_suffix(url_name=None, page_num=None):
    if url_name or page_num is not None:
        return f" | url_name={url_name or ''} page={page_num if page_num is not None else ''}"
    return ""

def _lyst_page_progress_data(url, country, url_name=None, page_num=None, attempt=None):
    data = {
        "url": url,
        "country": country,
        "url_name": url_name,
        "page_num": page_num,
    }
    if attempt is not None:
        data["attempt"] = attempt
    return data

def _mark_lyst_page_step(step, url, country, url_name=None, page_num=None, attempt=None):
    _touch_lyst_progress(step, **_lyst_page_progress_data(url, country, url_name, page_num, attempt))
    logger.info(f"LYST step={step} url={url}")

def _safe_page_final_url(page):
    try:
        return page.url
    except Exception:
        return None

def _lyst_debug_snapshot(page):
    return (
        datetime.now(KYIV_TZ).strftime('%Y-%m-%d %H:%M:%S'),
        tail_log_lines(BOT_LOG_FILE, line_count=200),
        _safe_page_final_url(page),
    )

async def _dump_lyst_debug_event_safe(
    prefix,
    *,
    reason,
    url,
    country,
    url_name,
    page_num,
    step,
    page,
    debug_events,
    context_lines,
    content=None,
    shield=False,
):
    now_kyiv, log_lines, final_url = _lyst_debug_snapshot(page)
    payload = dict(
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
        context_lines=context_lines,
        final_url=final_url,
    )
    if content is not None:
        payload["content"] = content
    try:
        if shield:
            await asyncio.shield(dump_lyst_debug_event(prefix, **payload))
        else:
            await dump_lyst_debug_event(prefix, **payload)
    except Exception:
        pass

async def get_page_content(
    url,
    country,
    max_scroll_attempts=None,
    url_name=None,
    page_num=None,
    attempt=None,
    max_retries=None,
    use_pagination=None,
):
    if LYST_HTTP_ONLY_ENABLED:
        try:
            content = await get_page_content_http(
                url,
                country,
                max_scroll_attempts=max_scroll_attempts,
                url_name=url_name,
                page_num=page_num,
                attempt=attempt,
                max_retries=max_retries,
                use_pagination=use_pagination,
            )
            if content:
                return content
            raise RuntimeError("http_only_empty_content")
        except LystHttpTerminalPage:
            # Preserve the terminal-page signal so the caller can stop pagination
            # cleanly. If we let the generic fallback catch this, the scraper will
            # incorrectly open Playwright for pages that are already past the end.
            raise
        except LystCloudflareChallenge:
            # When Lyst challenges the HTTP path on the instance, Playwright is also
            # challenged in the same run. Re-raise so the run aborts and resumes later
            # instead of increasing pressure with a browser fallback.
            raise
        except LystRunAborted:
            raise
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            suffix = _lyst_url_suffix(url_name, page_num)
            logger.warning(f"LYST HTTP-only failed, falling back to Playwright for {url}{suffix} | {exc}")
    elif LYST_HTTP_ONLY and not LYST_HTTP_ONLY_ENABLED:
        logger.info("LYST HTTP-only disabled for this run; using Playwright")
    if LYST_ABORT_EVENT.is_set():
        raise LystRunAborted("lyst_run_aborted")
    await lyst_context_pool.init()
    sem = lyst_context_pool.get_country_semaphore(country)
    async with sem:
        context, reused = await lyst_context_pool.get_context(country)
        page = await context.new_page()
        debug_events = []
        attach_lyst_debug_listeners(page, debug_events)
        if BLOCK_RESOURCES:
            await page.route("**/*", handle_route)
        step = "goto"
        context_lines = build_lyst_context_lines(
            attempt=attempt,
            max_retries=max_retries,
            max_scroll_attempts=max_scroll_attempts,
            use_pagination=use_pagination,
        )
        context_lines.append(f"country_context_reused: {reused}")
        try:
            _mark_lyst_page_step("goto", url, country, url_name=url_name, page_num=page_num, attempt=attempt)
            response = await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=int(LYST_PAGE_TIMEOUT_SEC * 1000),
            )
            try:
                if response is not None:
                    context_lines.append(f"goto_status: {response.status}")
                else:
                    context_lines.append("goto_status: ")
            except Exception:
                pass
                if max_scroll_attempts is not None and max_scroll_attempts <= 0:
                    step = "scroll_skip"
                    _mark_lyst_page_step("scroll_skip", url, country, url_name=url_name, page_num=page_num)
                else:
                    step = "scroll"
                    _mark_lyst_page_step("scroll", url, country, url_name=url_name, page_num=page_num)
                    await scroll_page(page, max_scroll_attempts)
            if LYST_IMAGE_STRATEGY == "settle":
                step = "settle_lazy_images"
                _mark_lyst_page_step("settle_lazy_images", url, country, url_name=url_name, page_num=page_num)
                await settle_lazy_images(page, passes=LYST_IMAGE_SETTLE_PASSES)
            step = "normalize_lazy_images"
            _mark_lyst_page_step("normalize_lazy_images", url, country, url_name=url_name, page_num=page_num)
            await normalize_lazy_images(page)
            step = "wait_selector"
            _mark_lyst_page_step("wait_selector", url, country, url_name=url_name, page_num=page_num)
            try:
                await page.wait_for_selector('._693owt3', timeout=20000)
            except Exception:
                # If selector wait fails, still return content for parsing
                pass
            step = "content"
            _mark_lyst_page_step("content", url, country, url_name=url_name, page_num=page_num)
            content = await page.content()
            if is_cloudflare_challenge(content):
                context_lines.append("cloudflare_detected: true")
                try:
                    title = await page.title()
                    context_lines.append(f"page_title: {title}")
                except Exception:
                    pass
                await _dump_lyst_debug_event_safe(
                    "lyst_cloudflare",
                    reason="Cloudflare challenge",
                    url=url,
                    country=country,
                    url_name=url_name,
                    page_num=page_num,
                    step=step,
                    page=page,
                    content=content,
                    debug_events=debug_events,
                    context_lines=context_lines,
                )
                mark_lyst_issue("Cloudflare challenge")
                await lyst_context_pool.reset_context(country)
                raise LystCloudflareChallenge()
            await lyst_identity_helpers.persist_context_storage_state(country, context, logger)
            return content
        except asyncio.CancelledError:
            await _dump_lyst_debug_event_safe(
                "lyst_timeout",
                reason="page timeout",
                url=url,
                country=country,
                url_name=url_name,
                page_num=page_num,
                step=step,
                page=page,
                debug_events=debug_events,
                context_lines=context_lines,
                shield=True,
            )
            try:
                await page.close()
            except Exception:
                pass
            raise
        except Exception as exc:
            if is_pipe_closed_error(exc):
                reason = "pipe closed"
            elif "Target page, context or browser has been closed" in str(exc):
                reason = "target closed"
            else:
                reason = f"exception: {exc}"
            if reason == "pipe closed":
                prefix = "lyst_pipe_closed"
            elif reason == "target closed":
                prefix = "lyst_target_closed"
            else:
                prefix = "lyst_error"
            try:
                context_lines = list(context_lines)
                context_lines.append(f"exception_type: {exc.__class__.__name__}")
                context_lines.append(f"exception_message: {exc}")
                await _dump_lyst_debug_event_safe(
                    prefix,
                    reason=reason,
                    url=url,
                    country=country,
                    url_name=url_name,
                    page_num=page_num,
                    step=step,
                    page=page,
                    debug_events=debug_events,
                    context_lines=context_lines,
                )
            except Exception:
                pass
            if is_pipe_closed_error(exc):
                await lyst_context_pool.reset_browser()
            elif is_target_closed_error(exc):
                await lyst_context_pool.reset_context(country)
            raise
        finally:
            try:
                await page.close()
            except Exception:
                pass

def _build_soup(content):
    try:
        return BeautifulSoup(content, 'lxml')
    except Exception:
        return BeautifulSoup(content, 'html.parser')

def _get_soup_request_kwargs(max_retries, max_scroll_attempts, url_name, page_num, use_pagination):
    return {
        "max_retries": max_retries,
        "max_scroll_attempts": max_scroll_attempts,
        "url_name": url_name,
        "page_num": page_num,
        "use_pagination": use_pagination,
    }

async def _get_soup_impl(
    url,
    country,
    *,
    return_content=False,
    max_retries=3,
    max_scroll_attempts=None,
    url_name=None,
    page_num=None,
    use_pagination=None,
    return_none_on_terminal_failure=False,
):
    attempt = 0
    target_closed_retry_used = False
    while attempt < max_retries:
        try:
            try:
                content = await asyncio.wait_for(
                    get_page_content(
                        url,
                        country,
                        max_scroll_attempts,
                        url_name=url_name,
                        page_num=page_num,
                        attempt=attempt + 1,
                        max_retries=max_retries,
                        use_pagination=use_pagination,
                    ),
                    timeout=LYST_PAGE_TIMEOUT_SEC,
                )
            except asyncio.TimeoutError:
                suffix = _lyst_url_suffix(url_name, page_num)
                logger.error(f"LYST timeout fetching page content for {url}{suffix}")
                mark_lyst_issue("page timeout")
                if attempt < max_retries - 1:
                    logger.info("LYST timeout: retrying with a fresh page/context")
                content = None
            if not content:
                mark_lyst_issue("Failed to get soup")
                if attempt < max_retries - 1:
                    await asyncio.sleep(5)
                    attempt += 1
                    continue
                return (None, None) if return_content else None
            soup = _build_soup(content)
            return (soup, content) if return_content else soup
        except Exception as e:
            suffix = _lyst_url_suffix(url_name, page_num)
            if isinstance(e, LystRunAborted):
                raise
            if isinstance(e, LystHttpTerminalPage):
                raise
            if isinstance(e, LystCloudflareChallenge):
                # The instance stays challenged for minutes once this starts happening,
                # so local retries inside the same run do not recover. Let the caller
                # abort the run and rely on resume state for the next scheduled cycle.
                raise
            if is_target_closed_error(e) and not target_closed_retry_used:
                target_closed_retry_used = True
                mark_lyst_issue("TargetClosedError")
                logger.warning(f"LYST TargetClosedError: retrying with a fresh page/context for {url}{suffix}")
                await asyncio.sleep(3)
                continue
            attempt += 1
            if attempt < max_retries:
                logger.warning(f"Failed to get soup (attempt {attempt}/{max_retries}). Retrying...")
                await asyncio.sleep(5)
            else:
                logger.error(f"Failed to get soup for {url}{suffix}")
                mark_lyst_issue("Failed to get soup")
                if return_none_on_terminal_failure:
                    return (None, None) if return_content else None
                raise

async def get_soup(url, country, max_retries=3, max_scroll_attempts=None, url_name=None, page_num=None, use_pagination=None):
    return await _get_soup_impl(
        url,
        country,
        return_content=False,
        return_none_on_terminal_failure=False,
        **_get_soup_request_kwargs(max_retries, max_scroll_attempts, url_name, page_num, use_pagination),
    )

async def get_soup_and_content(url, country, max_retries=3, max_scroll_attempts=None, url_name=None, page_num=None, use_pagination=None):
    return await _get_soup_impl(
        url,
        country,
        return_content=True,
        return_none_on_terminal_failure=True,
        **_get_soup_request_kwargs(max_retries, max_scroll_attempts, url_name, page_num, use_pagination),
    )

def is_lyst_domain(url):
    return 'lyst.com' in urllib.parse.urlparse(url).netloc

def _normalize_lyst_product_link(url: str | None) -> str | None:
    if not url:
        return None
    try:
        parsed = urllib.parse.urlsplit(url)
        if not parsed.scheme or not parsed.netloc:
            return url
        return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, '', ''))
    except Exception:
        return url

def extract_embedded_url(url):
    parsed = urllib.parse.urlparse(url); qs = urllib.parse.parse_qs(parsed.query)
    for p in ('URL','murl','destination','url'):
        v = qs.get(p)
        if v: return urllib.parse.unquote(v[0])
    return url

def _touch_lyst_progress(step: str | None = None, **details):
    global LYST_LAST_PROGRESS_TS, LYST_LAST_STEP_INFO
    LYST_LAST_PROGRESS_TS, info = lyst_state_helpers.touch_progress(
        step=step,
        details=details,
        kyiv_tz=KYIV_TZ,
    )
    if step is not None:
        LYST_LAST_STEP_INFO = info

def _lyst_step_snapshot():
    return lyst_state_helpers.step_snapshot(LYST_LAST_STEP_INFO)

def _format_task_stack(task):
    return lyst_state_helpers.format_task_stack(task)

def _format_tasks_snapshot(limit=10):
    return lyst_state_helpers.format_tasks_snapshot(file_hint="GroteskBotTg.py", limit=limit)

def _describe_task_wait_chain(task, max_depth=6):
    return lyst_state_helpers.describe_task_wait_chain(task, max_depth=max_depth)

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
PRICE_TRAILING_TOKEN_RE = re.compile(r'(\d[\d.,]*)\s*([^\d\s]+)')
# Canonical marker list (lower-cased) keeps token matching stable across locales.
CURRENCY_MARKERS = tuple(
    marker.lower()
    for marker in (
        "\u20ac",
        "\u00a3",
        "$",
        "EUR",
        "GBP",
        "USD",
        "UAH",
        "\u0433\u0440\u043d",
        "\u0433\u0440\u043d.",
        "uah",
    )
)

def extract_price(price_str):
    price_num = re.sub(r'[^\d.]', '', price_str)
    try: return float(price_num)
    except ValueError: return 0

def _normalize_currency_token(token: str) -> str:
    value = (token or "").replace("\xa0", "").strip()
    if not value:
        return ""
    normalized_values = [value]
    # Recover common UTF-8 -> latin1 mojibake when present.
    # This prevents silent misses like "215â‚¬" where the symbol got garbled.
    try:
        repaired = value.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
        if repaired:
            normalized_values.append(repaired)
    except Exception:
        pass
    return " ".join(normalized_values).lower()

def extract_price_tokens(text):
    if not text:
        return []
    tokens = []
    for m in PRICE_TOKEN_RE.finditer(text.replace('\xa0', ' ')):
        token = m.group(0).replace(' ', '')
        # Normalize trailing currency (e.g. "215€") to leading ("€215")
        if token and token[-1] not in '0123456789' and (token[0].isdigit() or token[0] == '.'):
            token = token[-1] + token[:-1]
        if any(marker.lower() in token.lower() for marker in CURRENCY_MARKERS):
            tokens.append(token)
    return tokens

def _extract_price_tokens_enhanced(text):
    if not text:
        return []
    tokens = []
    normalized_text = text.replace('\xa0', ' ')
    for m in PRICE_TOKEN_RE.finditer(normalized_text):
        raw_token = m.group(0).replace(' ', '')
        candidates = [raw_token]
        # Keep backward-compatible formatting for single-char trailing currency.
        if raw_token and raw_token[-1] not in '0123456789' and (raw_token[0].isdigit() or raw_token[0] == '.'):
            candidates.append(raw_token[-1] + raw_token[:-1])
        for token in candidates:
            normalized = _normalize_currency_token(token)
            if any(marker in normalized for marker in CURRENCY_MARKERS):
                tokens.append(token)
                break
    if tokens:
        return tokens

    # Second pass: run on repaired text too, so malformed encodings still produce tokens.
    fallback_inputs = [normalized_text]
    try:
        repaired = normalized_text.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
        if repaired:
            fallback_inputs.append(repaired)
    except Exception:
        pass

    seen = set()
    for source_text in fallback_inputs:
        for m in PRICE_TRAILING_TOKEN_RE.finditer(source_text):
            token = f"{m.group(1)}{m.group(2)}"
            normalized = _normalize_currency_token(token)
            if any(marker in normalized for marker in CURRENCY_MARKERS) and token not in seen:
                seen.add(token)
                tokens.append(token)
    return tokens

# Keep the old function for debugging, but route runtime calls to the resilient one.
extract_price_tokens = _extract_price_tokens_enhanced

def _parse_price_amount(raw: str) -> float:
    if not raw:
        return 0.0
    cleaned = re.sub(r'[^\d,\.]', '', raw)
    if not cleaned:
        return 0.0
    if ',' in cleaned and '.' in cleaned:
        cleaned = cleaned.replace(',', '')
    elif ',' in cleaned and '.' not in cleaned:
        parts = cleaned.split(',')
        if len(parts[-1]) == 3:
            cleaned = ''.join(parts)
        else:
            cleaned = '.'.join(parts)
    elif '.' in cleaned:
        parts = cleaned.split('.')
        if len(parts[-1]) == 3:
            cleaned = ''.join(parts)
    try:
        return float(cleaned)
    except ValueError:
        return 0.0

def _normalize_image_url(url: str | None) -> str | None:
    if not url:
        return None
    url = url.strip()
    if url.startswith("//"):
        return f"https:{url}"
    return url

def _pick_src_from_srcset(srcset_value):
    if not srcset_value:
        return None
    best_url = None
    best_score = -1.0
    for part in srcset_value.split(','):
        part = part.strip()
        if not part:
            continue
        tokens = part.split()
        url = tokens[0].strip() if tokens else ""
        score = 0.0
        if len(tokens) > 1:
            desc = tokens[1].strip().lower()
            if desc.endswith("w"):
                try:
                    score = float(desc[:-1])
                except Exception:
                    score = 0.0
            elif desc.endswith("x"):
                try:
                    score = float(desc[:-1]) * 1000.0
                except Exception:
                    score = 0.0
        if score >= best_score:
            best_score = score
            best_url = url
    best_url = _normalize_image_url(best_url)
    if best_url and best_url.startswith(("http://", "https://")):
        return best_url
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
        url = _normalize_image_url(url)
        if url and url.startswith(("http://", "https://")):
            return url
    return None

def _upgrade_lyst_image_url(url: str | None) -> str | None:
    if not url:
        return url
    url = _normalize_image_url(url)
    if not url or not url.startswith(("http://", "https://")):
        return url
    try:
        parsed = urllib.parse.urlsplit(url)
    except Exception:
        return url
    host = parsed.netloc.lower()
    if "lystit.com" not in host:
        return url
    path = parsed.path
    new_path = re.sub(r"^/\d+/\d+/(?:tr/)?photos/", "/photos/", path)
    if new_path != path:
        return parsed._replace(path=new_path).geturl()
    return url

def _image_url_candidates(url: str | None) -> list[str]:
    url = _normalize_image_url(url)
    upgraded = _upgrade_lyst_image_url(url)
    if upgraded and url and upgraded != url:
        return [upgraded, url]
    return [url] if url else []

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
            image_url = _upgrade_lyst_image_url(image_url)
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
        if full_name and "view all" in full_name.strip().lower():
            return None
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
        track_href = None
        product_href = None
        for a in card.find_all('a', href=True):
            href = a.get('href') or ''
            if not track_href and '/track/lead/' in href:
                track_href = href
            if not product_href and any(p in href for p in ['/clothing/', '/shoes/', '/accessories/', '/bags/', '/jewelry/']):
                product_href = href
            if track_href and product_href:
                break
        href = track_href or product_href
        if not href:
            link_elem = card.find('a', href=True)
            href = link_elem['href'] if link_elem and 'href' in link_elem.attrs else None
        full_url = f"https://www.lyst.com{href}" if href and href.startswith('/') else href if href and href.startswith('http') else None
        product_url = None
        if product_href:
            product_url = f"https://www.lyst.com{product_href}" if product_href.startswith('/') else product_href
        canonical_for_id = _normalize_lyst_product_link(product_url or full_url)
        if not unique_id and canonical_for_id:
            unique_id = str(uuid.uuid5(uuid.NAMESPACE_URL, canonical_for_id))

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
        image_url = _upgrade_lyst_image_url(image_url)
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

async def scrape_page(url, country, max_scroll_attempts=None, url_name=None, page_num=None, use_pagination=None):
    try:
        soup, content = await get_soup_and_content(
            url,
            country,
            max_scroll_attempts=max_scroll_attempts,
            url_name=url_name,
            page_num=page_num,
            use_pagination=use_pagination,
        )
    except LystCloudflareChallenge:
        return [], None, "cloudflare"
    except LystRunAborted:
        return [], None, "aborted"
    except LystHttpTerminalPage as exc:
        return [], exc.content, "terminal"
    if not soup:
        mark_lyst_issue("Failed to get soup")
        return [], content, "failed"
    
    shoe_cards = soup.find_all('div', class_='_693owt3')
    image_fallback_map = extract_ldjson_image_map(soup)
    return [data for card in shoe_cards if (data := extract_shoe_data(card, country, image_fallback_map))], content, "ok"

async def scrape_all_pages(base_url, country, use_pagination=None):
    if use_pagination is None:
        use_pagination = PAGE_SCRAPE
    
    max_scroll_attempts = LYST_MAX_SCROLL_ATTEMPTS
    all_shoes = []
    url_name = base_url['url_name']
    key = _resume_key(base_url, country)
    resume_active = LYST_RESUME_STATE.get("resume_active", False)
    entry = LYST_RESUME_STATE.get("entries", {}).get(key, {})
    if resume_active and entry.get("completed"):
        logger.info(f"Skipping {url_name} for {country} (completed in previous run)")
        return all_shoes
    page = entry.get("next_page", 1) if resume_active else 1
    started_from_resume_page = bool(resume_active and use_pagination and page > 1)
    last_scraped_page = entry.get("last_scraped_page", entry.get("last_success_page", 0))
    if use_pagination and page > 1:
        logger.info(f"Resuming {url_name} for {country} from page {page}")
    
    while True:
        if LYST_ABORT_EVENT.is_set():
            break
        _touch_lyst_progress(
            "scrape_page_start",
            url_name=url_name,
            country=country,
            page_num=page,
        )
        url = _scrape_target_url(base_url, page, use_pagination)
        _log_scrape_target(url_name, country, page, use_pagination)

        shoes, content, status = await scrape_page(
            url,
            country,
            max_scroll_attempts=max_scroll_attempts,
            url_name=url_name,
            page_num=page if use_pagination else None,
            use_pagination=use_pagination,
        )
        _touch_lyst_progress(
            "scrape_page_end",
            url=url,
            url_name=url_name,
            country=country,
            page_num=page,
            status=status,
        )
        if status == "cloudflare":
            LYST_RESUME_ENTRY_OUTCOMES[key] = "cloudflare"
            logger.error(f"Cloudflare challenge for {url_name} {country} page {page}")
            # Status tracking must record this as a failed run; otherwise the bot can
            # show a green last-run marker even though resume state says we aborted.
            mark_lyst_issue("Cloudflare challenge")
            await _update_resume_with_url(
                key,
                url,
                next_page=page,
                last_scraped_page=last_scraped_page,
                completed=False,
                failure_reason="Cloudflare challenge",
            )
            await mark_lyst_run_failed("Cloudflare challenge")
            log_lyst_run_progress_summary()
            break
        if status == "aborted":
            LYST_RESUME_ENTRY_OUTCOMES[key] = "aborted"
            logger.info(f"Aborting {url_name} for {country} after Lyst run abort signal")
            break
        if status == "failed":
            LYST_RESUME_ENTRY_OUTCOMES[key] = "failed"
            logger.error(f"Failed to fetch page for {url_name} {country} page {page}")
            await _update_resume_with_url(
                key,
                url,
                next_page=page,
                last_scraped_page=last_scraped_page,
                completed=False,
                failure_reason="Failed to get soup",
            )
            await mark_lyst_run_failed("Failed to get soup")
            log_lyst_run_progress_summary()
            break
        if status == "terminal":
            logger.info(f"{url_name} for {country} reached terminal page {page} (HTTP 410)")
            if started_from_resume_page and not all_shoes:
                LYST_RESUME_ENTRY_OUTCOMES[key] = "terminal_only_resume"
            else:
                LYST_RESUME_ENTRY_OUTCOMES[key] = "terminal"
            await _update_resume_with_url(
                key,
                url,
                scrape_complete=True,
                final_page=last_scraped_page,
                completed=False,
            )
            break
        if not shoes:
            LYST_RESUME_ENTRY_OUTCOMES[key] = "empty"
            if use_pagination and page < 3:
                logger.error(f"{url_name} for {country} Stopped too early. Please check for errors")
                mark_lyst_issue("Stopped too early")
                now_kyiv = datetime.now(KYIV_TZ).strftime('%Y-%m-%d %H:%M:%S')
                log_lines = tail_log_lines(BOT_LOG_FILE, line_count=200)
                context_lines = build_lyst_context_lines(
                    max_scroll_attempts=max_scroll_attempts,
                    use_pagination=use_pagination,
                )
                write_stop_too_early_dump(
                    reason="Stopped too early",
                    url=url,
                    country=country,
                    url_name=url_name,
                    page_num=page,
                    content=content,
                    now_kyiv=now_kyiv,
                    log_lines=log_lines,
                    context_lines=context_lines,
                )
                if use_pagination == PAGE_SCRAPE:
                    logger.info(f"Retrying {url_name} for {country} with PAGE_SCRAPE={not use_pagination}")
                    return await scrape_all_pages(base_url, country, use_pagination=not use_pagination)
            
            logger.info(f"Total for {country} {url_name}: {len(all_shoes)}. Stopped on page {page}")
            await _update_resume_with_url(
                key,
                url,
                scrape_complete=True,
                final_page=last_scraped_page,
                completed=False,
            )
            break
        all_shoes.extend(shoes)
        LYST_RESUME_ENTRY_OUTCOMES[key] = "scraped"
        LYST_RUN_PROGRESS[key] = page
        last_scraped_page = page
        await _update_resume_with_url(
            key,
            url,
            last_scraped_page=page,
            completed=False if use_pagination else True,
        )
        
        if not use_pagination:
            await _update_resume_with_url(
                key,
                url,
                last_scraped_page=page,
                scrape_complete=True,
                final_page=page,
                completed=False,
            )
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
    cached_rates = None
    try:
        with open(EXCHANGE_RATES_FILE, 'r') as f:
            data = json.load(f)
        cached_rates = data.get('rates')
        last_update = data.get('last_update')
        is_fresh = bool(last_update) and (datetime.now() - datetime.fromisoformat(last_update)).days < 1
        if is_fresh and cached_rates:
            return cached_rates
    except (FileNotFoundError, json.JSONDecodeError, KeyError, ValueError):
        cached_rates = None
    updated = update_exchange_rates()
    if updated:
        return updated
    if cached_rates:
        logger.warning("Using cached exchange rates due to update failure")
        return cached_rates
    return {'EUR': 1, 'USD': 1, 'GBP': 1}


def update_exchange_rates():
    try:
        resp = requests.get(
            f"https://v6.exchangerate-api.com/v6/{EXCHANGERATE_API_KEY}/latest/UAH",
            timeout=15,
        )
        resp.raise_for_status()
        payload = resp.json()
        rates = {k: payload['conversion_rates'][k] for k in ('EUR', 'USD', 'GBP')}
        with open(EXCHANGE_RATES_FILE, 'w') as f:
            json.dump({'last_update': datetime.now().isoformat(), 'rates': rates}, f)
        return rates
    except Exception as e:
        logger.error(f"Error updating exchange rates: {e}")
        return None

def convert_to_uah(price, country, exchange_rates, name):
    try:
        currency = None
        currency_symbol = ''
        for symbol, code in CURRENCY_CODE_BY_SYMBOL.items():
            if symbol in price:
                currency = code
                currency_symbol = CURRENCY_DISPLAY_SYMBOL_BY_SYMBOL.get(symbol, symbol)
                break
        if not currency:
            logger.error(f"Unrecognized currency symbol in price '{price}' for '{name}' country '{country}'")
            return EMPTY_CONVERSION_RESULT
        amount = _parse_price_amount(price)
        if amount <= 0:
            logger.error(f"Failed to parse price '{price}' for '{name}' country '{country}'")
            return ConversionResult(0, 0, currency_symbol)

        rate = exchange_rates.get(currency)
        if not rate:
            logger.error(f"Exchange rate not found for currency '{currency}' (country: {country})")
            return EMPTY_CONVERSION_RESULT

        uah_amount = amount / rate
        return ConversionResult(round(uah_amount / 10) * 10, round(1 / rate, 2), currency_symbol)
    except (ValueError, KeyError) as e:
        logger.error(f"Error converting price '{price}' for '{name}' country '{country}': {e}")
        return EMPTY_CONVERSION_RESULT

# Message formatting and sending
def get_sale_emoji(sale_percentage, uah_sale):
    if sale_percentage >= SALE_EMOJI_ROCKET_THRESHOLD: return "🚀🚀🚀"
    if uah_sale < SALE_EMOJI_UAH_THRESHOLD: return "🐚🐚🐚"
    return "🍄🍄🍄"

def build_shoe_message(shoe, sale_percentage, uah_sale, kurs, kurs_symbol, old_sale_price=None, status=None):
    # Telegram uses HTML parsing for captions/messages below.
    # Always escape dynamic site data to prevent malformed HTML and send failures.
    def _esc(value):
        return html.escape(str(value if value is not None else ""), quote=True)

    name = _esc(shoe.get('name'))
    original_price = _esc(shoe.get('original_price'))
    sale_price = _esc(shoe.get('sale_price'))
    lowest_price = _esc(shoe.get('lowest_price'))
    store = _esc(shoe.get('store'))
    country = _esc(shoe.get('country'))
    kurs_symbol_safe = _esc(kurs_symbol)
    old_sale_price_safe = _esc(old_sale_price)
    shoe_link = _esc(shoe.get('shoe_link'))
    store_line = f"🔗 Store : <a href='{shoe_link}'>{store}</a>" if shoe_link else f"🔗 Store : {store}"

    if status is None:  # New item
        sale_emoji = get_sale_emoji(sale_percentage, uah_sale)
        return (
            f"{sale_emoji}  New item  {sale_emoji}\n{name}\n\n"
            f"💀 Prices : <s>{original_price}</s>  <b>{sale_price}</b>  <i>(Sale: <b>{sale_percentage}%</b>)</i>\n"
            f"🤑 Grivniki : <b>{uah_sale} UAH </b>\n"
            f"🧊 Kurs : {kurs_symbol_safe} {kurs} \n"
            f"{store_line}\n"
            f"🌍 Country : {country}"
        )
    return (
        f"💎💎💎 {_esc(status)} 💎💎💎 \n{name}:\n\n"
        f"💀 Prices : <s>{original_price}</s>  <s>{old_sale_price_safe}</s>  <b>{sale_price}</b>  <i>(Sale: <b>{sale_percentage}%</b>)</i> \n"
        f"🤑 Grivniki : {uah_sale} UAH\n"
        f"📉 Lowest price : {lowest_price} ({shoe['lowest_price_uah']} UAH)\n"
        f"🧊 Kurs : {kurs_symbol_safe} {kurs} \n"
        f"{store_line}\n"
        f"🌍 Country : {country}"
    )

async def send_telegram_message(bot_token, chat_id, message, image_url=None, uah_price=None, sale_percentage=None, max_retries=3):
    return await telegram_runtime_helpers.send_telegram_message(
        bot_token,
        chat_id,
        message,
        image_url=image_url,
        uah_price=uah_price,
        sale_percentage=sale_percentage,
        max_retries=max_retries,
        process_image_func=process_image,
        upgrade_image_url_func=_upgrade_lyst_image_url,
        logger=logger,
    )

def get_allowed_chat_ids():
    return telegram_runtime_helpers.get_allowed_chat_ids(DANYLO_DEFAULT_CHAT_ID, TELEGRAM_CHAT_ID)

def tail_log_lines(path, line_count=LOG_TAIL_LINES):
    return telegram_runtime_helpers.tail_log_lines(path, line_count=line_count, logger=logger)

async def send_log_tail(bot, chat_id, log_path, line_count=LOG_TAIL_LINES):
    await telegram_runtime_helpers.send_log_tail(
        bot,
        chat_id,
        log_path,
        line_count=line_count,
        logger=logger,
    )

async def command_listener(bot_token, allowed_chat_ids, log_path):
    await telegram_runtime_helpers.command_listener(
        bot_token,
        allowed_chat_ids,
        log_path,
        line_count=LOG_TAIL_LINES,
        add_dynamic_url_func=add_dynamic_url,
        allow_log_commands=True,
        allow_add_commands=True,
        allow_unsubscribe_commands=False,
        logger=logger,
    )

# Processing functions
def _merge_base_url_into_shoes(result, base_url, country):
    merged = []
    for shoe in result:
        if isinstance(shoe, dict):
            shoe['base_url'] = base_url
            merged.append(shoe)
        else:
            logger.error(f"Unexpected item data type for {country}: {type(shoe)}")
    return merged

def filter_duplicates(shoes, exchange_rates):
    filtered_shoes, grouped_shoes = [], defaultdict(list)
    for shoe in shoes:
        grouped_shoes[_shoe_key(shoe)].append(shoe)

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

def _shoe_key(shoe):
    return f"{shoe['name']}_{shoe['unique_id']}"

def _apply_new_shoe_state(shoe, uah_sale):
    shoe.update({
        'lowest_price': shoe['sale_price'],
        'lowest_price_uah': uah_sale,
        'uah_price': uah_sale,
        'active': True
    })

def _apply_existing_shoe_state(shoe, old_shoe, uah_sale, exchange_rates):
    old_sale_price = old_shoe['sale_price']
    old_sale_country = old_shoe['country']
    old_uah = old_shoe.get('uah_price') or convert_to_uah(old_sale_price, old_sale_country, exchange_rates, shoe['name']).uah_amount
    lowest_price_uah = old_shoe.get('lowest_price_uah') or old_uah

    shoe['uah_price'] = uah_sale
    # Update lowest price if needed
    if uah_sale < lowest_price_uah:
        shoe['lowest_price'], shoe['lowest_price_uah'] = shoe['sale_price'], uah_sale
    else:
        shoe['lowest_price'], shoe['lowest_price_uah'] = old_shoe['lowest_price'], lowest_price_uah
    shoe['active'] = True

async def _save_single_shoe(key, shoe):
    # Save individual shoe instead of entire dataset
    await save_shoe_data_bulk([dict(shoe, key=key)])

async def process_shoe(shoe, old_data, message_queue, exchange_rates):
    key = _shoe_key(shoe)
    is_new_item = key not in old_data
    was_processed = await is_shoe_processed(key) if is_new_item else False

    # Calculate sale details
    sale_percentage = calculate_sale_percentage(shoe['original_price'], shoe['sale_price'], shoe['country'])
    sale_exchange_data = convert_to_uah(shoe['sale_price'], shoe['country'], exchange_rates, shoe['name'])
    kurs, uah_sale, kurs_symbol = sale_exchange_data.exchange_rate, sale_exchange_data.uah_amount, sale_exchange_data.currency_symbol

    # Handle new shoe
    if is_new_item:
        _apply_new_shoe_state(shoe, uah_sale)
        # Important: do NOT globally skip this function based on processed_shoes.
        # That old behavior prevented future state updates for existing items.
        # We only use processed_shoes to suppress duplicate "new item" notifications.
        if not was_processed:
            message = build_shoe_message(shoe, sale_percentage, uah_sale, kurs, kurs_symbol)
            await message_queue.add_message(shoe['base_url']['telegram_chat_id'], message, shoe['image_url'], uah_sale, sale_percentage)
            await mark_shoe_processed(key)
        old_data[key] = shoe
        await _save_single_shoe(key, shoe)
    else:
        # Update existing shoe
        old_shoe = old_data[key]
        _apply_existing_shoe_state(shoe, old_shoe, uah_sale, exchange_rates)
        old_data[key] = shoe
        await _save_single_shoe(key, shoe)

async def process_all_shoes(all_shoes, old_data, message_queue, exchange_rates):
    new_shoe_count = 0
    semaphore = asyncio.Semaphore(LYST_SHOE_CONCURRENCY)  # Reduce concurrency to prevent database locks
    total_items = len(all_shoes)
    _touch_lyst_progress("process_shoes_start", total_items=total_items)

    async def process_single_shoe(i, shoe):
        nonlocal new_shoe_count
        async with semaphore:  # Limit concurrency
            try:
                _touch_lyst_progress("process_shoe", index=i, total_items=total_items)
                country, name = shoe['country'], shoe['name']
                key = _shoe_key(shoe)
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
        _touch_lyst_progress("process_shoes_batch", batch_start=i, batch_size=len(batch))
        # Small delay between batches to prevent overwhelming the database
        await asyncio.sleep(0.1)
    
    logger.info(f"Processed {new_shoe_count} new shoes in total")

    # Only deactivate missing items after a full successful Lyst run. On partial runs
    # (Cloudflare aborts, timeouts, stalls), the scraper has not seen the untouched
    # pages yet, so treating them as removed would corrupt active/inactive state.
    removed_shoes = []
    if not LYST_RUN_FAILED:
        current_shoes = {_shoe_key(shoe) for shoe in all_shoes}
        removed_shoes = [dict(shoe, key=k, active=False) for k, shoe in old_data.items() if k not in current_shoes and shoe.get('active', True)]
        for s in removed_shoes:
            old_data[s['key']]['active'] = False
        if removed_shoes:
            logger.info(f"Marking {len(removed_shoes)} removed shoes inactive")
            chunk_size = 500
            for i in range(0, len(removed_shoes), chunk_size):
                chunk = removed_shoes[i:i + chunk_size]
                await save_shoe_data_bulk(chunk)
                _touch_lyst_progress("removed_shoes_batch", batch_start=i, batch_size=len(chunk), total_removed=len(removed_shoes))
                await asyncio.sleep(0)
    _touch_lyst_progress("process_shoes_done", removed_total=len(removed_shoes), new_total=new_shoe_count)

def _scrape_target_url(base_url, page, use_pagination):
    return base_url['url'] if not use_pagination or page == 1 else f"{base_url['url']}&page={page}"

def _log_scrape_target(url_name, country, page, use_pagination):
    if use_pagination:
        logger.info(f"Scraping page {page} for country {country} - {url_name}")
    else:
        logger.info(f"Scraping single page for country {country} - {url_name}")

async def _update_resume_with_url(key, url, **fields):
    await update_lyst_resume_entry(key, last_url=url, **fields)

async def process_url(base_url, countries, exchange_rates):
    _touch_lyst_progress()
    mark_lyst_start()
    all_shoes = []
    country_results = await asyncio.gather(*(scrape_all_pages(base_url, c) for c in countries))
    for country, result in zip(countries, country_results):
        all_shoes.extend(_merge_base_url_into_shoes(result, base_url, country))
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

def _collect_successful_lyst_results(url_results):
    all_shoes = []
    for result in url_results:
        if isinstance(result, Exception):
            logger.error(f"Lyst task failed: {result}")
            continue
        all_shoes.extend(result)
    return all_shoes


def _should_restart_after_terminal_resume(all_shoes):
    if all_shoes or not LYST_CYCLE_STARTED_IN_RESUME:
        return False
    if not LYST_RESUME_ENTRY_OUTCOMES:
        return False
    return all(outcome == "terminal_only_resume" for outcome in LYST_RESUME_ENTRY_OUTCOMES.values())

async def _clear_lyst_resume_state():
    async with LYST_RESUME_LOCK:
        LYST_RESUME_STATE["resume_active"] = False
        LYST_RESUME_STATE["entries"] = {}
        for key in ("last_run_progress", "last_failure_reason", "last_failure_at"):
            LYST_RESUME_STATE.pop(key, None)
        save_lyst_resume_state(LYST_RESUME_STATE)

async def _run_lyst_resume_step(progress_name, operation, timeout_issue, timeout_log):
    try:
        _touch_lyst_progress(f"{progress_name}_start")
        await asyncio.wait_for(operation(), timeout=60)
        _touch_lyst_progress(f"{progress_name}_done")
    except asyncio.TimeoutError:
        mark_lyst_issue(timeout_issue)
        logger.error(timeout_log)

async def _finalize_lyst_resume_state():
    await _run_lyst_resume_step(
        "finalize_resume",
        finalize_lyst_resume_after_processing,
        "resume finalize timeout",
        "LYST finalize resume timed out; continuing without resume update",
    )
    if LYST_RUN_FAILED:
        return
    await _run_lyst_resume_step(
        "finalize_clear",
        _clear_lyst_resume_state,
        "resume clear timeout",
        "LYST resume clear timed out; continuing",
    )

async def _shutdown_background_tasks(tasks):
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

def _log_lyst_collection_stats(all_shoes, exchange_rates):
    if not all_shoes and not LYST_RUN_FAILED:
        mark_lyst_issue("0 items scraped")

    collected_ids = {shoe['unique_id'] for shoe in all_shoes}
    recovered_count = sum(1 for uid in SKIPPED_ITEMS if uid in collected_ids)
    special_logger.stat(f"Items skipped due to image but present in final list: {recovered_count}/{len(SKIPPED_ITEMS)}")

    unfiltered_len = len(all_shoes)
    all_shoes = filter_duplicates(all_shoes, exchange_rates)
    special_logger.stat(f"Removed {unfiltered_len - len(all_shoes)} duplicates")
    return all_shoes

def _finalize_lyst_cycle(issue=None, error=None):
    if error is not None:
        logger.error(f"Lyst run failed: {error}")
    if issue is not None:
        mark_lyst_issue(issue)
    finalize_lyst_run()

async def run_lyst_cycle_impl(message_queue):
    global LYST_LAST_PROGRESS_TS, LYST_ACTIVE_TASK, LYST_CYCLE_STARTED_IN_RESUME, LYST_RESUME_ENTRY_OUTCOMES
    LYST_ACTIVE_TASK = asyncio.current_task()
    init_lyst_resume_state()
    SKIPPED_ITEMS.clear()
    reset_lyst_http_only_state()
    begin_lyst_cycle()
    _touch_lyst_progress("run_start")
    try:
        old_data = await load_shoe_data()
        exchange_rates = load_exchange_rates()

        async def _run_lyst_url_batch():
            url_tasks = [
                asyncio.wait_for(process_url(base_url, COUNTRIES, exchange_rates), timeout=LYST_URL_TIMEOUT_SEC)
                for base_url in BASE_URLS
            ]
            url_results = await asyncio.gather(*url_tasks, return_exceptions=True)
            return _collect_successful_lyst_results(url_results)

        all_shoes = await _run_lyst_url_batch()
        # When a stale resume state points only at already-finished pages, every URL
        # can cleanly return HTTP 410 with 0 items. That is resume cleanup, not a real
        # empty-catalog run. Clear resume state and immediately rerun once from page 1
        # before any "0 items" status or inactive-item marking can fire.
        if _should_restart_after_terminal_resume(all_shoes):
            logger.warning(
                "LYST resume pass reached only terminal pages with 0 items; "
                "clearing resume state and restarting once from page 1"
            )
            await _clear_lyst_resume_state()
            LYST_RUN_PROGRESS.clear()
            LYST_CYCLE_STARTED_IN_RESUME = False
            LYST_RESUME_ENTRY_OUTCOMES = {}
            all_shoes = await _run_lyst_url_batch()

        all_shoes = _log_lyst_collection_stats(all_shoes, exchange_rates)

        # Even when a later page aborts the run, keep already-scraped items. That is
        # the whole point of the fail-fast Cloudflare flow: stop new requests, but do
        # not throw away data we already paid to scrape successfully.
        await process_all_shoes(all_shoes, old_data, message_queue, exchange_rates)
        await _finalize_lyst_resume_state()
        _touch_lyst_progress("finalize_run")
        _finalize_lyst_cycle()
        logger.info("LYST run completed")

        print_statistics()
        print_link_statistics()
    except asyncio.CancelledError:
        _finalize_lyst_cycle(issue="stalled")
        raise
    except Exception as exc:
        _finalize_lyst_cycle(issue="failed", error=exc)
        raise
    finally:
        LYST_ACTIVE_TASK = None



def _run_db_retention_cleanup(conn, db_path):
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

def _db_maintenance_sync(db_files=None):
    try:
        run_runtime_db_maintenance(
            db_files=db_files or [SHOES_DB_FILE, OLX_DB_FILE, SHAFA_DB_FILE],
            vacuum=DB_VACUUM,
            retention_callback=_run_db_retention_cleanup,
        )
    except Exception as exc:
        logger.warning(f"DB maintenance failed: {exc}")

async def maintenance_loop(interval_s: int, *, service_health=None):
    if interval_s <= 0:
        return
    while True:
        started = time.perf_counter()
        try:
            # Serialize shoes.db maintenance with async writes to avoid lock contention.
            async with DB_SEMAPHORE:
                await asyncio.to_thread(_db_maintenance_sync, [SHOES_DB_FILE])
        except Exception as exc:
            if service_health is not None:
                service_health.record_failure("db_maintenance", exc, duration_seconds=time.perf_counter() - started)
            logger.warning(f"DB maintenance iteration failed: {exc}")
        else:
            if service_health is not None:
                service_health.record_success(
                    "db_maintenance",
                    duration_seconds=time.perf_counter() - started,
                    note=SHOES_DB_FILE.name,
                )
        await asyncio.sleep(interval_s)

# Main application
async def main(service_health=None):
    global LIVE_MODE
    load_last_runs_from_file()
    background_tasks = []
    if service_health is not None:
        service_health.start()
        service_health.mark_ready("lyst service starting")

    def _start_background_task(coro, task_name):
        # Keep handles so we can cancel/await gracefully during shutdown.
        task = asyncio.create_task(coro, name=task_name)
        background_tasks.append(task)
        return task

    if service_health is not None:
        _start_background_task(service_health.heartbeat_loop(note="lyst service running"), "lyst_health_heartbeat")

    # Initialize and start message queue
    message_queue = TelegramMessageQueue(TELEGRAM_BOT_TOKEN)
    _start_background_task(message_queue.process_queue(), "tg_message_queue")
    _start_background_task(
        command_listener(TELEGRAM_BOT_TOKEN, get_allowed_chat_ids(), BOT_LOG_FILE),
        "tg_command_listener",
    )
    try:
        chat_id = int((DANYLO_DEFAULT_CHAT_ID or "").strip())
    except Exception:
        chat_id = None
    if chat_id:
        lyst_stale_after_sec = max(0, CHECK_INTERVAL_SEC + CHECK_JITTER_SEC + 600)
        _start_background_task(
            status_heartbeat(
                TELEGRAM_BOT_TOKEN,
                chat_id,
                interval_s=600,
                lyst_stale_after_sec=lyst_stale_after_sec,
            ),
            "status_heartbeat",
        )
    if MAINTENANCE_INTERVAL_SEC > 0:
        _start_background_task(
            maintenance_loop(MAINTENANCE_INTERVAL_SEC, service_health=service_health),
            "db_maintenance",
        )
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

    try:
        async def _on_lyst_stall(lyst_task=None):
            step_info = _lyst_step_snapshot()
            finalize_hang = bool(step_info and step_info.get("step") == "finalize_run")
            if finalize_hang:
                logger.error("Lyst stalled after finalize_run; treating as post-finalize hang")
            else:
                mark_lyst_issue("stalled")
                try:
                    await mark_lyst_run_failed("stalled")
                except Exception as exc:
                    logger.warning(f"Failed to persist Lyst stall state: {exc}")
            if step_info:
                logger.error(f"Lyst last step before stall: {step_info}")
            try:
                stats = message_queue.stats()
                logger.error(f"Lyst message queue stats at stall: {stats}")
            except Exception:
                pass
            task = lyst_task or LYST_ACTIVE_TASK
            if task is not None:
                try:
                    coro = task.get_coro()
                    coro_name = getattr(coro, "__name__", repr(coro))
                except Exception:
                    coro_name = "unknown"
                logger.error(
                    f"Lyst task state at stall: id={id(task)} done={task.done()} cancelled={task.cancelled()} coro={coro_name}"
                )
            stack_lines = _format_task_stack(task)
            if stack_lines:
                logger.error("Lyst task stack at stall:")
                for line in stack_lines:
                    logger.error(f"  {line}")
            wait_chain = _describe_task_wait_chain(task)
            if wait_chain:
                logger.error("Lyst await chain at stall:")
                for line in wait_chain:
                    logger.error(line)
            else:
                snapshot = _format_tasks_snapshot()
                if snapshot:
                    logger.error("Lyst-related task snapshot at stall:")
                    for line in snapshot:
                        logger.error(line)
            log_lyst_run_progress_summary()
            if not finalize_hang:
                finalize_lyst_run()
            if service_health is not None:
                service_health.record_failure("lyst_run", "stalled")

        async def _run_lyst_and_track():
            started = time.perf_counter()
            try:
                result = await run_lyst_cycle_impl(message_queue)
            except Exception as exc:
                if service_health is not None:
                    service_health.record_failure("lyst_run", exc, duration_seconds=time.perf_counter() - started)
                raise
            if service_health is not None:
                service_health.record_success("lyst_run", duration_seconds=time.perf_counter() - started)
            return result

        await run_lyst_scheduler(
            run_lyst=_run_lyst_and_track,
            is_running_lyst=lambda: IS_RUNNING_LYST,
            get_lyst_progress_ts=lambda: LYST_LAST_PROGRESS_TS,
            check_interval_sec=CHECK_INTERVAL_SEC,
            check_jitter_sec=CHECK_JITTER_SEC,
            logger=logger,
            on_lyst_stall=_on_lyst_stall,
            lyst_stall_timeout_sec=LYST_STALL_TIMEOUT_SEC,
        )
    finally:
        if service_health is not None:
            service_health.mark_stopping("lyst service stopping")
        await _shutdown_background_tasks(background_tasks)

if __name__ == "__main__":
    if IS_RUNNING_LYST:
        create_tables()  # Create tables at startup instead of creating them just before using
    asyncio.run(main())

