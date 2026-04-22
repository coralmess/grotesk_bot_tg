import json, time, asyncio, logging, colorama, subprocess, shutil, traceback, urllib.parse, re, html, io, uuid, threading, hashlib, os, random
from pathlib import Path
from telegram.constants import ParseMode
from collections import defaultdict, deque
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
from telegram import Bot
from telegram.error import RetryAfter, TimedOut
from GroteskBotStatus import (
    status_heartbeat,
    load_last_runs_from_file,
    write_lyst_status,
)
from helpers.dynamic_sources import add_dynamic_url, detect_source
from helpers import lyst_identity as lyst_identity_helpers
from helpers import telegram_runtime as telegram_runtime_helpers
from helpers import lyst_state as lyst_state_helpers
from helpers.lyst import diagnostics as lyst_diagnostics_helpers
from helpers.lyst import fetch as lyst_fetch_helpers
from helpers.lyst import media as lyst_media_helpers
from helpers.lyst import notify as lyst_notify_helpers
from helpers.lyst import cycle as lyst_cycle_helpers
from helpers.lyst import parsing as lyst_parsing_helpers
from helpers.lyst import pricing as lyst_pricing_helpers
from helpers.lyst import processing as lyst_processing_helpers
from helpers.lyst.http_client import AsyncLystHttpClient
from helpers.lyst.models import FetchStatus
from helpers.lyst.status import LystStatusManager
from helpers.lyst.browser import (
    BrowserPool,
    LystContextPool,
    create_country_context as lyst_create_country_context,
    launch_browser as lyst_launch_browser,
)
from helpers.lyst.resume import LystResumeController
from helpers.lyst.storage import LystStorage
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
from helpers.runtime_paths import (
    PYTHON_LOG_FILE,
    SHOES_DB_FILE as RUNTIME_SHOES_DB_FILE,
    OLX_ITEMS_DB_FILE as RUNTIME_OLX_DB_FILE,
    SHAFA_ITEMS_DB_FILE as RUNTIME_SHAFA_DB_FILE,
    LYST_RESUME_JSON_FILE,
    SHOE_DATA_JSON_FILE,
    EXCHANGE_RATES_JSON_FILE,
)
from helpers.sqlite_runtime import run_runtime_db_maintenance
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
# These two globals distinguish a real empty run from a stale resume pass that only
# lands on terminal 410 pages. That case should clear resume state and rerun once
# from page 1, not mark the catalog empty or deactivate old items.
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

LYST_HTTP_SEMAPHORE = asyncio.Semaphore(LYST_HTTP_CONCURRENCY)
# The async HTTP client is shared for the whole service lifetime so Lyst keeps one
# transport boundary instead of recreating blocking request sessions per page fetch.
LYST_HTTP_CLIENT = None
LYST_STATUS_MANAGER = None

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

# The Lyst service still owns the overall scrape control flow, but shoe-state
# persistence now lives behind one storage adapter instead of raw sqlite helpers.
lyst_storage = LystStorage(
    db_name=DB_NAME,
    shoe_data_file=SHOE_DATA_FILE,
    logger=logger,
)

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

async def _launch_browser(browser_type):
    return await lyst_launch_browser(
        browser_type,
        live_mode=LIVE_MODE,
        browser_launch_args=BROWSER_LAUNCH_ARGS,
    )

async def _create_lyst_country_context(browser, country):
    return await lyst_create_country_context(
        browser,
        country,
        storage_state_path=lyst_identity_helpers.country_storage_state_path(country),
        stealth_user_agent=STEALTH_UA,
        stealth_headers=STEALTH_HEADERS,
        stealth_script=STEALTH_SCRIPT,
        persist_storage_state=lambda c, ctx: lyst_identity_helpers.persist_context_storage_state(c, ctx, logger),
    )


# Browser lifecycle moved behind helpers/lyst/browser.py so the service entrypoint
# keeps dependency wiring and not the Playwright pooling implementation.
browser_pool = BrowserPool(
    max_browsers=LYST_MAX_BROWSERS,
    launch_browser=_launch_browser,
)
lyst_context_pool = LystContextPool(
    launch_browser=_launch_browser,
    create_country_context=_create_lyst_country_context,
    country_concurrency=LYST_COUNTRY_CONCURRENCY,
)

# Compiled once because link cleanup runs for many outgoing messages.
DISPLAY_LINK_PREFIX_RE = re.compile(r'^(https?://)?(www\.)?', re.IGNORECASE)

def clean_link_for_display(link):
    cleaned_link = DISPLAY_LINK_PREFIX_RE.sub('', link or "")
    return (cleaned_link[:22] + '...') if len(cleaned_link) > 25 else cleaned_link

resume_controller = LystResumeController(
    resume_file=LYST_RESUME_FILE,
    kyiv_tz=KYIV_TZ,
    logger=logger,
    abort_event=LYST_ABORT_EVENT,
    resume_lock=LYST_RESUME_LOCK,
)


def _get_lyst_status_manager() -> LystStatusManager | None:
    return LYST_STATUS_MANAGER


def _mark_lyst_issue(note: str) -> None:
    manager = _get_lyst_status_manager()
    if manager is not None and note:
        manager.mark_issue(note)


def _resume_key(base_url, country):
    return resume_controller.resume_key(base_url, country)

def _now_kyiv_str():
    return resume_controller.now_kyiv_str()

def _write_json_atomic(path: Path, payload):
    # Delegated to shared helper to keep one atomic-write implementation across bot modules.
    lyst_state_helpers._write_json_atomic(path, payload)

def load_lyst_resume_state():
    global LYST_RESUME_STATE
    LYST_RESUME_STATE = resume_controller.load_state()
    return LYST_RESUME_STATE

def save_lyst_resume_state(state):
    global LYST_RESUME_STATE
    LYST_RESUME_STATE = state
    resume_controller.state = state
    resume_controller.save_state()

async def update_lyst_resume_entry(key, **fields):
    resume_controller.state = LYST_RESUME_STATE
    await resume_controller.update_entry(key, **fields)

def init_lyst_resume_state():
    global LYST_RESUME_STATE, LYST_RUN_FAILED, LYST_RUN_PROGRESS, LYST_CYCLE_STARTED_IN_RESUME, LYST_RESUME_ENTRY_OUTCOMES
    loaded_state = load_lyst_resume_state()
    resume_controller.state = loaded_state
    # The controller now owns resume-state rules so GroteskBotTg only coordinates
    # run-level globals while the package owns persistence transitions.
    LYST_RESUME_STATE = resume_controller.init_run(loaded_state)
    LYST_RUN_FAILED = False
    LYST_RUN_PROGRESS = {}
    LYST_CYCLE_STARTED_IN_RESUME = bool(LYST_RESUME_STATE.get("resume_active", False))
    LYST_RESUME_ENTRY_OUTCOMES = {}

async def mark_lyst_run_failed(reason: str):
    global LYST_RUN_FAILED
    resume_controller.state = LYST_RESUME_STATE
    LYST_RUN_FAILED = await resume_controller.mark_run_failed(reason, LYST_RUN_PROGRESS)

def log_lyst_run_progress_summary():
    resume_controller.log_run_progress_summary(LYST_RUN_PROGRESS)

async def finalize_lyst_resume_after_processing():
    resume_controller.state = LYST_RESUME_STATE
    await resume_controller.finalize_after_processing(run_failed=LYST_RUN_FAILED)

def load_font(font_size, prefer_heavy=False):
    return lyst_media_helpers.load_font(
        font_size,
        fonts_dir=Path(__file__).with_name("fonts"),
        prefer_heavy=prefer_heavy,
    )

def _ensure_edsr_weights():
    return lyst_media_helpers.ensure_edsr_weights(
        model_path=EDSR_MODEL_PATH,
        model_url=EDSR_MODEL_URL,
        logger=logger,
    )

def _get_edsr_superres():
    return lyst_media_helpers.get_edsr_superres(
        cv2_module=cv2,
        model_path=EDSR_MODEL_PATH,
        model_url=EDSR_MODEL_URL,
        logger=logger,
    )

def _upscale_with_edsr(pil_img):
    return lyst_media_helpers.upscale_with_edsr(
        pil_img,
        cv2_module=cv2,
        np_module=np,
        model_path=EDSR_MODEL_PATH,
        model_url=EDSR_MODEL_URL,
        logger=logger,
    )

def _fetch_image_bytes(image_url: str) -> bytes:
    return lyst_media_helpers.fetch_image_bytes(
        image_url,
        image_url_candidates_fn=_image_url_candidates,
    )

def process_image(image_url, uah_price, sale_percentage):
    # Image rendering now sits behind helpers/lyst/media.py so Telegram sending
    # does not own compression, font, or upscaling decisions directly.
    return lyst_media_helpers.process_image(
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

# Database functions now live in helpers/lyst/storage.py so the service lifecycle
# can depend on one storage adapter instead of reaching into sqlite everywhere.
create_tables = lyst_storage.create_tables
db_operation_with_retry = lyst_storage.db_operation_with_retry
is_shoe_processed = lyst_storage.is_shoe_processed
mark_shoe_processed = lyst_storage.mark_shoe_processed
load_shoe_data_from_db = lyst_storage.load_shoe_data_from_db
load_shoe_data_from_json = lyst_storage.load_shoe_data_from_json
save_shoe_data_bulk = lyst_storage.save_shoe_data_bulk
async_save_shoe_data = lyst_storage.async_save_shoe_data
migrate_json_to_sqlite = lyst_storage.migrate_json_to_sqlite
load_shoe_data = lyst_storage.load_shoe_data
save_shoe_data = lyst_storage.save_shoe_data

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
    await lyst_fetch_helpers.handle_route(
        route,
        blocked_resource_types=BLOCKED_RESOURCE_TYPES,
        blocked_url_parts=BLOCKED_URL_PARTS,
    )

async def normalize_lazy_images(page):
    await lyst_fetch_helpers.normalize_lazy_images(page)

def is_cloudflare_challenge(content: str) -> bool:
    return lyst_fetch_helpers.is_cloudflare_challenge(content)

class LystHttpTerminalPage(lyst_fetch_helpers.LystHttpTerminalPage):
    def __init__(self, status_code: int, content: str = ""):
        super().__init__(content or "")
        self.status_code = status_code


def _lyst_http_base_url(url: str) -> str:
    return lyst_fetch_helpers.http_base_url(url)


def _lyst_http_content_has_product_cards(content: str) -> bool:
    return lyst_fetch_helpers.http_content_has_product_cards(content)


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
    _touch_lyst_progress(
        "http_attempt",
        url=url,
        country=country,
        url_name=url_name,
        page_num=page_num,
        attempt=attempt,
    )
    async with LYST_HTTP_SEMAPHORE:
        if LYST_ABORT_EVENT.is_set():
            raise LystRunAborted("lyst_run_aborted")
        result = await lyst_fetch_helpers.fetch_http_page(
            url,
            country=country,
            timeout_sec=LYST_HTTP_TIMEOUT_SEC,
            request_jitter_sec=LYST_HTTP_REQUEST_JITTER_SEC,
            cloudflare_retry_count=LYST_CLOUDFLARE_RETRY_COUNT,
            cloudflare_retry_delay_sec=LYST_CLOUDFLARE_RETRY_DELAY_SEC,
            user_agent=STEALTH_UA,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": STEALTH_HEADERS.get("Accept-Language", "en-US,en;q=0.9"),
                "Upgrade-Insecure-Requests": "1",
                "DNT": "1",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            },
            logger=logger,
            http_client=LYST_HTTP_CLIENT,
        )
    if result.status == FetchStatus.TERMINAL:
        raise LystHttpTerminalPage(410, result.content)
    if result.status == FetchStatus.CLOUDFLARE:
        now_kyiv = datetime.now(KYIV_TZ).strftime('%Y-%m-%d %H:%M:%S')
        log_lines = tail_log_lines(BOT_LOG_FILE, line_count=200)
        try:
            await dump_lyst_debug_event(
                "lyst_cloudflare",
                reason="Cloudflare challenge",
                url=url,
                country=country,
                url_name=url_name,
                page_num=page_num,
                step="http_async",
                content=result.content or "",
                now_kyiv=now_kyiv,
                log_lines=log_lines,
                context_lines=context_lines,
            )
        except Exception:
            pass
        raise LystCloudflareChallenge()
    if not result.is_ok:
        raise RuntimeError(result.extra.get("error") or "http_only_unusable_response")
    return result.content

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
    return lyst_fetch_helpers.is_target_closed_error(exc)

def is_pipe_closed_error(exc: Exception) -> bool:
    return lyst_fetch_helpers.is_pipe_closed_error(exc)

def _lyst_url_suffix(url_name=None, page_num=None):
    return lyst_diagnostics_helpers.url_suffix(url_name=url_name, page_num=page_num)

def _lyst_page_progress_data(url, country, url_name=None, page_num=None, attempt=None):
    return lyst_diagnostics_helpers.page_progress_data(
        url,
        country,
        url_name=url_name,
        page_num=page_num,
        attempt=attempt,
    )

def _mark_lyst_page_step(step, url, country, url_name=None, page_num=None, attempt=None):
    _touch_lyst_progress(step, **_lyst_page_progress_data(url, country, url_name, page_num, attempt))
    logger.info(f"LYST step={step} url={url}")

def _safe_page_final_url(page):
    return lyst_diagnostics_helpers.safe_page_final_url(page)

def _lyst_debug_snapshot(page):
    return lyst_diagnostics_helpers.debug_snapshot(
        page=page,
        kyiv_tz=KYIV_TZ,
        log_lines_func=lambda: tail_log_lines(BOT_LOG_FILE, line_count=200),
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
    try:
        await lyst_diagnostics_helpers.dump_debug_event_safe(
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
            kyiv_tz=KYIV_TZ,
            log_lines_func=lambda: tail_log_lines(BOT_LOG_FILE, line_count=200),
            content=content,
            shield=shield,
        )
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
                _mark_lyst_issue("Cloudflare challenge")
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
                _mark_lyst_issue("page timeout")
                if attempt < max_retries - 1:
                    logger.info("LYST timeout: retrying with a fresh page/context")
                content = None
            if not content:
                _mark_lyst_issue("Failed to get soup")
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
                _mark_lyst_issue("TargetClosedError")
                logger.warning(f"LYST TargetClosedError: retrying with a fresh page/context for {url}{suffix}")
                await asyncio.sleep(3)
                continue
            attempt += 1
            if attempt < max_retries:
                logger.warning(f"Failed to get soup (attempt {attempt}/{max_retries}). Retrying...")
                await asyncio.sleep(5)
            else:
                logger.error(f"Failed to get soup for {url}{suffix}")
                _mark_lyst_issue("Failed to get soup")
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
    # Progress bookkeeping now flows through helpers/lyst/diagnostics.py so the
    # fetch and cycle layers can share one snapshot format during extraction.
    LYST_LAST_PROGRESS_TS, info = lyst_state_helpers.touch_progress(
        step=step,
        details=details,
        kyiv_tz=KYIV_TZ,
    )
    if step is not None:
        LYST_LAST_STEP_INFO = info

def _lyst_step_snapshot():
    return lyst_diagnostics_helpers.step_snapshot(LYST_LAST_STEP_INFO)

def _format_task_stack(task):
    return lyst_diagnostics_helpers.format_task_stack(task)

def _format_tasks_snapshot(limit=10):
    return lyst_diagnostics_helpers.format_tasks_snapshot(file_hint="GroteskBotTg.py", limit=limit)

def _describe_task_wait_chain(task, max_depth=6):
    return lyst_diagnostics_helpers.describe_task_wait_chain(task, max_depth=max_depth)

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
    return lyst_pricing_helpers.extract_price(price_str)

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
        _mark_lyst_issue("Failed to get soup")
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
            _mark_lyst_issue("Cloudflare challenge")
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
                _mark_lyst_issue("Stopped too early")
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
    return lyst_pricing_helpers.load_exchange_rates(
        exchange_rate_api_key=EXCHANGERATE_API_KEY,
        exchange_rates_file=EXCHANGE_RATES_FILE,
        logger=logger,
    )


async def async_load_exchange_rates():
    return await lyst_pricing_helpers.async_load_exchange_rates(
        exchange_rate_api_key=EXCHANGERATE_API_KEY,
        exchange_rates_file=EXCHANGE_RATES_FILE,
        logger=logger,
    )

def convert_to_uah(price, country, exchange_rates, name):
    return lyst_pricing_helpers.convert_to_uah(
        price,
        country,
        exchange_rates,
        name,
        logger=logger,
    )

# Route runtime calls through the internal Lyst package so orchestration can stay here
# while pricing and parsing logic live in cohesive modules with focused tests.
ConversionResult = lyst_pricing_helpers.ConversionResult
EMPTY_CONVERSION_RESULT = lyst_pricing_helpers.EMPTY_CONVERSION_RESULT

# Message formatting and sending
get_sale_emoji = lyst_notify_helpers.get_sale_emoji
build_shoe_message = lyst_notify_helpers.build_shoe_message

async def send_telegram_message(bot_token, chat_id, message, image_url=None, uah_price=None, sale_percentage=None, max_retries=3):
    return await lyst_notify_helpers.send_telegram_message(
        bot_token,
        chat_id,
        message,
        logger=logger,
        process_image_func=process_image,
        upgrade_image_url_func=_upgrade_lyst_image_url,
        image_url=image_url,
        uah_price=uah_price,
        sale_percentage=sale_percentage,
        max_retries=max_retries,
    )

def get_allowed_chat_ids():
    return lyst_notify_helpers.get_allowed_chat_ids(DANYLO_DEFAULT_CHAT_ID, TELEGRAM_CHAT_ID)

def tail_log_lines(path, line_count=LOG_TAIL_LINES):
    return lyst_notify_helpers.tail_log_lines(path, line_count=line_count, logger=logger)

async def send_log_tail(bot, chat_id, log_path, line_count=LOG_TAIL_LINES):
    await lyst_notify_helpers.send_log_tail(
        bot,
        chat_id,
        log_path,
        line_count=line_count,
        logger=logger,
    )

async def command_listener(bot_token, allowed_chat_ids, log_path):
    await lyst_notify_helpers.command_listener(
        bot_token,
        allowed_chat_ids,
        log_path,
        line_count=LOG_TAIL_LINES,
        add_dynamic_url_func=add_dynamic_url,
        logger=logger,
    )

# Processing functions
def _merge_base_url_into_shoes(result, base_url, country):
    return lyst_processing_helpers.merge_base_url_into_shoes(
        result,
        base_url,
        country,
        logger=logger,
    )

def filter_duplicates(shoes, exchange_rates):
    return lyst_processing_helpers.filter_duplicates(
        shoes,
        exchange_rates,
        country_priority=COUNTRY_PRIORITY,
        convert_to_uah=convert_to_uah,
    )

def _shoe_key(shoe):
    return lyst_processing_helpers.shoe_key(shoe)

def _apply_new_shoe_state(shoe, uah_sale):
    return lyst_processing_helpers.apply_new_shoe_state(shoe, uah_sale)

def _apply_existing_shoe_state(shoe, old_shoe, uah_sale, exchange_rates):
    return lyst_processing_helpers.apply_existing_shoe_state(
        shoe,
        old_shoe,
        uah_sale,
        exchange_rates,
        convert_to_uah=convert_to_uah,
    )

async def _save_single_shoe(key, shoe):
    await lyst_processing_helpers.save_single_shoe(
        key,
        shoe,
        save_shoe_data_bulk=save_shoe_data_bulk,
    )

async def process_shoe(shoe, old_data, message_queue, exchange_rates):
    await lyst_processing_helpers.process_shoe(
        shoe,
        old_data,
        message_queue,
        exchange_rates,
        is_shoe_processed=is_shoe_processed,
        mark_shoe_processed=mark_shoe_processed,
        save_shoe_data_bulk=save_shoe_data_bulk,
        build_shoe_message=build_shoe_message,
        calculate_sale_percentage=calculate_sale_percentage,
        convert_to_uah=convert_to_uah,
    )

async def process_all_shoes(all_shoes, old_data, message_queue, exchange_rates):
    await lyst_processing_helpers.process_all_shoes(
        all_shoes,
        old_data,
        message_queue,
        exchange_rates,
        shoe_concurrency=LYST_SHOE_CONCURRENCY,
        resolve_redirects=RESOLVE_REDIRECTS,
        run_failed=LYST_RUN_FAILED,
        logger=logger,
        touch_progress=_touch_lyst_progress,
        calculate_sale_percentage=calculate_sale_percentage,
        convert_to_uah=convert_to_uah,
        build_shoe_message=build_shoe_message,
        is_shoe_processed=is_shoe_processed,
        mark_shoe_processed=mark_shoe_processed,
        save_shoe_data_bulk=save_shoe_data_bulk,
        get_final_clear_link=get_final_clear_link,
    )

def _scrape_target_url(base_url, page, use_pagination):
    return lyst_cycle_helpers.build_scrape_target_url(base_url, page, use_pagination)

def _log_scrape_target(url_name, country, page, use_pagination):
    lyst_cycle_helpers.log_scrape_target(
        logger=logger,
        url_name=url_name,
        country=country,
        page=page,
        use_pagination=use_pagination,
    )

async def _update_resume_with_url(key, url, **fields):
    await lyst_cycle_helpers.update_resume_with_url(
        update_entry=update_lyst_resume_entry,
        key=key,
        url=url,
        **fields,
    )

async def process_url(base_url, countries, exchange_rates):
    return await lyst_cycle_helpers.process_url(
        base_url,
        countries,
        scrape_all_pages=scrape_all_pages,
        merge_base_url_into_shoes=_merge_base_url_into_shoes,
        touch_progress=_touch_lyst_progress,
        mark_start=lambda: None,
        special_logger=special_logger,
    )

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
    return lyst_cycle_helpers.collect_successful_results(url_results, logger=logger)


def _should_restart_after_terminal_resume(all_shoes):
    return resume_controller.should_restart_after_terminal_resume(
        all_shoes=all_shoes,
        cycle_started_in_resume=LYST_CYCLE_STARTED_IN_RESUME,
        entry_outcomes=LYST_RESUME_ENTRY_OUTCOMES,
    )

async def _clear_lyst_resume_state():
    global LYST_RESUME_STATE
    resume_controller.state = LYST_RESUME_STATE
    await lyst_cycle_helpers.clear_resume_state(resume_controller=resume_controller)
    LYST_RESUME_STATE = resume_controller.state

async def _run_lyst_resume_step(progress_name, operation, timeout_issue, timeout_log):
    await lyst_cycle_helpers.run_resume_step(
        progress_name=progress_name,
        operation=operation,
        touch_progress=_touch_lyst_progress,
        mark_issue=_mark_lyst_issue,
        logger=logger,
        timeout_issue=timeout_issue,
        timeout_log=timeout_log,
    )

async def _finalize_lyst_resume_state():
    await lyst_cycle_helpers.finalize_resume_state(
        finalize_resume_after_processing=finalize_lyst_resume_after_processing,
        clear_resume_state=_clear_lyst_resume_state,
        run_failed=LYST_RUN_FAILED,
        touch_progress=_touch_lyst_progress,
        mark_issue=_mark_lyst_issue,
        logger=logger,
    )

async def _shutdown_background_tasks(tasks):
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

def _log_lyst_collection_stats(all_shoes, exchange_rates):
    return lyst_cycle_helpers.log_collection_stats(
        all_shoes,
        exchange_rates,
        run_failed=LYST_RUN_FAILED,
        mark_issue=_mark_lyst_issue,
        special_logger=special_logger,
        skipped_items=SKIPPED_ITEMS,
        filter_duplicates=filter_duplicates,
    )

def _finalize_lyst_cycle(issue=None, error=None):
    if error is not None:
        logger.error(f"Lyst run failed: {error}")
    if issue is not None:
        _mark_lyst_issue(issue)

async def run_lyst_cycle_impl(message_queue, *, status_manager: LystStatusManager | None = None):
    global LYST_LAST_PROGRESS_TS, LYST_ACTIVE_TASK, LYST_CYCLE_STARTED_IN_RESUME, LYST_RESUME_ENTRY_OUTCOMES
    LYST_ACTIVE_TASK = asyncio.current_task()
    init_lyst_resume_state()
    SKIPPED_ITEMS.clear()
    reset_lyst_http_only_state()
    started = time.perf_counter()
    if status_manager is not None:
        status_manager.begin_cycle()
    _touch_lyst_progress("run_start")
    try:
        old_data = await load_shoe_data()
        exchange_rates = await async_load_exchange_rates()

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
        if status_manager is not None:
            status_manager.finish_success(duration_seconds=time.perf_counter() - started)
        logger.info("LYST run completed")

        print_statistics()
        print_link_statistics()
    except asyncio.CancelledError:
        _finalize_lyst_cycle(issue="stalled")
        if status_manager is not None:
            status_manager.finish_failure("stalled", duration_seconds=time.perf_counter() - started)
        raise
    except Exception as exc:
        _finalize_lyst_cycle(issue="failed", error=exc)
        if status_manager is not None:
            status_manager.finish_failure(exc, duration_seconds=time.perf_counter() - started)
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
            async with lyst_storage.db_semaphore:
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
    global LIVE_MODE, LYST_HTTP_CLIENT, LYST_STATUS_MANAGER
    load_last_runs_from_file()
    background_tasks = []
    if service_health is not None:
        service_health.start()
        service_health.mark_ready("lyst service starting")
        LYST_STATUS_MANAGER = LystStatusManager(
            reporter=service_health,
            legacy_write_status=write_lyst_status,
        )
    else:
        LYST_STATUS_MANAGER = None
    LYST_HTTP_CLIENT = AsyncLystHttpClient(
        timeout_sec=LYST_HTTP_TIMEOUT_SEC,
        user_agent=STEALTH_UA,
        default_headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": STEALTH_HEADERS.get("Accept-Language", "en-US,en;q=0.9"),
            "Upgrade-Insecure-Requests": "1",
            "DNT": "1",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
    )

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
                _mark_lyst_issue("stalled")
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
                manager = _get_lyst_status_manager()
                if manager is not None:
                    manager.finish_failure("stalled")
            elif service_health is not None:
                service_health.record_failure("lyst_run", "stalled_after_finalize")

        async def _run_lyst_and_track():
            return await run_lyst_cycle_impl(message_queue, status_manager=_get_lyst_status_manager())

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
        if LYST_HTTP_CLIENT is not None:
            await LYST_HTTP_CLIENT.close()
            LYST_HTTP_CLIENT = None
        LYST_STATUS_MANAGER = None
        await _shutdown_background_tasks(background_tasks)

if __name__ == "__main__":
    if IS_RUNNING_LYST:
        create_tables()  # Create tables at startup instead of creating them just before using
    asyncio.run(main())

