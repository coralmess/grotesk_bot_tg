import json, time, asyncio, logging, colorama, subprocess, shutil, traceback, urllib.parse, re, html, io, threading, hashlib, os, random
from pathlib import Path
from telegram.constants import ParseMode
from collections import Counter, defaultdict, deque
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
from helpers.lyst import page_scraper as lyst_page_scraper
from helpers.lyst import page_runner as lyst_page_runner
from helpers.lyst import pricing as lyst_pricing_helpers
from helpers.lyst import processing as lyst_processing_helpers
from helpers.lyst.cloudflare_backoff import CloudflareBackoff
from helpers.lyst.http_client import AsyncLystHttpClient
from helpers.lyst.models import FetchStatus
from helpers.lyst.outcome import LystRunOutcome
from helpers.lyst.status import LystStatusManager
from helpers.lyst.browser import (
    BrowserPool,
    LystContextPool,
    create_country_context as lyst_create_country_context,
    launch_browser as lyst_launch_browser,
)
from helpers.lyst.resume import LystResumeController
from helpers.lyst.storage import LystStorage
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DANYLO_DEFAULT_CHAT_ID, EXCHANGERATE_API_KEY, IS_RUNNING_LYST, CHECK_INTERVAL_SEC, CHECK_JITTER_SEC, MAINTENANCE_INTERVAL_SEC, DB_VACUUM, OLX_RETENTION_DAYS, SHAFA_RETENTION_DAYS, LYST_MAX_BROWSERS, LYST_SHOE_CONCURRENCY, LYST_COUNTRY_CONCURRENCY, UPSCALE_IMAGES, UPSCALE_METHOD, LYST_HTTP_ONLY, LYST_HTTP_TIMEOUT_SEC, LYST_HTTP_CONCURRENCY, LYST_HTTP_REQUEST_JITTER_SEC, LYST_CLOUDFLARE_RETRY_COUNT, LYST_CLOUDFLARE_RETRY_DELAY_SEC, LYST_CLOUDFLARE_BASE_COOLDOWN_SEC, LYST_CLOUDFLARE_MAX_COOLDOWN_SEC
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
    LYST_CLOUDFLARE_BACKOFF_FILE,
    SCRAPER_RUNS_JSONL_FILE,
    SHOE_DATA_JSON_FILE,
    EXCHANGE_RATES_JSON_FILE,
)
from helpers.scraper_stats import RunStatsCollector
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
LYST_LAST_CLOUDFLARE_EVENT = None
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
# This runtime backoff is process-global by design: all concurrent country tasks
# share one persisted view of which source/country pairs should cool down.
LYST_CLOUDFLARE_BACKOFF = CloudflareBackoff(
    LYST_CLOUDFLARE_BACKOFF_FILE,
    base_cooldown_sec=LYST_CLOUDFLARE_BASE_COOLDOWN_SEC,
    max_cooldown_sec=LYST_CLOUDFLARE_MAX_COOLDOWN_SEC,
)

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

async def finalize_lyst_resume_after_processing(*, preserve_resume: bool = False):
    resume_controller.state = LYST_RESUME_STATE
    await resume_controller.finalize_after_processing(
        run_failed=LYST_RUN_FAILED,
        preserve_resume=preserve_resume,
    )

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
def extract_price(price_str):
    return lyst_pricing_helpers.extract_price(price_str)

def _normalize_currency_token(token: str) -> str:
    # Compatibility wrapper: pricing normalization now lives in helpers/lyst so
    # parser fixes are tested once and reused by both runtime and unit tests.
    return lyst_pricing_helpers.normalize_currency_token(token)

def extract_price_tokens(text):
    # Keep the historical GroteskBotTg symbol while delegating implementation to
    # the focused pricing helper.
    return lyst_pricing_helpers.extract_price_tokens(text)

def _extract_price_tokens_enhanced(text):
    return lyst_pricing_helpers.extract_price_tokens(text)

def _parse_price_amount(raw: str) -> float:
    return lyst_pricing_helpers.parse_price_amount(raw)

def _normalize_image_url(url: str | None) -> str | None:
    return lyst_parsing_helpers.normalize_image_url(url)

def _pick_src_from_srcset(srcset_value):
    return lyst_parsing_helpers.pick_src_from_srcset(srcset_value)

def _extract_image_url_from_tag(tag):
    return lyst_parsing_helpers.extract_image_url_from_tag(tag)

def _upgrade_lyst_image_url(url: str | None) -> str | None:
    return lyst_parsing_helpers.upgrade_lyst_image_url(url)

def _image_url_candidates(url: str | None) -> list[str]:
    return lyst_parsing_helpers.image_url_candidates(url)

def _dedupe_preserve(items):
    return lyst_parsing_helpers.dedupe_preserve(items)

def find_price_strings(root):
    return lyst_parsing_helpers.find_price_strings(root)

def extract_ldjson_image_map(soup):
    return lyst_parsing_helpers.extract_ldjson_image_map(soup)

def extract_shoe_data(card, country, image_fallback_map=None):
    # The parser needs runtime context for logging, skipped-image accounting, and
    # stable product-link IDs; pass those explicitly instead of keeping parser code
    # inside the service entrypoint.
    return lyst_parsing_helpers.extract_shoe_data(
        card,
        country,
        logger=logger,
        skipped_items=SKIPPED_ITEMS,
        normalize_product_link=_normalize_lyst_product_link,
        image_fallback_map=image_fallback_map,
    )

async def scrape_page(url, country, max_scroll_attempts=None, url_name=None, page_num=None, use_pagination=None):
    # Keep this public runtime function stable while the actual single-page
    # parsing path lives in a focused helper that is easier to test.
    return await lyst_page_scraper.scrape_page(
        url,
        country,
        get_soup_and_content=get_soup_and_content,
        extract_ldjson_image_map=extract_ldjson_image_map,
        extract_shoe_data=extract_shoe_data,
        mark_issue=_mark_lyst_issue,
        cloudflare_exception=LystCloudflareChallenge,
        aborted_exception=LystRunAborted,
        terminal_exception=LystHttpTerminalPage,
        max_scroll_attempts=max_scroll_attempts,
        url_name=url_name,
        page_num=page_num,
        use_pagination=use_pagination,
    )

async def scrape_all_pages(base_url, country, use_pagination=None):
    # Pagination mutates resume/progress globals, so GroteskBotTg keeps the public
    # wrapper and injects those runtime hooks into the isolated page runner.
    config = lyst_page_runner.PageRunConfig(
        page_scrape=PAGE_SCRAPE,
        max_scroll_attempts=LYST_MAX_SCROLL_ATTEMPTS,
        log_tail_lines=200,
    )
    hooks = lyst_page_runner.PageRunHooks(
        logger=logger,
        scrape_page=scrape_page,
        resume_key=_resume_key,
        should_skip_source_for_backoff=_should_skip_lyst_source_for_backoff,
        touch_progress=_touch_lyst_progress,
        scrape_target_url=_scrape_target_url,
        log_scrape_target=_log_scrape_target,
        update_resume_with_url=_update_resume_with_url,
        record_cloudflare_failure=_record_lyst_cloudflare_failure,
        mark_issue=_mark_lyst_issue,
        mark_run_failed=mark_lyst_run_failed,
        log_run_progress_summary=log_lyst_run_progress_summary,
        build_context_lines=build_lyst_context_lines,
        tail_log_lines=lambda line_count: tail_log_lines(BOT_LOG_FILE, line_count=line_count),
        write_stop_too_early_dump=write_stop_too_early_dump,
        now_kyiv=_now_kyiv_str,
        record_source_success=_record_lyst_source_success,
        sleep=asyncio.sleep,
    )
    return await lyst_page_runner.scrape_all_pages(
        base_url,
        country,
        config=config,
        hooks=hooks,
        resume_state=LYST_RESUME_STATE,
        resume_entry_outcomes=LYST_RESUME_ENTRY_OUTCOMES,
        run_progress=LYST_RUN_PROGRESS,
        abort_event=LYST_ABORT_EVENT,
        use_pagination=use_pagination,
    )

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
    return await lyst_processing_helpers.process_all_shoes(
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
        preserve_resume=_has_pending_lyst_resume_outcome(LYST_RESUME_ENTRY_OUTCOMES),
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

def _format_lyst_completion_message(outcome: LystRunOutcome) -> str:
    if outcome.ok:
        if outcome.note:
            return (
                f"LYST run {outcome.phase}: {outcome.note}; "
                f"items_seen={outcome.items_seen}, new_items={outcome.new_items}"
            )
        return (
            f"LYST run {outcome.phase}: "
            f"items_seen={outcome.items_seen}, new_items={outcome.new_items}"
        )
    return (
        f"LYST run {outcome.phase}: {outcome.note}; "
        f"items_seen={outcome.items_seen}, new_items={outcome.new_items}"
    )


def _build_lyst_run_outcome(
    *,
    run_failed: bool,
    items_seen: int,
    new_items: int,
    cloudflare_event: dict | None,
    fallback_note: str,
) -> LystRunOutcome:
    if cloudflare_event:
        if not run_failed and items_seen > 0:
            return LystRunOutcome.cloudflare_partial_success(
                source_name=str(cloudflare_event.get("source_name") or ""),
                country=str(cloudflare_event.get("country") or ""),
                page=cloudflare_event.get("page"),
                items_seen=items_seen,
                new_items=new_items,
            )
        return LystRunOutcome.cloudflare_partial(
            source_name=str(cloudflare_event.get("source_name") or ""),
            country=str(cloudflare_event.get("country") or ""),
            page=cloudflare_event.get("page"),
            items_seen=items_seen,
            new_items=new_items,
        )
    if run_failed:
        return LystRunOutcome.failed(fallback_note or "failed")
    return LystRunOutcome.full_success(items_seen=items_seen, new_items=new_items)


def _has_pending_lyst_resume_outcome(entry_outcomes: dict[str, str]) -> bool:
    # Local Cloudflare/cancel outcomes need resume state kept even when the run
    # produced usable results and should not trigger the global abort path.
    return any(
        outcome in {"cloudflare", "cloudflare_cooldown", "failed", "aborted"}
        for outcome in entry_outcomes.values()
    )


def _should_skip_lyst_source_for_backoff(source_name: str, country: str, backoff=LYST_CLOUDFLARE_BACKOFF) -> bool:
    # Cooldown skips are deliberate: they reduce repeated Cloudflare hits without
    # disabling the whole LYST run when only one source/country pair is blocked.
    return not backoff.should_allow(source_name, country)


def _record_lyst_cloudflare_failure(
    source_name: str,
    country: str,
    page: int | None,
    backoff=LYST_CLOUDFLARE_BACKOFF,
):
    global LYST_LAST_CLOUDFLARE_EVENT
    # Keep the latest challenge context so final health state explains exactly
    # which source/country/page made the run fail.
    LYST_LAST_CLOUDFLARE_EVENT = {
        "source_name": source_name,
        "country": country,
        "page": page,
    }
    return backoff.record_failure(source_name, country)


def _record_lyst_source_success(source_name: str, country: str, backoff=LYST_CLOUDFLARE_BACKOFF) -> None:
    # Clear only after a clean source/country completion so old penalties do not
    # survive once LYST proves that pair can be fetched again.
    backoff.record_success(source_name, country)


async def run_lyst_cycle_impl(message_queue, *, status_manager: LystStatusManager | None = None):
    global LYST_LAST_PROGRESS_TS, LYST_ACTIVE_TASK, LYST_CYCLE_STARTED_IN_RESUME, LYST_RESUME_ENTRY_OUTCOMES, LYST_LAST_CLOUDFLARE_EVENT
    LYST_ACTIVE_TASK = asyncio.current_task()
    LYST_LAST_CLOUDFLARE_EVENT = None
    init_lyst_resume_state()
    SKIPPED_ITEMS.clear()
    reset_lyst_http_only_state()
    started = time.perf_counter()
    run_stats = RunStatsCollector("lyst")
    if status_manager is not None:
        status_manager.begin_cycle()
        status_manager.set_state_fields(lyst_cloudflare_backoff=LYST_CLOUDFLARE_BACKOFF.snapshot())
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
        processing_stats = await process_all_shoes(all_shoes, old_data, message_queue, exchange_rates)
        await _finalize_lyst_resume_state()
        _touch_lyst_progress("finalize_run")
        fallback_note = "; ".join(sorted(set(LYST_RESUME_ENTRY_OUTCOMES.values()))) if LYST_RUN_FAILED else ""
        outcome = _build_lyst_run_outcome(
            run_failed=LYST_RUN_FAILED,
            items_seen=len(all_shoes),
            new_items=processing_stats.new_total,
            cloudflare_event=LYST_LAST_CLOUDFLARE_EVENT,
            fallback_note=fallback_note,
        )
        # The JSONL summary mirrors the service status but also preserves source-level
        # resume outcomes, which makes Cloudflare/terminal-page behavior auditable later.
        run_stats.set_field("items_seen", len(all_shoes))
        run_stats.set_field("new_items", processing_stats.new_total)
        run_stats.set_field("removed_items", processing_stats.removed_total)
        run_stats.set_field("resume_outcomes", dict(Counter(LYST_RESUME_ENTRY_OUTCOMES.values())))
        if LYST_LAST_CLOUDFLARE_EVENT:
            run_stats.set_field("cloudflare_event", dict(LYST_LAST_CLOUDFLARE_EVENT))
        run_stats.write_jsonl(SCRAPER_RUNS_JSONL_FILE, run_stats.finish(outcome=outcome.state.value))
        _finalize_lyst_cycle(issue=outcome.note if not outcome.ok else None)
        if status_manager is not None:
            status_manager.finish_outcome(outcome, duration_seconds=time.perf_counter() - started)
        logger.info(_format_lyst_completion_message(outcome))

        print_statistics()
        print_link_statistics()
    except asyncio.CancelledError:
        _finalize_lyst_cycle(issue="stalled")
        run_stats.set_field("error", "stalled")
        run_stats.write_jsonl(SCRAPER_RUNS_JSONL_FILE, run_stats.finish(outcome="failed_stalled"))
        if status_manager is not None:
            status_manager.finish_outcome(
                LystRunOutcome.failed("stalled"),
                duration_seconds=time.perf_counter() - started,
            )
        raise
    except Exception as exc:
        _finalize_lyst_cycle(issue="failed", error=exc)
        run_stats.set_field("error", str(exc)[:200])
        run_stats.write_jsonl(SCRAPER_RUNS_JSONL_FILE, run_stats.finish(outcome="failed"))
        if status_manager is not None:
            status_manager.finish_outcome(
                LystRunOutcome.failed(str(exc)),
                duration_seconds=time.perf_counter() - started,
            )
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

