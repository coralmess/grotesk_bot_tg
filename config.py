import os
from dotenv import load_dotenv
import random

load_dotenv()


def _float_tuple_env(name: str, default: tuple[float, ...]) -> tuple[float, ...]:
    raw = os.getenv(name, "")
    values: list[float] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            value = float(part)
        except ValueError:
            return default
        if value > 0:
            values.append(value)
    return tuple(values) or default


TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
EXCHANGERATE_API_KEY = os.getenv('EXCHANGERATE_API_KEY')
DANYLO_DEFAULT_CHAT_ID = os.getenv('DANYLO_DEFAULT_CHAT_ID')
TELEGRAM_OLX_BOT_TOKEN = os.getenv('TELEGRAM_OLX_BOT_TOKEN')
TELEGRAM_TSEK_BOT_TOKEN = os.getenv('TELEGRAM_TSEK_BOT_TOKEN')
IS_RUNNING_LYST = os.getenv('IsRunningLyst', 'true').strip().lower() in ('1', 'true', 'yes', 'y', 'on')
IS_INSTANCE = os.getenv('IS_INSTANCE', 'false').strip().lower() in ('1', 'true', 'yes', 'y', 'on')
CHECK_INTERVAL_SEC = int(os.getenv('CHECK_INTERVAL_SEC', '3600'))
CHECK_JITTER_SEC = int(os.getenv('CHECK_JITTER_SEC', '300'))
OLX_REQUEST_JITTER_SEC = float(os.getenv('OLX_REQUEST_JITTER_SEC', '2.0'))
SHAFA_REQUEST_JITTER_SEC = float(os.getenv('SHAFA_REQUEST_JITTER_SEC', '2.0'))
# Marketplace scrapers are serialized by the market service, so these shorter
# windows make each feed more frequent without letting OLX and SHAFA overlap.
MARKET_OLX_MIN_SEC = int(os.getenv('MARKET_OLX_MIN_SEC', '900'))
MARKET_OLX_MAX_SEC = int(os.getenv('MARKET_OLX_MAX_SEC', '1800'))
MARKET_SHAFA_MIN_SEC = int(os.getenv('MARKET_SHAFA_MIN_SEC', '900'))
MARKET_SHAFA_MAX_SEC = int(os.getenv('MARKET_SHAFA_MAX_SEC', '1800'))
MAINTENANCE_INTERVAL_SEC = int(os.getenv('MAINTENANCE_INTERVAL_SEC', '21600'))
DB_VACUUM = os.getenv('DB_VACUUM', 'false').strip().lower() in ('1', 'true', 'yes', 'y', 'on')
OLX_RETENTION_DAYS = int(os.getenv('OLX_RETENTION_DAYS', '0'))
SHAFA_RETENTION_DAYS = int(os.getenv('SHAFA_RETENTION_DAYS', '0'))
UPSCALE_IMAGES = os.getenv('UPSCALE_IMAGES', 'false').strip().lower() in ('1', 'true', 'yes', 'y', 'on')
UPSCALE_METHOD = os.getenv('UPSCALE_METHOD', 'lanczos').strip().lower()
MARKET_IMAGE_UPSCALE_MIN_DIM = int(os.getenv('MARKET_IMAGE_UPSCALE_MIN_DIM', '1500'))
MARKET_IMAGE_UPSCALE_MAX_DIM = int(os.getenv('MARKET_IMAGE_UPSCALE_MAX_DIM', '5000'))
# Try larger Lanczos outputs first, but keep this configurable because Telegram's
# accepted geometry and source image sizes vary by marketplace.
MARKET_IMAGE_UPSCALE_FACTORS = _float_tuple_env('MARKET_IMAGE_UPSCALE_FACTORS', (2.0,))
BLOCK_RESOURCES = os.getenv('BLOCK_RESOURCES', 'true' if IS_INSTANCE else 'false').strip().lower() in ('1', 'true', 'yes', 'y', 'on')
# Lyst now prefers the HTTP parser first. It is cheaper, returns images via LD-JSON,
# and only falls back to Playwright when HTTP truly fails.
LYST_HTTP_ONLY = os.getenv('LYST_HTTP_ONLY', 'true').strip().lower() in ('1', 'true', 'yes', 'y', 'on')
LYST_HTTP_TIMEOUT_SEC = float(os.getenv('LYST_HTTP_TIMEOUT_SEC', '45'))
# Lyst HTTP-only works on the instance, but bursty parallel requests trip Cloudflare.
# Keep the HTTP fetch path serialized and slightly jittered there; local defaults stay looser.
LYST_HTTP_CONCURRENCY = int(os.getenv('LYST_HTTP_CONCURRENCY', '1' if IS_INSTANCE else '2'))
LYST_HTTP_REQUEST_JITTER_SEC = float(os.getenv('LYST_HTTP_REQUEST_JITTER_SEC', '1.0' if IS_INSTANCE else '0.25'))
# A single delayed retry is the highest-signal compromise here: enough to recover from a
# short-lived challenge, but not enough to keep burning the IP after Cloudflare engages.
LYST_CLOUDFLARE_RETRY_COUNT = int(os.getenv('LYST_CLOUDFLARE_RETRY_COUNT', '1'))
LYST_CLOUDFLARE_RETRY_DELAY_SEC = float(os.getenv('LYST_CLOUDFLARE_RETRY_DELAY_SEC', '30'))
# Persisted Cloudflare cooldowns stop the instance from immediately retrying the same
# source/country pair after a challenge, which protects the free Oracle IP from loops.
# The balanced defaults trade slower retry of blocked pairs for fewer repeated
# Cloudflare loops; operators can still shorten or lengthen these via env vars.
LYST_CLOUDFLARE_BASE_COOLDOWN_SEC = int(os.getenv('LYST_CLOUDFLARE_BASE_COOLDOWN_SEC', '3600'))
LYST_CLOUDFLARE_MAX_COOLDOWN_SEC = int(os.getenv('LYST_CLOUDFLARE_MAX_COOLDOWN_SEC', '21600'))

# Concurrency tuning (lower on instance to reduce CPU spikes)
LYST_MAX_BROWSERS = int(os.getenv('LYST_MAX_BROWSERS', '2' if IS_INSTANCE else '6'))
LYST_SHOE_CONCURRENCY = int(os.getenv('LYST_SHOE_CONCURRENCY', '3' if IS_INSTANCE else '9'))
LYST_COUNTRY_CONCURRENCY = int(os.getenv('LYST_COUNTRY_CONCURRENCY', '1' if IS_INSTANCE else '2'))

OLX_TASK_CONCURRENCY = int(os.getenv('OLX_TASK_CONCURRENCY', '3' if IS_INSTANCE else '3'))
OLX_HTTP_HTML_CONCURRENCY = int(os.getenv('OLX_HTTP_HTML_CONCURRENCY', '8' if IS_INSTANCE else '10'))
OLX_HTTP_IMAGE_CONCURRENCY = int(os.getenv('OLX_HTTP_IMAGE_CONCURRENCY', '4' if IS_INSTANCE else '6'))
OLX_UPSCALE_CONCURRENCY = int(os.getenv('OLX_UPSCALE_CONCURRENCY', '1' if IS_INSTANCE else '2'))
OLX_SEND_CONCURRENCY = int(os.getenv('OLX_SEND_CONCURRENCY', '3' if IS_INSTANCE else '3'))
OLX_HTTP_CONNECTOR_LIMIT = int(os.getenv('OLX_HTTP_CONNECTOR_LIMIT', '16' if IS_INSTANCE else '20'))
# OLX has many more sources than SHAFA. Chunking spreads site requests and image
# work across time instead of creating one short CPU/network burst.
OLX_SOURCE_CHUNK_SIZE = int(os.getenv('OLX_SOURCE_CHUNK_SIZE', '40'))
OLX_SOURCE_CHUNK_PAUSE_MIN_SEC = float(os.getenv('OLX_SOURCE_CHUNK_PAUSE_MIN_SEC', '20'))
OLX_SOURCE_CHUNK_PAUSE_MAX_SEC = float(os.getenv('OLX_SOURCE_CHUNK_PAUSE_MAX_SEC', '45'))

SHAFA_TASK_CONCURRENCY = int(os.getenv('SHAFA_TASK_CONCURRENCY', '3' if IS_INSTANCE else '3'))
SHAFA_HTTP_CONCURRENCY = int(os.getenv('SHAFA_HTTP_CONCURRENCY', '8' if IS_INSTANCE else '10'))
SHAFA_SEND_CONCURRENCY = int(os.getenv('SHAFA_SEND_CONCURRENCY', '3' if IS_INSTANCE else '3'))
SHAFA_UPSCALE_CONCURRENCY = int(os.getenv('SHAFA_UPSCALE_CONCURRENCY', '1' if IS_INSTANCE else '2'))
SHAFA_PLAYWRIGHT_CONCURRENCY = int(os.getenv('SHAFA_PLAYWRIGHT_CONCURRENCY', '2' if IS_INSTANCE else '2'))
SHAFA_HTTP_CONNECTOR_LIMIT = int(os.getenv('SHAFA_HTTP_CONNECTOR_LIMIT', '16' if IS_INSTANCE else '20'))

# Lightweight per-run header rotation (kept consistent during a single process run)
HEADER_PROFILES = [
    {
        "name": "chrome_win_124_uk",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "accept_language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
    },
    {
        "name": "chrome_win_123_uk",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "accept_language": "uk,ru;q=0.9,en;q=0.8",
    },
    {
        "name": "firefox_win_122_uk",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
        "accept_language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
    },
]
_header_profile = random.choice(HEADER_PROFILES)
RUN_USER_AGENT = _header_profile["user_agent"]
RUN_ACCEPT_LANGUAGE = _header_profile["accept_language"]
