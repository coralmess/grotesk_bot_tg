import os
from dotenv import load_dotenv
import random

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
EXCHANGERATE_API_KEY = os.getenv('EXCHANGERATE_API_KEY')
DANYLO_DEFAULT_CHAT_ID = os.getenv('DANYLO_DEFAULT_CHAT_ID')
TELEGRAM_OLX_BOT_TOKEN = os.getenv('TELEGRAM_OLX_BOT_TOKEN')
IS_RUNNING_LYST = os.getenv('IsRunningLyst', 'true').strip().lower() in ('1', 'true', 'yes', 'y', 'on')
IS_INSTANCE = os.getenv('IS_INSTANCE', 'false').strip().lower() in ('1', 'true', 'yes', 'y', 'on')
CHECK_INTERVAL_SEC = int(os.getenv('CHECK_INTERVAL_SEC', '3600'))
CHECK_JITTER_SEC = int(os.getenv('CHECK_JITTER_SEC', '300'))
OLX_REQUEST_JITTER_SEC = float(os.getenv('OLX_REQUEST_JITTER_SEC', '2.0'))
SHAFA_REQUEST_JITTER_SEC = float(os.getenv('SHAFA_REQUEST_JITTER_SEC', '2.0'))
MAINTENANCE_INTERVAL_SEC = int(os.getenv('MAINTENANCE_INTERVAL_SEC', '21600'))
DB_VACUUM = os.getenv('DB_VACUUM', 'false').strip().lower() in ('1', 'true', 'yes', 'y', 'on')
OLX_RETENTION_DAYS = int(os.getenv('OLX_RETENTION_DAYS', '0'))
SHAFA_RETENTION_DAYS = int(os.getenv('SHAFA_RETENTION_DAYS', '0'))
UPSCALE_IMAGES = os.getenv('UPSCALE_IMAGES', 'false').strip().lower() in ('1', 'true', 'yes', 'y', 'on')
UPSCALE_METHOD = os.getenv('UPSCALE_METHOD', 'lanczos').strip().lower()
BLOCK_RESOURCES = os.getenv('BLOCK_RESOURCES', 'true' if IS_INSTANCE else 'false').strip().lower() in ('1', 'true', 'yes', 'y', 'on')
LYST_HTTP_ONLY = os.getenv('LYST_HTTP_ONLY', 'false').strip().lower() in ('1', 'true', 'yes', 'y', 'on')
LYST_HTTP_TIMEOUT_SEC = float(os.getenv('LYST_HTTP_TIMEOUT_SEC', '45'))

# Concurrency tuning (lower on instance to reduce CPU spikes)
LYST_MAX_BROWSERS = int(os.getenv('LYST_MAX_BROWSERS', '2' if IS_INSTANCE else '6'))
LYST_SHOE_CONCURRENCY = int(os.getenv('LYST_SHOE_CONCURRENCY', '3' if IS_INSTANCE else '9'))
LYST_COUNTRY_CONCURRENCY = int(os.getenv('LYST_COUNTRY_CONCURRENCY', '1' if IS_INSTANCE else '2'))

OLX_TASK_CONCURRENCY = int(os.getenv('OLX_TASK_CONCURRENCY', '2' if IS_INSTANCE else '3'))
OLX_HTTP_HTML_CONCURRENCY = int(os.getenv('OLX_HTTP_HTML_CONCURRENCY', '6' if IS_INSTANCE else '10'))
OLX_HTTP_IMAGE_CONCURRENCY = int(os.getenv('OLX_HTTP_IMAGE_CONCURRENCY', '3' if IS_INSTANCE else '6'))
OLX_UPSCALE_CONCURRENCY = int(os.getenv('OLX_UPSCALE_CONCURRENCY', '1' if IS_INSTANCE else '2'))
OLX_SEND_CONCURRENCY = int(os.getenv('OLX_SEND_CONCURRENCY', '2' if IS_INSTANCE else '3'))
OLX_HTTP_CONNECTOR_LIMIT = int(os.getenv('OLX_HTTP_CONNECTOR_LIMIT', '12' if IS_INSTANCE else '20'))

SHAFA_TASK_CONCURRENCY = int(os.getenv('SHAFA_TASK_CONCURRENCY', '2' if IS_INSTANCE else '3'))
SHAFA_HTTP_CONCURRENCY = int(os.getenv('SHAFA_HTTP_CONCURRENCY', '6' if IS_INSTANCE else '10'))
SHAFA_SEND_CONCURRENCY = int(os.getenv('SHAFA_SEND_CONCURRENCY', '2' if IS_INSTANCE else '3'))
SHAFA_PLAYWRIGHT_CONCURRENCY = int(os.getenv('SHAFA_PLAYWRIGHT_CONCURRENCY', '1' if IS_INSTANCE else '2'))
SHAFA_HTTP_CONNECTOR_LIMIT = int(os.getenv('SHAFA_HTTP_CONNECTOR_LIMIT', '12' if IS_INSTANCE else '20'))

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
