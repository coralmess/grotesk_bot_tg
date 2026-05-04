from pathlib import Path
import shutil

_THIS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = _THIS_DIR.parent if _THIS_DIR.name == "helpers" else _THIS_DIR
RUNTIME_DATA_DIR = PROJECT_ROOT / "runtime_data"
RUNTIME_LOGS_DIR = RUNTIME_DATA_DIR / "logs"
RUNTIME_DB_DIR = RUNTIME_DATA_DIR / "db"
RUNTIME_TEXT_DIR = RUNTIME_DATA_DIR / "text"
RUNTIME_DEBUG_DIR = RUNTIME_DATA_DIR / "debug"
RUNTIME_JSON_DIR = RUNTIME_DATA_DIR / "json"
RUNTIME_STATUS_DIR = RUNTIME_DATA_DIR / "status"
RUNTIME_HEALTH_DIR = RUNTIME_DATA_DIR / "health"
RUNTIME_CACHE_DIR = RUNTIME_DATA_DIR / "cache"
RUNTIME_BROWSER_DIR = RUNTIME_DATA_DIR / "browser"
RUNTIME_TMP_DIR = RUNTIME_DATA_DIR / "tmp"
RUNTIME_ANALYTICS_DIR = RUNTIME_DATA_DIR / "analytics"


def ensure_runtime_dirs() -> None:
    for directory in (
        RUNTIME_DATA_DIR,
        RUNTIME_LOGS_DIR,
        RUNTIME_DB_DIR,
        RUNTIME_TEXT_DIR,
        RUNTIME_DEBUG_DIR,
        RUNTIME_JSON_DIR,
        RUNTIME_STATUS_DIR,
        RUNTIME_HEALTH_DIR,
        RUNTIME_CACHE_DIR,
        RUNTIME_BROWSER_DIR,
        RUNTIME_TMP_DIR,
        RUNTIME_ANALYTICS_DIR,
    ):
        directory.mkdir(parents=True, exist_ok=True)


def runtime_file(directory: Path, filename: str) -> Path:
    ensure_runtime_dirs()
    target = directory / filename
    legacy = PROJECT_ROOT / filename
    if legacy.exists() and not target.exists():
        try:
            shutil.move(str(legacy), str(target))
        except Exception:
            pass
    return target


PYTHON_LOG_FILE = runtime_file(RUNTIME_LOGS_DIR, "python.log")
MONITOR_LOG_FILE = runtime_file(RUNTIME_LOGS_DIR, "monitor.log")
STARTUP_ERROR_LOG_FILE = runtime_file(RUNTIME_LOGS_DIR, "startup_error.log")

SHOES_DB_FILE = runtime_file(RUNTIME_DB_DIR, "shoes.db")
OLX_ITEMS_DB_FILE = runtime_file(RUNTIME_DB_DIR, "olx_items.db")
SHAFA_ITEMS_DB_FILE = runtime_file(RUNTIME_DB_DIR, "shafa_items.db")
# Auto RIA needs its own dedupe ledger so car alerts can evolve independently from the
# fashion marketplaces without sharing state or risking cross-bot resend collisions.
AUTO_RIA_ITEMS_DB_FILE = runtime_file(RUNTIME_DB_DIR, "auto_ria_items.db")

STATUS_MESSAGE_ID_FILE = runtime_file(RUNTIME_TEXT_DIR, "status_message_id.txt")

LAST_RUNS_JSON_FILE = runtime_file(RUNTIME_JSON_DIR, "last_runs.json")
SCRAPER_RUNS_JSONL_FILE = runtime_file(RUNTIME_JSON_DIR, "scraper_runs.jsonl")
MARKET_OLX_RUN_STATUS_FILE = runtime_file(RUNTIME_STATUS_DIR, "market_olx_run.json")
MARKET_SHAFA_RUN_STATUS_FILE = runtime_file(RUNTIME_STATUS_DIR, "market_shafa_run.json")
LYST_RUN_STATUS_FILE = runtime_file(RUNTIME_STATUS_DIR, "lyst_run.json")
LYST_CLOUDFLARE_BACKOFF_FILE = runtime_file(RUNTIME_STATUS_DIR, "lyst_cloudflare_backoff.json")
LYST_RESUME_JSON_FILE = runtime_file(RUNTIME_JSON_DIR, "lyst_resume.json")
SHOE_DATA_JSON_FILE = runtime_file(RUNTIME_JSON_DIR, "shoe_data.json")
EXCHANGE_RATES_JSON_FILE = runtime_file(RUNTIME_JSON_DIR, "exchange_rates.json")
OLX_DYNAMIC_JSON_FILE = runtime_file(RUNTIME_JSON_DIR, "olx_dynamic_urls.json")
SHAFA_DYNAMIC_JSON_FILE = runtime_file(RUNTIME_JSON_DIR, "shafa_dynamic_urls.json")
SVITLO_SUBSCRIBERS_JSON_FILE = runtime_file(RUNTIME_JSON_DIR, "subscribers.json")
SVITLO_STATE_JSON_FILE = runtime_file(RUNTIME_JSON_DIR, "svitlo_state.json")
VIEWED_DESIGNERS_JSON_FILE = runtime_file(RUNTIME_JSON_DIR, "viewed_designers.json")


def service_health_file(service_name: str) -> Path:
    sanitized = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in service_name.strip().lower())
    if not sanitized:
        sanitized = "service"
    return runtime_file(RUNTIME_HEALTH_DIR, f"{sanitized}.json")
