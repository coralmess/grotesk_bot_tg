import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse


_BASE_DIR = Path(__file__).resolve().parent
DYNAMIC_OLX_FILE = _BASE_DIR / "olx_dynamic_urls.json"
DYNAMIC_SHAFA_FILE = _BASE_DIR / "shafa_dynamic_urls.json"


def _load_json(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return [d for d in data if isinstance(d, dict)]
    except Exception:
        pass
    return []


def _save_json(path: Path, data: List[Dict[str, str]]) -> None:
    try:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def normalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    # Remove trailing slashes for stable comparisons
    return url.rstrip("/")


def detect_source(url: str) -> Optional[str]:
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return None
    if "olx" in host:
        return "olx"
    if "shafa.ua" in host:
        return "shafa"
    return None


def _clean_name(text: str) -> str:
    cleaned = re.sub(r"[_\\-]+", " ", text)
    cleaned = re.sub(r"\\s+", " ", cleaned).strip()
    if cleaned.islower():
        return cleaned.title()
    return cleaned


def derive_url_name(url: str, source: str) -> str:
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    if source == "shafa":
        search = qs.get("search_text", [None])[0]
        if search:
            return _clean_name(unquote(search.replace("+", " ")))
        # Fallback to last path segment
        segment = parsed.path.rstrip("/").split("/")[-1]
        return _clean_name(unquote(segment)) or "Shafa link"
    if source == "olx":
        # /list/user/<id> or /uk/list/user/<id>
        if "/list/user/" in parsed.path:
            user_id = parsed.path.rstrip("/").split("/")[-1]
            return f"User {user_id}"
        # Look for /q-<term> in path
        match = re.search(r"/q-([^/]+)/?", parsed.path)
        if match:
            return _clean_name(unquote(match.group(1)))
        # Fallback to query-based text
        for key in qs:
            if key.lower().startswith("q-"):
                return _clean_name(unquote(key[2:]))
        return "OLX link"
    return "Link"


def load_dynamic_urls(source: str) -> List[Dict[str, str]]:
    if source == "olx":
        return _load_json(DYNAMIC_OLX_FILE)
    if source == "shafa":
        return _load_json(DYNAMIC_SHAFA_FILE)
    return []


def save_dynamic_urls(source: str, entries: List[Dict[str, str]]) -> None:
    if source == "olx":
        _save_json(DYNAMIC_OLX_FILE, entries)
    elif source == "shafa":
        _save_json(DYNAMIC_SHAFA_FILE, entries)


def merge_sources(static_list: List[Dict[str, str]], dynamic_list: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    merged = []
    for entry in (static_list or []) + (dynamic_list or []):
        url = normalize_url(entry.get("url", ""))
        if not url or url in seen:
            continue
        seen.add(url)
        merged.append(entry)
    return merged


def add_dynamic_url(url: str) -> Tuple[bool, Optional[str], Optional[str]]:
    url = normalize_url(url)
    if not url:
        return False, None, None
    source = detect_source(url)
    if not source:
        return False, None, None
    url_name = derive_url_name(url, source)

    # Load static lists lazily to avoid import cycles
    static_urls = []
    try:
        if source == "olx":
            from config_olx_urls import OLX_URLS
            static_urls = OLX_URLS or []
        elif source == "shafa":
            from config_shafa_urls import SHAFA_URLS
            static_urls = SHAFA_URLS or []
    except Exception:
        static_urls = []

    dynamic_urls = load_dynamic_urls(source)
    merged = merge_sources(static_urls, dynamic_urls)
    if any(normalize_url(e.get("url", "")) == url for e in merged):
        return False, source, url_name

    dynamic_urls.append(
        {
            "url": url,
            "url_name": url_name,
            "added_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    save_dynamic_urls(source, dynamic_urls)
    return True, source, url_name
