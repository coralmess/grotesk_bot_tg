from __future__ import annotations

import json
import re
from collections import namedtuple
from datetime import datetime
from pathlib import Path

import requests


ConversionResult = namedtuple("ConversionResult", ["uah_amount", "exchange_rate", "currency_symbol"])
EMPTY_CONVERSION_RESULT = ConversionResult(0, 0, "")
CURRENCY_CODE_BY_SYMBOL = {
    "\u20ac": "EUR",
    "\u00e2\u201a\u00ac": "EUR",
    "\u00a3": "GBP",
    "\u00c2\u00a3": "GBP",
    "$": "USD",
}
CURRENCY_DISPLAY_SYMBOL_BY_SYMBOL = {
    "\u00e2\u201a\u00ac": "\u20ac",
    "\u00c2\u00a3": "\u00a3",
}
PRICE_TOKEN_RE = re.compile(r"([\d.,]+\s*[^\d\s]+|[^\d\s]+\s*[\d.,]+)")
PRICE_TRAILING_TOKEN_RE = re.compile(r"(\d[\d.,]*)\s*([^\d\s]+)")
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


def extract_price(price_str: str) -> float:
    price_num = re.sub(r"[^\d.]", "", price_str or "")
    try:
        return float(price_num)
    except ValueError:
        return 0.0


def normalize_currency_token(token: str) -> str:
    value = (token or "").replace("\xa0", "").strip()
    if not value:
        return ""
    normalized_values = [value]
    # Lyst markup sometimes arrives with mojibake currency symbols, so token repair
    # needs to live with the parser helpers instead of being repeated ad hoc.
    try:
        repaired = value.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
        if repaired:
            normalized_values.append(repaired)
    except Exception:
        pass
    return " ".join(normalized_values).lower()


def extract_price_tokens(text: str) -> list[str]:
    if not text:
        return []
    tokens: list[str] = []
    normalized_text = text.replace("\xa0", " ")
    for match in PRICE_TOKEN_RE.finditer(normalized_text):
        raw_token = match.group(0).replace(" ", "")
        candidates = [raw_token]
        if raw_token and raw_token[-1] not in "0123456789" and (raw_token[0].isdigit() or raw_token[0] == "."):
            candidates.append(raw_token[-1] + raw_token[:-1])
        for token in candidates:
            normalized = normalize_currency_token(token)
            if any(marker in normalized for marker in CURRENCY_MARKERS):
                tokens.append(token)
                break
    if tokens:
        return tokens

    fallback_inputs = [normalized_text]
    try:
        repaired = normalized_text.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
        if repaired:
            fallback_inputs.append(repaired)
    except Exception:
        pass

    seen: set[str] = set()
    for source_text in fallback_inputs:
        for match in PRICE_TRAILING_TOKEN_RE.finditer(source_text):
            token = f"{match.group(1)}{match.group(2)}"
            normalized = normalize_currency_token(token)
            if any(marker in normalized for marker in CURRENCY_MARKERS) and token not in seen:
                seen.add(token)
                tokens.append(token)
    return tokens


def parse_price_amount(raw: str) -> float:
    if not raw:
        return 0.0
    cleaned = re.sub(r"[^\d,\.]", "", raw)
    if not cleaned:
        return 0.0
    if "," in cleaned and "." in cleaned:
        cleaned = cleaned.replace(",", "")
    elif "," in cleaned and "." not in cleaned:
        parts = cleaned.split(",")
        cleaned = "".join(parts) if len(parts[-1]) == 3 else ".".join(parts)
    elif "." in cleaned:
        parts = cleaned.split(".")
        if len(parts[-1]) == 3:
            cleaned = "".join(parts)
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def calculate_sale_percentage(original_price: str, sale_price: str, country: str) -> int:
    def _parse(price_value: str) -> float:
        symbol = "\u20ac" if country in ("PL", "IT") else "\u00a3" if country == "GB" else "$"
        cleaned = (price_value or "").replace(symbol, "").strip()
        cleaned = cleaned.replace(",", ".") if symbol == "\u20ac" and ("," in cleaned and "." not in cleaned) else cleaned.replace(",", "")
        return float(re.sub(r"[^\d.]", "", cleaned) or 0)

    try:
        original = _parse(original_price)
        sale = _parse(sale_price)
        return int((1 - sale / original) * 100) if original > 0 else 0
    except Exception:
        return 0


def update_exchange_rates(*, exchange_rate_api_key: str, exchange_rates_file: Path, logger):
    try:
        resp = requests.get(
            f"https://v6.exchangerate-api.com/v6/{exchange_rate_api_key}/latest/UAH",
            timeout=15,
        )
        resp.raise_for_status()
        payload = resp.json()
        rates = {k: payload["conversion_rates"][k] for k in ("EUR", "USD", "GBP")}
        with exchange_rates_file.open("w", encoding="utf-8") as handle:
            json.dump({"last_update": datetime.now().isoformat(), "rates": rates}, handle)
        return rates
    except Exception as exc:
        logger.error("Error updating exchange rates: %s", exc)
        return None


def load_exchange_rates(*, exchange_rate_api_key: str, exchange_rates_file: Path, logger):
    cached_rates = None
    try:
        with exchange_rates_file.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        cached_rates = data.get("rates")
        last_update = data.get("last_update")
        is_fresh = bool(last_update) and (datetime.now() - datetime.fromisoformat(last_update)).days < 1
        if is_fresh and cached_rates:
            return cached_rates
    except (FileNotFoundError, json.JSONDecodeError, KeyError, ValueError):
        cached_rates = None

    updated = update_exchange_rates(
        exchange_rate_api_key=exchange_rate_api_key,
        exchange_rates_file=exchange_rates_file,
        logger=logger,
    )
    if updated:
        return updated
    if cached_rates:
        logger.warning("Using cached exchange rates due to update failure")
        return cached_rates
    return {"EUR": 1, "USD": 1, "GBP": 1}


def convert_to_uah(price: str, country: str, exchange_rates: dict, name: str, *, logger) -> ConversionResult:
    try:
        currency = None
        currency_symbol = ""
        for symbol, code in CURRENCY_CODE_BY_SYMBOL.items():
            if symbol in price:
                currency = code
                currency_symbol = CURRENCY_DISPLAY_SYMBOL_BY_SYMBOL.get(symbol, symbol)
                break
        if not currency:
            logger.error("Unrecognized currency symbol in price '%s' for '%s' country '%s'", price, name, country)
            return EMPTY_CONVERSION_RESULT

        amount = parse_price_amount(price)
        if amount <= 0:
            logger.error("Failed to parse price '%s' for '%s' country '%s'", price, name, country)
            return ConversionResult(0, 0, currency_symbol)

        rate = exchange_rates.get(currency)
        if not rate:
            logger.error("Exchange rate not found for currency '%s' (country: %s)", currency, country)
            return EMPTY_CONVERSION_RESULT

        uah_amount = amount / rate
        return ConversionResult(round(uah_amount / 10) * 10, round(1 / rate, 2), currency_symbol)
    except (ValueError, KeyError) as exc:
        logger.error("Error converting price '%s' for '%s' country '%s': %s", price, name, country, exc)
        return EMPTY_CONVERSION_RESULT
