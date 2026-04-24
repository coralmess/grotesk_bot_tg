from __future__ import annotations

import html
import re
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup

from helpers.auto_ria.models import AutoRiaListing, VinDecoderDetails

AUTO_RIA_BASE_URL = "https://auto.ria.com"
_USD_PRICE_RE = re.compile(r"(\d[\d\s]*)\s*\$")
_SOLD_AVAILABILITY_RE = re.compile(r'"availability"\s*:\s*"https://schema\.org/SoldOut"', re.IGNORECASE)


def normalize_auto_ria_search_url(url: str, *, limit: int = 100) -> str:
    parsed = urlsplit(url)
    # Auto RIA already exposes page size in query params, so one large first page
    # covers the configured searches without extra browser pagination state.
    normalized_limit = str(max(1, int(limit)))
    query_pairs: list[tuple[str, str]] = []
    saw_page = False
    saw_limit = False
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key == "page":
            if not saw_page:
                query_pairs.append(("page", "0"))
                saw_page = True
            continue
        if key == "limit":
            if not saw_limit:
                query_pairs.append(("limit", normalized_limit))
                saw_limit = True
            continue
        query_pairs.append((key, value))
    if not saw_page:
        query_pairs.append(("page", "0"))
    if not saw_limit:
        query_pairs.append(("limit", normalized_limit))
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query_pairs), parsed.fragment))


def normalize_auto_ria_image_url(image_url: Optional[str]) -> Optional[str]:
    if not image_url:
        return None
    # Auto RIA card thumbnails end with the lightweight "x" size suffix; switching them
    # to "hd" preserves the same image asset while giving Telegram a much cleaner source.
    return re.sub(r"fx(\.(?:jpe?g|webp))$", r"fhd\1", image_url, flags=re.IGNORECASE)


def _clean_text(value: Optional[str]) -> str:
    return " ".join((value or "").split())


def _normalize_subtitle(value: Optional[str]) -> str:
    cleaned = _clean_text(value)
    # The listing subtitle already comes pre-separated with bullets; preserving the wider
    # spacing keeps the outgoing message visually aligned with the Auto RIA card wording.
    return cleaned.replace(" • ", "  •  ")


def _parse_price_usd(price_text: str) -> int:
    match = _USD_PRICE_RE.search(price_text or "")
    if not match:
        return 0
    return int(match.group(1).replace(" ", ""))


def parse_auto_ria_search_html(html_text: str, *, base_url: str = AUTO_RIA_BASE_URL) -> list[AutoRiaListing]:
    soup = BeautifulSoup(html_text, "html.parser")
    listings: list[AutoRiaListing] = []
    seen_ids: set[str] = set()

    for anchor in soup.select("a.link.product-card.horizontal"):
        car_id = _clean_text(anchor.get("data-car-id") or anchor.get("id"))
        if not car_id or car_id in seen_ids:
            continue

        title = _clean_text(anchor.select_one(".titleS").get_text(" ", strip=True) if anchor.select_one(".titleS") else "")
        price_node = anchor.select_one(".titleM.c-green")
        price_text = _clean_text(price_node.get_text(" ", strip=True) if price_node else "")
        if not title or not price_text:
            continue

        subtitle_node = anchor.select_one(".size-14-16.ellipsis-1.mb-8")
        image_node = anchor.select_one("img[data-src], img[src]")
        body_nodes = [_clean_text(node.get_text(" ", strip=True)) for node in anchor.select(".grid-wrapper .body")]

        listing = AutoRiaListing(
            id=car_id,
            url=urljoin(base_url, anchor.get("href") or ""),
            title=title,
            subtitle=_normalize_subtitle(subtitle_node.get_text(" ", strip=True) if subtitle_node else ""),
            price_usd=_parse_price_usd(price_text),
            price_text=price_text,
            mileage_text=body_nodes[0] if len(body_nodes) > 0 else "",
            fuel_engine_text=body_nodes[2] if len(body_nodes) > 2 else "",
            image_url=normalize_auto_ria_image_url(image_node.get("data-src") or image_node.get("src") if image_node else None),
        )
        listings.append(listing)
        seen_ids.add(car_id)

    return listings


def extract_vin_from_detail_html(html_text: str) -> Optional[str]:
    soup = BeautifulSoup(html_text, "html.parser")
    node = soup.select_one("#badgesVin > span")
    vin_text = _clean_text(node.get_text(" ", strip=True) if node else "")
    if not vin_text:
        return None
    vin = re.sub(r"[^A-Z0-9]", "", vin_text.upper())
    return vin or None


def is_auto_ria_sold_detail_html(html_text: str) -> bool:
    if not html_text:
        return False
    # Sold car pages still return HTTP 200, but their structured Offer metadata
    # switches availability to SoldOut. This is more stable than visible text.
    return bool(_SOLD_AVAILABILITY_RE.search(html_text))


def parse_vin_decoder_html(html_text: str) -> VinDecoderDetails:
    soup = BeautifulSoup(html_text, "html.parser")
    trim: Optional[str] = None
    transmission: Optional[str] = None

    # The decoder page structure is not under our control and may vary, so the parser is
    # label-driven instead of relying on a brittle fixed table shape.
    for row in soup.select("tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) < 2:
            continue
        label = _clean_text(cells[0].get_text(" ", strip=True)).casefold()
        value = _clean_text(cells[1].get_text(" ", strip=True))
        if not value:
            continue
        if label == "trim" and not trim:
            trim = value
        elif label == "transmission" and not transmission:
            transmission = value
        if trim and transmission:
            break

    return VinDecoderDetails(trim=trim, transmission=transmission)


def parse_nhtsa_vpic_payload(payload: dict) -> VinDecoderDetails:
    results = payload.get("Results") or []
    if not results:
        return VinDecoderDetails()

    row = results[0] or {}
    trim = _clean_text(row.get("Trim"))
    if not trim:
        trim = _clean_text(" ".join(filter(None, [row.get("Series"), row.get("Series2")])))

    transmission = _clean_text(row.get("TransmissionStyle"))
    speeds = _clean_text(row.get("TransmissionSpeeds"))
    if transmission and speeds:
        transmission = f"{transmission} ({speeds}-speed)"

    return VinDecoderDetails(
        trim=trim or None,
        transmission=transmission or None,
    )


def build_auto_ria_caption(
    listing: AutoRiaListing,
    *,
    transmission: Optional[str],
    trim: Optional[str],
) -> str:
    # Making the title itself clickable keeps the message compact while still letting the
    # user jump straight into the listing from the most visually prominent line.
    lines = [f'<a href="{html.escape(listing.url, quote=True)}"><b>{html.escape(listing.title)}</b></a>']
    if listing.subtitle:
        # Italicizing secondary trim/engine text separates it from hard facts below while
        # preserving the original Auto RIA wording for quick visual comparison.
        lines.append(f"<i>{html.escape(listing.subtitle)}</i>")
    lines.append("")
    # The price line was explicitly requested above mileage because that is how the alert
    # should scan in Telegram when the user triages multiple car candidates quickly.
    lines.append(f"<b>Ціна:</b> <b>{html.escape(listing.price_text)}</b>")
    if listing.mileage_text:
        lines.append(f"<b>Пробіг:</b> {html.escape(listing.mileage_text)}")
    if listing.fuel_engine_text:
        lines.append(f"<b>Двигун:</b> {html.escape(listing.fuel_engine_text)}")
    if transmission:
        lines.append(f"<b>Коробка:</b> {html.escape(transmission)}")
    if trim:
        lines.append(f"<b>Комплектація:</b> {html.escape(trim)}")
    return "\n".join(lines)


def build_auto_ria_sold_caption(listing: AutoRiaListing, *, original_caption: str) -> str:
    # Sold detection is asynchronous after the original alert, so the edit keeps the old
    # caption intact and only adds an obvious status banner above it.
    sold_line = "<b>Продано</b>"
    if original_caption.startswith(sold_line):
        return original_caption
    return f"{sold_line}\n{original_caption}"
