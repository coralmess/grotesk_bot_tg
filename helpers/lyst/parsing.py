from __future__ import annotations

import json
import re
import urllib.parse
import uuid

from helpers.lyst.pricing import extract_price, extract_price_tokens


def normalize_image_url(url: str | None) -> str | None:
    if not url:
        return None
    url = url.strip()
    if url.startswith("//"):
        return f"https:{url}"
    return url


def pick_src_from_srcset(srcset_value):
    if not srcset_value:
        return None
    best_url = None
    best_score = -1.0
    for part in srcset_value.split(","):
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
    best_url = normalize_image_url(best_url)
    if best_url and best_url.startswith(("http://", "https://")):
        return best_url
    return None


def extract_image_url_from_tag(tag):
    if not tag:
        return None
    candidates = [
        tag.get("src"),
        tag.get("data-src"),
        tag.get("data-lazy-src"),
        pick_src_from_srcset(tag.get("srcset")),
        pick_src_from_srcset(tag.get("data-srcset")),
        pick_src_from_srcset(tag.get("data-lazy-srcset")),
    ]
    for url in candidates:
        url = normalize_image_url(url)
        if url and url.startswith(("http://", "https://")):
            return url
    return None


def upgrade_lyst_image_url(url: str | None) -> str | None:
    if not url:
        return url
    url = normalize_image_url(url)
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


def image_url_candidates(url: str | None) -> list[str]:
    url = normalize_image_url(url)
    upgraded = upgrade_lyst_image_url(url)
    if upgraded and url and upgraded != url:
        return [upgraded, url]
    return [url] if url else []


def dedupe_preserve(items):
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
    del_el = root.find(["del", "s", "strike"])
    del_tokens = extract_price_tokens(del_el.get_text(" ", strip=True)) if del_el else []
    tokens = dedupe_preserve(extract_price_tokens(root.get_text(" ", strip=True)))
    if not tokens:
        return None, None
    if del_tokens:
        original = del_tokens[0]
        others = [token for token in tokens if token != original]
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
    for script in soup.find_all("script", type="application/ld+json"):
        text = script.string or script.get_text(strip=True)
        if not text or "ItemList" not in text:
            continue
        try:
            data = json.loads(text)
        except Exception:
            continue
        if data.get("@type") != "ItemList":
            continue
        for item in data.get("itemListElement", []):
            product = item.get("item", {}) if isinstance(item, dict) else {}
            url = product.get("url")
            images = product.get("image") or []
            if isinstance(images, str):
                images = [images]
            image_url = images[0] if images else None
            image_url = upgrade_lyst_image_url(image_url)
            if url and image_url:
                image_map[url] = image_url
                if url.startswith("https://www.lyst.com"):
                    image_map[url.replace("https://www.lyst.com", "")] = image_url
    return image_map


def extract_shoe_data(
    card,
    country: str,
    *,
    logger,
    skipped_items: set,
    normalize_product_link,
    image_fallback_map=None,
):
    if not card:
        logger.warning("Received None card in extract_shoe_data")
        return None

    try:
        finders = [
            lambda: card.find_all("span", class_=lambda x: x and "vjlibs5" in x),
            lambda: card.find_all("span", class_=lambda x: x and "vjlibs5" in x and "vjlibs2" in x),
            lambda: card.find_all("span", class_=re.compile(r".*vjlibs5.*")),
            lambda: card.find_all("span", class_=lambda x: x and ("_1b08vvh31" in x and "vjlibs" in x)),
        ]
        name_elements = []
        for fn in finders:
            name_elements = fn()
            if name_elements:
                break
        if not name_elements:
            img_tag = card.find("img", alt=True)
            img_alt = (img_tag.get("alt") or "").strip() if img_tag else None
            link_tag = card.find("a", href=True)
            link_text = link_tag.get_text(" ", strip=True) if link_tag else None
            full_name = (img_alt or link_text or "").strip()
            if not full_name:
                logger.warning("No name elements found")
                return None
        else:
            full_name = " ".join(e.text.strip() for e in name_elements if e and e.text)
        if full_name and "view all" in full_name.strip().lower():
            return None
        if "Giuseppe Zanotti" in full_name:
            return None

        price_container = card.find("div", attrs={"data-testid": "product-price"}) or card.find("div", class_="ducdwf0")
        if not price_container:
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
            price_div = card.find("div", class_="ducdwf0") or price_container
            strategies = [
                lambda: (
                    price_div.find("div", class_=lambda x: x and "_1b08vvhr6" in x and "vjlibs1" in x),
                    price_div.find("div", class_=lambda x: x and "_1b08vvh36" in x and "vjlibs2" in x),
                ),
                lambda: (
                    price_div.find("div", class_=lambda x: x and ("_1b08vvhos" in x and "vjlibs1" in x)),
                    price_div.find("div", class_=lambda x: x and ("_1b08vvh1w" in x and "vjlibs2" in x)),
                ),
                lambda: (
                    price_div.find("div", class_=lambda x: x and "vjlibs1" in x and "vjlibs2" in x and "_1b08vvhq2" in x and "_1b08vvh36" not in x),
                    price_div.find("div", class_=lambda x: x and "vjlibs2" in x and "_1b08vvh36" in x),
                ),
                lambda: (
                    price_div.find("div", class_=lambda x: x and "vjlibs1" in x and "_1b08vvhnk" in x and "_1b08vvh1q" not in x),
                    price_div.find("div", class_=lambda x: x and "vjlibs2" in x and "_1b08vvh1q" in x)
                    or price_div.find("div", class_=lambda x: x and "_1b08vvh1w" in x),
                ),
            ]
            for strat in strategies:
                original_node, sale_node = strat()
                if original_node and sale_node and original_node != sale_node:
                    original_tokens = extract_price_tokens(original_node.get_text(" ", strip=True))
                    sale_tokens = extract_price_tokens(sale_node.get_text(" ", strip=True))
                    if original_tokens and sale_tokens:
                        original_price, sale_price = original_tokens[0], sale_tokens[0]
                    break
            if not original_price or not sale_price:
                logger.warning("Price elements not found")
                return None
        if extract_price(original_price) < 80:
            logger.info("Skipping item '%s' with original price %s", full_name, original_price)
            return None

        product_card_div = card.find("div", attrs={"data-testid": "product-card"}) or card.find(
            "div", class_=lambda x: x and "kah5ce0" in x and "kah5ce2" in x
        )
        unique_id = product_card_div["id"] if product_card_div and "id" in product_card_div.attrs else None

        store = "Unknown Store"
        retailer_name = card.find("span", attrs={"data-testid": "retailer-name"})
        if retailer_name:
            store_span = retailer_name.find("span", class_="_1fcx6l24")
            store_text = store_span.get_text(" ", strip=True) if store_span else retailer_name.get_text(" ", strip=True)
            store = store_text if store_text else store
        else:
            store_elem = card.find("div", attrs={"data-testid": "retailer"}) or card.find("span", class_="_1fcx6l24")
            if store_elem:
                store_text = store_elem.get_text(" ", strip=True)
                store = store_text if store_text else store

        track_href = None
        product_href = None
        for anchor in card.find_all("a", href=True):
            href = anchor.get("href") or ""
            if not track_href and "/track/lead/" in href:
                track_href = href
            if not product_href and any(part in href for part in ["/clothing/", "/shoes/", "/accessories/", "/bags/", "/jewelry/"]):
                product_href = href
            if track_href and product_href:
                break
        href = track_href or product_href
        if not href:
            link_elem = card.find("a", href=True)
            href = link_elem["href"] if link_elem and "href" in link_elem.attrs else None
        full_url = f"https://www.lyst.com{href}" if href and href.startswith("/") else href if href and href.startswith("http") else None
        product_url = f"https://www.lyst.com{product_href}" if product_href and product_href.startswith("/") else product_href
        canonical_for_id = normalize_product_link(product_url or full_url)
        if not unique_id and canonical_for_id:
            unique_id = str(uuid.uuid5(uuid.NAMESPACE_URL, canonical_for_id))

        img_elem = (
            card.find("img", src=True)
            or card.find("img", attrs={"data-src": True})
            or card.find("img", attrs={"data-lazy-src": True})
            or card.find("img", attrs={"data-srcset": True})
            or card.find("img", attrs={"data-lazy-srcset": True})
            or card.find("img", srcset=True)
        )
        image_url = extract_image_url_from_tag(img_elem)
        if not image_url:
            source_elem = card.find("source", srcset=True) or card.find("source", attrs={"data-srcset": True})
            image_url = extract_image_url_from_tag(source_elem)
        if (not image_url or not image_url.startswith(("http://", "https://"))) and image_fallback_map:
            if full_url and full_url in image_fallback_map:
                image_url = image_fallback_map.get(full_url)
            elif href and href in image_fallback_map:
                image_url = image_fallback_map.get(href)
        image_url = upgrade_lyst_image_url(image_url)
        if not image_url or not image_url.startswith(("http://", "https://")):
            if unique_id:
                skipped_items.add(unique_id)
            return None

        required_fields = {
            "name": full_name,
            "original_price": original_price,
            "sale_price": sale_price,
            "image_url": image_url,
            "store": store,
            "shoe_link": full_url,
            "unique_id": unique_id,
        }
        if any(not value for value in required_fields.values()):
            missing_fields = [field for field, value in required_fields.items() if not value]
            logger.warning("Missing required fields: %s", ", ".join(missing_fields))
            return None

        return {
            "name": full_name,
            "original_price": original_price,
            "sale_price": sale_price,
            "image_url": image_url,
            "store": store,
            "country": country,
            "shoe_link": full_url,
            "unique_id": unique_id,
        }
    except Exception as exc:
        logger.error("Error extracting shoe data: %s", exc)
        return None
