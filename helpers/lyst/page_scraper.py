from __future__ import annotations

from typing import Any, Awaitable, Callable


async def scrape_page(
    url: str,
    country: str,
    *,
    get_soup_and_content: Callable[..., Awaitable[tuple[Any, str | None]]],
    extract_ldjson_image_map: Callable[[Any], dict],
    extract_shoe_data: Callable[[Any, str, dict], dict | None],
    mark_issue: Callable[[str], None],
    cloudflare_exception: type[BaseException],
    aborted_exception: type[BaseException],
    terminal_exception: type[BaseException],
    max_scroll_attempts=None,
    url_name=None,
    page_num=None,
    use_pagination=None,
) -> tuple[list[dict], str | None, str]:
    try:
        # Runtime hooks are injected so this helper stays independent from
        # GroteskBotTg.py globals and can be tested without importing the bot.
        soup, content = await get_soup_and_content(
            url,
            country,
            max_scroll_attempts=max_scroll_attempts,
            url_name=url_name,
            page_num=page_num,
            use_pagination=use_pagination,
        )
    except cloudflare_exception:
        return [], None, "cloudflare"
    except aborted_exception:
        return [], None, "aborted"
    except terminal_exception as exc:
        return [], getattr(exc, "content", None), "terminal"
    if not soup:
        mark_issue("Failed to get soup")
        return [], content, "failed"

    shoe_cards = soup.find_all("div", class_="_693owt3")
    image_fallback_map = extract_ldjson_image_map(soup)
    shoes = [
        data
        for card in shoe_cards
        if (data := extract_shoe_data(card, country, image_fallback_map))
    ]
    return shoes, content, "ok"
