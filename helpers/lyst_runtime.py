from __future__ import annotations

"""Runtime-facing helpers for the Lyst/control bot.

This module holds Telegram command/listener plumbing and message rendering.
The scraper core in ``GroteskBotTg.py`` stays focused on fetch/state logic, while
Telegram-facing behavior remains importable and easier to test in isolation.
"""

import html
from typing import Callable, Optional

from helpers import telegram_runtime as telegram_runtime_helpers

SALE_EMOJI_ROCKET_THRESHOLD = 90
SALE_EMOJI_UAH_THRESHOLD = 4000


def get_sale_emoji(sale_percentage, uah_sale):
    if sale_percentage >= SALE_EMOJI_ROCKET_THRESHOLD:
        return "??????"
    if uah_sale < SALE_EMOJI_UAH_THRESHOLD:
        return "??????"
    return "??????"


def build_shoe_message(shoe, sale_percentage, uah_sale, kurs, kurs_symbol, old_sale_price=None, status=None):
    def _esc(value):
        return html.escape(str(value if value is not None else ""), quote=True)

    name = _esc(shoe.get("name"))
    original_price = _esc(shoe.get("original_price"))
    sale_price = _esc(shoe.get("sale_price"))
    lowest_price = _esc(shoe.get("lowest_price"))
    store = _esc(shoe.get("store"))
    country = _esc(shoe.get("country"))
    kurs_symbol_safe = _esc(kurs_symbol)
    old_sale_price_safe = _esc(old_sale_price)
    shoe_link = _esc(shoe.get("shoe_link"))
    store_line = f"?? Store : <a href='{shoe_link}'>{store}</a>" if shoe_link else f"?? Store : {store}"

    if status is None:
        sale_emoji = get_sale_emoji(sale_percentage, uah_sale)
        return (
            f"{sale_emoji}  New item  {sale_emoji}\n{name}\n\n"
            f"?? Prices : <s>{original_price}</s>  <b>{sale_price}</b>  <i>(Sale: <b>{sale_percentage}%</b>)</i>\n"
            f"?? Grivniki : <b>{uah_sale} UAH </b>\n"
            f"?? Kurs : {kurs_symbol_safe} {kurs} \n"
            f"{store_line}\n"
            f"?? Country : {country}"
        )
    return (
        f"?????? {_esc(status)} ?????? \n{name}:\n\n"
        f"?? Prices : <s>{original_price}</s>  <s>{old_sale_price_safe}</s>  <b>{sale_price}</b>  <i>(Sale: <b>{sale_percentage}%</b>)</i> \n"
        f"?? Grivniki : {uah_sale} UAH\n"
        f"?? Lowest price : {lowest_price} ({shoe['lowest_price_uah']} UAH)\n"
        f"?? Kurs : {kurs_symbol_safe} {kurs} \n"
        f"{store_line}\n"
        f"?? Country : {country}"
    )


async def send_telegram_message(
    bot_token,
    chat_id,
    message,
    *,
    logger,
    process_image_func,
    upgrade_image_url_func,
    image_url=None,
    uah_price=None,
    sale_percentage=None,
    max_retries=3,
):
    return await telegram_runtime_helpers.send_telegram_message(
        bot_token,
        chat_id,
        message,
        image_url=image_url,
        uah_price=uah_price,
        sale_percentage=sale_percentage,
        max_retries=max_retries,
        process_image_func=process_image_func,
        upgrade_image_url_func=upgrade_image_url_func,
        logger=logger,
    )


def get_allowed_chat_ids(default_chat_id, telegram_chat_id):
    return telegram_runtime_helpers.get_allowed_chat_ids(default_chat_id, telegram_chat_id)


def tail_log_lines(path, *, line_count, logger):
    return telegram_runtime_helpers.tail_log_lines(path, line_count=line_count, logger=logger)


async def send_log_tail(bot, chat_id, log_path, *, line_count, logger):
    await telegram_runtime_helpers.send_log_tail(
        bot,
        chat_id,
        log_path,
        line_count=line_count,
        logger=logger,
    )


async def command_listener(
    bot_token,
    allowed_chat_ids,
    log_path,
    *,
    line_count,
    add_dynamic_url_func: Callable[[str], tuple[bool, Optional[str], Optional[str]]],
    logger,
):
    # The Lyst/control bot intentionally exposes only the local operator commands.
    # Marketplace unsubscribe handling lives in the market bot because those updates
    # come through the OLX bot identity, not the Lyst/control bot identity.
    await telegram_runtime_helpers.command_listener(
        bot_token,
        allowed_chat_ids,
        log_path,
        line_count=line_count,
        add_dynamic_url_func=add_dynamic_url_func,
        allow_log_commands=True,
        allow_add_commands=True,
        allow_unsubscribe_commands=False,
        logger=logger,
    )
