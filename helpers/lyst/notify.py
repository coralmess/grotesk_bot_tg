from __future__ import annotations

from helpers import lyst_runtime as lyst_runtime_helpers


get_sale_emoji = lyst_runtime_helpers.get_sale_emoji
build_shoe_message = lyst_runtime_helpers.build_shoe_message


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
    # Lyst-specific message sending stays behind one package boundary so the
    # service entrypoint is not responsible for Telegram retry/image policy.
    return await lyst_runtime_helpers.send_telegram_message(
        bot_token,
        chat_id,
        message,
        logger=logger,
        process_image_func=process_image_func,
        upgrade_image_url_func=upgrade_image_url_func,
        image_url=image_url,
        uah_price=uah_price,
        sale_percentage=sale_percentage,
        max_retries=max_retries,
    )


def get_allowed_chat_ids(default_chat_id, telegram_chat_id):
    return lyst_runtime_helpers.get_allowed_chat_ids(default_chat_id, telegram_chat_id)


def tail_log_lines(path, *, line_count, logger):
    return lyst_runtime_helpers.tail_log_lines(path, line_count=line_count, logger=logger)


async def send_log_tail(bot, chat_id, log_path, *, line_count, logger):
    await lyst_runtime_helpers.send_log_tail(
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
    add_dynamic_url_func,
    logger,
):
    await lyst_runtime_helpers.command_listener(
        bot_token,
        allowed_chat_ids,
        log_path,
        line_count=line_count,
        add_dynamic_url_func=add_dynamic_url_func,
        logger=logger,
    )
