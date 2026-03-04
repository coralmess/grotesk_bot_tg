import logging
import os
import sys
from pathlib import Path
from typing import Protocol

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from useful_bot.exchange_rate_helper import ExchangeRateHelper


class UsefulHelper(Protocol):
    helper_name: str

    def register_handlers(self, application: Application) -> None:
        ...

    def start_lines(self) -> list[str]:
        ...

    async def on_startup(self, application: Application) -> None:
        ...

    async def on_shutdown(self, application: Application) -> None:
        ...


class UsefulBotIndex:
    def __init__(self, helpers: list[UsefulHelper]) -> None:
        self._helpers = helpers

    def register_handlers(self, application: Application) -> None:
        application.add_handler(CommandHandler("start", self.start_command))
        application.add_handler(CommandHandler("help", self.start_command))
        for helper in self._helpers:
            helper.register_handlers(application)

    async def on_startup(self, application: Application) -> None:
        for helper in self._helpers:
            await helper.on_startup(application)

    async def on_shutdown(self, application: Application) -> None:
        for helper in self._helpers:
            await helper.on_shutdown(application)

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.message:
            return

        lines = ["Useful bot is online.", "Available micro-programs and commands:"]
        for helper in self._helpers:
            lines.append("")
            lines.extend(helper.start_lines())
        await update.message.reply_text("\n".join(lines))


def build_application() -> Application:
    load_dotenv()

    token = os.getenv("GROTESK_USEFUL_BOT_TOKEN")
    if not token:
        raise RuntimeError("Missing GROTESK_USEFUL_BOT_TOKEN in .env")

    chat_id_raw = os.getenv("DANYLO_DEFAULT_CHAT_ID")
    if not chat_id_raw:
        raise RuntimeError("Missing DANYLO_DEFAULT_CHAT_ID in .env")

    try:
        chat_id = int(chat_id_raw)
    except ValueError as error:
        raise RuntimeError("DANYLO_DEFAULT_CHAT_ID must be an integer") from error

    helpers: list[UsefulHelper] = [
        ExchangeRateHelper(chat_id=chat_id),
    ]

    index = UsefulBotIndex(helpers=helpers)
    application = (
        Application.builder()
        .token(token)
        .post_init(index.on_startup)
        .post_shutdown(index.on_shutdown)
        .build()
    )
    index.register_handlers(application)
    return application


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    app = build_application()
    app.run_polling(close_loop=False, drop_pending_updates=True)


if __name__ == "__main__":
    main()
