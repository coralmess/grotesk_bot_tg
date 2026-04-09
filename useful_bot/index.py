import asyncio
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
from useful_bot.ibkr_portfolio_helper import IBKRPortfolioHelper
from helpers.health_summary import write_health_summary_files
from helpers.logging_utils import configure_third_party_loggers, install_secret_redaction
from helpers.service_health import build_service_health


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
    def __init__(self, helpers: list[UsefulHelper], *, service_health) -> None:
        self._helpers = helpers
        self._service_health = service_health
        self._heartbeat_task = None
        self._summary_task = None

    def register_handlers(self, application: Application) -> None:
        application.add_handler(CommandHandler("start", self.start_command))
        application.add_handler(CommandHandler("help", self.start_command))
        for helper in self._helpers:
            helper.register_handlers(application)

    async def on_startup(self, application: Application) -> None:
        self._service_health.start()
        self._service_health.mark_ready("useful bot starting")
        application.bot_data["service_health"] = self._service_health
        self._heartbeat_task = asyncio.create_task(
            self._service_health.heartbeat_loop(note="useful bot running"),
            name="usefulbot-health-heartbeat",
        )
        self._summary_task = asyncio.create_task(
            self._summary_loop(),
            name="usefulbot-health-summary",
        )
        for helper in self._helpers:
            await helper.on_startup(application)

    async def on_shutdown(self, application: Application) -> None:
        self._service_health.mark_stopping("useful bot stopping")
        for task in (self._heartbeat_task, self._summary_task):
            if task is None:
                continue
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
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

    async def _summary_loop(self) -> None:
        while True:
            try:
                payload = await asyncio.to_thread(write_health_summary_files)
                service_count = len(payload.get("services") or {})
                self._service_health.record_success("health_summary", note=f"services={service_count}")
                await asyncio.sleep(15 * 60)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._service_health.record_failure("health_summary", exc)
                logging.exception("Health summary update failed")
                await asyncio.sleep(60)


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
        IBKRPortfolioHelper(chat_id=chat_id),
    ]

    index = UsefulBotIndex(helpers=helpers, service_health=build_service_health("usefulbot"))
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
    configure_third_party_loggers()
    install_secret_redaction(logging.getLogger())
    app = build_application()
    app.run_polling(close_loop=False, drop_pending_updates=True)


if __name__ == "__main__":
    main()
