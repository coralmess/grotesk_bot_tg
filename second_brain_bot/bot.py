from __future__ import annotations

import asyncio
import html
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from helpers.analytics_events import AnalyticsSink
from helpers.logging_utils import configure_third_party_loggers, install_secret_redaction
from helpers.service_health import build_service_health
from second_brain_bot.ai import AIOrchestrator, OpenAICompatibleProvider, clean_note_excerpt
from second_brain_bot.config import SecondBrainConfig, load_config
from second_brain_bot.service import SecondBrainService

LOGGER = logging.getLogger(__name__)
THINKING_MESSAGE = "🧠Thinking🧠"


class SecondBrainTelegramBot:
    def __init__(self, config: SecondBrainConfig, service: SecondBrainService, *, service_health) -> None:
        self.config = config
        self.service = service
        self.service_health = service_health
        self._digest_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None

    def register_handlers(self, application: Application) -> None:
        application.add_handler(CommandHandler("start", self.start_command))
        application.add_handler(CommandHandler("help", self.start_command))
        application.add_handler(CommandHandler("brain_status", self.status_command))
        application.add_handler(CommandHandler("brain_inbox", self.inbox_command))
        application.add_handler(CommandHandler("brain_note", self.note_command))
        application.add_handler(CommandHandler("brain_accept", self.accept_command))
        application.add_handler(CommandHandler("brain_skip", self.skip_command))
        application.add_handler(CommandHandler("brain_search", self.search_command))
        application.add_handler(CommandHandler("brain_ask", self.ask_command))
        application.add_handler(CommandHandler("brain_distill", self.distill_command))
        application.add_handler(CommandHandler("brain_digest_now", self.digest_now_command))
        application.add_handler(CommandHandler("brain_ai_retry", self.ai_retry_command))
        application.add_handler(CommandHandler("brain_web_enrich", self.web_enrich_command))
        application.add_handler(MessageHandler(filters.PHOTO & ~filters.COMMAND, self.capture_message))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.capture_message))

    async def on_startup(self, application: Application) -> None:
        self.service_health.start()
        self.service_health.mark_ready("second brain bot starting")
        application.bot_data["service_health"] = self.service_health
        self._heartbeat_task = asyncio.create_task(
            self.service_health.heartbeat_loop(note="second brain bot running"),
            name="second-brain-health-heartbeat",
        )
        self._digest_task = asyncio.create_task(self._digest_loop(application), name="second-brain-digest")

    async def on_shutdown(self, application: Application) -> None:
        self.service_health.mark_stopping("second brain bot stopping")
        for task in (self._digest_task, self._heartbeat_task):
            if task is None:
                continue
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._allowed(update):
            return
        await update.effective_message.reply_text(build_help_text())

    async def capture_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._allowed(update) or not update.effective_message:
            return
        message = update.effective_message
        thinking = await message.reply_text(THINKING_MESSAGE)
        try:
            if message.photo:
                photo = message.photo[-1]
                telegram_file = await context.bot.get_file(photo.file_id)
                data = bytes(await telegram_file.download_as_bytearray())
                note = await self.service.capture_photo(
                    caption=message.caption or "",
                    photo_bytes=data,
                    original_name=f"telegram-{message.message_id}.jpg",
                    telegram_message_id=message.message_id,
                )
            else:
                note = await self.service.capture_text(message.text or "", telegram_message_id=message.message_id)
            self.service_health.record_success("capture", note=f"id={note.note_id}")
            await _edit_or_reply(thinking, _format_capture_confirmation(note))
        except Exception as exc:
            self.service_health.record_failure("capture", exc)
            LOGGER.exception("Second Brain capture failed")
            await _edit_or_reply(thinking, "Capture failed. Check service logs.")

    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._allowed(update):
            return
        status = self.service.status()
        counts = status["counts"]
        providers = ", ".join(self.service.ai.providers.keys()) or "none"
        await update.effective_message.reply_text(
            f"Vault: {status['vault_dir']}\n"
            f"Notes: {counts.get('total', 0)} | Active: {counts.get('Active', 0)} | Incubating: {counts.get('Incubating', 0)} | Reference: {counts.get('Reference', 0)}\n"
            f"AI providers: {providers}\n"
            f"Daily digest: {self.config.digest_hour:02d}:00 {self.config.digest_tz}"
        )

    async def inbox_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._allowed(update):
            return
        limit = _first_int(context.args, default=10, min_value=1, max_value=30)
        notes = self.service.inbox(limit=limit)
        if not notes:
            await update.effective_message.reply_text("No Incubating notes right now.")
            return
        await update.effective_message.reply_text("\n".join(f"{n.note_id}: {n.title}" for n in notes))

    async def note_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._allowed(update):
            return
        note_id = _arg_text(context.args)
        if not note_id:
            await update.effective_message.reply_text("Usage: /brain_note <id>")
            return
        result = self.service.index.get_note(note_id)
        if result is None:
            await update.effective_message.reply_text("Note not found.")
            return
        await update.effective_message.reply_text(_format_note_preview_html(result), parse_mode="HTML")

    async def accept_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self._note_action(update, context, action="accept")

    async def skip_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self._note_action(update, context, action="skip")

    async def search_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._allowed(update):
            return
        query = _arg_text(context.args)
        if not query:
            await update.effective_message.reply_text("Usage: /brain_search <query>")
            return
        results = self.service.search(query, limit=8)
        await update.effective_message.reply_text(
            "\n".join(f"{item.note_id}: {item.title} ({item.path})" for item in results) or "No matches."
        )

    async def ask_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._allowed(update):
            return
        question = _arg_text(context.args)
        if not question:
            await update.effective_message.reply_text("Usage: /brain_ask <question>")
            return
        thinking = await update.effective_message.reply_text(THINKING_MESSAGE)
        try:
            answer = await self.service.ask(question)
            await _edit_or_reply(thinking, answer)
        except Exception as exc:
            self.service_health.record_failure("brain_ask", exc)
            LOGGER.exception("Second Brain ask failed")
            await _edit_or_reply(thinking, "I could not answer that. Check service logs.")

    async def distill_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._allowed(update):
            return
        selector = _arg_text(context.args) or "today"
        thinking = await update.effective_message.reply_text(THINKING_MESSAGE)
        try:
            text = await self.service.distill(selector)
            await _edit_or_reply(thinking, text)
        except Exception as exc:
            self.service_health.record_failure("brain_distill", exc)
            LOGGER.exception("Second Brain distill failed")
            await _edit_or_reply(thinking, "Distill failed. Check service logs.")

    async def digest_now_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._allowed(update):
            return
        thinking = await update.effective_message.reply_text(THINKING_MESSAGE)
        try:
            digest = await self.service.build_daily_digest()
            await _edit_or_reply(thinking, digest)
        except Exception as exc:
            self.service_health.record_failure("brain_digest_now", exc)
            LOGGER.exception("Second Brain digest failed")
            await _edit_or_reply(thinking, "Digest failed. Check service logs.")

    async def ai_retry_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._allowed(update):
            return
        note_id = _arg_text(context.args)
        if not note_id:
            await update.effective_message.reply_text("Usage: /brain_ai_retry <id>")
            return
        thinking = await update.effective_message.reply_text(THINKING_MESSAGE)
        try:
            metadata, body, _ = self.service.vault.read_note(note_id)
            note = await self.service.capture_text(body, capture_type=str(metadata.get("capture_type") or "text"))
            await _edit_or_reply(thinking, f"Created enriched retry note: {note.note_id}")
        except KeyError:
            await _edit_or_reply(thinking, "Note not found.")
        except Exception as exc:
            self.service_health.record_failure("brain_ai_retry", exc)
            LOGGER.exception("Second Brain AI retry failed")
            await _edit_or_reply(thinking, "AI retry failed. Check service logs.")

    async def web_enrich_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._allowed(update):
            return
        note_id = _arg_text(context.args)
        if not note_id:
            await update.effective_message.reply_text("Usage: /brain_web_enrich <id>")
            return
        thinking = await update.effective_message.reply_text(THINKING_MESSAGE)
        try:
            metadata, body, _ = self.service.vault.read_note(note_id)
            note = await self.service.capture_text(
                body,
                capture_type=str(metadata.get("capture_type") or "text"),
                allow_web=True,
            )
            await _edit_or_reply(thinking, f"Created web-enriched note: {note.note_id}")
        except KeyError:
            await _edit_or_reply(thinking, "Note not found.")
        except Exception as exc:
            self.service_health.record_failure("brain_web_enrich", exc)
            LOGGER.exception("Second Brain web enrich failed")
            await _edit_or_reply(thinking, "Web enrichment failed. Check service logs.")

    async def _note_action(self, update: Update, context: ContextTypes.DEFAULT_TYPE, *, action: str) -> None:
        if not await self._allowed(update):
            return
        note_id = _arg_text(context.args)
        if not note_id:
            await update.effective_message.reply_text(f"Usage: /brain_{action} <id>")
            return
        try:
            note = self.service.accept(note_id) if action == "accept" else self.service.skip(note_id)
        except KeyError:
            await update.effective_message.reply_text("Note not found.")
            return
        await update.effective_message.reply_text(f"{action}: {note.title}\n{note.path}")

    async def _digest_loop(self, application: Application) -> None:
        last_sent_date: str | None = None
        tz = ZoneInfo(self.config.digest_tz)
        while True:
            try:
                now = datetime.now(tz)
                target = now.replace(hour=self.config.digest_hour, minute=0, second=0, microsecond=0)
                if target <= now:
                    target += timedelta(days=1)
                await asyncio.sleep(max(1.0, (target - now).total_seconds()))
                now = datetime.now(tz)
                date_key = now.date().isoformat()
                if last_sent_date == date_key:
                    continue
                digest = await self.service.build_daily_digest(now_iso=now.isoformat())
                await application.bot.send_message(chat_id=self.config.owner_chat_id, text=digest[:3900])
                last_sent_date = date_key
                self.service_health.record_success("daily_digest", note=date_key)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.service_health.record_failure("daily_digest", exc)
                LOGGER.exception("Second Brain daily digest failed")
                await asyncio.sleep(300)

    async def _allowed(self, update: Update) -> bool:
        chat = update.effective_chat
        if chat is None or int(chat.id) != self.config.owner_chat_id:
            return False
        return True


def build_ai_orchestrator(config: SecondBrainConfig, *, analytics_sink: AnalyticsSink | None = None) -> AIOrchestrator:
    sink = analytics_sink or AnalyticsSink()
    providers = {}
    if config.gemini_api_key:
        # Gemini Flash is the preferred free-tier route for the personal brain:
        # strong enough for synthesis while keeping Modal GLM as a slower fallback.
        providers["gemini"] = OpenAICompatibleProvider(
            name="gemini",
            api_key=config.gemini_api_key,
            base_url=config.gemini_base_url,
            model=config.gemini_model,
            analytics_sink=sink,
        )
    if config.modal_glm_api_key:
        providers["modal_glm"] = OpenAICompatibleProvider(
            name="modal_glm",
            api_key=config.modal_glm_api_key,
            base_url=config.modal_glm_base_url,
            model=config.modal_glm_model,
            analytics_sink=sink,
        )
    if config.cerebras_api_key:
        providers["cerebras"] = OpenAICompatibleProvider(
            name="cerebras",
            api_key=config.cerebras_api_key,
            base_url=config.cerebras_base_url,
            model=config.cerebras_model,
            analytics_sink=sink,
        )
    if config.groq_api_key:
        providers["groq"] = OpenAICompatibleProvider(
            name="groq",
            api_key=config.groq_api_key,
            base_url=config.groq_base_url,
            model=config.groq_model,
            analytics_sink=sink,
        )
    return AIOrchestrator(providers=providers, analytics_sink=sink)


def build_application() -> Application:
    config = load_config()
    analytics_sink = AnalyticsSink()
    service = SecondBrainService(
        vault_dir=config.vault_dir,
        ai=build_ai_orchestrator(config, analytics_sink=analytics_sink),
        analytics_sink=analytics_sink,
    )
    controller = SecondBrainTelegramBot(
        config=config,
        service=service,
        service_health=build_service_health("second-brain-bot"),
    )
    application = (
        Application.builder()
        .token(config.bot_token)
        .post_init(controller.on_startup)
        .post_shutdown(controller.on_shutdown)
        .build()
    )
    controller.register_handlers(application)
    return application


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    configure_third_party_loggers()
    install_secret_redaction(logging.getLogger())
    app = build_application()
    app.run_polling(close_loop=False, drop_pending_updates=True)


def _arg_text(args: list[str]) -> str:
    return " ".join(args or []).strip()


def _first_int(args: list[str], *, default: int, min_value: int, max_value: int) -> int:
    if not args:
        return default
    try:
        value = int(args[0])
    except ValueError:
        return default
    return min(max(value, min_value), max_value)


def build_help_text() -> str:
    # Keep command help self-explanatory because this bot is operated from Telegram,
    # where there is no surrounding documentation next to the command list.
    return "\n".join(
        [
            "Second Brain bot is online.",
            "",
            "Send me any text, link, or photo and I will save it into your Obsidian-style vault, enrich it when useful, and connect it to related notes.",
            "",
            "Main commands:",
            "/brain_ask <question> - ask something based on your saved notes.",
            "/brain_search <query> - find notes by words, tags, or topics.",
            "/brain_inbox [limit] - show recent Incubating notes and ideas.",
            "/brain_note <id> - open a saved note preview by ID.",
            "",
            "Organize notes:",
            "/brain_accept <id> - accept AI title/tags/folder for a note.",
            "/brain_skip <id> - mark a note as needing manual review.",
            "/brain_ai_retry <id> - rerun AI enrichment and save a new enriched version.",
            "/brain_web_enrich <id> - rerun enrichment with public web lookup allowed.",
            "",
            "Summaries:",
            "/brain_distill <id|today|week> - create a concise insight/next-actions summary.",
            "/brain_digest_now - generate the daily digest immediately.",
            "/brain_status - show vault path, note counts, and connected AI providers.",
            "",
            "Tip: when AI is working, I first show \"🧠Thinking🧠\" and then edit that message with the answer.",
        ]
    )


def _format_note_preview_html(result) -> str:
    title = html.escape(str(result.title or "Untitled note"))
    path = html.escape(str(result.path or ""))
    excerpt = html.escape(clean_note_excerpt(result.body)[:1400].strip() or "No note body.")
    return _shorten_for_telegram(f"<b>{title}</b>\n<u>{path}</u>\n\n<i>Preview</i>\n{excerpt}")


def _format_capture_confirmation(note) -> str:
    # The saved file path is more useful than the AI title because it shows
    # where the note landed in the vault and avoids misleading title-only echoes.
    saved_path = str(getattr(note, "path", "") or getattr(note, "title", "") or "saved note")
    note_id = str(getattr(note, "note_id", "") or "")
    return f"🧠 Memorized: {saved_path}\n📄 ID: {note_id}"


async def _edit_or_reply(message, text: str) -> None:
    safe_text = _shorten_for_telegram(text)
    try:
        await message.edit_text(safe_text)
    except Exception:
        await message.reply_text(safe_text)


def _shorten_for_telegram(text: str, *, limit: int = 3900) -> str:
    text = str(text or "")
    if len(text) <= limit:
        return text
    marker = "\n\n..."
    return text[: max(0, limit - len(marker))].rstrip() + marker


if __name__ == "__main__":
    main()
