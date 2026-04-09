import asyncio
import contextlib
import json
import logging
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

import aiohttp
from bs4 import BeautifulSoup
from telegram import Update
from telegram.error import NetworkError, RetryAfter, TimedOut
from telegram.ext import Application, CommandHandler, ContextTypes

from helpers.runtime_paths import RUNTIME_JSON_DIR, ensure_runtime_dirs
from helpers.process_pool import run_cpu_bound
from useful_bot.exchange_rate_image import render_exchange_rate_card
from useful_bot.exchange_rate_presenter import build_exchange_rate_render_kwargs

MONOBANK_RATES_URL = "https://minfin.com.ua/ua/company/monobank/currency/"
STATE_FILE = RUNTIME_JSON_DIR / "useful_monobank_rates_state.json"
KYIV_TZ = ZoneInfo("Europe/Kyiv")
CHECK_HOURS_KYIV = tuple(range(8, 20))
REQUEST_TIMEOUT_SECONDS = 20
HISTORY_LIMIT = 600
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
}


@dataclass
class RateSnapshot:
    fetched_at: str
    source_date: str
    usd_buy: float
    usd_sell: float
    eur_buy: float
    eur_sell: float


class ExchangeRateHelper:
    helper_name = "Exchange Rate Helper"

    def __init__(self, chat_id: int) -> None:
        ensure_runtime_dirs()
        self._chat_id = chat_id
        self._state = self._load_state()
        self._lock = asyncio.Lock()
        self._monitor_task: Optional[asyncio.Task] = None
        self._http_session: Optional[aiohttp.ClientSession] = None

    def register_handlers(self, application: Application) -> None:
        application.add_handler(CommandHandler("exchange_status", self.status_command))
        application.add_handler(CommandHandler("exchange_checknow", self.checknow_command))

    def start_lines(self) -> list[str]:
        schedule_line = "Schedule: every hour from 08:00 to 19:00 (Kyiv time)"
        return [
            "Exchange rate helper",
            schedule_line,
            "Commands: /exchange_status, /exchange_checknow",
        ]

    async def on_startup(self, application: Application) -> None:
        await self._run_check(application, reason="startup")
        self._monitor_task = asyncio.create_task(
            self._monitor_loop(application),
            name="exchange-rate-helper-monitor",
        )

    async def on_shutdown(self, application: Application) -> None:
        if not self._monitor_task:
            await self._close_http_session()
            return
        self._monitor_task.cancel()
        try:
            await self._monitor_task
        except asyncio.CancelledError:
            pass
        await self._close_http_session()

    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self._run_check(context.application, reason="status")

    async def checknow_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self._run_check(context.application, reason="manual")

    async def _monitor_loop(self, application: Application) -> None:
        while True:
            try:
                now = datetime.now(KYIV_TZ)
                sleep_seconds, next_run, mode = self._seconds_until_next_run(now)
                logging.info(
                    "Next monobank check at %s (%s mode)",
                    next_run.strftime("%Y-%m-%d %H:%M:%S %Z"),
                    mode,
                )
                await asyncio.sleep(sleep_seconds)
                await self._run_check(application, reason="scheduled")
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception("Exchange-rate monitor iteration failed")
                await asyncio.sleep(30)

    async def _run_check(self, application: Application, reason: str) -> bool:
        bot_data = getattr(application, "bot_data", {}) or {}
        service_health = bot_data.get("service_health")
        async with self._lock:
            try:
                snapshot = await self._fetch_snapshot()
            except Exception:
                logging.exception("Could not fetch/parse exchange rates from Minfin")
                if service_health is not None:
                    service_health.record_failure("exchange_rate_check", "fetch_failed")
                return False

            last_snapshot_data = self._state.get("last_snapshot")
            last_snapshot: Optional[RateSnapshot] = None
            if isinstance(last_snapshot_data, dict):
                try:
                    last_snapshot = RateSnapshot(
                        fetched_at=str(last_snapshot_data.get("fetched_at", "")),
                        source_date=str(last_snapshot_data.get("source_date", "")),
                        usd_buy=float(last_snapshot_data["usd_buy"]),
                        usd_sell=float(last_snapshot_data["usd_sell"]),
                        eur_buy=float(last_snapshot_data["eur_buy"]),
                        eur_sell=float(last_snapshot_data["eur_sell"]),
                    )
                except (TypeError, ValueError, KeyError):
                    last_snapshot = None

            changed = last_snapshot is None or self._snapshot_signature(snapshot) != self._snapshot_signature(last_snapshot)
            if not changed:
                if service_health is not None:
                    service_health.record_success("exchange_rate_check", note=f"{reason}:no_change")
                return False

            history = self._state.get("history", [])
            if not isinstance(history, list):
                history = []

            render_kwargs, history_plus_today = build_exchange_rate_render_kwargs(
                snapshot,
                last_snapshot,
                history,
            )

            image_buf = await run_cpu_bound(
                render_exchange_rate_card,
                **render_kwargs,
            )
            sent = False
            for attempt in (1, 2):
                try:
                    await application.bot.send_photo(chat_id=self._chat_id, photo=image_buf)
                    sent = True
                    break
                except RetryAfter as error:
                    await asyncio.sleep(float(error.retry_after) + 1.0)
                except (TimedOut, NetworkError):
                    logging.warning("Temporary Telegram send_photo error (attempt %s)", attempt)

            if not sent:
                logging.error("Could not send exchange-rate update to Telegram; will retry on next check.")
                if service_health is not None:
                    service_health.record_failure("exchange_rate_check", "telegram_send_failed")
                return False

            history.append(
                {
                    "fetched_at": snapshot.fetched_at,
                    "source_date": snapshot.source_date,
                    "usd_buy": snapshot.usd_buy,
                    "usd_sell": snapshot.usd_sell,
                    "eur_buy": snapshot.eur_buy,
                    "eur_sell": snapshot.eur_sell,
                    "usd_spread": usd_spread,
                    "eur_sell_minus_usd_buy": eur_sell_minus_usd_buy,
                }
            )
            history = history[-HISTORY_LIMIT:]
            self._state["history"] = history
            self._state["last_snapshot"] = asdict(snapshot)
            self._save_state()
            if service_health is not None:
                service_health.record_success("exchange_rate_check", note=reason)
            return True

    async def _fetch_snapshot(self) -> RateSnapshot:
        session = await self._get_http_session()
        async with session.get(MONOBANK_RATES_URL) as response:
            response.raise_for_status()
            html = await response.text()
        parsed = self._parse_rates_from_html(html)
        now_kyiv = datetime.now(KYIV_TZ)
        return RateSnapshot(
            fetched_at=now_kyiv.isoformat(timespec="seconds"),
            source_date=parsed["source_date"],
            usd_buy=parsed["usd_buy"],
            usd_sell=parsed["usd_sell"],
            eur_buy=parsed["eur_buy"],
            eur_sell=parsed["eur_sell"],
        )

    @staticmethod
    def _parse_rates_from_html(html: str) -> Dict[str, float | str]:
        soup = BeautifulSoup(html, "lxml")
        rates: Dict[str, Dict[str, Any]] = {}

        for row in soup.select("tbody tr"):
            columns = row.find_all("td")
            if len(columns) < 3:
                continue
            code = columns[0].get_text(strip=True).upper()
            if code not in {"USD", "EUR"}:
                continue

            buy_text = columns[1].get_text(" ", strip=True)
            sell_text = columns[2].get_text(" ", strip=True)
            source_date = columns[3].get_text(" ", strip=True) if len(columns) > 3 else ""

            buy = ExchangeRateHelper._first_float(buy_text)
            sell = ExchangeRateHelper._first_float(sell_text)
            if buy is None or sell is None:
                continue

            rates[code] = {
                "buy": buy,
                "sell": sell,
                "source_date": source_date,
            }

        if "USD" not in rates or "EUR" not in rates:
            raise ValueError("Could not find USD/EUR rows in Minfin HTML")

        source_date = str(rates["USD"].get("source_date") or rates["EUR"].get("source_date") or "")
        return {
            "source_date": source_date,
            "usd_buy": float(rates["USD"]["buy"]),
            "usd_sell": float(rates["USD"]["sell"]),
            "eur_buy": float(rates["EUR"]["buy"]),
            "eur_sell": float(rates["EUR"]["sell"]),
        }

    @staticmethod
    def _first_float(text: str) -> Optional[float]:
        match = re.search(r"[-+]?\d+(?:[.,]\d+)?", text)
        if not match:
            return None
        try:
            return float(match.group(0).replace(",", "."))
        except ValueError:
            return None

    @staticmethod
    def _snapshot_signature(snapshot: RateSnapshot) -> Tuple[float, float, float, float]:
        return (
            round(snapshot.usd_buy, 2),
            round(snapshot.usd_sell, 2),
            round(snapshot.eur_buy, 2),
            round(snapshot.eur_sell, 2),
        )

    def _seconds_until_next_run(self, now: datetime) -> Tuple[float, datetime, str]:
        candidates = []
        for hour in CHECK_HOURS_KYIV:
            candidate = now.replace(hour=hour, minute=0, second=0, microsecond=0)
            if candidate <= now:
                candidate += timedelta(days=1)
            candidates.append(candidate)

        next_run = min(candidates)
        seconds = max(1.0, (next_run - now).total_seconds())
        return seconds, next_run, "hourly-window"

    async def _get_http_session(self) -> aiohttp.ClientSession:
        if self._http_session is None or self._http_session.closed:
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
            connector = aiohttp.TCPConnector(limit=4, ttl_dns_cache=300)
            self._http_session = aiohttp.ClientSession(
                timeout=timeout,
                headers=HTTP_HEADERS,
                connector=connector,
            )
        return self._http_session

    async def _close_http_session(self) -> None:
        if self._http_session is None:
            return
        with contextlib.suppress(Exception):
            await self._http_session.close()
        self._http_session = None

    def _load_state(self) -> Dict[str, Any]:
        if not STATE_FILE.exists():
            return {"last_snapshot": None, "history": []}
        try:
            data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return {
                    "last_snapshot": data.get("last_snapshot"),
                    "history": data.get("history", []),
                }
        except Exception:
            logging.exception("Could not load state from %s", STATE_FILE)
        return {"last_snapshot": None, "history": []}

    def _save_state(self) -> None:
        temp_file = STATE_FILE.with_suffix(".tmp")
        temp_file.write_text(
            json.dumps(self._state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp_file.replace(STATE_FILE)

