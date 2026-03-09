import asyncio
import json
import logging
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from statistics import mean
from typing import Any, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

import aiohttp
from bs4 import BeautifulSoup
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from helpers.runtime_paths import RUNTIME_JSON_DIR, ensure_runtime_dirs
from useful_bot.exchange_rate_image import render_exchange_rate_card

MONOBANK_RATES_URL = "https://minfin.com.ua/ua/company/monobank/currency/"
STATE_FILE = RUNTIME_JSON_DIR / "useful_monobank_rates_state.json"
KYIV_TZ = ZoneInfo("Europe/Kyiv")
CHECK_TIMES_KYIV = ((12, 0),)
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

    def register_handlers(self, application: Application) -> None:
        application.add_handler(CommandHandler("exchange_status", self.status_command))
        application.add_handler(CommandHandler("exchange_checknow", self.checknow_command))

    def start_lines(self) -> list[str]:
        return [
            "Exchange rate helper",
            "Schedule: once per day at 12:00 (Kyiv time)",
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
            return
        self._monitor_task.cancel()
        try:
            await self._monitor_task
        except asyncio.CancelledError:
            pass

    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self._run_check(context.application, reason="status")

    async def checknow_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self._run_check(context.application, reason="manual")

    async def _monitor_loop(self, application: Application) -> None:
        while True:
            now = datetime.now(KYIV_TZ)
            sleep_seconds, next_run = self._seconds_until_next_run(now)
            logging.info("Next monobank check at %s", next_run.strftime("%Y-%m-%d %H:%M:%S %Z"))
            await asyncio.sleep(sleep_seconds)
            await self._run_check(application, reason="scheduled")

    async def _run_check(self, application: Application, reason: str) -> bool:
        async with self._lock:
            try:
                snapshot = await self._fetch_snapshot()
            except Exception:
                logging.exception("Could not fetch/parse exchange rates from Minfin")
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
            if not changed and reason not in ("scheduled", "manual", "status"):
                return False

            history = self._state.get("history", [])
            if not isinstance(history, list):
                history = []

            usd_spread = snapshot.usd_sell - snapshot.usd_buy
            eur_sell_minus_usd_buy = snapshot.eur_sell - snapshot.usd_buy
            usd_average = self._mean_from_history(history, "usd_spread")
            cross_average = self._mean_from_history(history, "eur_sell_minus_usd_buy")
            usd_spread_min = self._min_from_history(history, "usd_spread")
            usd_spread_max = self._max_from_history(history, "usd_spread")
            cross_min = self._min_from_history(history, "eur_sell_minus_usd_buy")
            cross_max = self._max_from_history(history, "eur_sell_minus_usd_buy")

            image_buf = render_exchange_rate_card(
                usd_buy=snapshot.usd_buy,
                usd_sell=snapshot.usd_sell,
                eur_buy=snapshot.eur_buy,
                eur_sell=snapshot.eur_sell,
                prev_usd_buy=last_snapshot.usd_buy if last_snapshot else None,
                prev_usd_sell=last_snapshot.usd_sell if last_snapshot else None,
                prev_eur_buy=last_snapshot.eur_buy if last_snapshot else None,
                prev_eur_sell=last_snapshot.eur_sell if last_snapshot else None,
                usd_spread=usd_spread,
                eur_sell_minus_usd_buy=eur_sell_minus_usd_buy,
                usd_spread_avg=usd_average,
                cross_avg=cross_average,
                usd_spread_min=usd_spread_min,
                usd_spread_max=usd_spread_max,
                cross_min=cross_min,
                cross_max=cross_max,
            )
            await application.bot.send_photo(
                chat_id=self._chat_id, photo=image_buf,
            )

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
            return True

    async def _fetch_snapshot(self) -> RateSnapshot:
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        async with aiohttp.ClientSession(timeout=timeout, headers=HTTP_HEADERS) as session:
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

    @staticmethod
    def _mean_from_history(history: list[dict[str, Any]], key: str) -> Optional[float]:
        values = []
        for item in history:
            try:
                values.append(float(item[key]))
            except (KeyError, TypeError, ValueError):
                continue
        return mean(values) if values else None

    @staticmethod
    def _min_from_history(history: list[dict[str, Any]], key: str) -> Optional[float]:
        values = []
        for item in history:
            try:
                values.append(float(item[key]))
            except (KeyError, TypeError, ValueError):
                continue
        return min(values) if values else None

    @staticmethod
    def _max_from_history(history: list[dict[str, Any]], key: str) -> Optional[float]:
        values = []
        for item in history:
            try:
                values.append(float(item[key]))
            except (KeyError, TypeError, ValueError):
                continue
        return max(values) if values else None

    @staticmethod
    def _seconds_until_next_run(now: datetime) -> Tuple[float, datetime]:
        candidates = []
        for hour, minute in CHECK_TIMES_KYIV:
            candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if candidate <= now:
                candidate += timedelta(days=1)
            candidates.append(candidate)

        next_run = min(candidates)
        seconds = max(1.0, (next_run - now).total_seconds())
        return seconds, next_run

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

