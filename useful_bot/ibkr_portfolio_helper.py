from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

from telegram import Update
from telegram.error import NetworkError, RetryAfter, TimedOut
from telegram.ext import Application, CommandHandler, ContextTypes

from helpers.runtime_paths import RUNTIME_JSON_DIR, ensure_runtime_dirs
from useful_bot.ibkr_portfolio_core import (
    NEW_YORK_TZ,
    PortfolioSnapshot,
    build_portfolio_snapshot,
    coerce_optional_float,
    seconds_until_next_run,
    should_run_daily_snapshot,
    should_skip_for_missing_daily_data,
)
from useful_bot.ibkr_portfolio_image import render_ibkr_portfolio_card

try:
    from ibapi.client import EClient
    from ibapi.wrapper import EWrapper

    IBAPI_IMPORT_ERROR: Optional[Exception] = None
except Exception as error:  # pragma: no cover - exercised when dependency is absent
    class EWrapper:  # type: ignore[no-redef]
        pass

    class EClient:  # type: ignore[no-redef]
        def __init__(self, wrapper=None) -> None:
            self.wrapper = wrapper

    IBAPI_IMPORT_ERROR = error


STATE_FILE = RUNTIME_JSON_DIR / "useful_ibkr_portfolio_state.json"
DEFAULT_CONNECT_TIMEOUT_SECONDS = 15.0
DEFAULT_FETCH_TIMEOUT_SECONDS = 30.0
ACCOUNT_SUMMARY_REQ_ID = 7101
PNL_REQ_ID_BASE = 7200
IGNORED_ERROR_CODES = {2104, 2106, 2108, 2158, 1101, 1102}
FLEX_SEND_REQUEST_URL = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/SendRequest"
FLEX_GET_STATEMENT_URL = "https://gdcdyn.interactivebrokers.com/AccountManagement/FlexWebService/GetStatement"
FLEX_API_VERSION = "3"
FLEX_PENDING_ERROR_CODE = "1019"
DEFAULT_FLEX_POLL_INTERVAL_SECONDS = 2.0


@dataclass
class IBKRConnectionSettings:
    account_id: str
    host: str = "127.0.0.1"
    port: int = 7496
    client_id: int = 7
    source: str = "socket"
    query_id: str = ""
    query_token: str = ""
    connect_timeout_seconds: float = DEFAULT_CONNECT_TIMEOUT_SECONDS
    fetch_timeout_seconds: float = DEFAULT_FETCH_TIMEOUT_SECONDS

    @classmethod
    def from_env(cls) -> "IBKRConnectionSettings":
        account_id = os.getenv("IBKR_ACCOUNT_ID")
        if not account_id:
            raise RuntimeError("Missing IBKR_ACCOUNT_ID in .env")

        connect_timeout = float(os.getenv("IBKR_CONNECT_TIMEOUT_SECONDS", DEFAULT_CONNECT_TIMEOUT_SECONDS))
        fetch_timeout = float(os.getenv("IBKR_FETCH_TIMEOUT_SECONDS", DEFAULT_FETCH_TIMEOUT_SECONDS))
        configured_source = (os.getenv("IBKR_SOURCE") or "").strip().lower()
        query_id = (os.getenv("IBKR_QUERY_ID") or "").strip()
        query_token = (os.getenv("IBKR_QUERY_TOKEN") or "").strip()
        use_flex = configured_source == "flex" or (configured_source == "" and query_id and query_token)
        if use_flex:
            if not query_id:
                raise RuntimeError("Missing IBKR_QUERY_ID in .env")
            if not query_token:
                raise RuntimeError("Missing IBKR_QUERY_TOKEN in .env")
            return cls(
                account_id=account_id,
                source="flex",
                query_id=query_id,
                query_token=query_token,
                connect_timeout_seconds=connect_timeout,
                fetch_timeout_seconds=fetch_timeout,
            )

        host = os.getenv("IBKR_HOST")
        port_raw = os.getenv("IBKR_PORT")
        client_id_raw = os.getenv("IBKR_CLIENT_ID")
        if not host:
            raise RuntimeError("Missing IBKR_HOST in .env")
        if not port_raw:
            raise RuntimeError("Missing IBKR_PORT in .env")
        if not client_id_raw:
            raise RuntimeError("Missing IBKR_CLIENT_ID in .env")
        try:
            port = int(port_raw)
            client_id = int(client_id_raw)
        except ValueError as error:
            raise RuntimeError("IBKR_PORT and IBKR_CLIENT_ID must be integers") from error

        return cls(
            host=host,
            port=port,
            client_id=client_id,
            source="socket",
            account_id=account_id,
            connect_timeout_seconds=connect_timeout,
            fetch_timeout_seconds=fetch_timeout,
        )


class IBKRPortfolioHelper:
    helper_name = "IBKR Daily Portfolio"

    def __init__(
        self,
        chat_id: int,
        *,
        snapshot_fetcher: Optional[Callable[[IBKRConnectionSettings, datetime], PortfolioSnapshot]] = None,
        settings_factory: Optional[Callable[[], IBKRConnectionSettings]] = None,
        now_provider: Optional[Callable[[Any], datetime]] = None,
        state_file: Optional[Path] = None,
    ) -> None:
        ensure_runtime_dirs()
        self._chat_id = chat_id
        self._state_file = state_file or STATE_FILE
        self._fetcher = snapshot_fetcher or fetch_ibkr_snapshot
        self._settings_factory = settings_factory or IBKRConnectionSettings.from_env
        self._now_provider = now_provider or datetime.now
        self._state = self._load_state()
        self._lock = asyncio.Lock()
        self._monitor_task: Optional[asyncio.Task] = None

    def register_handlers(self, application: Application) -> None:
        application.add_handler(CommandHandler("ibkr_status", self.status_command))
        application.add_handler(CommandHandler("ibkr_checknow", self.checknow_command))

    def start_lines(self) -> list[str]:
        return [
            "IBKR daily portfolio helper",
            "Schedule: every US trading day at 16:30 America/New_York",
            "Commands: /ibkr_status, /ibkr_checknow",
        ]

    async def on_startup(self, application: Application) -> None:
        now_ny = self._now_provider(NEW_YORK_TZ)
        if should_run_daily_snapshot(now_ny, self._last_trade_date()):
            await self._run_check(application, reason="startup", force=False)
        self._monitor_task = asyncio.create_task(
            self._monitor_loop(application),
            name="ibkr-portfolio-helper-monitor",
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
        if not update.message:
            return
        snapshot = self._last_snapshot()
        if snapshot is None:
            await update.message.reply_text("No IBKR snapshot saved yet.")
            return

        lines = [
            f"Trade date: {snapshot.trade_date}",
            f"Net liquidation: ${snapshot.net_liquidation:,.2f}",
            f"Cash: ${snapshot.cash_value:,.2f}",
            f"Unrealized P&L: ${snapshot.total_unrealized_pnl:,.2f}",
            f"Positions: {len(snapshot.positions)}",
        ]
        await update.message.reply_text("\n".join(lines))

    async def checknow_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self._run_check(context.application, reason="manual", force=True)

    async def _monitor_loop(self, application: Application) -> None:
        while True:
            try:
                now_ny = self._now_provider(NEW_YORK_TZ)
                sleep_seconds, next_run = seconds_until_next_run(now_ny)
                logging.info(
                    "Next IBKR portfolio check at %s",
                    next_run.strftime("%Y-%m-%d %H:%M:%S %Z"),
                )
                await asyncio.sleep(sleep_seconds)
                await self._run_check(application, reason="scheduled", force=False)
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception("IBKR portfolio monitor iteration failed")
                await asyncio.sleep(30)

    async def _run_check(self, application: Application, reason: str, force: bool) -> bool:
        async with self._lock:
            now_ny = self._now_provider(NEW_YORK_TZ)
            if not force and not should_run_daily_snapshot(now_ny, self._last_trade_date()):
                return False

            try:
                settings = self._settings_factory()
            except Exception:
                logging.exception("IBKR helper is not configured correctly")
                return False

            try:
                snapshot = await self._fetch_snapshot_with_retries(settings=settings, now_ny=now_ny)
            except Exception:
                logging.exception("Could not fetch IBKR portfolio snapshot")
                return False

            previous_snapshot = self._last_snapshot()
            if not force and previous_snapshot and previous_snapshot.trade_date == snapshot.trade_date:
                return False
            if should_skip_for_missing_daily_data(snapshot):
                logging.info(
                    "Skipping IBKR card for %s because current-session daily data is incomplete.",
                    snapshot.trade_date,
                )
                return False

            image_buf = render_ibkr_portfolio_card(
                snapshot=snapshot,
                previous_snapshot=previous_snapshot,
            )
            sent = False
            for attempt in (1, 2):
                try:
                    image_buf.seek(0)
                    await application.bot.send_photo(chat_id=self._chat_id, photo=image_buf)
                    sent = True
                    break
                except RetryAfter as error:
                    await asyncio.sleep(float(error.retry_after) + 1.0)
                except (TimedOut, NetworkError):
                    logging.warning("Temporary Telegram send_photo error for IBKR card (attempt %s)", attempt)

            if not sent:
                logging.error("Could not send IBKR card to Telegram; will retry on the next run.")
                return False

            self._state["last_snapshot"] = snapshot.to_dict()
            self._state["last_reason"] = reason
            self._state["last_sent_at"] = datetime.now(NEW_YORK_TZ).isoformat(timespec="seconds")
            self._save_state()
            return True

    async def _fetch_snapshot_with_retries(
        self,
        *,
        settings: IBKRConnectionSettings,
        now_ny: datetime,
    ) -> PortfolioSnapshot:
        last_error: Optional[Exception] = None
        for attempt in range(1, 4):
            try:
                return await asyncio.to_thread(self._fetcher, settings, now_ny)
            except Exception as error:
                last_error = error
                logging.warning("IBKR fetch attempt %s failed: %s", attempt, error)
                if attempt < 3:
                    await asyncio.sleep(3)
        if last_error is None:
            raise RuntimeError("IBKR fetch failed without an exception")
        raise last_error

    def _last_trade_date(self) -> Optional[str]:
        snapshot = self._last_snapshot()
        if snapshot is None:
            return None
        return snapshot.trade_date or None

    def _last_snapshot(self) -> Optional[PortfolioSnapshot]:
        last_snapshot = self._state.get("last_snapshot")
        if not isinstance(last_snapshot, dict):
            return None
        try:
            return PortfolioSnapshot.from_dict(last_snapshot)
        except Exception:
            logging.exception("Could not parse saved IBKR snapshot")
            return None

    def _load_state(self) -> dict[str, Any]:
        if not self._state_file.exists():
            return {"last_snapshot": None}
        try:
            data = json.loads(self._state_file.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
        except Exception:
            logging.exception("Could not load state from %s", self._state_file)
        return {"last_snapshot": None}

    def _save_state(self) -> None:
        temp_file = self._state_file.with_suffix(".tmp")
        temp_file.write_text(
            json.dumps(self._state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp_file.replace(self._state_file)


def fetch_ibkr_snapshot(settings: IBKRConnectionSettings, now_ny: datetime) -> PortfolioSnapshot:
    if settings.source == "flex":
        return fetch_ibkr_flex_snapshot(settings, now_ny)
    if IBAPI_IMPORT_ERROR is not None:
        raise RuntimeError(f"ibapi is unavailable: {IBAPI_IMPORT_ERROR}")
    collector = _IBKRCollector(settings=settings)
    return collector.fetch_snapshot(now_ny=now_ny)


def fetch_ibkr_flex_snapshot(settings: IBKRConnectionSettings, now_ny: datetime) -> PortfolioSnapshot:
    statement_xml = _download_flex_statement_xml(settings)
    return _parse_flex_statement_xml(statement_xml=statement_xml, settings=settings, now_ny=now_ny)


def _download_flex_statement_xml(settings: IBKRConnectionSettings) -> str:
    response_xml = _flex_http_get(
        FLEX_SEND_REQUEST_URL,
        {
            "t": settings.query_token,
            "q": settings.query_id,
            "v": FLEX_API_VERSION,
        },
        timeout_seconds=settings.connect_timeout_seconds,
    )
    response_root = ET.fromstring(response_xml)
    status = (response_root.findtext("Status") or "").strip()
    if status != "Success":
        error_message = (response_root.findtext("ErrorMessage") or "Unknown Flex SendRequest error").strip()
        error_code = (response_root.findtext("ErrorCode") or "").strip()
        raise RuntimeError(f"IBKR Flex SendRequest failed ({error_code}): {error_message}")

    reference_code = (response_root.findtext("ReferenceCode") or "").strip()
    if not reference_code:
        raise RuntimeError("IBKR Flex SendRequest returned no ReferenceCode")
    statement_url = (response_root.findtext("Url") or FLEX_GET_STATEMENT_URL).strip() or FLEX_GET_STATEMENT_URL

    deadline = time.monotonic() + settings.fetch_timeout_seconds
    while True:
        remaining = max(0.1, deadline - time.monotonic())
        if remaining <= 0:
            raise TimeoutError("Timed out waiting for IBKR Flex statement")
        statement_xml = _flex_http_get(
            statement_url,
            {
                "t": settings.query_token,
                "q": reference_code,
                "v": FLEX_API_VERSION,
            },
            timeout_seconds=remaining,
        )
        root = ET.fromstring(statement_xml)
        if root.tag != "FlexStatementResponse":
            return statement_xml

        status = (root.findtext("Status") or "").strip()
        if status == "Success":
            return statement_xml
        error_code = (root.findtext("ErrorCode") or "").strip()
        error_message = (root.findtext("ErrorMessage") or "Unknown Flex GetStatement error").strip()
        if error_code != FLEX_PENDING_ERROR_CODE:
            raise RuntimeError(f"IBKR Flex GetStatement failed ({error_code}): {error_message}")
        time.sleep(min(DEFAULT_FLEX_POLL_INTERVAL_SECONDS, remaining))


def _flex_http_get(url: str, params: dict[str, str], *, timeout_seconds: float) -> str:
    query_string = urllib.parse.urlencode(params)
    request = urllib.request.Request(
        f"{url}?{query_string}",
        method="GET",
        headers={"User-Agent": "LystTgFirefox/IBKRFlex"},
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        return response.read().decode("utf-8")


def _parse_flex_statement_xml(
    *,
    statement_xml: str,
    settings: IBKRConnectionSettings,
    now_ny: datetime,
) -> PortfolioSnapshot:
    root = ET.fromstring(statement_xml)
    if root.tag != "FlexQueryResponse":
        raise RuntimeError(f"Unexpected IBKR Flex statement root: {root.tag}")

    statements = root.findall("./FlexStatements/FlexStatement")
    if not statements:
        raise RuntimeError("IBKR Flex statement did not contain any FlexStatement nodes")

    statement = next(
        (item for item in statements if (item.attrib.get("accountId") or "").strip() == settings.account_id),
        statements[0],
    )
    statement_account_id = (statement.attrib.get("accountId") or "").strip()
    if statement_account_id and statement_account_id != settings.account_id:
        raise RuntimeError(
            f"Configured IBKR account {settings.account_id} is not present in the returned Flex statement."
        )

    nav_node = statement.find("./ChangeInNAV")
    if nav_node is None:
        raise RuntimeError("IBKR Flex statement is missing ChangeInNAV")

    nav_starting_value = coerce_optional_float(nav_node.attrib.get("startingValue"))
    net_liquidation = coerce_optional_float(nav_node.attrib.get("endingValue"))
    trade_date_raw = (statement.attrib.get("toDate") or nav_node.attrib.get("toDate") or "").strip()
    trade_date = _format_flex_trade_date(trade_date_raw) or now_ny.date().isoformat()
    source_from_date = _format_flex_trade_date((statement.attrib.get("fromDate") or "").strip()) or ""
    source_to_date = _format_flex_trade_date((statement.attrib.get("toDate") or "").strip()) or trade_date
    source_period = str(statement.attrib.get("period", "") or "")

    mtm_by_key: dict[int | str, float] = {}
    for mtm_node in statement.findall("./MTMPerformanceSummaryInBase/MTMPerformanceSummaryUnderlying"):
        position_key = _flex_position_key(
            symbol=mtm_node.attrib.get("symbol"),
            con_id=mtm_node.attrib.get("conid"),
        )
        if position_key is None:
            continue
        daily_pnl = coerce_optional_float(mtm_node.attrib.get("total"))
        if daily_pnl is not None:
            mtm_by_key[position_key] = daily_pnl

    raw_positions: list[dict[str, Any]] = []
    total_position_value = 0.0
    for open_position in statement.findall("./OpenPositions/OpenPosition"):
        position_value = coerce_optional_float(open_position.attrib.get("positionValue"))
        if position_value is not None:
            total_position_value += position_value
        raw_position = _parse_flex_open_position(
            node=open_position,
            account_id=settings.account_id,
            daily_pnl_map=mtm_by_key,
        )
        if raw_position is not None:
            raw_positions.append(raw_position)

    raw_trades: list[dict[str, Any]] = []
    for trade_node in statement.findall("./Trades/*"):
        raw_trade = _parse_flex_trade(node=trade_node, account_id=settings.account_id)
        if raw_trade is not None:
            raw_trades.append(raw_trade)

    raw_corporate_actions: list[dict[str, Any]] = []
    for action_node in statement.findall("./CorporateActions/*"):
        raw_action = dict(action_node.attrib)
        if raw_action:
            raw_corporate_actions.append(raw_action)

    cash_value = None
    if net_liquidation is not None:
        cash_value = net_liquidation - total_position_value

    return build_portfolio_snapshot(
        fetched_at=now_ny,
        trade_date=trade_date,
        account_id=settings.account_id,
        nav_starting_value=nav_starting_value,
        net_liquidation=net_liquidation,
        cash_value=cash_value,
        raw_positions=raw_positions,
        raw_trades=raw_trades,
        raw_corporate_actions=raw_corporate_actions,
        source_from_date=source_from_date,
        source_to_date=source_to_date,
        source_period=source_period,
    )


def _parse_flex_open_position(
    *,
    node: ET.Element,
    account_id: str,
    daily_pnl_map: dict[int | str, float],
) -> Optional[dict[str, Any]]:
    symbol = str(node.attrib.get("symbol", "") or "").upper()
    position_key = _flex_position_key(symbol=symbol, con_id=node.attrib.get("conid"))
    if position_key is None:
        return None

    asset_category = str(node.attrib.get("assetCategory", "") or "")
    return {
        "symbol": symbol,
        "con_id": int(node.attrib.get("conid", 0) or 0),
        "sec_type": asset_category,
        "currency": str(node.attrib.get("currency", "USD") or "USD"),
        "quantity": coerce_optional_float(node.attrib.get("position")) or 0.0,
        "market_price": coerce_optional_float(node.attrib.get("markPrice")) or 0.0,
        "market_value": coerce_optional_float(node.attrib.get("positionValue")),
        "average_cost": coerce_optional_float(node.attrib.get("costBasisPrice"))
        or coerce_optional_float(node.attrib.get("openPrice")),
        "unrealized_pnl": coerce_optional_float(node.attrib.get("fifoPnlUnrealized")),
        "daily_pnl": daily_pnl_map.get(position_key),
        "account": account_id,
    }


def _parse_flex_trade(*, node: ET.Element, account_id: str) -> Optional[dict[str, Any]]:
    symbol = str(node.attrib.get("symbol", "") or "").upper()
    trade_date = _format_flex_trade_date(
        str(
            node.attrib.get("tradeDate")
            or node.attrib.get("reportDate")
            or node.attrib.get("dateTime")
            or ""
        ).strip()
    )
    if not symbol or not trade_date:
        return None

    quantity = coerce_optional_float(node.attrib.get("quantity")) or 0.0
    buy_sell = str(node.attrib.get("buySell", "") or "").strip().upper()
    if not buy_sell:
        if quantity > 0:
            buy_sell = "BUY"
        elif quantity < 0:
            buy_sell = "SELL"

    return {
        "symbol": symbol,
        "con_id": int(node.attrib.get("conid", 0) or 0),
        "sec_type": str(node.attrib.get("assetCategory", "") or ""),
        "trade_date": trade_date,
        "buy_sell": buy_sell,
        "quantity": quantity,
        "trade_price": coerce_optional_float(node.attrib.get("tradePrice")),
        "trade_money": coerce_optional_float(node.attrib.get("tradeMoney")),
        "proceeds": coerce_optional_float(node.attrib.get("proceeds")),
        "net_cash": coerce_optional_float(node.attrib.get("netCash")),
        "commission": coerce_optional_float(node.attrib.get("ibCommission")),
        "currency": str(node.attrib.get("currency", "USD") or "USD"),
        "account": account_id,
    }


def _flex_position_key(*, symbol: Optional[str], con_id: Optional[str]) -> Optional[int | str]:
    parsed_con_id = int(con_id or 0)
    if parsed_con_id:
        return parsed_con_id
    normalized_symbol = str(symbol or "").strip().upper()
    if normalized_symbol:
        return normalized_symbol
    return None


def _format_flex_trade_date(value: str) -> Optional[str]:
    normalized = value.split(";")[0].strip()
    if len(normalized) == 10 and normalized[4] == "-" and normalized[7] == "-":
        return normalized
    digits = "".join(character for character in normalized if character.isdigit())
    if len(digits) != 8:
        return None
    return f"{digits[0:4]}-{digits[4:6]}-{digits[6:8]}"


class _IBKRCollector(EWrapper, EClient):
    def __init__(self, *, settings: IBKRConnectionSettings) -> None:
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        self._settings = settings
        self._connected_event = threading.Event()
        self._account_summary_event = threading.Event()
        self._portfolio_download_event = threading.Event()
        self._pnl_event = threading.Event()
        self._fatal_errors: list[str] = []
        self._account_summary_values: dict[str, float] = {}
        self._raw_positions: dict[int | str, dict[str, Any]] = {}
        self._pnl_req_to_key: dict[int, int | str] = {}
        self._pending_pnl_req_ids: set[int] = set()
        self._received_pnl_req_ids: set[int] = set()
        self._reader_thread: Optional[threading.Thread] = None

    def fetch_snapshot(self, *, now_ny: datetime) -> PortfolioSnapshot:
        deadline = time.monotonic() + self._settings.fetch_timeout_seconds
        self.connect(self._settings.host, self._settings.port, self._settings.client_id)
        self._reader_thread = threading.Thread(
            target=self.run,
            name="ibkr-api-loop",
            daemon=True,
        )
        self._reader_thread.start()
        try:
            self.reqManagedAccts()
            self._wait_for(self._connected_event, self._settings.connect_timeout_seconds, "IBKR connection")
            self.reqAccountSummary(
                ACCOUNT_SUMMARY_REQ_ID,
                "All",
                "NetLiquidation,TotalCashValue",
            )
            self.reqAccountUpdates(True, self._settings.account_id)
            self._wait_for(self._account_summary_event, self._remaining_time(deadline), "account summary")
            self._wait_for(self._portfolio_download_event, self._remaining_time(deadline), "portfolio download")
            self._request_position_pnl()
            if self._pending_pnl_req_ids:
                self._pnl_event.wait(self._remaining_time(deadline))

            return build_portfolio_snapshot(
                fetched_at=now_ny,
                trade_date=now_ny.date().isoformat(),
                account_id=self._settings.account_id,
                net_liquidation=self._account_summary_values.get("NetLiquidation"),
                cash_value=self._account_summary_values.get("TotalCashValue"),
                raw_positions=self._raw_positions.values(),
            )
        finally:
            self._teardown()

    def nextValidId(self, orderId: int) -> None:  # noqa: N802 - IBKR naming
        self._connected_event.set()

    def managedAccounts(self, accountsList: str) -> None:  # noqa: N802 - IBKR naming
        available_accounts = [item.strip() for item in accountsList.split(",") if item.strip()]
        if available_accounts and self._settings.account_id not in available_accounts:
            self._fatal_errors.append(
                f"Configured IBKR account {self._settings.account_id} is not visible to this API session."
            )
            self._connected_event.set()
            self._account_summary_event.set()
            self._portfolio_download_event.set()

    def accountSummary(  # noqa: N802 - IBKR naming
        self,
        reqId: int,
        account: str,
        tag: str,
        value: str,
        currency: str,
    ) -> None:
        if reqId != ACCOUNT_SUMMARY_REQ_ID or account != self._settings.account_id:
            return
        parsed = coerce_optional_float(value)
        if parsed is not None:
            self._account_summary_values[tag] = parsed

    def accountSummaryEnd(self, reqId: int) -> None:  # noqa: N802 - IBKR naming
        if reqId == ACCOUNT_SUMMARY_REQ_ID:
            self._account_summary_event.set()

    def updatePortfolio(  # noqa: N802 - IBKR naming
        self,
        contract,
        position,
        marketPrice,
        marketValue,
        averageCost,
        unrealizedPNL,
        realizedPNL,
        accountName,
    ) -> None:
        if accountName != self._settings.account_id:
            return
        con_id = int(getattr(contract, "conId", 0) or 0)
        key: int | str = con_id if con_id else f"{getattr(contract, 'symbol', 'UNKNOWN')}:{accountName}"
        self._raw_positions[key] = {
            "symbol": str(getattr(contract, "symbol", "") or "").upper(),
            "con_id": con_id,
            "sec_type": str(getattr(contract, "secType", "") or ""),
            "currency": str(getattr(contract, "currency", "USD") or "USD"),
            "quantity": coerce_optional_float(position) or 0.0,
            "market_price": coerce_optional_float(marketPrice) or 0.0,
            "market_value": coerce_optional_float(marketValue),
            "average_cost": coerce_optional_float(averageCost),
            "unrealized_pnl": coerce_optional_float(unrealizedPNL),
            "daily_pnl": None,
            "account": accountName,
        }

    def accountDownloadEnd(self, accountName: str) -> None:  # noqa: N802 - IBKR naming
        if accountName == self._settings.account_id:
            self._portfolio_download_event.set()

    def pnlSingle(  # noqa: N802 - IBKR naming
        self,
        reqId: int,
        pos: int,
        dailyPnL,
        unrealizedPnL,
        realizedPnL,
        value,
    ) -> None:
        key = self._pnl_req_to_key.get(reqId)
        if key is None:
            return
        position = self._raw_positions.get(key)
        if position is None:
            return
        position["daily_pnl"] = coerce_optional_float(dailyPnL)
        market_value = coerce_optional_float(value)
        if market_value is not None:
            position["market_value"] = market_value
        unrealized = coerce_optional_float(unrealizedPnL)
        if unrealized is not None:
            position["unrealized_pnl"] = unrealized
        self._received_pnl_req_ids.add(reqId)
        if self._pending_pnl_req_ids.issubset(self._received_pnl_req_ids):
            self._pnl_event.set()

    def error(self, reqId: int, errorCode: int, errorString: str, advancedOrderRejectJson: str = "") -> None:
        if errorCode in IGNORED_ERROR_CODES:
            return
        message = f"IBKR error {errorCode} (req {reqId}): {errorString}"
        if errorCode >= 2000 or errorCode in {502, 504, 1100}:
            self._fatal_errors.append(message)
            self._connected_event.set()
            self._account_summary_event.set()
            self._portfolio_download_event.set()
            self._pnl_event.set()
        else:
            logging.info(message)

    def _request_position_pnl(self) -> None:
        next_req_id = PNL_REQ_ID_BASE
        for key, position in self._raw_positions.items():
            sec_type = str(position.get("sec_type", ""))
            quantity = coerce_optional_float(position.get("quantity")) or 0.0
            con_id = int(position.get("con_id", 0) or 0)
            if not con_id or sec_type.upper() not in {"STK", "ETF"} or abs(quantity) < 1e-9:
                continue
            req_id = next_req_id
            next_req_id += 1
            self._pnl_req_to_key[req_id] = key
            self._pending_pnl_req_ids.add(req_id)
            self.reqPnLSingle(req_id, self._settings.account_id, "", con_id)
        if not self._pending_pnl_req_ids:
            self._pnl_event.set()

    def _wait_for(self, event: threading.Event, timeout: float, label: str) -> None:
        if self._fatal_errors:
            raise RuntimeError("; ".join(self._fatal_errors))
        if timeout <= 0:
            raise TimeoutError(f"Timed out waiting for {label}")
        if not event.wait(timeout):
            raise TimeoutError(f"Timed out waiting for {label}")
        if self._fatal_errors:
            raise RuntimeError("; ".join(self._fatal_errors))

    @staticmethod
    def _remaining_time(deadline: float) -> float:
        return max(0.1, deadline - time.monotonic())

    def _teardown(self) -> None:
        try:
            for req_id in list(self._pending_pnl_req_ids):
                try:
                    self.cancelPnLSingle(req_id)
                except Exception:
                    pass
            try:
                self.cancelAccountSummary(ACCOUNT_SUMMARY_REQ_ID)
            except Exception:
                pass
            try:
                self.reqAccountUpdates(False, self._settings.account_id)
            except Exception:
                pass
        finally:
            try:
                self.disconnect()
            finally:
                if self._reader_thread and self._reader_thread.is_alive():
                    self._reader_thread.join(timeout=1.0)
