from __future__ import annotations

import asyncio
import io
import logging
import os
import random
import re
from dataclasses import dataclass
from typing import Iterable, Optional

import requests
from telegram import Bot
from telegram.constants import ParseMode

from config import RUN_ACCEPT_LANGUAGE, RUN_USER_AGENT
from config_auto_ria_urls import AUTO_RIA_URLS
from helpers.analytics_events import AnalyticsSink, fingerprint_url
from helpers.auto_ria.models import AutoRiaListing, VinDecoderDetails
from helpers.auto_ria.parsing import (
    build_auto_ria_caption,
    build_auto_ria_sold_caption,
    extract_vin_from_detail_html,
    is_auto_ria_sold_detail_html,
    normalize_auto_ria_search_url,
    parse_nhtsa_vpic_payload,
    parse_auto_ria_search_html,
)
from helpers.auto_ria.storage import AutoRiaSentItem, AutoRiaStorage
from helpers.image_pipeline import upscale_image_bytes_for_telegram_sync
from helpers.auto_ria.vin import build_vin_decoder_url
from helpers.marketplace_sender import async_retry
from helpers.process_pool import run_cpu_bound
from helpers.runtime_paths import AUTO_RIA_ITEMS_DB_FILE, SCRAPER_RUNS_JSONL_FILE
from helpers.scraper_stats import RunStatsCollector, utc_now_iso
from helpers.service_health import build_service_health


@dataclass(frozen=True)
class AutoRiaSource:
    url: str
    url_name: str


@dataclass(frozen=True)
class AutoRiaSendOutcome:
    sent: bool
    message_id: int | None = None
    message_kind: str = "photo"


class AutoRiaBotRuntime:
    def __init__(
        self,
        *,
        bot_token: str,
        chat_id: int,
        sources: Iterable[dict[str, str]],
        check_interval_sec: int = 900,
        check_jitter_sec: int = 60,
        request_timeout_sec: int = 30,
        connector_limit: int = 8,
        service_health=None,
        analytics_sink: AnalyticsSink | None = None,
        logger=None,
    ) -> None:
        self._bot = Bot(token=bot_token)
        self._chat_id = str(chat_id)
        self._sources = [
            # Auto RIA supports page size through URL parameters, so normalize once at
            # startup instead of depending on a brittle UI pagination click flow.
            AutoRiaSource(url=normalize_auto_ria_search_url(source["url"]), url_name=source["url_name"])
            for source in sources
            if source.get("url") and source.get("url_name")
        ]
        # This bot is intended to check Auto RIA on an hourly cadence, so the default
        # interval is a true hour instead of the shorter marketplace polling cadence.
        self._check_interval_sec = max(3600, int(check_interval_sec))
        self._check_jitter_sec = max(0, int(check_jitter_sec))
        self._request_timeout_sec = max(5, int(request_timeout_sec))
        self._storage = AutoRiaStorage(AUTO_RIA_ITEMS_DB_FILE)
        self._service_health = service_health or build_service_health("auto-ria-bot")
        self._analytics_sink = analytics_sink or AnalyticsSink()
        self._logger = logger or logging.getLogger("auto_ria_bot")
        self._http_semaphore = asyncio.Semaphore(max(2, connector_limit))
        self._send_semaphore = asyncio.Semaphore(2)
        self._connector_limit = max(4, int(connector_limit))

    async def start(self) -> None:
        self._storage.create_tables()
        await self._bot.initialize()
        self._service_health.start()
        self._service_health.mark_ready("auto ria bot starting")

    async def shutdown(self) -> None:
        self._service_health.mark_stopping("auto ria bot stopping")
        await self._bot.shutdown()

    async def run_forever(self) -> None:
        while True:
            started = asyncio.get_running_loop().time()
            try:
                sent_count = await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._service_health.record_failure("auto_ria_run", exc)
                self._logger.exception("Auto RIA run failed")
            else:
                self._service_health.record_success(
                    "auto_ria_run",
                    duration_seconds=asyncio.get_running_loop().time() - started,
                    note=f"sent={sent_count}",
                )

            sleep_for = self._check_interval_sec
            if self._check_jitter_sec:
                sleep_for += random.randint(0, self._check_jitter_sec)
            await asyncio.sleep(sleep_for)

    async def run_once(self) -> int:
        sent_count = 0
        run_seen_ids: set[str] = set()
        run_stats = RunStatsCollector("auto_ria")
        run_stats.set_deploy_metadata(sources_total=len(self._sources))

        await self._refresh_sold_statuses()

        for source in self._sources:
            run_stats.inc("sources_attempted")
            html_text = await self._fetch_text(source.url)
            if not html_text:
                run_stats.inc("sources_failed")
                run_stats.record_source(source.url_name, status="fetch_failed", url=source.url)
                run_stats.record_error("fetch_failed", source=source.url_name)
                continue

            listings = parse_auto_ria_search_html(html_text)
            run_stats.inc("items_seen", len(listings))
            known_ids = self._storage.fetch_seen_ids([listing.id for listing in listings])
            if not listings:
                run_stats.inc("sources_empty")
                run_stats.record_source(source.url_name, status="empty", url=source.url, items_seen=0)
            else:
                run_stats.inc("sources_with_items")

            source_sent_count = 0
            for listing in listings:
                if listing.id in run_seen_ids or listing.id in known_ids:
                    run_stats.inc("items_skipped_known")
                    continue
                sent = await self._process_listing(listing)
                run_seen_ids.add(listing.id)
                if sent:
                    sent_count += 1
                    source_sent_count += 1
                    run_stats.inc("items_sent")
                else:
                    run_stats.inc("send_failed")
            if listings:
                run_stats.record_source(
                    source.url_name,
                    status="ok",
                    url=source.url,
                    items_seen=len(listings),
                    sent_items=source_sent_count,
                )

        run_stats.set_coverage(
            expected=len(self._sources),
            attempted=run_stats.counters.get("sources_attempted", 0),
            completed=run_stats.counters.get("sources_with_items", 0) + run_stats.counters.get("sources_empty", 0),
            blocked=run_stats.counters.get("sources_failed", 0),
        )
        run_stats.set_notification_funnel(
            seen=run_stats.counters.get("items_seen", 0),
            candidates=run_stats.counters.get("items_sent", 0) + run_stats.counters.get("send_failed", 0),
            new=run_stats.counters.get("items_sent", 0),
            sent=run_stats.counters.get("items_sent", 0),
            failed=run_stats.counters.get("send_failed", 0),
            skipped=run_stats.counters.get("items_skipped_known", 0),
        )
        run_stats.set_data_freshness(latest_source_check_utc=utc_now_iso())
        run_stats.set_resource_snapshot()
        run_stats.write_jsonl(
            SCRAPER_RUNS_JSONL_FILE,
            run_stats.finish(outcome="error" if run_stats.counters.get("sources_failed", 0) else "success"),
        )
        return sent_count

    async def _process_listing(self, listing: AutoRiaListing) -> bool:
        vin_details = await self._fetch_vin_details_for_listing(listing)
        caption = build_auto_ria_caption(
            listing,
            transmission=vin_details.transmission,
            trim=vin_details.trim,
        )

        sent = await self._send_listing_alert(caption, listing.image_url)
        if sent.sent:
            self._storage.mark_sent(
                car_id=listing.id,
                title=listing.title,
                url=listing.url,
                price_usd=listing.price_usd,
                message_id=sent.message_id,
                message_kind=sent.message_kind,
                caption=caption,
            )
        return bool(sent.sent)

    async def _refresh_sold_statuses(self) -> None:
        for item in self._storage.fetch_active_sent_items():
            if not item.message_id or not item.caption:
                continue
            html_text = await self._fetch_text(item.url)
            if not is_auto_ria_sold_detail_html(html_text or ""):
                continue
            edited = await self._edit_sold_message(item)
            if edited:
                self._storage.mark_sold(car_id=item.car_id)

    async def _edit_sold_message(self, item: AutoRiaSentItem) -> bool:
        listing = AutoRiaListing(
            id=item.car_id,
            url=item.url,
            title=item.title,
            subtitle="",
            price_usd=item.price_usd,
            price_text="",
            mileage_text="",
            fuel_engine_text="",
            image_url=None,
        )
        caption = build_auto_ria_sold_caption(listing, original_caption=item.caption)
        try:
            # Sold pages stay HTTP 200, so editing the existing alert is less noisy than
            # sending a new "sold" message and keeps the original car context in place.
            if item.message_kind == "text":
                await self._bot.edit_message_text(
                    chat_id=self._chat_id,
                    message_id=item.message_id,
                    text=caption,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=False,
                )
            else:
                await self._bot.edit_message_caption(
                    chat_id=self._chat_id,
                    message_id=item.message_id,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                )
            return True
        except Exception as exc:
            self._logger.warning("Failed to mark Auto RIA message as sold for %s: %s", item.url, exc)
            return False

    async def _send_listing_alert(self, caption: str, image_url: Optional[str]) -> AutoRiaSendOutcome:
        if not self._is_valid_image_url(image_url):
            outcome = await self._coerce_send_outcome(await self._send_text_alert(caption), message_kind="text")
            self._record_send_analytics(event="sent" if outcome.sent else "failed", message_kind="text", fallback_reason="invalid_or_missing_url")
            return outcome

        try:
            raw = await self._download_image_bytes(image_url)
        except Exception as exc:
            self._logger.warning("Photo download failed; sending Auto RIA text alert: %s", exc)
            raw = None
        if not raw:
            outcome = await self._coerce_send_outcome(await self._send_text_alert(caption), message_kind="text")
            self._record_send_analytics(event="sent" if outcome.sent else "failed", message_kind="text", image_url=image_url, fallback_reason="download_failed")
            return outcome

        photo_bytes = await run_cpu_bound(
            upscale_image_bytes_for_telegram_sync,
            raw,
            logger=self._logger,
        )
        photo_bytes = photo_bytes or raw
        result = await self._send_photo_alert(photo_bytes, caption)
        outcome = await self._coerce_send_outcome(result, message_kind="photo")
        if outcome.sent:
            self._record_send_analytics(
                event="sent",
                message_kind="photo",
                image_url=image_url,
                raw_bytes=len(raw),
                output_bytes=len(photo_bytes),
            )
            return outcome

        self._logger.warning("Photo send failed; sending Auto RIA text alert")
        outcome = await self._coerce_send_outcome(await self._send_text_alert(caption), message_kind="text")
        self._record_send_analytics(
            event="sent" if outcome.sent else "failed",
            message_kind="text",
            image_url=image_url,
            raw_bytes=len(raw),
            output_bytes=len(photo_bytes),
            fallback_reason="photo_send_failed",
        )
        return outcome

    def _record_send_analytics(
        self,
        *,
        event: str,
        message_kind: str,
        image_url: Optional[str] = None,
        raw_bytes: int = 0,
        output_bytes: int = 0,
        fallback_reason: str = "",
    ) -> None:
        try:
            payload = {
                "event": event,
                "message_kind": message_kind,
                "raw_bytes": int(raw_bytes or 0),
                "output_bytes": int(output_bytes or 0),
                "fallback_reason": fallback_reason,
            }
            payload.update(fingerprint_url(image_url))
            self._analytics_sink.append_event("auto_ria_send", payload)
            self._analytics_sink.add_daily_counters(
                "auto_ria_send",
                dimensions={"event": event, "message_kind": message_kind},
                counters={
                    "messages": 1,
                    "failures": 1 if event == "failed" else 0,
                    "raw_bytes": int(raw_bytes or 0),
                    "output_bytes": int(output_bytes or 0),
                },
            )
        except Exception:
            # Auto RIA dedupe/send flow must not depend on analytics file availability.
            return

    async def _coerce_send_outcome(self, result, *, message_kind: str) -> AutoRiaSendOutcome:
        if isinstance(result, AutoRiaSendOutcome):
            return result
        # Telegram timeout may mean the message was delivered without a response; storing
        # no message_id preserves duplicate suppression while sold edits remain unavailable.
        if result is True:
            return AutoRiaSendOutcome(sent=True, message_id=None, message_kind=message_kind)
        return AutoRiaSendOutcome(sent=False, message_id=None, message_kind=message_kind)

    @async_retry(max_retries=3, backoff_base=2.0, assume_timeout_success=True)
    async def _send_text_alert(self, caption: str):
        async with self._send_semaphore:
            message = await self._bot.send_message(
                chat_id=self._chat_id,
                text=caption,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=False,
            )
        return AutoRiaSendOutcome(sent=True, message_id=getattr(message, "message_id", None), message_kind="text")

    @async_retry(max_retries=3, backoff_base=2.0, assume_timeout_success=True)
    async def _send_photo_alert(self, photo_bytes: bytes, caption: str):
        async with self._send_semaphore:
            message = await self._bot.send_photo(
                chat_id=self._chat_id,
                photo=io.BytesIO(photo_bytes),
                caption=caption,
                parse_mode=ParseMode.HTML,
            )
        return AutoRiaSendOutcome(sent=True, message_id=getattr(message, "message_id", None), message_kind="photo")

    async def _fetch_vin_details_for_listing(self, listing: AutoRiaListing) -> VinDecoderDetails:
        detail_html = await self._fetch_text(listing.url)
        if not detail_html:
            return VinDecoderDetails()

        vin = extract_vin_from_detail_html(detail_html)
        if not vin:
            return VinDecoderDetails()

        model_year = self._extract_model_year(listing.title)
        decoder_payload = await self._fetch_json(
            build_vin_decoder_url(vin, model_year=model_year),
            allow_statuses={403, 404, 429},
        )
        if not decoder_payload:
            # VIN enrichment is intentionally best-effort so third-party decoder failures
            # never suppress a real car alert that already matched the monitored search page.
            return VinDecoderDetails()

        return parse_nhtsa_vpic_payload(decoder_payload)

    async def _fetch_text(self, url: str, *, allow_statuses: Optional[set[int]] = None) -> Optional[str]:
        try:
            return await asyncio.to_thread(self._fetch_text_sync, url, allow_statuses)
        except Exception as exc:
            self._logger.warning("HTTP fetch failed for %s: %s", url, exc)
            return None

    async def _fetch_json(self, url: str, *, allow_statuses: Optional[set[int]] = None) -> Optional[dict]:
        try:
            return await asyncio.to_thread(self._fetch_json_sync, url, allow_statuses)
        except Exception as exc:
            self._logger.warning("JSON fetch failed for %s: %s", url, exc)
            return None

    def _fetch_text_sync(self, url: str, allow_statuses: Optional[set[int]] = None) -> Optional[str]:
        headers = {
            "User-Agent": RUN_USER_AGENT,
            "Accept-Language": RUN_ACCEPT_LANGUAGE,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        response = requests.get(url, headers=headers, timeout=self._request_timeout_sec)
        if allow_statuses and response.status_code in allow_statuses:
            self._logger.info("Allowed non-success status %s for %s", response.status_code, url)
            return None
        response.raise_for_status()
        return response.text

    def _fetch_json_sync(self, url: str, allow_statuses: Optional[set[int]] = None) -> Optional[dict]:
        headers = {
            "User-Agent": RUN_USER_AGENT,
            "Accept-Language": RUN_ACCEPT_LANGUAGE,
            "Accept": "application/json,text/plain,*/*",
        }
        response = requests.get(url, headers=headers, timeout=self._request_timeout_sec)
        if allow_statuses and response.status_code in allow_statuses:
            self._logger.info("Allowed non-success status %s for %s", response.status_code, url)
            return None
        response.raise_for_status()
        return response.json()

    def _extract_model_year(self, title: str) -> Optional[int]:
        match = re.search(r"\b(19|20)\d{2}\b", title or "")
        return int(match.group(0)) if match else None

    def _is_valid_image_url(self, image_url: Optional[str]) -> bool:
        return bool(image_url and image_url.startswith(("http://", "https://")))

    @property
    def _download_image_bytes(self):
        async def _download(url: str) -> Optional[bytes]:
            # Requests-based image downloads match the verified local preview path and avoid
            # the DNS issue we observed from aiohttp against Auto RIA infrastructure here.
            return await asyncio.to_thread(self._download_image_bytes_sync, url)

        return _download

    def _download_image_bytes_sync(self, url: str) -> Optional[bytes]:
        headers = {
            "User-Agent": RUN_USER_AGENT,
            "Accept-Language": RUN_ACCEPT_LANGUAGE,
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        }
        response = requests.get(url, headers=headers, timeout=self._request_timeout_sec)
        if response.status_code == 403:
            self._logger.warning("Image forbidden (403). Falling back to text-only send.")
            return None
        response.raise_for_status()
        return response.content


def build_auto_ria_runtime(*, logger=None) -> AutoRiaBotRuntime:
    bot_token = os.getenv("FINANCE_BOT_TOKEN")
    if not bot_token:
        raise RuntimeError("Missing FINANCE_BOT_TOKEN in .env")

    chat_id_raw = os.getenv("DANYLO_DEFAULT_CHAT_ID")
    if not chat_id_raw:
        raise RuntimeError("Missing DANYLO_DEFAULT_CHAT_ID in .env")

    try:
        chat_id = int(chat_id_raw)
    except ValueError as error:
        raise RuntimeError("DANYLO_DEFAULT_CHAT_ID must be an integer") from error

    return AutoRiaBotRuntime(
        bot_token=bot_token,
        chat_id=chat_id,
        sources=AUTO_RIA_URLS,
        check_interval_sec=int(os.getenv("AUTO_RIA_CHECK_INTERVAL_SEC", "3600")),
        check_jitter_sec=int(os.getenv("AUTO_RIA_CHECK_JITTER_SEC", "0")),
        request_timeout_sec=int(os.getenv("AUTO_RIA_REQUEST_TIMEOUT_SEC", "30")),
        connector_limit=int(os.getenv("AUTO_RIA_HTTP_CONNECTOR_LIMIT", "8")),
        logger=logger,
    )
