from __future__ import annotations

import asyncio
import logging
import os
import random
import re
from dataclasses import dataclass
from typing import Iterable, Optional

import requests
from telegram import Bot

from config import RUN_ACCEPT_LANGUAGE, RUN_USER_AGENT
from config_auto_ria_urls import AUTO_RIA_URLS
from helpers.auto_ria.models import AutoRiaListing, VinDecoderDetails
from helpers.auto_ria.parsing import (
    build_auto_ria_caption,
    extract_vin_from_detail_html,
    parse_nhtsa_vpic_payload,
    parse_auto_ria_search_html,
)
from helpers.auto_ria.storage import AutoRiaStorage
from helpers.auto_ria.vin import build_vin_decoder_url
from helpers.marketplace_sender import (
    build_media_sender,
    build_message_sender,
    build_photo_sender,
)
from helpers.process_pool import run_cpu_bound
from helpers.runtime_paths import AUTO_RIA_ITEMS_DB_FILE
from helpers.service_health import build_service_health


@dataclass(frozen=True)
class AutoRiaSource:
    url: str
    url_name: str


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
        logger=None,
    ) -> None:
        self._bot = Bot(token=bot_token)
        self._chat_id = str(chat_id)
        self._sources = [
            AutoRiaSource(url=source["url"], url_name=source["url_name"])
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
        self._logger = logger or logging.getLogger("auto_ria_bot")
        self._http_semaphore = asyncio.Semaphore(max(2, connector_limit))
        self._send_semaphore = asyncio.Semaphore(2)
        self._connector_limit = max(4, int(connector_limit))

        self._send_message = build_message_sender(send_semaphore=self._send_semaphore)
        self._send_photo_by_bytes = build_photo_sender(send_semaphore=self._send_semaphore)
        self._send_media = build_media_sender(
            is_valid_image_url=self._is_valid_image_url,
            download_bytes=self._download_image_bytes,
            send_message=self._send_message,
            send_photo_by_bytes=self._send_photo_by_bytes,
            run_cpu_bound_fn=run_cpu_bound,
            logger=self._logger,
        )

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

        for source in self._sources:
            html_text = await self._fetch_text(source.url)
            if not html_text:
                continue

            listings = parse_auto_ria_search_html(html_text)
            known_ids = self._storage.fetch_seen_ids([listing.id for listing in listings])

            for listing in listings:
                if listing.id in run_seen_ids or listing.id in known_ids:
                    continue
                sent = await self._process_listing(listing)
                run_seen_ids.add(listing.id)
                if sent:
                    sent_count += 1

        return sent_count

    async def _process_listing(self, listing: AutoRiaListing) -> bool:
        vin_details = await self._fetch_vin_details_for_listing(listing)
        caption = build_auto_ria_caption(
            listing,
            transmission=vin_details.transmission,
            trim=vin_details.trim,
        )

        sent = await self._send_media(
            self._bot,
            self._chat_id,
            caption,
            listing.image_url,
        )
        if sent:
            self._storage.mark_sent(
                car_id=listing.id,
                title=listing.title,
                url=listing.url,
                price_usd=listing.price_usd,
            )
        return bool(sent)

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
