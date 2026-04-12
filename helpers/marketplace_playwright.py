from __future__ import annotations

import asyncio
from typing import Optional


class PlaywrightRuntimeManager:
    def __init__(self, *, async_playwright_factory, user_agent: str, logger, chromium_launch_kwargs: Optional[dict] = None):
        self._async_playwright_factory = async_playwright_factory
        self._user_agent = user_agent
        self._logger = logger
        self._chromium_launch_kwargs = chromium_launch_kwargs or {"headless": True}
        self._lock = asyncio.Lock()
        self._playwright = None
        self._browser = None
        self._context = None

    async def ensure_started(self) -> None:
        async with self._lock:
            if self._context is not None:
                return
            if self._playwright is None:
                self._playwright = await self._async_playwright_factory().start()
            if self._browser is None:
                self._browser = await self._playwright.chromium.launch(**self._chromium_launch_kwargs)
            # SHAFA used to cold-start Playwright every run. Reusing a warm context reduces
            # startup overhead and keeps the shared market service less bursty.
            self._context = await self._browser.new_context(user_agent=self._user_agent)
            self._logger.info("Playwright runtime ready")

    async def new_page(self):
        await self.ensure_started()
        return await self._context.new_page()

    async def reset(self, reason: str = "") -> None:
        if reason:
            self._logger.warning("Resetting Playwright runtime: %s", reason)
        # A hard reset is safer than trying to recover a poisoned browser/context after
        # navigation failures because the market service is long-lived.
        await self.close()

    async def close(self) -> None:
        async with self._lock:
            if self._context is not None:
                try:
                    await self._context.close()
                except Exception:
                    pass
                self._context = None
            if self._browser is not None:
                try:
                    await self._browser.close()
                except Exception:
                    pass
                self._browser = None
            if self._playwright is not None:
                try:
                    await self._playwright.stop()
                except Exception:
                    pass
                self._playwright = None
