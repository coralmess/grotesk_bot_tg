from __future__ import annotations

import asyncio

from playwright.async_api import async_playwright


async def launch_browser(browser_type, *, live_mode: bool, browser_launch_args: list[str]):
    # Browser launch policy is shared so the service entrypoint does not have to
    # duplicate headless and anti-bot arguments in several call sites.
    return await browser_type.launch(
        headless=not live_mode,
        args=browser_launch_args,
    )


async def create_country_context(
    browser,
    country: str,
    *,
    storage_state_path,
    stealth_user_agent: str,
    stealth_headers: dict,
    stealth_script: str,
    persist_storage_state,
):
    context_kwargs = {
        "user_agent": stealth_user_agent,
        "locale": "en-US",
        "timezone_id": "Europe/Kyiv",
        "extra_http_headers": stealth_headers,
    }
    if storage_state_path.exists():
        context_kwargs["storage_state"] = str(storage_state_path)
    ctx = await browser.new_context(**context_kwargs)
    await ctx.add_init_script(stealth_script)
    await ctx.add_cookies([{"name": "country", "value": country, "domain": ".lyst.com", "path": "/"}])
    await persist_storage_state(country, ctx)
    return ctx


class BrowserWrapper:
    # The wrapper keeps semaphore release coupled to browser teardown so the
    # orchestrator cannot leak capacity when Playwright exits through errors.
    def __init__(self, browser, semaphore: asyncio.Semaphore) -> None:
        self.browser = browser
        self._semaphore = semaphore

    async def __aenter__(self):
        return self.browser

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.browser.close()
        self._semaphore.release()


class BrowserPool:
    # Lyst opens many short-lived Playwright browsers. This pool centralizes the
    # launch semaphore so the scrape layer can request browsers without owning
    # Playwright startup details itself.
    def __init__(self, *, max_browsers: int, launch_browser):
        self.max_browsers = max_browsers
        self._launch_browser = launch_browser
        self._semaphore = asyncio.Semaphore(max_browsers)
        self._playwright = None
        self._browser_type = None

    async def init(self) -> None:
        if not self._playwright:
            self._playwright = await async_playwright().start()
            self._browser_type = self._playwright.chromium

    async def close(self) -> None:
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None
            self._browser_type = None

    async def get_browser(self) -> BrowserWrapper:
        await self.init()
        await self._semaphore.acquire()
        browser = await self._launch_browser(self._browser_type)
        return BrowserWrapper(browser, self._semaphore)


class LystContextPool:
    # Country contexts persist anti-bot and country cookies between runs. This
    # pool owns that cache so fetch code can ask for a country context without
    # duplicating lifecycle or reset behavior.
    def __init__(
        self,
        *,
        launch_browser,
        create_country_context,
        country_concurrency: int,
    ) -> None:
        self._launch_browser = launch_browser
        self._create_country_context = create_country_context
        self._country_concurrency = country_concurrency
        self._playwright = None
        self._browser = None
        self._contexts: dict[str, object] = {}
        self._context_init_locks: dict[str, asyncio.Lock] = {}
        self._country_semaphores: dict[str, asyncio.Semaphore] = {}
        self._init_lock = asyncio.Lock()

    async def init(self) -> None:
        async with self._init_lock:
            if self._playwright:
                return
            self._playwright = await async_playwright().start()
            self._browser = await self._launch_browser(self._playwright.chromium)

    def get_country_semaphore(self, country: str) -> asyncio.Semaphore:
        sem = self._country_semaphores.get(country)
        if sem is None:
            sem = asyncio.Semaphore(self._country_concurrency)
            self._country_semaphores[country] = sem
        return sem

    async def get_context(self, country: str):
        await self.init()
        lock = self._context_init_locks.get(country)
        if lock is None:
            lock = asyncio.Lock()
            self._context_init_locks[country] = lock
        async with lock:
            ctx = self._contexts.get(country)
            if ctx is not None:
                return ctx, True
            ctx = await self._create_country_context(self._browser, country)
            self._contexts[country] = ctx
            return ctx, False

    async def reset_context(self, country: str) -> None:
        ctx = self._contexts.pop(country, None)
        if ctx is None:
            return
        try:
            await ctx.close()
        except Exception:
            pass

    async def reset_browser(self) -> None:
        try:
            for ctx in list(self._contexts.values()):
                try:
                    await ctx.close()
                except Exception:
                    pass
            self._contexts.clear()
        except Exception:
            pass
        try:
            if self._browser:
                await self._browser.close()
        except Exception:
            pass
        if self._playwright:
            self._browser = await self._launch_browser(self._playwright.chromium)
