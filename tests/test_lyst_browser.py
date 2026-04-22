import asyncio
import unittest

from helpers.lyst.browser import LystContextPool


class LystBrowserTests(unittest.IsolatedAsyncioTestCase):
    async def test_country_semaphore_is_cached_per_country(self) -> None:
        pool = LystContextPool(
            launch_browser=lambda *_args, **_kwargs: None,
            create_country_context=lambda *_args, **_kwargs: None,
            country_concurrency=3,
        )

        pl_a = pool.get_country_semaphore("PL")
        pl_b = pool.get_country_semaphore("PL")
        us = pool.get_country_semaphore("US")

        self.assertIs(pl_a, pl_b)
        self.assertIsNot(pl_a, us)
        self.assertIsInstance(pl_a, asyncio.Semaphore)


if __name__ == "__main__":
    unittest.main()
