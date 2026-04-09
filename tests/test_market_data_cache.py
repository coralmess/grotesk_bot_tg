import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from useful_bot import market_data_cache


class MarketDataCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.cache_dir = Path(self._tmpdir.name)
        patcher = mock.patch.object(market_data_cache, "MARKET_DATA_CACHE_DIR", self.cache_dir)
        patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_cached_history_reuses_disk_entry_within_ttl(self) -> None:
        calls = 0
        frame = pd.DataFrame({"Close": [1.0, 2.0]})

        def fetch():
            nonlocal calls
            calls += 1
            return frame

        first = market_data_cache.cached_history(
            "QQQM",
            ttl_seconds=600,
            period="5d",
            interval="1d",
            fetch_history=fetch,
        )
        second = market_data_cache.cached_history(
            "QQQM",
            ttl_seconds=600,
            period="5d",
            interval="1d",
            fetch_history=fetch,
        )

        self.assertEqual(calls, 1)
        self.assertEqual(first["Close"].tolist(), [1.0, 2.0])
        self.assertEqual(second["Close"].tolist(), [1.0, 2.0])

    def test_expired_cache_refetches(self) -> None:
        calls = 0

        def fetch():
            nonlocal calls
            calls += 1
            return pd.DataFrame({"Close": [float(calls)]})

        first = market_data_cache.cached_history(
            "QQQM",
            ttl_seconds=0,
            period="5d",
            interval="1d",
            fetch_history=fetch,
        )
        second = market_data_cache.cached_history(
            "QQQM",
            ttl_seconds=0,
            period="5d",
            interval="1d",
            fetch_history=fetch,
        )

        self.assertEqual(calls, 2)
        self.assertEqual(first["Close"].tolist(), [1.0])
        self.assertEqual(second["Close"].tolist(), [2.0])


if __name__ == "__main__":
    unittest.main()
