import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock

import pandas as pd

import crypto_paper_bot as cpb


def make_indicator_frame(rows: int = 220) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "close": [100.0] * rows,
            "open": [100.0] * rows,
            "high": [101.0] * rows,
            "low": [99.0] * rows,
            "volume": [100.0] * rows,
            "bb_upper": [110.0] * rows,
            "bb_lower": [90.0] * rows,
            "bb_bandwidth": [20.0] * rows,
            "volume_sma_20": [100.0] * rows,
            "ema_50": [105.0] * rows,
            "ema_200": [95.0] * rows,
            "rsi_14": [45.0] * rows,
            "macd_line": [-0.5] * rows,
            "macd_signal": [-0.4] * rows,
        }
    )
    return frame


class FakeNotifier:
    def __init__(self) -> None:
        self.messages = []

    def send_message(self, message: str) -> bool:
        self.messages.append(message)
        return True


class FakeGateway:
    def __init__(self, frames: dict[str, pd.DataFrame], ranked_symbols: list[str] | None = None) -> None:
        self.frames = frames
        self.ranked_symbols = ranked_symbols or []

    def fetch_top_usdt_symbols(self, limit: int = cpb.TOP_SYMBOL_LIMIT) -> list[str]:
        return self.ranked_symbols[:limit]

    def fetch_ohlcv_frame(self, symbol: str, *, limit: int = cpb.OHLCV_LIMIT) -> pd.DataFrame:
        return self.frames[symbol].copy()


class StrategyEngineTests(unittest.TestCase):
    def test_algo_1_squeeze_breakout_true(self) -> None:
        frame = make_indicator_frame()
        frame.loc[205:218, "bb_bandwidth"] = 10.0
        frame.loc[219, "bb_bandwidth"] = 5.0
        frame.loc[219, "close"] = 120.0
        frame.loc[219, "bb_upper"] = 110.0
        frame.loc[219, "volume"] = 250.0
        frame.loc[219, "volume_sma_20"] = 100.0
        self.assertTrue(cpb.StrategyEngine.algo_1_squeeze_breakout(frame))

    def test_algo_2_golden_dip_true(self) -> None:
        frame = make_indicator_frame()
        frame.loc[219, "ema_50"] = 105.0
        frame.loc[219, "ema_200"] = 95.0
        frame.loc[219, "low"] = 104.0
        frame.loc[219, "rsi_14"] = 30.0
        self.assertTrue(cpb.StrategyEngine.algo_2_golden_dip(frame))

    def test_algo_3_reversal_true(self) -> None:
        frame = make_indicator_frame()
        frame.loc[218, "macd_line"] = -0.7
        frame.loc[218, "macd_signal"] = -0.6
        frame.loc[219, "macd_line"] = -0.2
        frame.loc[219, "macd_signal"] = -0.3
        frame.loc[218, "rsi_14"] = 49.0
        frame.loc[219, "rsi_14"] = 51.0
        self.assertTrue(cpb.StrategyEngine.algo_3_reversal(frame))

    def test_evaluate_all_false_when_history_is_short(self) -> None:
        frame = make_indicator_frame(rows=100)
        self.assertEqual(
            cpb.StrategyEngine.evaluate_all(frame),
            {"Algo 1": False, "Algo 2": False, "Algo 3": False},
        )


class TradeRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmpdir.name) / "paper.db"
        self.repo = cpb.TradeRepository(self.db_path)

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_bootstrap_seeds_accounts_once(self) -> None:
        first = self.repo.get_balances()
        second_repo = cpb.TradeRepository(self.db_path)
        second = second_repo.get_balances()
        self.assertEqual(first, {"Algo 1": 10000.0, "Algo 2": 10000.0, "Algo 3": 10000.0})
        self.assertEqual(second, first)

    def test_create_open_trade_calculates_tp_and_sl(self) -> None:
        trade_id = self.repo.create_open_trade("BTC/USDT", "Algo 1", 100.0, "2026-01-01T00:00:00+00:00")
        with closing(sqlite3.connect(self.db_path)) as conn:
            row = conn.execute(
                "SELECT id, tp_price, sl_price, status FROM paper_trades WHERE id = ?",
                (trade_id,),
            ).fetchone()
        self.assertEqual(row[0], trade_id)
        self.assertAlmostEqual(row[1], 115.0)
        self.assertAlmostEqual(row[2], 95.0)
        self.assertEqual(row[3], "OPEN")

    def test_partial_unique_index_blocks_second_open_trade_for_same_coin(self) -> None:
        self.repo.create_open_trade("BTC/USDT", "Algo 1", 100.0, "2026-01-01T00:00:00+00:00")
        with self.assertRaises(sqlite3.IntegrityError):
            self.repo.create_open_trade("BTC/USDT", "Algo 2", 100.0, "2026-01-01T01:00:00+00:00")

    def test_close_trade_win_updates_balance(self) -> None:
        self.repo.create_open_trade("BTC/USDT", "Algo 1", 100.0, "2026-01-01T00:00:00+00:00")
        trade = self.repo.get_open_trades()[0]
        closed = self.repo.close_trade(
            trade,
            status="WIN",
            exit_price=115.0,
            exit_time="2026-01-01T02:00:00+00:00",
        )
        balances = self.repo.get_balances()
        self.assertEqual(closed.status, "WIN")
        self.assertAlmostEqual(closed.pnl_usd, 150.0)
        self.assertAlmostEqual(balances["Algo 1"], 10150.0)

    def test_close_trade_loss_updates_balance(self) -> None:
        self.repo.create_open_trade("BTC/USDT", "Algo 2", 100.0, "2026-01-01T00:00:00+00:00")
        trade = self.repo.get_open_trades()[0]
        closed = self.repo.close_trade(
            trade,
            status="LOSS",
            exit_price=95.0,
            exit_time="2026-01-01T02:00:00+00:00",
        )
        balances = self.repo.get_balances()
        self.assertEqual(closed.status, "LOSS")
        self.assertAlmostEqual(closed.pnl_usd, -50.0)
        self.assertAlmostEqual(balances["Algo 2"], 9950.0)


class ExchangeGatewayTests(unittest.TestCase):
    def test_fetch_top_usdt_symbols_ranks_by_quote_volume_and_filters_unwanted_markets(self) -> None:
        gateway = cpb.ExchangeGateway.__new__(cpb.ExchangeGateway)
        gateway.exchange = mock.Mock()
        gateway._markets = {
            "BTC/USDT": {"quote": "USDT", "base": "BTC", "spot": True, "active": True},
            "ETH/USDT": {"quote": "USDT", "base": "ETH", "spot": True, "active": True},
            "USDC/USDT": {"quote": "USDT", "base": "USDC", "spot": True, "active": True},
            "ETHUP/USDT": {"quote": "USDT", "base": "ETHUP", "spot": True, "active": True},
            "BNB/BTC": {"quote": "BTC", "base": "BNB", "spot": True, "active": True},
        }
        gateway.exchange.fetch_tickers.return_value = {
            "BTC/USDT": {"quoteVolume": 1000.0},
            "ETH/USDT": {"quoteVolume": 2000.0},
            "USDC/USDT": {"quoteVolume": 5000.0},
            "ETHUP/USDT": {"quoteVolume": 7000.0},
            "BNB/BTC": {"quoteVolume": 9999.0},
        }
        symbols = gateway.fetch_top_usdt_symbols(limit=10)
        self.assertEqual(symbols, ["ETH/USDT", "BTC/USDT"])


class BotOrchestrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.repo = cpb.TradeRepository(Path(self._tmpdir.name) / "paper.db")

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    @mock.patch.object(cpb.time, "sleep", return_value=None)
    def test_same_candle_tp_and_sl_resolves_as_loss(self, _sleep: mock.Mock) -> None:
        self.repo.create_open_trade("BTC/USDT", "Algo 1", 100.0, "2026-01-01T00:00:00+00:00")
        frame = pd.DataFrame(
            [{"timestamp": pd.Timestamp("2026-01-01T01:00:00Z"), "open": 100.0, "high": 116.0, "low": 94.0, "close": 101.0, "volume": 1000.0}]
        )
        bot = cpb.CryptoPaperBot(self.repo, FakeGateway({"BTC/USDT": frame}), FakeNotifier())
        bot._check_open_trades()
        balances = self.repo.get_balances()
        open_trades = self.repo.get_open_trades()
        self.assertEqual(open_trades, [])
        self.assertAlmostEqual(balances["Algo 1"], 9950.0)

    @mock.patch.object(cpb.time, "sleep", return_value=None)
    def test_find_new_setups_opens_only_one_trade_per_coin(self, _sleep: mock.Mock) -> None:
        frame = make_indicator_frame()
        gateway = FakeGateway({"BTC/USDT": frame}, ranked_symbols=["BTC/USDT"])
        notifier = FakeNotifier()
        bot = cpb.CryptoPaperBot(self.repo, gateway, notifier)
        with mock.patch.object(cpb.IndicatorEngine, "add_indicators", side_effect=lambda value: value):
            with mock.patch.object(
                cpb.StrategyEngine,
                "evaluate_all",
                return_value={"Algo 1": True, "Algo 2": True, "Algo 3": False},
            ):
                bot._find_new_setups()
        open_trades = self.repo.get_open_trades()
        self.assertEqual(len(open_trades), 1)
        self.assertEqual(open_trades[0].algorithm, "Algo 1")
        self.assertEqual(len(notifier.messages), 1)
        self.assertIn("🟢 OPENED:", notifier.messages[0])

    @mock.patch.object(cpb.time, "sleep", return_value=None)
    def test_run_cycle_skips_ranked_symbol_that_already_has_open_trade(self, _sleep: mock.Mock) -> None:
        self.repo.create_open_trade("BTC/USDT", "Algo 1", 100.0, "2026-01-01T00:00:00+00:00")
        frame = pd.DataFrame(
            [{"timestamp": pd.Timestamp("2026-01-01T01:00:00Z"), "open": 100.0, "high": 105.0, "low": 99.0, "close": 103.0, "volume": 1000.0}]
        )
        gateway = FakeGateway({"BTC/USDT": frame}, ranked_symbols=["BTC/USDT"])
        notifier = FakeNotifier()
        bot = cpb.CryptoPaperBot(self.repo, gateway, notifier)
        with mock.patch.object(cpb.IndicatorEngine, "add_indicators", side_effect=lambda value: value) as add_indicators:
            with mock.patch.object(cpb.StrategyEngine, "evaluate_all") as evaluate_all:
                bot.run_cycle()
        add_indicators.assert_not_called()
        evaluate_all.assert_not_called()

    def test_close_message_includes_balances(self) -> None:
        closed_trade = cpb.ClosedTrade(
            id=1,
            coin="BTC/USDT",
            algorithm="Algo 1",
            entry_price=100.0,
            exit_price=115.0,
            status="WIN",
            pnl_usd=150.0,
            entry_time="2026-01-01T00:00:00+00:00",
            exit_time="2026-01-01T01:00:00+00:00",
        )
        message = cpb.build_close_message(
            closed_trade,
            {"Algo 1": 10150.0, "Algo 2": 10000.0, "Algo 3": 10000.0},
        )
        self.assertIn("🔔 TRADE CLOSED", message)
        self.assertIn("Coin: $BTC", message)
        self.assertIn("Algorithm: Algo 1 (Squeeze)", message)
        self.assertIn("PnL: +$150.00", message)
        self.assertIn("Algo 2 (Golden Dip): $10,000.00", message)

    @mock.patch.object(cpb.requests, "post")
    def test_telegram_notifier_posts_to_bot_api(self, post: mock.Mock) -> None:
        response = mock.Mock()
        response.json.return_value = {"ok": True}
        response.raise_for_status.return_value = None
        post.return_value = response
        notifier = cpb.TelegramNotifier("123:abc", 42)
        self.assertTrue(notifier.send_message("hello"))
        post.assert_called_once()
        self.assertIn("https://api.telegram.org/bot123:abc/sendMessage", post.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
