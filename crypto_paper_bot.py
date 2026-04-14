from __future__ import annotations

import logging
import os
import sqlite3
import threading
import time
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

from helpers.logging_utils import configure_third_party_loggers, install_secret_redaction
from helpers.runtime_paths import (
    CRYPTO_PAPER_BALANCES_MESSAGE_ID_FILE,
    CRYPTO_PAPER_TRADES_DB_FILE,
    ensure_runtime_dirs,
)
from helpers.sqlite_runtime import apply_runtime_pragmas

try:
    import ccxt  # type: ignore
except ImportError:  # pragma: no cover - exercised only in environments missing optional deps
    ccxt = None

try:
    import pandas_ta as ta  # type: ignore
except ImportError:  # pragma: no cover - exercised only in environments missing optional deps
    ta = None

try:
    import schedule
except ImportError:  # pragma: no cover - exercised only in environments missing optional deps
    schedule = None


LOGGER = logging.getLogger("crypto_paper_bot")

TIMEFRAME = "1h"
TIMEFRAME_MS = 60 * 60 * 1000
OHLCV_LIMIT = 300
TOP_SYMBOL_LIMIT = 100
FETCH_SLEEP_SECONDS = 0.2
POSITION_SIZE_USD = 1000.0
TP_MULTIPLIER = 1.15
SL_MULTIPLIER = 0.95
TAKE_PROFIT_USD = POSITION_SIZE_USD * (TP_MULTIPLIER - 1.0)
STOP_LOSS_USD = POSITION_SIZE_USD * (1.0 - SL_MULTIPLIER)

ALGORITHM_ORDER = (
    "Algo 1",
    "Algo 2",
    "Algo 3",
    "Algo 4",
    "Algo 5",
    "Algo 6",
)
ALGO_ACCOUNT_NAMES = {
    "Algo 1": "Algo 1 (Squeeze)",
    "Algo 2": "Algo 2 (Golden Dip)",
    "Algo 3": "Algo 3 (Reversal)",
    "Algo 4": "Algo 4 (Improved Squeeze)",
    "Algo 5": "Algo 5 (Improved Golden Dip)",
    "Algo 6": "Algo 6 (Improved Reversal)",
}
TREND_ADX_THRESHOLD = 20.0
RANGE_ADX_THRESHOLD = 15.0
MIN_AVG_DOLLAR_VOLUME = 2_000_000.0
BREAKOUT_VOLUME_MULTIPLIER = 1.5
PULLBACK_RSI_THRESHOLD = 40.0
SQUEEZE_PERCENTILE_MAX = 0.15

STABLECOIN_BASES = {
    "USDT",
    "USDC",
    "BUSD",
    "FDUSD",
    "TUSD",
    "USDP",
    "DAI",
    "USD1",
    "USDD",
    "USDE",
    "USDS",
    "USDX",
    "PYUSD",
    "RLUSD",
    "SUSD",
    "EUR",
    "TRY",
}
FILTERED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")


def require_dependency(module: Any, package_name: str) -> Any:
    if module is None:
        raise RuntimeError(f"Missing required dependency: {package_name}")
    return module


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def base_coin_from_symbol(symbol: str) -> str:
    return symbol.split("/", 1)[0]


def format_price(value: float) -> str:
    return f"{value:,.4f}".rstrip("0").rstrip(".")


def format_usd(value: float) -> str:
    sign = "-" if value < 0 else ""
    return f"{sign}${abs(value):,.2f}"


def format_signed_usd(value: float) -> str:
    sign = "+" if value > 0 else "-"
    return f"{sign}${abs(value):,.2f}"


def should_run_on_startup() -> bool:
    return os.getenv("CRYPTO_PAPER_RUN_ON_STARTUP", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }


def _write_text_atomic(path: Path, text: str) -> None:
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass


def trailing_percentile_rank(series: pd.Series, window: int) -> pd.Series:
    def _rank(values: pd.Series) -> float:
        sample = pd.Series(values)
        return float(sample.rank(pct=True).iloc[-1])

    return series.rolling(window).apply(_rank, raw=False)


@dataclass(frozen=True)
class OpenTrade:
    id: int
    coin: str
    algorithm: str
    entry_price: float
    tp_price: float
    sl_price: float
    entry_time: str


@dataclass(frozen=True)
class ClosedTrade:
    id: int
    coin: str
    algorithm: str
    entry_price: float
    exit_price: float
    status: str
    pnl_usd: float
    entry_time: str
    exit_time: str


@dataclass(frozen=True)
class RegimeState:
    trend_up: bool
    range_bound: bool
    liquid: bool
    strong_downtrend: bool
    squeeze_ready: bool


class ExchangeGateway:
    def __init__(self) -> None:
        exchange_cls = require_dependency(ccxt, "ccxt").binance
        # Binance public market data is enough here, so keep the client read-only and
        # let ccxt pace itself before we add the explicit per-symbol fetch delay.
        self.exchange = exchange_cls({"enableRateLimit": True, "options": {"defaultType": "spot"}})
        self._markets: Optional[dict[str, dict[str, Any]]] = None

    def load_markets(self) -> dict[str, dict[str, Any]]:
        if self._markets is None:
            self._markets = self.exchange.load_markets()
        return self._markets

    def fetch_top_usdt_symbols(self, limit: int = TOP_SYMBOL_LIMIT) -> list[str]:
        markets = self.load_markets()
        tickers = self.exchange.fetch_tickers()
        ranked: list[tuple[float, str]] = []
        for symbol, ticker in tickers.items():
            market = markets.get(symbol)
            if not self._is_supported_market(symbol, market):
                continue
            quote_volume = self._quote_volume_from_ticker(ticker)
            if quote_volume <= 0:
                continue
            ranked.append((quote_volume, symbol))
        ranked.sort(reverse=True)
        return [symbol for _, symbol in ranked[:limit]]

    def fetch_ohlcv_frame(self, symbol: str, *, limit: int = OHLCV_LIMIT) -> pd.DataFrame:
        raw = self.exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=limit)
        frame = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
        if frame.empty:
            return frame
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], unit="ms", utc=True)
        for col in ("open", "high", "low", "close", "volume"):
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
        frame = frame.dropna().reset_index(drop=True)
        if len(frame) >= 2:
            last_candle_open_ms = int(frame.iloc[-1]["timestamp"].timestamp() * 1000)
            if last_candle_open_ms + TIMEFRAME_MS > int(time.time() * 1000):
                frame = frame.iloc[:-1].reset_index(drop=True)
        return frame

    @staticmethod
    def _quote_volume_from_ticker(ticker: dict[str, Any]) -> float:
        candidates = (
            ticker.get("quoteVolume"),
            (ticker.get("info") or {}).get("quoteVolume"),
        )
        for value in candidates:
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return 0.0

    @staticmethod
    def _is_supported_market(symbol: str, market: Optional[dict[str, Any]]) -> bool:
        if not market:
            return False
        if market.get("quote") != "USDT":
            return False
        if not market.get("spot", False):
            return False
        if market.get("active") is False:
            return False
        base = str(market.get("base") or base_coin_from_symbol(symbol)).upper()
        if base in STABLECOIN_BASES:
            return False
        if any(base.endswith(suffix) for suffix in FILTERED_SUFFIXES):
            return False
        return True


class IndicatorEngine:
    @staticmethod
    def add_indicators(frame: pd.DataFrame) -> pd.DataFrame:
        require_dependency(ta, "pandas_ta")
        enriched = frame.copy()
        bbands = ta.bbands(enriched["close"], length=20, std=2)
        macd = ta.macd(enriched["close"])
        adx = ta.adx(enriched["high"], enriched["low"], enriched["close"], length=14)
        if bbands is None or macd is None:
            return enriched
        pieces = [enriched, bbands, macd]
        if adx is not None:
            pieces.append(adx)
        enriched = pd.concat(pieces, axis=1)
        enriched["bb_upper"] = enriched.get("BBU_20_2.0")
        enriched["bb_lower"] = enriched.get("BBL_20_2.0")
        enriched["bb_bandwidth"] = ((enriched["bb_upper"] - enriched["bb_lower"]) / enriched["close"]) * 100.0
        enriched["bb_bandwidth_pct_rank_100"] = trailing_percentile_rank(enriched["bb_bandwidth"], 100)
        enriched["volume_sma_20"] = ta.sma(enriched["volume"], length=20)
        enriched["ema_20"] = ta.ema(enriched["close"], length=20)
        enriched["ema_50"] = ta.ema(enriched["close"], length=50)
        enriched["ema_200"] = ta.ema(enriched["close"], length=200)
        enriched["ema_50_slope_5"] = enriched["ema_50"].pct_change(5)
        enriched["rsi_14"] = ta.rsi(enriched["close"], length=14)
        enriched["atr_14"] = ta.atr(enriched["high"], enriched["low"], enriched["close"], length=14)
        enriched["adx_14"] = enriched.get("ADX_14")
        enriched["macd_line"] = enriched.get("MACD_12_26_9")
        enriched["macd_signal"] = enriched.get("MACDs_12_26_9")
        enriched["macd_hist"] = enriched["macd_line"] - enriched["macd_signal"]
        enriched["macd_hist_slope"] = enriched["macd_hist"].diff()
        enriched["dollar_volume"] = enriched["close"] * enriched["volume"]
        enriched["avg_dollar_volume_24"] = enriched["dollar_volume"].rolling(24).mean()
        return enriched


class StrategyEngine:
    MIN_ROWS = 220

    @classmethod
    def evaluate_all(cls, frame: pd.DataFrame) -> dict[str, bool]:
        if len(frame) < cls.MIN_ROWS:
            return {algorithm: False for algorithm in ALGORITHM_ORDER}
        return {
            "Algo 1": cls.algo_1_squeeze_breakout(frame),
            "Algo 2": cls.algo_2_golden_dip(frame),
            "Algo 3": cls.algo_3_reversal(frame),
            "Algo 4": cls.algo_4_improved_squeeze_breakout(frame),
            "Algo 5": cls.algo_5_improved_golden_dip(frame),
            "Algo 6": cls.algo_6_improved_reversal(frame),
        }

    @staticmethod
    def algo_1_squeeze_breakout(frame: pd.DataFrame) -> bool:
        latest = frame.iloc[-1]
        previous_bandwidth = frame["bb_bandwidth"].iloc[-15:-1]
        if len(previous_bandwidth) != 14 or previous_bandwidth.isna().any():
            return False
        return bool(
            pd.notna(latest.get("bb_bandwidth"))
            and latest["bb_bandwidth"] < previous_bandwidth.min()
            and pd.notna(latest.get("bb_upper"))
            and latest["close"] > latest["bb_upper"]
            and pd.notna(latest.get("volume_sma_20"))
            and latest["volume"] > 2.0 * latest["volume_sma_20"]
        )

    @staticmethod
    def algo_2_golden_dip(frame: pd.DataFrame) -> bool:
        latest = frame.iloc[-1]
        return bool(
            pd.notna(latest.get("ema_50"))
            and pd.notna(latest.get("ema_200"))
            and latest["ema_50"] > latest["ema_200"]
            and latest["low"] <= latest["ema_50"]
            and pd.notna(latest.get("rsi_14"))
            and latest["rsi_14"] < 35.0
        )

    @staticmethod
    def algo_3_reversal(frame: pd.DataFrame) -> bool:
        previous = frame.iloc[-2]
        latest = frame.iloc[-1]
        macd_cross_up = (
            pd.notna(previous.get("macd_line"))
            and pd.notna(previous.get("macd_signal"))
            and pd.notna(latest.get("macd_line"))
            and pd.notna(latest.get("macd_signal"))
            and previous["macd_line"] <= previous["macd_signal"]
            and latest["macd_line"] > latest["macd_signal"]
        )
        rsi_cross_up = (
            pd.notna(previous.get("rsi_14"))
            and pd.notna(latest.get("rsi_14"))
            and previous["rsi_14"] <= 50.0
            and latest["rsi_14"] > 50.0
        )
        return bool(
            macd_cross_up
            and latest["macd_line"] < 0
            and latest["macd_signal"] < 0
            and rsi_cross_up
        )

    @staticmethod
    def _classify_regime(frame: pd.DataFrame) -> RegimeState:
        latest = frame.iloc[-1]
        trend_up = bool(
            pd.notna(latest.get("ema_50"))
            and pd.notna(latest.get("ema_200"))
            and pd.notna(latest.get("adx_14"))
            and pd.notna(latest.get("ema_50_slope_5"))
            and latest["ema_50"] > latest["ema_200"]
            and latest["adx_14"] >= TREND_ADX_THRESHOLD
            and latest["ema_50_slope_5"] > 0
        )
        strong_downtrend = bool(
            pd.notna(latest.get("ema_50"))
            and pd.notna(latest.get("ema_200"))
            and pd.notna(latest.get("adx_14"))
            and latest["ema_50"] < latest["ema_200"]
            and latest["adx_14"] >= TREND_ADX_THRESHOLD
        )
        range_bound = bool(pd.notna(latest.get("adx_14")) and latest["adx_14"] <= RANGE_ADX_THRESHOLD)
        liquid = bool(
            pd.notna(latest.get("avg_dollar_volume_24"))
            and latest["avg_dollar_volume_24"] >= MIN_AVG_DOLLAR_VOLUME
        )
        squeeze_ready = bool(
            pd.notna(latest.get("bb_bandwidth_pct_rank_100"))
            and latest["bb_bandwidth_pct_rank_100"] <= SQUEEZE_PERCENTILE_MAX
        )
        return RegimeState(
            trend_up=trend_up,
            range_bound=range_bound,
            liquid=liquid,
            strong_downtrend=strong_downtrend,
            squeeze_ready=squeeze_ready,
        )

    @classmethod
    def algo_4_improved_squeeze_breakout(cls, frame: pd.DataFrame) -> bool:
        previous = frame.iloc[-2]
        latest = frame.iloc[-1]
        regime = cls._classify_regime(frame)
        return bool(
            regime.trend_up
            and regime.squeeze_ready
            and pd.notna(latest.get("bb_upper"))
            and latest["close"] > latest["bb_upper"]
            and latest["close"] > previous["high"]
            and pd.notna(latest.get("volume_sma_20"))
            and latest["volume"] >= BREAKOUT_VOLUME_MULTIPLIER * latest["volume_sma_20"]
        )

    @classmethod
    def algo_5_improved_golden_dip(cls, frame: pd.DataFrame) -> bool:
        previous = frame.iloc[-2]
        latest = frame.iloc[-1]
        regime = cls._classify_regime(frame)
        return bool(
            regime.trend_up
            and pd.notna(latest.get("ema_20"))
            and pd.notna(latest.get("ema_50"))
            and latest["low"] <= latest["ema_50"]
            and latest["close"] >= latest["ema_20"]
            and latest["close"] > previous["high"]
            and pd.notna(latest.get("rsi_14"))
            and latest["rsi_14"] <= PULLBACK_RSI_THRESHOLD
        )

    @classmethod
    def algo_6_improved_reversal(cls, frame: pd.DataFrame) -> bool:
        previous = frame.iloc[-2]
        latest = frame.iloc[-1]
        regime = cls._classify_regime(frame)
        macd_cross_up = (
            pd.notna(previous.get("macd_line"))
            and pd.notna(previous.get("macd_signal"))
            and pd.notna(latest.get("macd_line"))
            and pd.notna(latest.get("macd_signal"))
            and previous["macd_line"] <= previous["macd_signal"]
            and latest["macd_line"] > latest["macd_signal"]
        )
        rsi_cross_up = (
            pd.notna(previous.get("rsi_14"))
            and pd.notna(latest.get("rsi_14"))
            and previous["rsi_14"] <= 50.0
            and latest["rsi_14"] > 50.0
        )
        return bool(
            regime.liquid
            and regime.range_bound
            and not regime.strong_downtrend
            and macd_cross_up
            and latest["macd_line"] < 0
            and latest["macd_signal"] < 0
            and pd.notna(latest.get("macd_hist_slope"))
            and latest["macd_hist_slope"] >= 0
            and rsi_cross_up
            and latest["close"] > previous["high"]
        )


class TradeRepository:
    def __init__(self, db_path: str | os.PathLike[str] = CRYPTO_PAPER_TRADES_DB_FILE) -> None:
        ensure_runtime_dirs()
        self.db_path = str(db_path)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        apply_runtime_pragmas(conn)
        return conn

    def _initialize(self) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS paper_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    coin TEXT NOT NULL,
                    algorithm TEXT NOT NULL,
                    entry_price REAL NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('OPEN', 'WIN', 'LOSS')),
                    tp_price REAL NOT NULL,
                    sl_price REAL NOT NULL,
                    entry_time TEXT NOT NULL,
                    exit_time TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS accounts (
                    algorithm TEXT PRIMARY KEY,
                    balance REAL NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_paper_trades_status ON paper_trades(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_paper_trades_coin ON paper_trades(coin)")
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_one_open_trade_per_coin
                ON paper_trades(coin)
                WHERE status = 'OPEN'
                """
            )
            for algorithm in ALGORITHM_ORDER:
                conn.execute(
                    "INSERT OR IGNORE INTO accounts (algorithm, balance) VALUES (?, ?)",
                    (algorithm, 10000.0),
                )
            conn.commit()

    def get_open_trades(self) -> list[OpenTrade]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT id, coin, algorithm, entry_price, tp_price, sl_price, entry_time
                FROM paper_trades
                WHERE status = 'OPEN'
                ORDER BY id ASC
                """
            ).fetchall()
        return [
            OpenTrade(
                id=int(row["id"]),
                coin=str(row["coin"]),
                algorithm=str(row["algorithm"]),
                entry_price=float(row["entry_price"]),
                tp_price=float(row["tp_price"]),
                sl_price=float(row["sl_price"]),
                entry_time=str(row["entry_time"]),
            )
            for row in rows
        ]

    def has_open_trade(self, coin: str) -> bool:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT 1 FROM paper_trades WHERE coin = ? AND status = 'OPEN' LIMIT 1",
                (coin,),
            ).fetchone()
        return row is not None

    def create_open_trade(self, coin: str, algorithm: str, entry_price: float, entry_time: str) -> int:
        tp_price = entry_price * TP_MULTIPLIER
        sl_price = entry_price * SL_MULTIPLIER
        with closing(self._connect()) as conn:
            cursor = conn.execute(
                """
                INSERT INTO paper_trades (
                    coin, algorithm, entry_price, status, tp_price, sl_price, entry_time, exit_time
                ) VALUES (?, ?, ?, 'OPEN', ?, ?, ?, NULL)
                """,
                (coin, algorithm, entry_price, tp_price, sl_price, entry_time),
            )
            conn.commit()
            return int(cursor.lastrowid)

    def close_trade(self, trade: OpenTrade, *, status: str, exit_price: float, exit_time: str) -> ClosedTrade:
        if status not in {"WIN", "LOSS"}:
            raise ValueError(f"Unsupported close status: {status}")
        pnl_usd = TAKE_PROFIT_USD if status == "WIN" else -STOP_LOSS_USD
        with closing(self._connect()) as conn:
            conn.execute("BEGIN")
            conn.execute(
                """
                UPDATE paper_trades
                SET status = ?, exit_time = ?
                WHERE id = ? AND status = 'OPEN'
                """,
                (status, exit_time, trade.id),
            )
            conn.execute(
                "UPDATE accounts SET balance = balance + ? WHERE algorithm = ?",
                (pnl_usd, trade.algorithm),
            )
            conn.commit()
        return ClosedTrade(
            id=trade.id,
            coin=trade.coin,
            algorithm=trade.algorithm,
            entry_price=trade.entry_price,
            exit_price=exit_price,
            status=status,
            pnl_usd=pnl_usd,
            entry_time=trade.entry_time,
            exit_time=exit_time,
        )

    def get_balances(self) -> dict[str, float]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT algorithm, balance FROM accounts ORDER BY algorithm ASC").fetchall()
        return {str(row["algorithm"]): float(row["balance"]) for row in rows}


class TelegramNotifier:
    def __init__(self, token: str, chat_id: int) -> None:
        self.token = token
        self.chat_id = chat_id

    def send_message(self, message: str) -> bool:
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            response = requests.post(
                url,
                json={"chat_id": self.chat_id, "text": message},
                timeout=20,
            )
            response.raise_for_status()
            payload = response.json()
            if not payload.get("ok", False):
                LOGGER.error("Telegram API returned non-ok payload: %s", payload)
                return False
            return True
        except Exception as exc:
            LOGGER.error("Failed to send Telegram message: %s", exc)
            return False


def build_open_message(algorithm: str, coin: str, entry_price: float) -> str:
    return (
        f"🟢 OPENED: {ALGO_ACCOUNT_NAMES[algorithm]} bought "
        f"${base_coin_from_symbol(coin)} at ${format_price(entry_price)}."
    )


def build_close_message(closed_trade: ClosedTrade, balances: dict[str, float]) -> str:
    lines = [
        "🔔 TRADE CLOSED",
        f"Coin: ${base_coin_from_symbol(closed_trade.coin)}",
        f"Algorithm: {ALGO_ACCOUNT_NAMES[closed_trade.algorithm]}",
        f"Result: {closed_trade.status}",
        (
            f"Entry: ${format_price(closed_trade.entry_price)} | "
            f"Exit: ${format_price(closed_trade.exit_price)} | "
            f"PnL: {format_signed_usd(closed_trade.pnl_usd)}"
        ),
        "🏦 CURRENT SYNTHETIC BALANCES",
    ]
    for algorithm in ("Algo 1", "Algo 2", "Algo 3"):
        lines.append(f"{ALGO_ACCOUNT_NAMES[algorithm]}: {format_usd(balances.get(algorithm, 0.0))}")
    return "\n".join(lines)


def build_close_message(closed_trade: ClosedTrade) -> str:
    lines = [
        "TRADE CLOSED",
        f"Coin: ${base_coin_from_symbol(closed_trade.coin)}",
        f"Algorithm: {ALGO_ACCOUNT_NAMES[closed_trade.algorithm]}",
        f"Result: {closed_trade.status}",
        (
            f"Entry: ${format_price(closed_trade.entry_price)} | "
            f"Exit: ${format_price(closed_trade.exit_price)} | "
            f"PnL: {format_signed_usd(closed_trade.pnl_usd)}"
        ),
    ]
    return "\n".join(lines)


def build_balances_message(balances: dict[str, float]) -> str:
    lines = [
        "SYNTHETIC ACCOUNTS",
        f"Updated (UTC): {utc_now_iso()}",
    ]
    for algorithm in ALGORITHM_ORDER:
        lines.append(f"{ALGO_ACCOUNT_NAMES[algorithm]}: {format_usd(balances.get(algorithm, 0.0))}")
    return "\n".join(lines)


class TelegramNotifier:
    def __init__(
        self,
        token: str,
        chat_id: int,
        balance_message_id_path: str | os.PathLike[str] = CRYPTO_PAPER_BALANCES_MESSAGE_ID_FILE,
    ) -> None:
        self.token = token
        self.chat_id = chat_id
        self.balance_message_id_path = Path(balance_message_id_path)

    def _post(self, method: str, payload: dict[str, Any]) -> Optional[dict[str, Any]]:
        url = f"https://api.telegram.org/bot{self.token}/{method}"
        try:
            response = requests.post(url, json=payload, timeout=20)
            response.raise_for_status()
            body = response.json()
            if not body.get("ok", False):
                LOGGER.error("Telegram API returned non-ok payload for %s: %s", method, body)
                return None
            return body
        except Exception as exc:
            LOGGER.error("Failed to call Telegram method %s: %s", method, exc)
            return None

    def _read_balance_message_id(self) -> Optional[int]:
        if not self.balance_message_id_path.exists():
            return None
        try:
            stored = self.balance_message_id_path.read_text(encoding="utf-8").strip()
        except Exception as exc:
            LOGGER.warning("Failed to read balances message id from %s: %s", self.balance_message_id_path, exc)
            return None
        return int(stored) if stored.isdigit() else None

    def _write_balance_message_id(self, message_id: int) -> None:
        try:
            _write_text_atomic(self.balance_message_id_path, str(message_id))
        except Exception as exc:
            LOGGER.error("Failed to save balances message id to %s: %s", self.balance_message_id_path, exc)

    def send_message(self, message: str) -> bool:
        return self._post("sendMessage", {"chat_id": self.chat_id, "text": message}) is not None

    def refresh_balances_message(self, balances: dict[str, float]) -> bool:
        text = build_balances_message(balances)
        message_id = self._read_balance_message_id()
        if message_id and self._post(
            "editMessageText",
            {"chat_id": self.chat_id, "message_id": message_id, "text": text},
        ):
            self._ensure_pinned(message_id)
            return True

        created = self._post("sendMessage", {"chat_id": self.chat_id, "text": text})
        if created is None:
            return False
        created_message_id = int(created["result"]["message_id"])
        self._write_balance_message_id(created_message_id)
        self._ensure_pinned(created_message_id)
        return True

    def _ensure_pinned(self, message_id: int) -> None:
        chat_payload = self._post("getChat", {"chat_id": self.chat_id})
        pinned_message = ((chat_payload or {}).get("result") or {}).get("pinned_message") or {}
        pinned_message_id = pinned_message.get("message_id")
        if isinstance(pinned_message_id, int) and pinned_message_id != message_id:
            self._post("unpinChatMessage", {"chat_id": self.chat_id, "message_id": pinned_message_id})
        self._post(
            "pinChatMessage",
            {"chat_id": self.chat_id, "message_id": message_id, "disable_notification": True},
        )


class CryptoPaperBot:
    def __init__(
        self,
        repository: TradeRepository,
        exchange_gateway: ExchangeGateway,
        notifier: TelegramNotifier,
    ) -> None:
        self.repository = repository
        self.exchange_gateway = exchange_gateway
        self.notifier = notifier
        self._job_lock = threading.Lock()

    def run_cycle(self) -> None:
        if not self._job_lock.acquire(blocking=False):
            LOGGER.warning("Previous crypto paper-trading cycle still running; skipping this slot")
            return
        try:
            LOGGER.info("Starting crypto paper-trading cycle")
            # The balances message is the operator's single source of truth, so refresh
            # it at the start of each cycle even when no trades fire during that hour.
            self.notifier.refresh_balances_message(self.repository.get_balances())
            self._check_open_trades()
            self._find_new_setups()
            LOGGER.info("Crypto paper-trading cycle completed")
        finally:
            self._job_lock.release()

    def _check_open_trades(self) -> None:
        for trade in self.repository.get_open_trades():
            frame = self.exchange_gateway.fetch_ohlcv_frame(trade.coin, limit=OHLCV_LIMIT)
            time.sleep(FETCH_SLEEP_SECONDS)
            if frame.empty:
                LOGGER.warning("No candles available for open trade %s", trade.coin)
                continue
            latest = frame.iloc[-1]
            high_price = float(latest["high"])
            low_price = float(latest["low"])
            close_status: Optional[str] = None
            exit_price: Optional[float] = None

            # Conservative same-candle resolution avoids overstating a strategy that never
            # recorded intrabar sequencing.
            if low_price <= trade.sl_price:
                close_status = "LOSS"
                exit_price = trade.sl_price
            elif high_price >= trade.tp_price:
                close_status = "WIN"
                exit_price = trade.tp_price

            if close_status is None or exit_price is None:
                continue

            closed_trade = self.repository.close_trade(
                trade,
                status=close_status,
                exit_price=exit_price,
                exit_time=utc_now_iso(),
            )
            balances = self.repository.get_balances()
            self.notifier.send_message(build_close_message(closed_trade))
            self.notifier.refresh_balances_message(balances)

    def _find_new_setups(self) -> None:
        ranked_symbols = self.exchange_gateway.fetch_top_usdt_symbols(limit=TOP_SYMBOL_LIMIT)
        blocked_symbols = {trade.coin for trade in self.repository.get_open_trades()}
        for symbol in ranked_symbols:
            if symbol in blocked_symbols or self.repository.has_open_trade(symbol):
                continue
            frame = self.exchange_gateway.fetch_ohlcv_frame(symbol, limit=OHLCV_LIMIT)
            time.sleep(FETCH_SLEEP_SECONDS)
            if frame.empty:
                continue
            enriched = IndicatorEngine.add_indicators(frame)
            signals = StrategyEngine.evaluate_all(enriched)
            for algorithm, should_open in signals.items():
                if not should_open:
                    continue
                if symbol in blocked_symbols or self.repository.has_open_trade(symbol):
                    break
                entry_price = float(enriched.iloc[-1]["close"])
                self.repository.create_open_trade(symbol, algorithm, entry_price, utc_now_iso())
                blocked_symbols.add(symbol)
                self.notifier.send_message(build_open_message(algorithm, symbol, entry_price))
                self.notifier.refresh_balances_message(self.repository.get_balances())
                break


def build_bot() -> CryptoPaperBot:
    load_dotenv()
    ensure_runtime_dirs()

    token = (os.getenv("FINANCE_BOT_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("Missing FINANCE_BOT_TOKEN in .env")
    chat_id_raw = os.getenv("DANYLO_DEFAULT_CHAT_ID")
    if not chat_id_raw:
        raise RuntimeError("Missing DANYLO_DEFAULT_CHAT_ID in .env")
    chat_id = int(chat_id_raw)

    repository = TradeRepository()
    exchange_gateway = ExchangeGateway()
    notifier = TelegramNotifier(token=token, chat_id=chat_id)
    return CryptoPaperBot(repository, exchange_gateway, notifier)


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    configure_third_party_loggers()
    install_secret_redaction(logging.getLogger())


def main() -> None:
    require_dependency(schedule, "schedule")
    configure_logging()
    bot = build_bot()

    if should_run_on_startup():
        bot.run_cycle()

    schedule.every().hour.at(":00").do(bot.run_cycle)
    LOGGER.info("Crypto paper-trading bot scheduled every hour at :00")
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
