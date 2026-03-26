import io
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from helpers.runtime_paths import RUNTIME_DEBUG_DIR, ensure_runtime_dirs
from useful_bot.ibkr_portfolio_core import (
    NEW_YORK_TZ,
    build_portfolio_snapshot,
    seconds_until_next_run,
    should_run_daily_snapshot,
    top_daily_movers,
    top_lifetime_gainers,
)
from useful_bot.ibkr_portfolio_helper import (
    IBKRConnectionSettings,
    IBKRPortfolioHelper,
    _parse_flex_statement_xml,
)
from useful_bot.ibkr_portfolio_image import (
    QQQMBenchmarkQuote,
    QQQMCloseHistory,
    RENDER_STYLE_VERSION,
    _build_portfolio_html,
    _qqqm_hypothetical_pnl,
    compute_qqqm_total_diff,
    initialize_qqqm_benchmark_baseline,
    render_ibkr_portfolio_card,
)


FLEX_STATEMENT_SAMPLE = """\
<FlexQueryResponse queryName="TG Bot" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U20984427" fromDate="20260323" toDate="20260323" period="LastBusinessDay" whenGenerated="20260324;101454">
      <ChangeInNAV startingValue="10173.47" mtm="71.81" depositsWithdrawals="0" dividends="0" endingValue="10251.19" accountId="U20984427" currency="USD" fromDate="20260323" toDate="20260323" />
      <MTMPerformanceSummaryInBase>
        <MTMPerformanceSummaryUnderlying assetCategory="STK" symbol="EL" closePrice="79.29" total="-53.04" accountId="U20984427" subCategory="COMMON" conid="1448477" reportDate="20260323" prevCloseQuantity="8" closeQuantity="8" />
        <MTMPerformanceSummaryUnderlying assetCategory="STK" symbol="XMMO" closePrice="145.15" total="29.07" accountId="U20984427" subCategory="ETF" conid="319357127" reportDate="20260323" prevCloseQuantity="9" closeQuantity="9" />
      </MTMPerformanceSummaryInBase>
      <OpenPositions>
        <OpenPosition symbol="EL" position="8" markPrice="79.29" positionValue="634.32" costBasisPrice="89.590022" costBasisMoney="716.720176" fifoPnlUnrealized="-82.400176" accountId="U20984427" currency="USD" assetCategory="STK" subCategory="COMMON" conid="1448477" reportDate="20260323" />
        <OpenPosition symbol="XMMO" position="9" markPrice="145.15" positionValue="1306.35" costBasisPrice="136.591133111" costBasisMoney="1229.320198" fifoPnlUnrealized="77.029802" accountId="U20984427" currency="USD" assetCategory="STK" subCategory="ETF" conid="319357127" reportDate="20260323" />
      </OpenPositions>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""


FLEX_STATEMENT_WITH_TRADES_SAMPLE = """\
<FlexQueryResponse queryName="TG Bot" type="AF">
  <FlexStatements count="1">
    <FlexStatement accountId="U20984427" fromDate="20240102" toDate="20260323" period="DateRange" whenGenerated="20260324;101454">
      <ChangeInNAV startingValue="10173.47" mtm="71.81" depositsWithdrawals="0" dividends="0" endingValue="10251.19" accountId="U20984427" currency="USD" fromDate="20240102" toDate="20260323" />
      <MTMPerformanceSummaryInBase>
        <MTMPerformanceSummaryUnderlying assetCategory="STK" symbol="EL" closePrice="79.29" total="-53.04" accountId="U20984427" subCategory="COMMON" conid="1448477" reportDate="20260323" prevCloseQuantity="8" closeQuantity="8" />
      </MTMPerformanceSummaryInBase>
      <OpenPositions>
        <OpenPosition symbol="EL" position="8" markPrice="79.29" positionValue="634.32" costBasisPrice="89.590022" costBasisMoney="716.720176" fifoPnlUnrealized="-82.400176" accountId="U20984427" currency="USD" assetCategory="STK" subCategory="COMMON" conid="1448477" reportDate="20260323" />
      </OpenPositions>
      <Trades>
        <Trade transactionType="ExchTrade" buySell="BUY" assetCategory="STK" accountId="U20984427" currency="USD" symbol="AAPL" conid="265598" reportDate="20240103" tradeDate="20240103" quantity="5" tradePrice="180" tradeMoney="900" ibCommission="-1" netCash="-901" proceeds="900" />
        <Trade transactionType="ExchTrade" buySell="SELL" assetCategory="STK" accountId="U20984427" currency="USD" symbol="MSFT" conid="272093" reportDate="20240105" tradeDate="20240105" quantity="-2" tradePrice="400" tradeMoney="-800" ibCommission="-1" netCash="799" proceeds="800" />
      </Trades>
      <CashTransactions>
        <CashTransaction accountId="U20984427" currency="USD" reportDate="20240104" type="Deposits &amp; Withdrawals" description="Deposit" amount="250" />
      </CashTransactions>
      <Transfers>
        <Transfer accountId="U20984427" currency="USD" reportDate="20240106" type="Transfer" description="Transfer Out" amount="-100" />
      </Transfers>
      <CorporateActions>
        <CorporateAction accountId="U20984427" assetCategory="STK" symbol="AAPL" conid="265598" reportDate="20240201" description="Split" proceeds="0" />
      </CorporateActions>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>
"""


def make_full_positive_positions():
    return [
        {
            "symbol": "NVDA",
            "con_id": 101,
            "sec_type": "STK",
            "quantity": 20,
            "market_price": 980.0,
            "market_value": 19600.0,
            "average_cost": 620.0,
            "unrealized_pnl": 7200.0,
            "daily_pnl": 780.0,
        },
        {
            "symbol": "QQQ",
            "con_id": 102,
            "sec_type": "STK",
            "quantity": 18,
            "market_price": 515.0,
            "market_value": 9270.0,
            "average_cost": 420.0,
            "unrealized_pnl": 1710.0,
            "daily_pnl": 265.0,
        },
        {
            "symbol": "XLK",
            "con_id": 103,
            "sec_type": "STK",
            "quantity": 32,
            "market_price": 248.0,
            "market_value": 7936.0,
            "average_cost": 208.0,
            "unrealized_pnl": 1280.0,
            "daily_pnl": 154.0,
        },
        {
            "symbol": "VOO",
            "con_id": 104,
            "sec_type": "STK",
            "quantity": 14,
            "market_price": 540.0,
            "market_value": 7560.0,
            "average_cost": 470.0,
            "unrealized_pnl": 980.0,
            "daily_pnl": 102.0,
        },
        {
            "symbol": "IWM",
            "con_id": 105,
            "sec_type": "STK",
            "quantity": 28,
            "market_price": 228.0,
            "market_value": 6384.0,
            "average_cost": 205.0,
            "unrealized_pnl": 644.0,
            "daily_pnl": 58.0,
        },
    ]


def make_full_negative_positions():
    return [
        {
            "symbol": "ARKK",
            "con_id": 201,
            "sec_type": "STK",
            "quantity": 110,
            "market_price": 42.0,
            "market_value": 4620.0,
            "average_cost": 67.0,
            "unrealized_pnl": -2750.0,
            "daily_pnl": -286.0,
        },
        {
            "symbol": "SHOP",
            "con_id": 202,
            "sec_type": "STK",
            "quantity": 40,
            "market_price": 78.0,
            "market_value": 3120.0,
            "average_cost": 101.0,
            "unrealized_pnl": -920.0,
            "daily_pnl": -180.0,
        },
        {
            "symbol": "TAN",
            "con_id": 203,
            "sec_type": "STK",
            "quantity": 55,
            "market_price": 39.0,
            "market_value": 2145.0,
            "average_cost": 48.0,
            "unrealized_pnl": -495.0,
            "daily_pnl": -121.0,
        },
        {
            "symbol": "XBI",
            "con_id": 204,
            "sec_type": "STK",
            "quantity": 36,
            "market_price": 81.0,
            "market_value": 2916.0,
            "average_cost": 96.0,
            "unrealized_pnl": -540.0,
            "daily_pnl": -98.0,
        },
        {
            "symbol": "ICLN",
            "con_id": 205,
            "sec_type": "STK",
            "quantity": 150,
            "market_price": 14.0,
            "market_value": 2100.0,
            "average_cost": 16.0,
            "unrealized_pnl": -300.0,
            "daily_pnl": -44.0,
        },
    ]


def make_snapshot(
    *,
    trade_date: str = "2026-03-23",
    nav_starting_value: float | None = None,
    net_liquidation: float = 100000.0,
    cash_value: float = 25000.0,
    raw_positions=None,
    raw_trades=(),
    raw_cash_events=(),
    raw_corporate_actions=(),
    source_from_date: str = "",
    source_to_date: str = "",
    source_period: str = "",
    qqqm_total_diff: float | None = None,
):
    if raw_positions is None:
        raw_positions = [
            {
                "symbol": "AAPL",
                "con_id": 101,
                "sec_type": "STK",
                "quantity": 10,
                "market_price": 210.0,
                "market_value": 2100.0,
                "average_cost": 150.0,
                "unrealized_pnl": 600.0,
                "daily_pnl": 42.0,
            },
            {
                "symbol": "MSFT",
                "con_id": 202,
                "sec_type": "STK",
                "quantity": 5,
                "market_price": 420.0,
                "market_value": 2100.0,
                "average_cost": 390.0,
                "unrealized_pnl": 150.0,
                "daily_pnl": -21.0,
            },
        ]
    return build_portfolio_snapshot(
        fetched_at=datetime(2026, 3, 23, 16, 30, tzinfo=NEW_YORK_TZ),
        trade_date=trade_date,
        account_id="U1234567",
        nav_starting_value=nav_starting_value,
        net_liquidation=net_liquidation,
        cash_value=cash_value,
        raw_positions=raw_positions,
        raw_trades=raw_trades,
        raw_cash_events=raw_cash_events,
        raw_corporate_actions=raw_corporate_actions,
        source_from_date=source_from_date,
        source_to_date=source_to_date,
        source_period=source_period,
        qqqm_total_diff=qqqm_total_diff,
    )


def make_settings() -> IBKRConnectionSettings:
    return IBKRConnectionSettings(
        host="127.0.0.1",
        port=7497,
        client_id=7,
        account_id="U1234567",
    )


class FakeBot:
    def __init__(self) -> None:
        self.sent_photos = []

    async def send_photo(self, *, chat_id: int, photo) -> None:
        photo.seek(0)
        self.sent_photos.append((chat_id, len(photo.read())))


class FakeApplication:
    def __init__(self) -> None:
        self.bot = FakeBot()


class IBKRPortfolioCoreTests(unittest.TestCase):
    def test_settings_from_env_prefers_flex_when_query_credentials_are_present(self) -> None:
        with patch.dict(
            os.environ,
            {
                "IBKR_ACCOUNT_ID": "U20984427",
                "IBKR_QUERY_ID": "1445890",
                "IBKR_QUERY_TOKEN": "secret-token",
            },
            clear=True,
        ):
            settings = IBKRConnectionSettings.from_env()

        self.assertEqual(settings.source, "flex")
        self.assertEqual(settings.account_id, "U20984427")
        self.assertEqual(settings.query_id, "1445890")
        self.assertEqual(settings.query_token, "secret-token")

    def test_parse_flex_statement_xml_maps_live_query_shape(self) -> None:
        settings = IBKRConnectionSettings(
            account_id="U20984427",
            source="flex",
            query_id="1445890",
            query_token="secret-token",
        )

        snapshot = _parse_flex_statement_xml(
            statement_xml=FLEX_STATEMENT_SAMPLE,
            settings=settings,
            now_ny=datetime(2026, 3, 24, 10, 14, tzinfo=NEW_YORK_TZ),
        )

        self.assertEqual(snapshot.trade_date, "2026-03-23")
        self.assertAlmostEqual(snapshot.net_liquidation, 10251.19)
        self.assertAlmostEqual(snapshot.cash_value, 8310.52, places=2)
        self.assertEqual([item.symbol for item in snapshot.positions], ["EL", "XMMO"])
        self.assertAlmostEqual(snapshot.positions[0].daily_pnl, -53.04)
        self.assertAlmostEqual(snapshot.positions[1].daily_pnl, 29.07)
        self.assertAlmostEqual(snapshot.total_unrealized_pnl, -5.370374, places=6)
        self.assertEqual(snapshot.source_period, "LastBusinessDay")
        self.assertEqual(snapshot.source_from_date, "2026-03-23")
        self.assertAlmostEqual(snapshot.nav_starting_value or 0.0, 10173.47)
        self.assertEqual(len(snapshot.trades), 0)

    def test_parse_flex_statement_xml_collects_trade_history_and_corporate_actions(self) -> None:
        settings = IBKRConnectionSettings(
            account_id="U20984427",
            source="flex",
            query_id="1445890",
            query_token="secret-token",
        )

        snapshot = _parse_flex_statement_xml(
            statement_xml=FLEX_STATEMENT_WITH_TRADES_SAMPLE,
            settings=settings,
            now_ny=datetime(2026, 3, 24, 10, 14, tzinfo=NEW_YORK_TZ),
        )

        self.assertEqual(snapshot.source_period, "DateRange")
        self.assertEqual(snapshot.source_from_date, "2024-01-02")
        self.assertEqual(snapshot.source_to_date, "2026-03-23")
        self.assertAlmostEqual(snapshot.nav_starting_value or 0.0, 10173.47)
        self.assertEqual(len(snapshot.trades), 2)
        self.assertEqual(snapshot.trades[0].symbol, "AAPL")
        self.assertEqual(snapshot.trades[0].buy_sell, "BUY")
        self.assertAlmostEqual(snapshot.trades[0].cash_spent or 0.0, 901.0)
        self.assertEqual(snapshot.trades[1].buy_sell, "SELL")
        self.assertEqual(len(snapshot.cash_events), 2)
        self.assertEqual(snapshot.cash_events[0]["event_date"], "2024-01-04")
        self.assertAlmostEqual(snapshot.cash_events[0]["amount"], 250.0)
        self.assertEqual(snapshot.cash_events[1]["section"], "Transfers")
        self.assertEqual(len(snapshot.corporate_actions), 1)

    def test_build_portfolio_snapshot_filters_and_normalizes_positions(self) -> None:
        snapshot = build_portfolio_snapshot(
            fetched_at=datetime(2026, 3, 23, 16, 30, tzinfo=NEW_YORK_TZ),
            trade_date="2026-03-23",
            account_id="U1234567",
            net_liquidation=123456.0,
            cash_value=34567.0,
            raw_positions=[
                {
                    "symbol": "AAPL",
                    "con_id": 1,
                    "sec_type": "STK",
                    "quantity": 10,
                    "market_price": 210.0,
                    "market_value": 2100.0,
                    "average_cost": 150.0,
                    "unrealized_pnl": 600.0,
                    "daily_pnl": 42.0,
                },
                {
                    "symbol": "SPY240621C00550000",
                    "con_id": 2,
                    "sec_type": "OPT",
                    "quantity": 1,
                    "market_price": 5.0,
                    "market_value": 500.0,
                    "average_cost": 4.0,
                    "unrealized_pnl": 100.0,
                    "daily_pnl": 10.0,
                },
                {
                    "symbol": "TSLA",
                    "con_id": 3,
                    "sec_type": "STK",
                    "quantity": 0,
                    "market_price": 180.0,
                    "market_value": 0.0,
                    "average_cost": 210.0,
                    "unrealized_pnl": 0.0,
                    "daily_pnl": None,
                },
                {
                    "symbol": "NVDA",
                    "con_id": 4,
                    "sec_type": "STK",
                    "quantity": 3,
                    "market_price": 800.0,
                    "market_value": 2400.0,
                    "average_cost": 700.0,
                    "unrealized_pnl": None,
                    "daily_pnl": -15.0,
                },
            ],
        )

        self.assertEqual([item.symbol for item in snapshot.positions], ["AAPL", "NVDA"])
        self.assertAlmostEqual(snapshot.total_unrealized_pnl, 900.0)
        self.assertTrue(snapshot.daily_data_complete)

    def test_rankings_exclude_invalid_denominators(self) -> None:
        snapshot = build_portfolio_snapshot(
            fetched_at=datetime(2026, 3, 23, 16, 30, tzinfo=NEW_YORK_TZ),
            trade_date="2026-03-23",
            account_id="U1234567",
            net_liquidation=90000.0,
            cash_value=20000.0,
            raw_positions=[
                {
                    "symbol": "AAPL",
                    "con_id": 1,
                    "sec_type": "STK",
                    "quantity": 10,
                    "market_price": 210.0,
                    "market_value": 2100.0,
                    "average_cost": 150.0,
                    "unrealized_pnl": 600.0,
                    "daily_pnl": 42.0,
                },
                {
                    "symbol": "AMD",
                    "con_id": 2,
                    "sec_type": "STK",
                    "quantity": 8,
                    "market_price": 160.0,
                    "market_value": 1280.0,
                    "average_cost": 0.0,
                    "unrealized_pnl": 0.0,
                    "daily_pnl": 12.0,
                },
                {
                    "symbol": "META",
                    "con_id": 3,
                    "sec_type": "STK",
                    "quantity": 2,
                    "market_price": 500.0,
                    "market_value": 1000.0,
                    "average_cost": 450.0,
                    "unrealized_pnl": 100.0,
                    "daily_pnl": 1000.0,
                },
            ],
        )

        gainers = top_lifetime_gainers(snapshot.positions)
        movers = top_daily_movers(snapshot.positions)
        self.assertEqual([item.symbol for item in gainers], ["AAPL", "META"])
        self.assertEqual([item.symbol for item in movers], ["AAPL", "AMD"])

    def test_schedule_logic_handles_before_after_duplicate_and_weekend(self) -> None:
        before_close = datetime(2026, 3, 23, 16, 0, tzinfo=NEW_YORK_TZ)
        after_close = datetime(2026, 3, 23, 16, 31, tzinfo=NEW_YORK_TZ)
        saturday = datetime(2026, 3, 21, 16, 31, tzinfo=NEW_YORK_TZ)

        self.assertFalse(should_run_daily_snapshot(before_close, None))
        self.assertTrue(should_run_daily_snapshot(after_close, None))
        self.assertFalse(should_run_daily_snapshot(after_close, "2026-03-23"))
        self.assertFalse(should_run_daily_snapshot(saturday, None))

        seconds, next_run = seconds_until_next_run(after_close)
        self.assertGreater(seconds, 0.0)
        self.assertEqual(next_run.date().isoformat(), "2026-03-24")

    def test_qqqm_values_use_yfinance_quote_and_total_difference(self) -> None:
        snapshot = make_snapshot(
            trade_date="2026-03-23",
            net_liquidation=105000.0,
            raw_trades=[
                {
                    "symbol": "AAPL",
                    "con_id": 1,
                    "sec_type": "STK",
                    "trade_date": "2026-03-21",
                    "buy_sell": "BUY",
                    "quantity": 1,
                    "trade_price": 549.0,
                    "trade_money": 549.0,
                    "net_cash": -550.0,
                    "commission": -1.0,
                },
                {
                    "symbol": "MSFT",
                    "con_id": 2,
                    "sec_type": "STK",
                    "trade_date": "2026-03-23",
                    "buy_sell": "SELL",
                    "quantity": -1,
                    "trade_price": 221.0,
                    "trade_money": -221.0,
                    "proceeds": 221.0,
                    "net_cash": 220.0,
                    "commission": -1.0,
                },
            ],
            raw_cash_events=[
                {
                    "event_date": "2026-03-23",
                    "amount": 110.0,
                    "section": "CashTransactions",
                    "description": "Dividend",
                }
            ],
        )
        benchmark = QQQMBenchmarkQuote(current_price=210.0, prior_close=200.0, daily_return_pct=5.0)
        close_history = QQQMCloseHistory(
            latest_close=110.0,
            close_by_date={"2026-03-20": 100.0, "2026-03-23": 110.0},
        )

        with patch("useful_bot.ibkr_portfolio_image._fetch_qqqm_benchmark_quote", return_value=benchmark), patch(
            "useful_bot.ibkr_portfolio_image._fetch_qqqm_close_history",
            return_value=close_history,
        ):
            qqqm_pnl = _qqqm_hypothetical_pnl(snapshot)
            qqqm_total_difference = compute_qqqm_total_diff(
                snapshot,
                baseline_trade_date="2026-03-20",
                baseline_net_liquidation=100000.0,
                baseline_qqqm_start_close=100.0,
            )

        self.assertAlmostEqual(qqqm_pnl, 5250.0)
        self.assertAlmostEqual(qqqm_total_difference, -5110.0)

    def test_qqqm_values_return_none_when_quote_is_unavailable(self) -> None:
        snapshot = make_snapshot()

        with patch("useful_bot.ibkr_portfolio_image._fetch_qqqm_benchmark_quote", return_value=None), patch(
            "useful_bot.ibkr_portfolio_image._fetch_qqqm_close_history",
            return_value=None,
        ):
            qqqm_pnl = _qqqm_hypothetical_pnl(snapshot)
            baseline = initialize_qqqm_benchmark_baseline(snapshot)

        self.assertIsNone(qqqm_pnl)
        self.assertIsNone(baseline)

    def test_initialize_qqqm_benchmark_baseline_uses_first_available_close(self) -> None:
        snapshot = make_snapshot(trade_date="2026-03-23", net_liquidation=95000.0)

        with patch(
            "useful_bot.ibkr_portfolio_image._fetch_qqqm_close_history",
            return_value=QQQMCloseHistory(latest_close=101.0, close_by_date={"2026-03-23": 101.0}),
        ):
            baseline = initialize_qqqm_benchmark_baseline(snapshot)

        self.assertIsNotNone(baseline)
        self.assertEqual(baseline["trade_date"], "2026-03-23")
        self.assertAlmostEqual(float(baseline["net_liquidation"]), 95000.0)
        self.assertAlmostEqual(float(baseline["qqqm_start_close"]), 101.0)

    def test_qqqm_total_difference_returns_none_when_close_history_is_missing(self) -> None:
        snapshot = make_snapshot()

        with patch(
            "useful_bot.ibkr_portfolio_image._fetch_qqqm_close_history",
            return_value=None,
        ):
            qqqm_total_difference = compute_qqqm_total_diff(
                snapshot,
                baseline_trade_date="2026-03-20",
                baseline_net_liquidation=100000.0,
                baseline_qqqm_start_close=100.0,
            )

        self.assertIsNone(qqqm_total_difference)

    def test_qqqm_total_difference_returns_none_when_trade_price_lookup_is_missing(self) -> None:
        snapshot = make_snapshot(
            trade_date="2026-03-23",
            raw_trades=[
                {
                    "symbol": "AAPL",
                    "con_id": 1,
                    "sec_type": "STK",
                    "trade_date": "2026-03-21",
                    "buy_sell": "BUY",
                    "quantity": 1,
                    "trade_price": 549.0,
                    "trade_money": 549.0,
                    "net_cash": -550.0,
                    "commission": -1.0,
                }
            ],
        )

        with patch(
            "useful_bot.ibkr_portfolio_image._fetch_qqqm_close_history",
            return_value=QQQMCloseHistory(latest_close=110.0, close_by_date={"2026-03-20": 100.0}),
        ):
            qqqm_total_difference = compute_qqqm_total_diff(
                snapshot,
                baseline_trade_date="2026-03-20",
                baseline_net_liquidation=100000.0,
                baseline_qqqm_start_close=100.0,
            )

        self.assertIsNone(qqqm_total_difference)

    def test_build_portfolio_html_renders_positive_negative_and_missing_qqqm_proxy_states(self) -> None:
        snapshot = make_snapshot(
            net_liquidation=100000.0,
            qqqm_total_diff=1250.0,
        )

        with patch(
            "useful_bot.ibkr_portfolio_image._fetch_qqqm_benchmark_quote",
            return_value=QQQMBenchmarkQuote(current_price=210.0, prior_close=200.0, daily_return_pct=5.0),
        ):
            positive_html = _build_portfolio_html(snapshot=snapshot, previous_snapshot=None)

        negative_snapshot = make_snapshot(net_liquidation=100000.0, qqqm_total_diff=-4200.0)
        with patch(
            "useful_bot.ibkr_portfolio_image._fetch_qqqm_benchmark_quote",
            return_value=QQQMBenchmarkQuote(current_price=190.0, prior_close=200.0, daily_return_pct=-5.0),
        ):
            negative_html = _build_portfolio_html(snapshot=negative_snapshot, previous_snapshot=None)

        missing_snapshot = make_snapshot(net_liquidation=100000.0, qqqm_total_diff=None)
        with patch("useful_bot.ibkr_portfolio_image._fetch_qqqm_benchmark_quote", return_value=None):
            missing_html = _build_portfolio_html(snapshot=missing_snapshot, previous_snapshot=None)

        self.assertIn("QQQM Total Diff", positive_html)
        self.assertIn("QQQM P&amp;L", positive_html)
        self.assertIn("+$5,000", positive_html)
        self.assertIn("+$1,250", positive_html)
        self.assertIn("$-5,000", negative_html)
        self.assertIn("$-4,200", negative_html)
        self.assertEqual(missing_html.count("No proxy"), 2)


class IBKRPortfolioHelperTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_check_sends_after_close_and_persists_state(self) -> None:
        app = FakeApplication()
        snapshot = make_snapshot()
        calls = {"count": 0}

        def fetcher(settings, now_ny):
            calls["count"] += 1
            return snapshot

        with tempfile.TemporaryDirectory() as temp_dir:
            helper = IBKRPortfolioHelper(
                chat_id=328968480,
                snapshot_fetcher=fetcher,
                settings_factory=make_settings,
                now_provider=lambda tz: datetime(2026, 3, 23, 16, 31, tzinfo=tz),
                state_file=Path(temp_dir) / "ibkr_state.json",
            )
            with patch(
                "useful_bot.ibkr_portfolio_helper.render_ibkr_portfolio_card",
                return_value=io.BytesIO(b"fake-image"),
            ):
                sent = await helper._run_check(app, reason="scheduled", force=False)

        self.assertTrue(sent)
        self.assertEqual(calls["count"], 1)
        self.assertEqual(len(app.bot.sent_photos), 1)
        self.assertEqual(helper._last_trade_date(), "2026-03-23")

    async def test_run_check_renders_via_worker_thread(self) -> None:
        app = FakeApplication()
        snapshot = make_snapshot()
        thread_handoff = {"called": False}

        async def fake_to_thread(func, *args, **kwargs):
            thread_handoff["called"] = True
            return func(*args, **kwargs)

        with tempfile.TemporaryDirectory() as temp_dir:
            helper = IBKRPortfolioHelper(
                chat_id=328968480,
                snapshot_fetcher=lambda settings, now_ny: snapshot,
                settings_factory=make_settings,
                now_provider=lambda tz: datetime(2026, 3, 23, 16, 31, tzinfo=tz),
                state_file=Path(temp_dir) / "ibkr_state.json",
            )
            with patch(
                "useful_bot.ibkr_portfolio_helper.render_ibkr_portfolio_card",
                return_value=io.BytesIO(b"fake-image"),
            ), patch("useful_bot.ibkr_portfolio_helper.asyncio.to_thread", side_effect=fake_to_thread):
                sent = await helper._run_check(app, reason="manual", force=True)

        self.assertTrue(sent)
        self.assertTrue(thread_handoff["called"])
        self.assertEqual(len(app.bot.sent_photos), 1)

    async def test_run_check_initializes_qqqm_benchmark_baseline(self) -> None:
        app = FakeApplication()
        snapshot = make_snapshot(net_liquidation=95000.0, trade_date="2026-03-23")

        with tempfile.TemporaryDirectory() as temp_dir:
            helper = IBKRPortfolioHelper(
                chat_id=328968480,
                snapshot_fetcher=lambda settings, now_ny: snapshot,
                settings_factory=make_settings,
                now_provider=lambda tz: datetime(2026, 3, 23, 16, 31, tzinfo=tz),
                state_file=Path(temp_dir) / "ibkr_state.json",
            )
            with patch(
                "useful_bot.ibkr_portfolio_helper.initialize_qqqm_benchmark_baseline",
                return_value={
                    "trade_date": "2026-03-23",
                    "net_liquidation": 95000.0,
                    "qqqm_start_close": 101.0,
                    "started_at": "2026-03-23T16:30:00-04:00",
                },
            ), patch(
                "useful_bot.ibkr_portfolio_helper.compute_qqqm_total_diff",
                return_value=1234.0,
            ), patch(
                "useful_bot.ibkr_portfolio_helper.render_ibkr_portfolio_card",
                return_value=io.BytesIO(b"fake-image"),
            ):
                sent = await helper._run_check(app, reason="manual", force=True)

        self.assertTrue(sent)
        self.assertEqual(helper._state["qqqm_benchmark"]["trade_date"], "2026-03-23")
        self.assertAlmostEqual(helper._last_snapshot().qqqm_total_diff or 0.0, 1234.0)

    async def test_run_check_skips_before_close(self) -> None:
        app = FakeApplication()
        calls = {"count": 0}

        def fetcher(settings, now_ny):
            calls["count"] += 1
            return make_snapshot()

        with tempfile.TemporaryDirectory() as temp_dir:
            helper = IBKRPortfolioHelper(
                chat_id=328968480,
                snapshot_fetcher=fetcher,
                settings_factory=make_settings,
                now_provider=lambda tz: datetime(2026, 3, 23, 15, 0, tzinfo=tz),
                state_file=Path(temp_dir) / "ibkr_state.json",
            )

            sent = await helper._run_check(app, reason="scheduled", force=False)

        self.assertFalse(sent)
        self.assertEqual(calls["count"], 0)
        self.assertEqual(len(app.bot.sent_photos), 0)

    async def test_run_check_skips_when_daily_data_is_incomplete(self) -> None:
        app = FakeApplication()

        with tempfile.TemporaryDirectory() as temp_dir:
            helper = IBKRPortfolioHelper(
                chat_id=328968480,
                snapshot_fetcher=lambda settings, now_ny: make_snapshot(
                    raw_positions=[
                        {
                            "symbol": "AAPL",
                            "con_id": 1,
                            "sec_type": "STK",
                            "quantity": 10,
                            "market_price": 210.0,
                            "market_value": 2100.0,
                            "average_cost": 150.0,
                            "unrealized_pnl": 600.0,
                            "daily_pnl": None,
                        }
                    ]
                ),
                settings_factory=make_settings,
                now_provider=lambda tz: datetime(2026, 3, 23, 16, 31, tzinfo=tz),
                state_file=Path(temp_dir) / "ibkr_state.json",
            )
            with patch(
                "useful_bot.ibkr_portfolio_helper.render_ibkr_portfolio_card",
                return_value=io.BytesIO(b"fake-image"),
            ):
                sent = await helper._run_check(app, reason="scheduled", force=False)

        self.assertFalse(sent)
        self.assertEqual(len(app.bot.sent_photos), 0)


class IBKRPortfolioRenderSmokeTests(unittest.TestCase):
    def test_render_smoke_images(self) -> None:
        ensure_runtime_dirs()
        debug_dir = RUNTIME_DEBUG_DIR / "ibkr_portfolio_render_tests"
        debug_dir.mkdir(parents=True, exist_ok=True)

        scenarios = {
            "positive_pnl": (
                make_snapshot(
                    net_liquidation=142000.0,
                    cash_value=38000.0,
                    raw_positions=make_full_positive_positions(),
                ),
                make_snapshot(
                    trade_date="2026-03-22",
                    net_liquidation=129500.0,
                    cash_value=36000.0,
                    raw_positions=make_full_positive_positions(),
                ),
            ),
            "negative_pnl": (
                make_snapshot(
                    raw_positions=make_full_negative_positions(),
                    net_liquidation=58000.0,
                    cash_value=12500.0,
                ),
                make_snapshot(
                    trade_date="2026-03-22",
                    net_liquidation=67600.0,
                    cash_value=14800.0,
                    raw_positions=make_full_negative_positions(),
                ),
            ),
            "high_cash": (
                make_snapshot(
                    net_liquidation=160000.0,
                    cash_value=118000.0,
                    raw_positions=make_full_positive_positions(),
                ),
                make_snapshot(
                    trade_date="2026-03-22",
                    net_liquidation=153800.0,
                    cash_value=112500.0,
                    raw_positions=make_full_positive_positions(),
                ),
            ),
            "sparse": (
                make_snapshot(
                    raw_positions=[
                        {
                            "symbol": "VOO",
                            "con_id": 1,
                            "sec_type": "STK",
                            "quantity": 2,
                            "market_price": 490.0,
                            "market_value": 980.0,
                            "average_cost": 430.0,
                            "unrealized_pnl": 120.0,
                            "daily_pnl": 5.0,
                        }
                    ]
                ),
                None,
            ),
        }

        with patch(
            "useful_bot.ibkr_portfolio_image._fetch_qqqm_benchmark_quote",
            return_value=QQQMBenchmarkQuote(current_price=210.0, prior_close=200.0, daily_return_pct=5.0),
        ), patch(
            "useful_bot.ibkr_portfolio_image._fetch_qqqm_close_history",
            return_value=QQQMCloseHistory(latest_close=220.0, close_by_date={}),
        ):
            for name, (snapshot, previous) in scenarios.items():
                image = render_ibkr_portfolio_card(snapshot=snapshot, previous_snapshot=previous)
                output_path = debug_dir / f"{name}_{RENDER_STYLE_VERSION}.png"
                output_path.write_bytes(image.getvalue())
                self.assertGreater(output_path.stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()
