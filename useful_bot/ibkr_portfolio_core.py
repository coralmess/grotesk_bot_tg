from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from math import isfinite
from typing import Any, Iterable, Optional
from zoneinfo import ZoneInfo

NEW_YORK_TZ = ZoneInfo("America/New_York")
SCHEDULE_HOUR = 16
SCHEDULE_MINUTE = 30
_IB_UNSET_THRESHOLD = 1e100


@dataclass
class PositionSnapshot:
    symbol: str
    con_id: int
    sec_type: str
    quantity: float
    market_price: float
    market_value: float
    average_cost: Optional[float]
    unrealized_pnl: float
    daily_pnl: Optional[float]
    currency: str = "USD"
    account: str = ""

    @property
    def lifetime_return_pct(self) -> Optional[float]:
        if self.average_cost is None or abs(self.average_cost) < 1e-9:
            return None
        return ((self.market_price - self.average_cost) / self.average_cost) * 100.0

    @property
    def prior_close_value(self) -> Optional[float]:
        if self.daily_pnl is None:
            return None
        prior_close_value = self.market_value - self.daily_pnl
        if not is_valid_number(prior_close_value) or abs(prior_close_value) < 1e-9:
            return None
        return prior_close_value

    @property
    def daily_change_pct(self) -> Optional[float]:
        prior_close_value = self.prior_close_value
        if prior_close_value is None:
            return None
        if self.daily_pnl is None:
            return None
        return (self.daily_pnl / prior_close_value) * 100.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PositionSnapshot":
        return cls(
            symbol=str(data.get("symbol", "")).upper(),
            con_id=int(data.get("con_id", 0) or 0),
            sec_type=str(data.get("sec_type", "")),
            quantity=float(data.get("quantity", 0.0) or 0.0),
            market_price=float(data.get("market_price", 0.0) or 0.0),
            market_value=float(data.get("market_value", 0.0) or 0.0),
            average_cost=coerce_optional_float(data.get("average_cost")),
            unrealized_pnl=float(data.get("unrealized_pnl", 0.0) or 0.0),
            daily_pnl=coerce_optional_float(data.get("daily_pnl")),
            currency=str(data.get("currency", "USD") or "USD"),
            account=str(data.get("account", "")),
        )


@dataclass
class TradeSnapshot:
    symbol: str
    con_id: int
    sec_type: str
    trade_date: str
    buy_sell: str
    quantity: float
    trade_price: Optional[float]
    trade_money: Optional[float]
    proceeds: Optional[float]
    net_cash: Optional[float]
    commission: Optional[float]
    currency: str = "USD"
    account: str = ""

    @property
    def is_equity_buy(self) -> bool:
        return self.buy_sell.upper() == "BUY" and self.sec_type.upper() in {"STK", "ETF"}

    @property
    def cash_spent(self) -> Optional[float]:
        if not self.is_equity_buy:
            return None
        if self.net_cash is not None and abs(self.net_cash) > 1e-9:
            return abs(self.net_cash)

        if self.trade_money is not None:
            cash_spent = abs(self.trade_money)
            if self.commission is not None:
                cash_spent += abs(self.commission)
            return cash_spent

        if self.proceeds is not None:
            cash_spent = abs(self.proceeds)
            if self.commission is not None:
                cash_spent += abs(self.commission)
            return cash_spent

        if self.trade_price is not None and abs(self.quantity) > 1e-9:
            cash_spent = abs(self.trade_price * self.quantity)
            if self.commission is not None:
                cash_spent += abs(self.commission)
            return cash_spent
        return None

    @property
    def is_equity_sell(self) -> bool:
        return self.buy_sell.upper() == "SELL" and self.sec_type.upper() in {"STK", "ETF"}

    @property
    def cash_received(self) -> Optional[float]:
        if not self.is_equity_sell:
            return None
        if self.net_cash is not None and abs(self.net_cash) > 1e-9:
            return abs(self.net_cash)

        if self.proceeds is not None:
            cash_received = abs(self.proceeds)
            if self.commission is not None:
                cash_received -= abs(self.commission)
            return max(cash_received, 0.0)

        if self.trade_money is not None:
            cash_received = abs(self.trade_money)
            if self.commission is not None:
                cash_received -= abs(self.commission)
            return max(cash_received, 0.0)

        if self.trade_price is not None and abs(self.quantity) > 1e-9:
            cash_received = abs(self.trade_price * self.quantity)
            if self.commission is not None:
                cash_received -= abs(self.commission)
            return max(cash_received, 0.0)
        return None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TradeSnapshot":
        return cls(
            symbol=str(data.get("symbol", "")).upper(),
            con_id=int(data.get("con_id", 0) or 0),
            sec_type=str(data.get("sec_type", "")),
            trade_date=str(data.get("trade_date", "")),
            buy_sell=str(data.get("buy_sell", "")),
            quantity=float(data.get("quantity", 0.0) or 0.0),
            trade_price=coerce_optional_float(data.get("trade_price")),
            trade_money=coerce_optional_float(data.get("trade_money")),
            proceeds=coerce_optional_float(data.get("proceeds")),
            net_cash=coerce_optional_float(data.get("net_cash")),
            commission=coerce_optional_float(data.get("commission")),
            currency=str(data.get("currency", "USD") or "USD"),
            account=str(data.get("account", "")),
        )


@dataclass
class PortfolioSnapshot:
    fetched_at: str
    trade_date: str
    account_id: str
    nav_starting_value: Optional[float]
    net_liquidation: float
    cash_value: float
    total_unrealized_pnl: float
    positions: list[PositionSnapshot]
    daily_data_complete: bool
    trades: list[TradeSnapshot]
    cash_events: list[dict[str, Any]]
    corporate_actions: list[dict[str, Any]]
    source_from_date: str = ""
    source_to_date: str = ""
    source_period: str = ""
    qqqm_total_diff: Optional[float] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "fetched_at": self.fetched_at,
            "trade_date": self.trade_date,
            "account_id": self.account_id,
            "nav_starting_value": self.nav_starting_value,
            "net_liquidation": self.net_liquidation,
            "cash_value": self.cash_value,
            "total_unrealized_pnl": self.total_unrealized_pnl,
            "daily_data_complete": self.daily_data_complete,
            "positions": [position.to_dict() for position in self.positions],
            "trades": [trade.to_dict() for trade in self.trades],
            "cash_events": self.cash_events,
            "corporate_actions": self.corporate_actions,
            "source_from_date": self.source_from_date,
            "source_to_date": self.source_to_date,
            "source_period": self.source_period,
            "qqqm_total_diff": self.qqqm_total_diff,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PortfolioSnapshot":
        positions = data.get("positions", [])
        if not isinstance(positions, list):
            positions = []
        trades = data.get("trades", [])
        if not isinstance(trades, list):
            trades = []
        cash_events = data.get("cash_events", [])
        if not isinstance(cash_events, list):
            cash_events = []
        corporate_actions = data.get("corporate_actions", [])
        if not isinstance(corporate_actions, list):
            corporate_actions = []
        return cls(
            fetched_at=str(data.get("fetched_at", "")),
            trade_date=str(data.get("trade_date", "")),
            account_id=str(data.get("account_id", "")),
            nav_starting_value=coerce_optional_float(data.get("nav_starting_value")),
            net_liquidation=float(data.get("net_liquidation", 0.0) or 0.0),
            cash_value=float(data.get("cash_value", 0.0) or 0.0),
            total_unrealized_pnl=float(data.get("total_unrealized_pnl", 0.0) or 0.0),
            positions=[PositionSnapshot.from_dict(item) for item in positions if isinstance(item, dict)],
            daily_data_complete=bool(data.get("daily_data_complete", False)),
            trades=[TradeSnapshot.from_dict(item) for item in trades if isinstance(item, dict)],
            cash_events=[item for item in cash_events if isinstance(item, dict)],
            corporate_actions=[item for item in corporate_actions if isinstance(item, dict)],
            source_from_date=str(data.get("source_from_date", "")),
            source_to_date=str(data.get("source_to_date", "")),
            source_period=str(data.get("source_period", "")),
            qqqm_total_diff=coerce_optional_float(data.get("qqqm_total_diff")),
        )


@dataclass
class RankedPosition:
    symbol: str
    percent: float


def is_valid_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and isfinite(float(value)) and abs(float(value)) < _IB_UNSET_THRESHOLD


def coerce_optional_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not is_valid_number(parsed):
        return None
    return parsed


def is_equity_position(sec_type: str, quantity: float) -> bool:
    return sec_type.upper() in {"STK", "ETF"} and abs(quantity) > 1e-9


def build_portfolio_snapshot(
    *,
    fetched_at: datetime,
    trade_date: str,
    account_id: str,
    net_liquidation: Optional[float],
    cash_value: Optional[float],
    raw_positions: Iterable[dict[str, Any]],
    nav_starting_value: Optional[float] = None,
    raw_trades: Iterable[dict[str, Any]] = (),
    raw_cash_events: Iterable[dict[str, Any]] = (),
    raw_corporate_actions: Iterable[dict[str, Any]] = (),
    source_from_date: str = "",
    source_to_date: str = "",
    source_period: str = "",
    qqqm_total_diff: Optional[float] = None,
) -> PortfolioSnapshot:
    positions: list[PositionSnapshot] = []
    for raw_position in raw_positions:
        symbol = str(raw_position.get("symbol", "") or "").upper()
        con_id = int(raw_position.get("con_id", 0) or 0)
        sec_type = str(raw_position.get("sec_type", "") or "")
        quantity = coerce_optional_float(raw_position.get("quantity")) or 0.0
        if not symbol or not is_equity_position(sec_type, quantity):
            continue

        market_price = coerce_optional_float(raw_position.get("market_price")) or 0.0
        market_value = coerce_optional_float(raw_position.get("market_value"))
        if market_value is None:
            market_value = market_price * quantity
        average_cost = coerce_optional_float(raw_position.get("average_cost"))
        unrealized_pnl = coerce_optional_float(raw_position.get("unrealized_pnl"))
        if unrealized_pnl is None and average_cost is not None:
            unrealized_pnl = market_value - (average_cost * quantity)
        daily_pnl = coerce_optional_float(raw_position.get("daily_pnl"))
        currency = str(raw_position.get("currency", "USD") or "USD")
        account = str(raw_position.get("account", account_id) or account_id)

        positions.append(
            PositionSnapshot(
                symbol=symbol,
                con_id=con_id,
                sec_type=sec_type,
                quantity=quantity,
                market_price=market_price,
                market_value=market_value,
                average_cost=average_cost,
                unrealized_pnl=unrealized_pnl or 0.0,
                daily_pnl=daily_pnl,
                currency=currency,
                account=account,
            )
        )

    trades: list[TradeSnapshot] = []
    for raw_trade in raw_trades:
        symbol = str(raw_trade.get("symbol", "") or "").upper()
        trade_date_value = str(raw_trade.get("trade_date", "") or "")
        if not symbol or not trade_date_value:
            continue
        trades.append(
            TradeSnapshot(
                symbol=symbol,
                con_id=int(raw_trade.get("con_id", 0) or 0),
                sec_type=str(raw_trade.get("sec_type", "") or ""),
                trade_date=trade_date_value,
                buy_sell=str(raw_trade.get("buy_sell", "") or "").upper(),
                quantity=coerce_optional_float(raw_trade.get("quantity")) or 0.0,
                trade_price=coerce_optional_float(raw_trade.get("trade_price")),
                trade_money=coerce_optional_float(raw_trade.get("trade_money")),
                proceeds=coerce_optional_float(raw_trade.get("proceeds")),
                net_cash=coerce_optional_float(raw_trade.get("net_cash")),
                commission=coerce_optional_float(raw_trade.get("commission")),
                currency=str(raw_trade.get("currency", "USD") or "USD"),
                account=str(raw_trade.get("account", account_id) or account_id),
            )
        )

    cash_events = [item for item in raw_cash_events if isinstance(item, dict) and str(item.get("event_date", "")).strip()]
    cash_events.sort(
        key=lambda item: (
            str(item.get("event_date", "")),
            str(item.get("section", "")),
            str(item.get("description", "")),
            float(coerce_optional_float(item.get("amount")) or 0.0),
        )
    )

    positions.sort(key=lambda item: item.symbol)
    trades.sort(key=lambda item: (item.trade_date, item.symbol, item.con_id, item.quantity))
    total_unrealized_pnl = sum(position.unrealized_pnl for position in positions)
    daily_data_complete = not positions or all(position.daily_pnl is not None for position in positions)
    return PortfolioSnapshot(
        fetched_at=fetched_at.isoformat(timespec="seconds"),
        trade_date=trade_date,
        account_id=account_id,
        nav_starting_value=nav_starting_value,
        net_liquidation=net_liquidation or 0.0,
        cash_value=cash_value or 0.0,
        total_unrealized_pnl=total_unrealized_pnl,
        positions=positions,
        daily_data_complete=daily_data_complete,
        trades=trades,
        cash_events=cash_events,
        corporate_actions=[item for item in raw_corporate_actions if isinstance(item, dict)],
        source_from_date=source_from_date,
        source_to_date=source_to_date,
        source_period=source_period,
        qqqm_total_diff=qqqm_total_diff,
    )


def top_lifetime_gainers(positions: Iterable[PositionSnapshot], limit: int = 5) -> list[RankedPosition]:
    ranked = [
        RankedPosition(symbol=position.symbol, percent=percent)
        for position in positions
        for percent in [position.lifetime_return_pct]
        if percent is not None
    ]
    ranked.sort(key=lambda item: (item.percent, item.symbol), reverse=True)
    return ranked[:limit]


def top_daily_movers(positions: Iterable[PositionSnapshot], limit: int = 5) -> list[RankedPosition]:
    ranked = [
        RankedPosition(symbol=position.symbol, percent=percent)
        for position in positions
        for percent in [position.daily_change_pct]
        if percent is not None
    ]
    ranked.sort(key=lambda item: (item.percent, item.symbol), reverse=True)
    return ranked[:limit]


def compute_balance_delta(
    current_snapshot: PortfolioSnapshot,
    previous_snapshot: Optional[PortfolioSnapshot],
) -> Optional[float]:
    if previous_snapshot is None:
        return None
    return current_snapshot.net_liquidation - previous_snapshot.net_liquidation


def should_skip_for_missing_daily_data(snapshot: PortfolioSnapshot) -> bool:
    return bool(snapshot.positions) and not snapshot.daily_data_complete


def should_run_daily_snapshot(now_ny: datetime, last_trade_date: Optional[str]) -> bool:
    if now_ny.tzinfo is None:
        now_ny = now_ny.replace(tzinfo=NEW_YORK_TZ)
    if now_ny.weekday() >= 5:
        return False
    scheduled = now_ny.replace(
        hour=SCHEDULE_HOUR,
        minute=SCHEDULE_MINUTE,
        second=0,
        microsecond=0,
    )
    if now_ny < scheduled:
        return False
    return last_trade_date != now_ny.date().isoformat()


def seconds_until_next_run(now_ny: datetime) -> tuple[float, datetime]:
    if now_ny.tzinfo is None:
        now_ny = now_ny.replace(tzinfo=NEW_YORK_TZ)
    candidate = now_ny.replace(
        hour=SCHEDULE_HOUR,
        minute=SCHEDULE_MINUTE,
        second=0,
        microsecond=0,
    )
    if now_ny.weekday() < 5 and now_ny < candidate:
        next_run = candidate
    else:
        next_run = candidate + timedelta(days=1)
        while next_run.weekday() >= 5:
            next_run += timedelta(days=1)
    return max(1.0, (next_run - now_ny).total_seconds()), next_run
