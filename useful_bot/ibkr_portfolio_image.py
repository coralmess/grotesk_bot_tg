from __future__ import annotations

import html
import io
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

from playwright.sync_api import sync_playwright
try:
    import yfinance as yf
except Exception:  # pragma: no cover - exercised when dependency is absent
    yf = None

from helpers.runtime_paths import PROJECT_ROOT
from useful_bot.ibkr_portfolio_core import (
    PortfolioSnapshot,
    RankedPosition,
    compute_balance_delta,
    top_daily_movers,
    top_lifetime_gainers,
)

FONTS_DIR = PROJECT_ROOT / "fonts"
CANVAS_W = 1200
CANVAS_H = 1200

RENDER_STYLE_VERSION = "v27"

BACKGROUND = "#ECF0F3"
SURFACE = "#EDF1F4"
SURFACE_SOFT = "#F2F5F8"
SHADOW_DARK = "#D1D9E6"
TEXT_PRIMARY = "#364250"
TEXT_MUTED = "#758190"
TEXT_FAINT = "#A9B2BC"
GREEN = "#24C65A"
RED = "#F04B4B"
YELLOW = "#F2A500"
TRACK = "#D8E1EC"

RING_SWEEP = 252.0
RING_START = 234.0

NUNITO_REGULAR = FONTS_DIR / "NunitoSans-Regular.ttf"
NUNITO_SEMIBOLD = FONTS_DIR / "NunitoSans-SemiBold.ttf"
NUNITO_BOLD = FONTS_DIR / "NunitoSans-Bold.ttf"
NUNITO_EXTRABOLD = FONTS_DIR / "NunitoSans-ExtraBold.ttf"


@dataclass(frozen=True)
class QQQMBenchmarkQuote:
    current_price: float
    prior_close: float
    daily_return_pct: float


@dataclass(frozen=True)
class QQQMCloseHistory:
    latest_close: float
    close_by_date: dict[str, float]

def render_ibkr_portfolio_card(
    *,
    snapshot: PortfolioSnapshot,
    previous_snapshot: Optional[PortfolioSnapshot] = None,
) -> io.BytesIO:
    html_doc = _build_portfolio_html(snapshot=snapshot, previous_snapshot=previous_snapshot)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": CANVAS_W, "height": CANVAS_H}, device_scale_factor=1)
        page.set_content(html_doc, wait_until="load")
        page.wait_for_timeout(250)
        png_bytes = page.screenshot(type="png", clip={"x": 0, "y": 0, "width": CANVAS_W, "height": CANVAS_H})
        browser.close()

    buf = io.BytesIO(png_bytes)
    buf.seek(0)
    return buf


def _build_portfolio_html(
    *,
    snapshot: PortfolioSnapshot,
    previous_snapshot: Optional[PortfolioSnapshot],
) -> str:
    delta = compute_balance_delta(snapshot, previous_snapshot)
    delta_text, delta_class = _format_delta(delta)
    gainers_html = _build_gainer_rows(snapshot, top_lifetime_gainers(snapshot.positions))
    movers_html = _build_rank_rows(top_daily_movers(snapshot.positions))
    ring = _ring_segments(snapshot.net_liquidation, snapshot.total_unrealized_pnl)
    font_face_css = _font_face_css()
    today_change = _format_optional_currency(_portfolio_daily_pnl(snapshot), missing_text="No daily data")
    qqqm_change = _format_optional_currency(_qqqm_hypothetical_pnl(snapshot), missing_text="No proxy")
    qqqm_unrealized = _format_optional_currency(_qqqm_ytd_difference(snapshot), missing_text="No proxy")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width={CANVAS_W}, initial-scale=1">
  <style>
    {font_face_css}
    :root {{
      --bg: {BACKGROUND};
      --surface: {SURFACE};
      --surface-soft: {SURFACE_SOFT};
      --white-shadow: rgba(255, 255, 255, 0.98);
      --dark-shadow: rgba(209, 217, 230, 0.92);
      --text: {TEXT_PRIMARY};
      --muted: {TEXT_MUTED};
      --faint: {TEXT_FAINT};
      --green: {GREEN};
      --red: {RED};
      --yellow: {YELLOW};
      --track: {TRACK};
      --panel-shadow: -12px -12px 24px rgba(255,255,255,0.88), 12px 12px 24px rgba(209,217,230,0.74);
      --chip-shadow: -8px -8px 16px rgba(255,255,255,0.92), 8px 8px 16px rgba(209,217,230,0.72);
      --inset-shadow-lg: inset -10px -10px 20px rgba(255,255,255,0.94), inset 10px 10px 20px rgba(209,217,230,0.84);
      --inset-shadow-sm: inset -4px -4px 8px rgba(255,255,255,0.95), inset 4px 4px 8px rgba(209,217,230,0.8);
      --ring-start: {RING_START}deg;
      --ring-sweep: {RING_SWEEP}deg;
      --ring-yellow-end: {ring["yellow_end_deg"]}deg;
      --ring-end: {ring["end_deg"]}deg;
      --ring-accent-1: {ring["accent_1"]};
      --ring-accent-2: {ring["accent_2"]};
    }}

    * {{
      box-sizing: border-box;
    }}

    html, body {{
      margin: 0;
      width: {CANVAS_W}px;
      height: {CANVAS_H}px;
      overflow: hidden;
      background: var(--bg);
      font-family: "Nunito Sans", "Segoe UI", sans-serif;
      color: var(--text);
    }}

    body {{
      background:
        radial-gradient(circle at 18% 10%, rgba(255,255,255,0.45), transparent 32%),
        radial-gradient(circle at 82% 84%, rgba(255,255,255,0.22), transparent 30%),
        var(--bg);
    }}

    .canvas {{
      width: 100%;
      height: 100%;
      padding: 54px 36px 36px;
    }}

    .header {{
      padding-left: 18px;
      margin-bottom: 18px;
    }}

    .title {{
      font-size: 57px;
      line-height: 1;
      font-weight: 800;
      color: var(--text);
      letter-spacing: -0.04em;
    }}

    .subtitle {{
      margin-top: 10px;
      font-size: 25px;
      font-weight: 600;
      color: var(--muted);
      letter-spacing: -0.02em;
    }}

    .grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 18px;
      height: calc(100% - 106px);
    }}

    .panel {{
      position: relative;
      background: var(--surface);
      border-radius: 36px;
      box-shadow: var(--panel-shadow);
      overflow: hidden;
    }}

    .top-panel {{
      grid-column: 1 / -1;
      height: 470px;
      padding: 28px 42px 36px;
    }}

    .bottom-panel {{
      padding: 28px 24px 12px;
    }}

    .dial-wrap {{
      position: absolute;
      left: 50%;
      top: 76px;
      width: 356px;
      height: 356px;
      transform: translateX(-50%);
      border-radius: 50%;
      background: var(--surface);
      box-shadow: var(--inset-shadow-lg);
      display: grid;
      place-items: center;
    }}

    .dial-ring {{
      position: absolute;
      width: 332px;
      height: 332px;
      border-radius: 50%;
    }}

    .dial-ring.track {{
      background: conic-gradient(
        from var(--ring-start),
        var(--track) 0deg var(--ring-sweep),
        transparent var(--ring-sweep) 360deg
      );
      -webkit-mask: radial-gradient(farthest-side, transparent calc(100% - 38px), #000 calc(100% - 37px));
      mask: radial-gradient(farthest-side, transparent calc(100% - 38px), #000 calc(100% - 37px));
      opacity: 1;
    }}

    .dial-ring.accent {{
      width: 332px;
      height: 332px;
      background: conic-gradient(
        from var(--ring-start),
        color-mix(in srgb, white 16%, var(--ring-accent-1)) 0deg,
        var(--ring-accent-1) calc(var(--ring-yellow-end) - 10deg),
        color-mix(in srgb, black 8%, var(--ring-accent-1)) var(--ring-yellow-end),
        color-mix(in srgb, white 12%, var(--ring-accent-2)) calc(var(--ring-yellow-end) + 2deg),
        var(--ring-accent-2) calc(var(--ring-end) - 6deg),
        color-mix(in srgb, black 10%, var(--ring-accent-2)) var(--ring-end),
        transparent var(--ring-end) 360deg
      );
      -webkit-mask: radial-gradient(farthest-side, transparent calc(100% - 38px), #000 calc(100% - 37px));
      mask: radial-gradient(farthest-side, transparent calc(100% - 38px), #000 calc(100% - 37px));
      filter: drop-shadow(-1px -1px 1px rgba(255,255,255,0.38)) drop-shadow(1px 2px 2px rgba(160,176,200,0.26));
      opacity: 1;
    }}

    .dial-center {{
      position: relative;
      z-index: 2;
      width: 282px;
      height: 282px;
      border-radius: 50%;
      background: var(--surface-soft);
      box-shadow: -7px -7px 14px rgba(255,255,255,0.92), 7px 7px 14px rgba(209,217,230,0.58);
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      text-align: center;
      padding-top: 2px;
    }}

    .hero-label {{
      font-size: 24px;
      font-weight: 600;
      color: var(--muted);
      letter-spacing: -0.02em;
      margin-bottom: 6px;
    }}

    .hero-value {{
      font-size: 50px;
      line-height: 1;
      font-weight: 800;
      letter-spacing: -0.05em;
      color: var(--text);
    }}

    .hero-delta {{
      margin-top: 8px;
      font-size: 22px;
      font-weight: 700;
      letter-spacing: -0.02em;
    }}

    .hero-delta.positive {{
      color: var(--green);
    }}

    .hero-delta.negative {{
      color: var(--red);
    }}

    .hero-delta.neutral {{
      color: var(--faint);
    }}

    .chip-row {{
      position: absolute;
      left: 42px;
      right: 42px;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }}

    .chip-row.top {{
      top: 30px;
    }}

    .chip-row.bottom {{
      bottom: 28px;
    }}

    .chip {{
      width: 272px;
      height: 108px;
      border-radius: 24px;
      background: var(--surface);
      box-shadow: var(--chip-shadow);
      padding: 18px 22px 16px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
    }}

    .chip-label {{
      display: flex;
      align-items: center;
      gap: 10px;
      font-size: 18px;
      font-weight: 700;
      color: var(--text);
      letter-spacing: -0.02em;
    }}

    .chip-dot {{
      width: 8px;
      height: 8px;
      border-radius: 50%;
      flex: 0 0 auto;
    }}

    .chip-value {{
      font-size: 28px;
      line-height: 1;
      font-weight: 800;
      letter-spacing: -0.03em;
      color: var(--text);
    }}

    .chip-value.positive {{
      color: var(--green);
    }}

    .chip-value.negative {{
      color: var(--red);
    }}

    .panel-title {{
      font-size: 29px;
      line-height: 1.05;
      font-weight: 800;
      letter-spacing: -0.03em;
      color: var(--text);
      margin-bottom: 6px;
    }}

    .panel-subtitle {{
      font-size: 15px;
      font-weight: 700;
      color: var(--muted);
      letter-spacing: -0.02em;
      margin-bottom: 16px;
    }}

    .rows {{
      display: flex;
      flex-direction: column;
      gap: 14px;
      padding-bottom: 2px;
    }}

    .row {{
      height: 52px;
      border-radius: 18px;
      background: var(--surface);
      box-shadow: -5px -5px 10px rgba(255,255,255,0.92), 5px 5px 10px rgba(209,217,230,0.62);
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 18px;
      font-size: 17px;
      letter-spacing: -0.02em;
    }}

    .row-left {{
      display: flex;
      align-items: baseline;
      gap: 10px;
      min-width: 0;
    }}

    .row-symbol {{
      font-weight: 800;
      color: var(--text);
    }}

    .row-symbol.empty {{
      color: var(--faint);
    }}

    .row-value {{
      font-weight: 800;
    }}

    .row-value.positive {{
      color: var(--green);
    }}

    .row-value.negative {{
      color: var(--red);
    }}

    .row-value.neutral {{
      color: var(--faint);
      font-weight: 700;
    }}

    .row-weight {{
      font-size: 14px;
      font-weight: 700;
      color: var(--muted);
      white-space: nowrap;
    }}

  </style>
</head>
<body>
  <div class="canvas">
    <div class="header">
      <div class="title">IBKR Daily Portfolio</div>
      <div class="subtitle">{_e(snapshot.trade_date)} | {_e(snapshot.account_id)}</div>
    </div>

    <div class="grid">
      <section class="panel top-panel">
        <div class="dial-wrap">
          <div class="dial-ring track"></div>
          <div class="dial-ring accent"></div>
          <div class="dial-center">
            <div class="hero-label">Net liquidation</div>
            <div class="hero-value">{_e(_format_currency(snapshot.net_liquidation, show_sign=False))}</div>
            <div class="hero-delta {delta_class}">{_e(delta_text)}</div>
          </div>
        </div>

        <div class="chip-row top">
          {_build_chip("Unrealized P&L", _format_currency(snapshot.total_unrealized_pnl, show_sign=True), GREEN if snapshot.total_unrealized_pnl >= 0 else RED, value_class="positive" if snapshot.total_unrealized_pnl >= 0 else "negative")}
          {_build_chip("QQQM YTD Diff", qqqm_unrealized["text"], GREEN if qqqm_unrealized["class_name"] == "positive" else RED if qqqm_unrealized["class_name"] == "negative" else TEXT_FAINT, value_class=qqqm_unrealized["class_name"])}
        </div>

        <div class="chip-row bottom">
          {_build_chip("Today's P&L", today_change["text"], GREEN if today_change["class_name"] == "positive" else RED if today_change["class_name"] == "negative" else TEXT_FAINT, value_class=today_change["class_name"])}
          {_build_chip("QQQM P&L", qqqm_change["text"], GREEN if qqqm_change["class_name"] == "positive" else RED if qqqm_change["class_name"] == "negative" else TEXT_FAINT, value_class=qqqm_change["class_name"])}
        </div>
      </section>

      <section class="panel bottom-panel">
        <div class="panel-title">Top Gainers</div>
        <div class="panel-subtitle">All time vs average cost</div>
        <div class="rows">{gainers_html}</div>
      </section>

      <section class="panel bottom-panel">
        <div class="panel-title">Top Movers Today</div>
        <div class="panel-subtitle">Current day change vs prior close</div>
        <div class="rows">{movers_html}</div>
      </section>
    </div>
  </div>
</body>
</html>"""


def _build_chip(label: str, value: str, accent: str, *, value_class: str = "") -> str:
    suffix = f" {value_class}" if value_class else ""
    return (
        '<div class="chip">'
        f'<div class="chip-label"><span class="chip-dot" style="background:{accent};"></span>{_e(label)}</div>'
        f'<div class="chip-value{suffix}">{_e(value)}</div>'
        "</div>"
    )


def _build_rank_rows(rows: list[RankedPosition]) -> str:
    markup: list[str] = []
    for index in range(5):
        item = rows[index] if index < len(rows) else None
        if item is None:
            markup.append(
                '<div class="row">'
                '<div class="row-symbol empty">-</div>'
                '<div class="row-value neutral">No data</div>'
                "</div>"
            )
            continue

        value_class = "positive" if item.percent >= 0 else "negative"
        markup.append(
            '<div class="row">'
            f'<div class="row-left"><div class="row-symbol">{_e(item.symbol)}</div></div>'
            f'<div class="row-value {value_class}">{_e(_format_percent(item.percent))}</div>'
            "</div>"
        )
    return "".join(markup)


def _build_gainer_rows(snapshot: PortfolioSnapshot, rows: list[RankedPosition]) -> str:
    positions_by_symbol = {position.symbol: position for position in snapshot.positions}
    markup: list[str] = []
    for index in range(5):
        item = rows[index] if index < len(rows) else None
        if item is None:
            markup.append(
                '<div class="row">'
                '<div class="row-left"><div class="row-symbol empty">-</div><div class="row-weight">No data</div></div>'
                '<div class="row-value neutral">No data</div>'
                "</div>"
            )
            continue

        value_class = "positive" if item.percent >= 0 else "negative"
        position = positions_by_symbol.get(item.symbol)
        weight = 0.0
        if position is not None and abs(snapshot.net_liquidation) > 1e-9:
            weight = (position.market_value / snapshot.net_liquidation) * 100.0
        markup.append(
            '<div class="row">'
            f'<div class="row-left"><div class="row-symbol">{_e(item.symbol)}</div><div class="row-weight">{_e(_format_portfolio_weight(weight))}</div></div>'
            f'<div class="row-value {value_class}">{_e(_format_percent(item.percent))}</div>'
            "</div>"
        )
    return "".join(markup)


def _portfolio_daily_pnl(snapshot: PortfolioSnapshot) -> Optional[float]:
    if snapshot.positions and not snapshot.daily_data_complete:
        return None
    total = 0.0
    has_data = False
    for position in snapshot.positions:
        if position.daily_pnl is None:
            continue
        total += position.daily_pnl
        has_data = True
    return total if has_data else None


def _qqqm_hypothetical_pnl(snapshot: PortfolioSnapshot) -> Optional[float]:
    benchmark = _fetch_qqqm_benchmark_quote()
    if benchmark is None:
        return None
    return snapshot.net_liquidation * benchmark.daily_return_pct / 100.0


def _qqqm_ytd_difference(snapshot: PortfolioSnapshot) -> Optional[float]:
    if snapshot.nav_starting_value is None or snapshot.nav_starting_value <= 0:
        return None
    if not snapshot.source_from_date or not snapshot.source_to_date:
        return None

    benchmark_history = _fetch_qqqm_close_history(snapshot.source_from_date, snapshot.source_to_date)
    if benchmark_history is None:
        return None

    start_close = _first_close_on_or_after(benchmark_history.close_by_date, snapshot.source_from_date)
    end_close = _last_close_on_or_before(benchmark_history.close_by_date, snapshot.source_to_date)
    if start_close is None or end_close is None or start_close <= 0 or end_close <= 0:
        return None

    portfolio_end_closes = _fetch_position_end_closes(snapshot, snapshot.source_to_date)
    if portfolio_end_closes is None:
        return None

    actual_portfolio_end_value = snapshot.cash_value
    for position in snapshot.positions:
        close_value = portfolio_end_closes.get(position.symbol)
        if close_value is None or close_value <= 0:
            return None
        actual_portfolio_end_value += position.quantity * close_value

    qqqm_ending_value = snapshot.nav_starting_value * (end_close / start_close)
    return actual_portfolio_end_value - qqqm_ending_value


def _fetch_qqqm_benchmark_quote() -> Optional[QQQMBenchmarkQuote]:
    if yf is None:
        return None
    try:
        history = yf.Ticker("QQQM").history(period="5d", interval="1d", auto_adjust=False)
    except Exception:
        return None
    if history is None or getattr(history, "empty", True):
        return None
    columns = getattr(history, "columns", [])
    if "Close" not in columns:
        return None
    try:
        closes = [float(value) for value in history["Close"].tolist() if float(value) > 0]
    except Exception:
        return None
    if len(closes) < 2:
        return None
    prior_close = closes[-2]
    current_price = closes[-1]
    if prior_close <= 0 or current_price <= 0:
        return None
    return QQQMBenchmarkQuote(
        current_price=current_price,
        prior_close=prior_close,
        daily_return_pct=((current_price - prior_close) / prior_close) * 100.0,
    )


def _fetch_qqqm_close_history(start_date: str, end_date: str) -> Optional[QQQMCloseHistory]:
    if yf is None:
        return None
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d").date() - timedelta(days=7)
        end = datetime.strptime(end_date, "%Y-%m-%d").date() + timedelta(days=1)
    except ValueError:
        return None

    try:
        history = yf.Ticker("QQQM").history(
            start=start.isoformat(),
            end=end.isoformat(),
            interval="1d",
            auto_adjust=False,
        )
    except Exception:
        return None
    if history is None or getattr(history, "empty", True):
        return None
    columns = getattr(history, "columns", [])
    if "Close" not in columns:
        return None

    close_by_date: dict[str, float] = {}
    latest_close: Optional[float] = None
    try:
        for index, close in history["Close"].items():
            parsed_close = float(close)
            if parsed_close <= 0:
                continue
            iso_date = index.date().isoformat()
            close_by_date[iso_date] = parsed_close
            latest_close = parsed_close
    except Exception:
        return None
    if latest_close is None:
        return None
    return QQQMCloseHistory(latest_close=latest_close, close_by_date=close_by_date)


def _fetch_position_end_closes(snapshot: PortfolioSnapshot, target_date: str) -> Optional[dict[str, float]]:
    if yf is None:
        return None
    symbols = sorted({position.symbol for position in snapshot.positions if position.symbol})
    if not symbols:
        return {}
    close_by_symbol: dict[str, float] = {}
    for symbol in symbols:
        history = _fetch_symbol_close_history(symbol, target_date)
        if history is None:
            return None
        close_value = _last_close_on_or_before(history.close_by_date, target_date)
        if close_value is None or close_value <= 0:
            return None
        close_by_symbol[symbol] = close_value
    return close_by_symbol


def _fetch_symbol_close_history(symbol: str, end_date: str) -> Optional[QQQMCloseHistory]:
    try:
        end = datetime.strptime(end_date, "%Y-%m-%d").date() + timedelta(days=1)
        start = end - timedelta(days=10)
    except ValueError:
        return None

    try:
        history = yf.Ticker(symbol).history(
            start=start.isoformat(),
            end=end.isoformat(),
            interval="1d",
            auto_adjust=False,
        )
    except Exception:
        return None
    if history is None or getattr(history, "empty", True):
        return None
    columns = getattr(history, "columns", [])
    if "Close" not in columns:
        return None

    close_by_date: dict[str, float] = {}
    latest_close: Optional[float] = None
    try:
        for index, close in history["Close"].items():
            parsed_close = float(close)
            if parsed_close <= 0:
                continue
            iso_date = index.date().isoformat()
            close_by_date[iso_date] = parsed_close
            latest_close = parsed_close
    except Exception:
        return None
    if latest_close is None:
        return None
    return QQQMCloseHistory(latest_close=latest_close, close_by_date=close_by_date)


def _first_close_on_or_after(close_by_date: dict[str, float], target_date: str) -> Optional[float]:
    for trade_date in sorted(close_by_date):
        if trade_date >= target_date:
            return close_by_date[trade_date]
    return None


def _last_close_on_or_before(close_by_date: dict[str, float], target_date: str) -> Optional[float]:
    for trade_date in sorted(close_by_date, reverse=True):
        if trade_date <= target_date:
            return close_by_date[trade_date]
    return None


def _format_optional_currency(value: Optional[float], *, missing_text: str) -> dict[str, str]:
    if value is None:
        return {"text": missing_text, "class_name": "neutral"}
    if value > 0:
        return {"text": _format_currency(value, show_sign=True), "class_name": "positive"}
    if value < 0:
        return {"text": _format_currency(value, show_sign=True), "class_name": "negative"}
    return {"text": "$0", "class_name": "neutral"}


def _ring_segments(net_liquidation: float, pnl: float) -> dict[str, object]:
    pnl_abs = abs(pnl)
    total = max(net_liquidation + pnl_abs, 1e-9)
    yellow_sweep = RING_SWEEP * max(0.0, min(1.0, net_liquidation / total))
    pnl_sweep = RING_SWEEP - yellow_sweep
    accent_2 = GREEN if pnl >= 0 else RED
    return {
        "yellow_end_deg": round(yellow_sweep, 3),
        "end_deg": round(yellow_sweep + pnl_sweep, 3),
        "accent_1": YELLOW,
        "accent_2": accent_2,
    }


def _font_face_css() -> str:
    faces: list[tuple[str, Path, int]] = [
        ("Nunito Sans", NUNITO_REGULAR, 400),
        ("Nunito Sans", NUNITO_SEMIBOLD, 600),
        ("Nunito Sans", NUNITO_BOLD, 700),
        ("Nunito Sans", NUNITO_EXTRABOLD, 800),
    ]
    css_parts: list[str] = []
    for family, path, weight in faces:
        if not path.exists():
            continue
        css_parts.append(
            f"""
            @font-face {{
              font-family: "{family}";
              src: url("{path.as_uri()}") format("truetype");
              font-weight: {weight};
              font-style: normal;
            }}
            """
        )
    return "\n".join(css_parts)


def _format_delta(delta: Optional[float]) -> tuple[str, str]:
    if delta is None:
        return "No prior snapshot", "neutral"
    prefix = "+" if delta >= 0 else "-"
    return prefix + _format_currency(abs(delta), show_sign=False), "positive" if delta >= 0 else "negative"


def _format_currency(value: float, *, show_sign: bool) -> str:
    sign = "+" if show_sign and value > 0 else ""
    return f"{sign}${round(value):,}"


def _format_percent(value: float) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.2f}%"


def _format_portfolio_weight(value: float) -> str:
    return f"{value:.1f}%"


def _e(value: object) -> str:
    return html.escape(str(value), quote=True)
