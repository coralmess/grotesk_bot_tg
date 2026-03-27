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
    coerce_optional_float,
    compute_balance_delta,
    top_daily_movers,
    top_lifetime_gainers,
)

FONTS_DIR = PROJECT_ROOT / "fonts"
CANVAS_W = 1200
CANVAS_H = 1500

RENDER_STYLE_VERSION = "v41"

BACKGROUND = "#0A0F17"
SURFACE = "#111824"
SURFACE_SOFT = "#182131"
SHADOW_DARK = "#02050A"
TEXT_PRIMARY = "#F6F8FC"
TEXT_MUTED = "#98A5BB"
TEXT_FAINT = "#69758A"
GREEN = "#2DDA87"
RED = "#FF6B6B"
YELLOW = "#FFB62E"
TRACK = "#2B3647"

RING_SWEEP = 180.0
RING_START = 180.0

NUNITO_REGULAR = FONTS_DIR / "NunitoSans-Regular.ttf"
NUNITO_SEMIBOLD = FONTS_DIR / "NunitoSans-SemiBold.ttf"
NUNITO_BOLD = FONTS_DIR / "NunitoSans-Bold.ttf"
NUNITO_EXTRABOLD = FONTS_DIR / "NunitoSans-ExtraBold.ttf"
SFPRO_BOLD = FONTS_DIR / "SFPro-Bold.ttf"
SFPRO_HEAVY = FONTS_DIR / "SFPro-Heavy.ttf"


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
    return _render_html_to_png(html_doc)


def _render_html_to_png(html_doc: str) -> io.BytesIO:
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
    hero_variant: str = "half_ring",
) -> str:
    delta = compute_balance_delta(snapshot, previous_snapshot)
    delta_text, delta_class = _format_delta(delta)
    gainers_html = _build_gainer_rows(snapshot, top_lifetime_gainers(snapshot.positions))
    movers_html = _build_rank_rows(top_daily_movers(snapshot.positions) if snapshot.daily_data_complete else [])
    ring = _ring_segments(snapshot.net_liquidation, snapshot.total_unrealized_pnl)
    font_face_css = _font_face_css()
    today_change = _format_optional_currency(_portfolio_daily_pnl(snapshot), missing_text="No daily data")
    qqqm_change = _format_optional_currency(_qqqm_hypothetical_pnl(snapshot), missing_text="No proxy")
    qqqm_total_diff = _format_optional_currency(snapshot.qqqm_total_diff, missing_text="No proxy")
    holdings_text = f"{len(snapshot.positions)} holdings"
    hero_visual_html = _build_hero_visual(snapshot, ring, hero_variant)

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
      --peach: #2a1d1e;
      --mint: #13251f;
      --sky: #15202f;
      --lemon: #2a2413;
      --rose: #291b20;
      --lavender: #1b2034;
      --white-shadow: rgba(255, 255, 255, 0.08);
      --dark-shadow: rgba(1, 4, 10, 0.5);
      --text: {TEXT_PRIMARY};
      --muted: {TEXT_MUTED};
      --faint: {TEXT_FAINT};
      --green: {GREEN};
      --red: {RED};
      --yellow: {YELLOW};
      --track: {TRACK};
      --tile-shadow: 0 26px 64px rgba(1, 4, 10, 0.48), 0 6px 18px rgba(1, 4, 10, 0.26);
      --tile-shadow-strong: 0 38px 86px rgba(1, 4, 10, 0.62), 0 10px 22px rgba(1, 4, 10, 0.3);
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
      font-family: "SF Pro Display", "Nunito Sans", "Segoe UI", sans-serif;
      color: var(--text);
    }}

    body {{
      background:
        radial-gradient(circle at 12% 8%, rgba(75, 119, 255, 0.18), transparent 26%),
        radial-gradient(circle at 84% 12%, rgba(255, 182, 46, 0.12), transparent 24%),
        radial-gradient(circle at 82% 84%, rgba(45, 218, 135, 0.08), transparent 20%),
        linear-gradient(180deg, rgba(22, 31, 47, 0.8), rgba(10, 15, 23, 1)),
        var(--bg);
    }}

    .canvas {{
      width: 100%;
      height: 100%;
      padding: 32px;
    }}

    .layout {{
      display: grid;
      grid-template-columns: repeat(12, minmax(0, 1fr));
      grid-template-rows: 92px 560px 1fr;
      gap: 24px;
      height: 100%;
    }}

    .tile {{
      position: relative;
      background: rgba(17, 24, 36, 0.88);
      border: 1px solid rgba(255, 255, 255, 0.06);
      border-radius: 34px;
      box-shadow: var(--tile-shadow);
      overflow: hidden;
    }}

    .topbar {{
      grid-column: 1 / -1;
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 20px 28px;
      background:
        radial-gradient(circle at 100% 0%, rgba(93, 135, 255, 0.14), transparent 34%),
        linear-gradient(135deg, rgba(21, 29, 43, 0.96), rgba(14, 20, 31, 0.98));
      backdrop-filter: blur(8px);
    }}

    .brand {{
      display: flex;
      flex-direction: column;
      gap: 2px;
    }}

    .title {{
      font-size: 43px;
      line-height: 0.98;
      font-weight: 800;
      color: var(--text);
      letter-spacing: -0.045em;
      text-shadow: none;
    }}

    .subtitle {{
      font-family: "Nunito Sans", "Segoe UI", sans-serif;
      font-size: 16px;
      font-weight: 700;
      color: #7f8ba0;
      letter-spacing: -0.01em;
    }}

    .meta {{
      display: flex;
      align-items: center;
      gap: 10px;
    }}

    .meta-pill {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      min-height: 42px;
      padding: 0 16px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.04);
      border: 1px solid rgba(255,255,255,0.08);
      color: #dce3ef;
      font-size: 14px;
      font-weight: 700;
      letter-spacing: -0.02em;
      white-space: nowrap;
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.04);
    }}

    .meta-pill.soft {{
      background: rgba(93, 135, 255, 0.1);
      color: #b3c3df;
    }}

    .hero {{
      grid-column: 1 / span 7;
      grid-row: 2;
      padding: 36px 36px 34px;
      background:
        radial-gradient(circle at 82% 18%, rgba(255, 182, 46, 0.14), transparent 28%),
        radial-gradient(circle at 14% 6%, rgba(93, 135, 255, 0.12), transparent 22%),
        linear-gradient(145deg, rgba(18, 26, 39, 0.98), rgba(11, 16, 25, 0.98)),
        linear-gradient(180deg, rgba(255,255,255,0.03), rgba(255,255,255,0));
      box-shadow: var(--tile-shadow-strong);
    }}

    .hero-copy {{
      position: relative;
      z-index: 4;
      display: flex;
      flex-direction: column;
      align-items: flex-start;
      gap: 20px;
      width: 58%;
      min-width: 0;
    }}

    .eyebrow {{
      display: inline-flex;
      align-items: center;
      gap: 10px;
      padding: 8px 14px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.05);
      color: #aab8ce;
      font-family: "Nunito Sans", "Segoe UI", sans-serif;
      font-size: 15px;
      font-weight: 800;
      letter-spacing: -0.02em;
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.05);
    }}

    .eyebrow-dot {{
      width: 9px;
      height: 9px;
      border-radius: 50%;
      background: linear-gradient(135deg, var(--yellow), color-mix(in srgb, white 10%, var(--green)));
      flex: 0 0 auto;
    }}

    .hero-value {{
      font-size: 82px;
      line-height: 0.9;
      font-weight: 800;
      letter-spacing: -0.075em;
      color: var(--text);
      white-space: nowrap;
      text-shadow: none;
    }}

    .hero-label {{
      font-family: "Nunito Sans", "Segoe UI", sans-serif;
      font-size: 24px;
      font-weight: 700;
      color: #94a2b7;
      letter-spacing: -0.02em;
    }}

    .hero-delta {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      min-height: 54px;
      padding: 0 18px;
      border-radius: 999px;
      background: rgba(255,255,255,0.05);
      border: 1px solid rgba(255,255,255,0.08);
      font-size: 19px;
      font-weight: 800;
      letter-spacing: -0.02em;
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.04);
    }}

    .hero-delta.positive {{
      color: var(--green);
    }}

    .hero-delta.negative {{
      color: var(--red);
    }}

    .hero-delta.neutral {{
      color: #93a0b5;
    }}

    .hero-foot {{
      display: flex;
      flex-direction: column;
      align-items: flex-start;
      gap: 10px;
      font-family: "Nunito Sans", "Segoe UI", sans-serif;
      font-size: 15px;
      font-weight: 700;
      color: var(--muted);
    }}

    .hero-foot-chip {{
      display: inline-flex;
      align-items: center;
      min-height: 36px;
      padding: 0 14px;
      border-radius: 999px;
      background: rgba(255, 182, 46, 0.12);
      color: #ffc761;
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.05);
    }}

    .hero-visual {{
      position: absolute;
      pointer-events: none;
      z-index: 1;
    }}

    .hero-orbit {{
      position: absolute;
      right: -83px;
      top: 79px;
      width: 330px;
      height: 330px;
      border-radius: 50%;
      pointer-events: none;
      z-index: 1;
      transform: none;
    }}

    .hero-orbit-ring {{
      position: absolute;
      inset: 0;
      border-radius: 50%;
    }}

    .hero-orbit-ring.track {{
      background: conic-gradient(
        from var(--ring-start),
        var(--track) 0deg var(--ring-sweep),
        transparent var(--ring-sweep) 360deg
      );
      -webkit-mask: radial-gradient(farthest-side, transparent calc(100% - 47px), #000 calc(100% - 46px));
      mask: radial-gradient(farthest-side, transparent calc(100% - 47px), #000 calc(100% - 46px));
      opacity: 1;
    }}

    .hero-orbit-ring.accent {{
      background: conic-gradient(
        from var(--ring-start),
        color-mix(in srgb, white 18%, var(--ring-accent-1)) 0deg,
        var(--ring-accent-1) calc(var(--ring-yellow-end) - 10deg),
        color-mix(in srgb, black 10%, var(--ring-accent-1)) var(--ring-yellow-end),
        color-mix(in srgb, white 14%, var(--ring-accent-2)) calc(var(--ring-yellow-end) + 2deg),
        var(--ring-accent-2) calc(var(--ring-end) - 6deg),
        color-mix(in srgb, black 12%, var(--ring-accent-2)) var(--ring-end),
        transparent var(--ring-end) 360deg
      );
      -webkit-mask: radial-gradient(farthest-side, transparent calc(100% - 47px), #000 calc(100% - 46px));
      mask: radial-gradient(farthest-side, transparent calc(100% - 47px), #000 calc(100% - 46px));
      filter: drop-shadow(0 12px 24px rgba(255, 182, 46, 0.18));
    }}

    .hero-pillars {{
      right: 34px;
      top: 60px;
      width: 230px;
      height: 356px;
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 18px;
    }}

    .hero-pillars-grid {{
      position: absolute;
      inset: 16px 6px 18px 6px;
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 14px;
    }}

    .hero-pillar {{
      position: relative;
      flex: 1 1 0;
      border-radius: 32px 32px 18px 18px;
      background: linear-gradient(180deg, rgba(30, 40, 58, 0.9), rgba(13, 19, 29, 0.96));
      border: 1px solid rgba(255,255,255,0.06);
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.04), 0 18px 34px rgba(1,4,10,0.28);
      overflow: hidden;
    }}

    .hero-pillar-fill {{
      position: absolute;
      left: 0;
      right: 0;
      bottom: 0;
      border-radius: 24px 24px 16px 16px;
    }}

    .hero-pillar-fill.actual {{
      background: linear-gradient(180deg, rgba(255, 193, 75, 0.96), rgba(255, 182, 46, 0.82));
    }}

    .hero-pillar-fill.benchmark {{
      background: linear-gradient(180deg, rgba(102, 143, 255, 0.9), rgba(74, 110, 214, 0.78));
    }}

    .hero-pillar-tag {{
      position: absolute;
      left: 50%;
      bottom: 12px;
      transform: translateX(-50%);
      min-width: 58px;
      min-height: 28px;
      padding: 0 10px;
      border-radius: 999px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      background: rgba(10,15,23,0.72);
      color: #dbe4f2;
      font-family: "Nunito Sans", "Segoe UI", sans-serif;
      font-size: 11px;
      font-weight: 800;
      letter-spacing: 0.02em;
    }}

    .hero-stack {{
      right: 26px;
      top: 92px;
      width: 286px;
      height: 164px;
      padding: 18px;
      border-radius: 30px;
      background: linear-gradient(180deg, rgba(22, 30, 44, 0.9), rgba(14, 20, 31, 0.96));
      border: 1px solid rgba(255,255,255,0.05);
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.04), 0 18px 34px rgba(1,4,10,0.28);
    }}

    .hero-stack-track {{
      position: relative;
      width: 100%;
      height: 56px;
      border-radius: 999px;
      overflow: hidden;
      background: rgba(255,255,255,0.05);
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.04);
    }}

    .hero-stack-segment {{
      position: absolute;
      top: 0;
      bottom: 0;
    }}

    .hero-stack-segment.cash {{
      background: linear-gradient(180deg, rgba(102, 143, 255, 0.94), rgba(78, 112, 208, 0.84));
    }}

    .hero-stack-segment.invested {{
      background: linear-gradient(180deg, rgba(255, 193, 75, 0.96), rgba(255, 182, 46, 0.86));
    }}

    .hero-stack-marker {{
      position: absolute;
      top: -6px;
      width: 6px;
      height: 68px;
      border-radius: 999px;
      background: rgba(45, 218, 135, 0.95);
      box-shadow: 0 0 0 6px rgba(45, 218, 135, 0.08);
    }}

    .hero-stack-legend {{
      display: flex;
      align-items: center;
      gap: 14px;
      margin-top: 18px;
      flex-wrap: wrap;
    }}

    .hero-stack-key {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      color: #a6b2c5;
      font-family: "Nunito Sans", "Segoe UI", sans-serif;
      font-size: 13px;
      font-weight: 800;
    }}

    .hero-stack-swatch {{
      width: 10px;
      height: 10px;
      border-radius: 50%;
      flex: 0 0 auto;
    }}

    .hero-skyline {{
      right: 28px;
      top: 76px;
      width: 290px;
      height: 280px;
      display: flex;
      align-items: flex-end;
      gap: 14px;
      padding: 0 4px 8px;
    }}

    .hero-skyline-bar {{
      position: relative;
      flex: 1 1 0;
      border-radius: 24px 24px 12px 12px;
      background: linear-gradient(180deg, rgba(27, 36, 52, 0.9), rgba(14, 20, 31, 0.96));
      border: 1px solid rgba(255,255,255,0.05);
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.03);
      overflow: hidden;
    }}

    .hero-skyline-bar::after {{
      content: "";
      position: absolute;
      left: 0;
      right: 0;
      bottom: 0;
      height: var(--bar-fill, 60%);
      background: linear-gradient(180deg, rgba(255, 193, 75, 0.95), rgba(255, 182, 46, 0.78));
      border-radius: 18px 18px 10px 10px;
    }}

    .hero-skyline-bar.alt::after {{
      background: linear-gradient(180deg, rgba(102, 143, 255, 0.9), rgba(74, 110, 214, 0.72));
    }}

    .hero-skyline-cap {{
      position: absolute;
      left: 50%;
      top: 12px;
      transform: translateX(-50%);
      min-width: 34px;
      padding: 4px 8px;
      border-radius: 999px;
      background: rgba(10,15,23,0.7);
      color: #d5deed;
      font-family: "Nunito Sans", "Segoe UI", sans-serif;
      font-size: 10px;
      font-weight: 800;
      text-align: center;
    }}

    .hero-arcs {{
      right: 6px;
      top: 54px;
      width: 340px;
      height: 340px;
    }}

    .hero-arc {{
      position: absolute;
      inset: 0;
      border-radius: 50%;
      background: conic-gradient(from 180deg, transparent 0deg 180deg, rgba(255,255,255,0.05) 180deg 360deg);
      -webkit-mask: radial-gradient(farthest-side, transparent calc(100% - 28px), #000 calc(100% - 27px));
      mask: radial-gradient(farthest-side, transparent calc(100% - 28px), #000 calc(100% - 27px));
    }}

    .hero-arc.outer {{
      background: conic-gradient(
        from 180deg,
        transparent 0deg 180deg,
        rgba(255, 182, 46, 0.14) 180deg 360deg
      );
    }}

    .hero-arc.outer-fill {{
      background: conic-gradient(
        from 180deg,
        transparent 0deg 180deg,
        rgba(255, 182, 46, 0.98) 180deg var(--actual-end),
        transparent var(--actual-end) 360deg
      );
      filter: drop-shadow(0 10px 18px rgba(255, 182, 46, 0.16));
    }}

    .hero-arc.inner {{
      inset: 44px;
      background: conic-gradient(
        from 180deg,
        transparent 0deg 180deg,
        rgba(93, 135, 255, 0.14) 180deg 360deg
      );
    }}

    .hero-arc.inner-fill {{
      inset: 44px;
      background: conic-gradient(
        from 180deg,
        transparent 0deg 180deg,
        rgba(93, 135, 255, 0.94) 180deg var(--benchmark-end),
        transparent var(--benchmark-end) 360deg
      );
      filter: drop-shadow(0 10px 18px rgba(93, 135, 255, 0.14));
    }}

    .hero-arc-labels {{
      position: absolute;
      right: 6px;
      bottom: 6px;
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }}

    .hero-arc-pill {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      min-height: 28px;
      padding: 0 10px;
      border-radius: 999px;
      background: rgba(10,15,23,0.72);
      color: #d8e0ef;
      font-family: "Nunito Sans", "Segoe UI", sans-serif;
      font-size: 11px;
      font-weight: 800;
    }}

    .metrics-cluster {{
      grid-column: 8 / -1;
      grid-row: 2;
      display: grid;
      grid-template-columns: 1.03fr 0.97fr;
      grid-template-rows: 1fr 1fr;
      gap: 18px;
      align-items: stretch;
      padding: 0;
    }}

    .metric-tile {{
      padding: 24px 22px 22px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      min-height: 0;
    }}

    .metric-tile::before {{
      content: "";
      position: absolute;
      left: 0;
      top: 18px;
      bottom: 18px;
      width: 4px;
      border-radius: 0 999px 999px 0;
      background: rgba(255,255,255,0.08);
      pointer-events: none;
      z-index: 0;
    }}

    .metric-tile:nth-child(1) {{
      box-shadow: 0 22px 48px rgba(1, 4, 10, 0.34), inset 0 1px 0 rgba(255,255,255,0.03);
    }}

    .metric-tile:nth-child(1)::before {{
      background: linear-gradient(180deg, rgba(45, 218, 135, 0.95), rgba(45, 218, 135, 0.28));
    }}

    .metric-tile:nth-child(2) {{
      box-shadow: 0 26px 60px rgba(1, 4, 10, 0.4), inset 0 1px 0 rgba(255,255,255,0.03);
    }}

    .metric-tile:nth-child(2)::before {{
      background: linear-gradient(180deg, rgba(255, 182, 46, 0.95), rgba(255, 182, 46, 0.28));
    }}

    .metric-tile:nth-child(3) {{
      box-shadow: 0 22px 48px rgba(1, 4, 10, 0.34), inset 0 1px 0 rgba(255,255,255,0.03);
    }}

    .metric-tile:nth-child(3)::before {{
      background: linear-gradient(180deg, rgba(255, 107, 107, 0.95), rgba(255, 107, 107, 0.28));
    }}

    .metric-tile:nth-child(4) {{
      box-shadow: 0 22px 48px rgba(1, 4, 10, 0.34), inset 0 1px 0 rgba(255,255,255,0.03);
    }}

    .metric-tile:nth-child(4)::before {{
      background: linear-gradient(180deg, rgba(93, 135, 255, 0.95), rgba(93, 135, 255, 0.28));
    }}

    .metric-tile.soft-mint {{
      background:
        radial-gradient(circle at 100% 0%, rgba(45, 218, 135, 0.08), transparent 34%),
        linear-gradient(165deg, rgba(20, 31, 43, 0.98), rgba(16, 23, 34, 0.96));
    }}

    .metric-tile.soft-peach {{
      background:
        radial-gradient(circle at 0% 100%, rgba(255,255,255,0.04), transparent 28%),
        linear-gradient(165deg, rgba(24, 32, 47, 0.98), rgba(18, 24, 36, 0.96));
    }}

    .metric-tile.soft-rose {{
      background:
        radial-gradient(circle at 12% 16%, rgba(255, 107, 107, 0.08), transparent 32%),
        linear-gradient(165deg, rgba(23, 29, 42, 0.98), rgba(17, 22, 33, 0.96));
    }}

    .metric-tile.soft-sky {{
      background:
        radial-gradient(circle at 100% 18%, rgba(93, 135, 255, 0.08), transparent 30%),
        linear-gradient(165deg, rgba(21, 28, 42, 0.98), rgba(16, 22, 33, 0.96));
    }}

    .metric-tile.soft-lemon {{
      background: linear-gradient(180deg, rgba(255,242,201,0.98), rgba(255,255,255,0.92));
    }}

    .metric-tile.highlight {{
      background:
        radial-gradient(circle at 100% 0%, rgba(255,255,255,0.06), transparent 36%),
        radial-gradient(circle at 12% 82%, rgba(255, 182, 46, 0.18), transparent 28%),
        linear-gradient(135deg, rgba(34, 28, 18, 0.98), rgba(24, 20, 15, 0.96));
      box-shadow: var(--tile-shadow-strong);
    }}

    .metric-tile.highlight::before {{
      content: "";
      position: absolute;
      right: 18px;
      top: 18px;
      width: 70px;
      height: 70px;
      border-radius: 22px;
      background:
        linear-gradient(135deg, rgba(255,182,46,0.16), rgba(255,255,255,0.02));
      transform: none;
      opacity: 0.85;
      pointer-events: none;
      z-index: 0;
    }}

    .chip {{
      width: 100%;
      height: 100%;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
    }}

    .chip-label {{
      display: flex;
      align-items: center;
      gap: 10px;
      font-family: "Nunito Sans", "Segoe UI", sans-serif;
      font-size: 16px;
      font-weight: 800;
      color: #97a4bb;
      letter-spacing: -0.03em;
      line-height: 1.1;
    }}

    .chip-dot {{
      width: 10px;
      height: 10px;
      border-radius: 50%;
      flex: 0 0 auto;
      box-shadow: 0 0 0 4px rgba(255,255,255,0.08);
    }}

    .chip-value {{
      font-size: 50px;
      line-height: 0.92;
      font-weight: 800;
      letter-spacing: -0.06em;
      color: var(--text);
    }}

    .chip-value.positive {{
      color: var(--green);
    }}

    .chip-value.negative {{
      color: var(--red);
    }}

    .chip-note {{
      font-family: "Nunito Sans", "Segoe UI", sans-serif;
      font-size: 12px;
      font-weight: 700;
      color: #728097;
      letter-spacing: -0.02em;
    }}

    .list-tile {{
      padding: 28px 24px 24px;
      display: flex;
      flex-direction: column;
    }}

    .gainers {{
      grid-column: 1 / span 6;
      grid-row: 3;
      background:
        radial-gradient(circle at 0% 0%, rgba(255, 182, 46, 0.08), transparent 28%),
        linear-gradient(180deg, rgba(18,24,36,0.98), rgba(13,18,28,0.98));
    }}

    .gainers::before,
    .movers::before {{
      content: "";
      position: absolute;
      left: 24px;
      right: 24px;
      top: 0;
      height: 3px;
      border-radius: 0 0 999px 999px;
      pointer-events: none;
      z-index: 0;
    }}

    .gainers::before {{
      background: linear-gradient(90deg, rgba(255, 182, 46, 0.88), rgba(255, 182, 46, 0.08));
    }}

    .movers {{
      grid-column: 7 / -1;
      grid-row: 3;
      background:
        radial-gradient(circle at 100% 0%, rgba(93, 135, 255, 0.1), transparent 28%),
        linear-gradient(180deg, rgba(18,24,36,0.98), rgba(13,18,28,0.98));
    }}

    .movers::before {{
      background: linear-gradient(90deg, rgba(93, 135, 255, 0.88), rgba(93, 135, 255, 0.08));
    }}

    .panel-head {{
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 14px;
      margin-bottom: 18px;
    }}

    .panel-kicker {{
      font-family: "Nunito Sans", "Segoe UI", sans-serif;
      font-size: 12px;
      font-weight: 800;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--faint);
      margin-bottom: 8px;
    }}

    .panel-title {{
      font-size: 36px;
      line-height: 1;
      font-weight: 800;
      letter-spacing: -0.045em;
      color: var(--text);
      margin-bottom: 8px;
    }}

    .panel-subtitle {{
      font-family: "Nunito Sans", "Segoe UI", sans-serif;
      font-size: 14px;
      font-weight: 700;
      color: var(--muted);
      letter-spacing: -0.02em;
    }}

    .panel-badge {{
      display: inline-flex;
      align-items: center;
      min-height: 38px;
      padding: 0 14px;
      border-radius: 999px;
      background: rgba(255,255,255,0.05);
      color: var(--muted);
      font-family: "Nunito Sans", "Segoe UI", sans-serif;
      font-size: 13px;
      font-weight: 800;
      white-space: nowrap;
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.04);
    }}

    .gainers .panel-badge {{
      color: #ffc761;
      background: rgba(255, 182, 46, 0.12);
    }}

    .movers .panel-badge {{
      color: #9fbcff;
      background: rgba(93, 135, 255, 0.12);
    }}

    .rows {{
      display: flex;
      flex: 1 1 auto;
      flex-direction: column;
      justify-content: space-between;
      gap: 6px;
      padding-bottom: 0;
      margin-top: 6px;
    }}

    .row {{
      min-height: 78px;
      border-radius: 24px;
      background:
        linear-gradient(180deg, rgba(24,31,46,0.98), rgba(18,24,35,0.96));
      border: 1px solid rgba(255,255,255,0.05);
      box-shadow: 0 18px 34px rgba(1, 4, 10, 0.24);
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 18px 0 16px;
      gap: 14px;
    }}

    .row-left {{
      display: flex;
      align-items: center;
      gap: 12px;
      min-width: 0;
      flex: 1 1 auto;
    }}

    .row-rank {{
      width: 38px;
      height: 38px;
      border-radius: 12px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      background: rgba(255, 182, 46, 0.12);
      color: #ffc761;
      font-family: "Nunito Sans", "Segoe UI", sans-serif;
      font-size: 13px;
      font-weight: 800;
      letter-spacing: -0.02em;
      flex: 0 0 auto;
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.04);
    }}

    .movers .row-rank {{
      background: rgba(93, 135, 255, 0.12);
      color: #9fbcff;
    }}

    .gainers .row-rank {{
      background: rgba(255, 182, 46, 0.12);
      color: #ffc761;
    }}

    .row-copy {{
      display: flex;
      flex-direction: column;
      justify-content: center;
      gap: 4px;
      min-width: 0;
    }}

    .row-symbol {{
      font-size: 20px;
      font-weight: 800;
      letter-spacing: -0.03em;
      color: var(--text);
    }}

    .row-symbol.empty {{
      color: var(--faint);
    }}

    .row-weight {{
      font-family: "Nunito Sans", "Segoe UI", sans-serif;
      font-size: 13px;
      font-weight: 700;
      color: var(--muted);
      white-space: nowrap;
      letter-spacing: -0.02em;
    }}

    .row-value {{
      min-width: 112px;
      text-align: right;
      font-size: 21px;
      font-weight: 800;
      letter-spacing: -0.04em;
    }}

    .row-value.positive {{
      color: var(--green);
      text-shadow: 0 0 18px rgba(45, 218, 135, 0.12);
    }}

    .row-value.negative {{
      color: var(--red);
      text-shadow: 0 0 18px rgba(255, 107, 107, 0.12);
    }}

    .row-value.neutral {{
      color: var(--faint);
      font-weight: 700;
    }}

    .hero-value,
    .chip-value,
    .panel-title,
    .title {{
      text-wrap: balance;
    }}

    .topbar::after,
    .hero::after,
    .metric-tile::after,
    .list-tile::after {{
      content: "";
      position: absolute;
      inset: 0;
      border-radius: inherit;
      background:
        linear-gradient(180deg, rgba(255,255,255,0.04), transparent 34%),
        radial-gradient(circle at 0% 0%, rgba(255,255,255,0.03), transparent 24%);
      pointer-events: none;
      z-index: 0;
    }}

    .hero::before {{
      content: "";
      position: absolute;
      left: 26px;
      bottom: 22px;
      width: 180px;
      height: 180px;
      border-radius: 50%;
      background: radial-gradient(circle, rgba(255,182,46,0.12), transparent 68%);
      filter: blur(4px);
      z-index: 0;
    }}

    .hero::after {{
      background:
        radial-gradient(circle at 78% 18%, rgba(255,255,255,0.05), transparent 18%),
        linear-gradient(180deg, rgba(255,255,255,0.03), transparent 34%);
    }}

    .topbar,
    .hero,
    .metric-tile,
    .list-tile {{
      isolation: isolate;
    }}

    .metric-tile .chip,
    .list-tile,
    .hero-copy,
    .panel-head,
    .rows {{
      position: relative;
      z-index: 1;
    }}
  </style>
</head>
<body>
  <div class="canvas">
    <div class="layout">
      <header class="tile topbar">
        <div class="brand">
          <div class="title">IBKR Daily Portfolio</div>
          <div class="subtitle">Daily close snapshot</div>
        </div>
        <div class="meta">
          <div class="meta-pill soft">IBKR / QQQM</div>
          <div class="meta-pill">{_e(snapshot.trade_date)} | {_e(snapshot.account_id)}</div>
        </div>
      </header>

      <section class="tile hero">
        <div class="hero-copy">
          <div class="eyebrow"><span class="eyebrow-dot"></span>Daily snapshot</div>
          <div>
            <div class="hero-label">Net liquidation</div>
            <div class="hero-value">{_e(_format_currency(snapshot.net_liquidation, show_sign=False))}</div>
          </div>
          <div class="hero-delta {delta_class}">{_e(delta_text)}</div>
          <div class="hero-foot">
            <div class="hero-foot-chip">{_e(holdings_text)}</div>
            <div>Benchmark tracking active</div>
          </div>
        </div>
        {hero_visual_html}
      </section>

      <section class="metrics-cluster">
        <div class="tile metric-tile soft-mint">
          {_build_chip("Unrealized P&L", _format_currency(snapshot.total_unrealized_pnl, show_sign=True), GREEN if snapshot.total_unrealized_pnl >= 0 else RED, value_class="positive" if snapshot.total_unrealized_pnl >= 0 else "negative", tile_note="Open positions")}
        </div>
        <div class="tile metric-tile highlight">
          {_build_chip("QQQM Total Diff", qqqm_total_diff["text"], GREEN if qqqm_total_diff["class_name"] == "positive" else RED if qqqm_total_diff["class_name"] == "negative" else TEXT_FAINT, value_class=qqqm_total_diff["class_name"], tile_note="Actual vs synthetic QQQM")}
        </div>
        <div class="tile metric-tile soft-rose">
          {_build_chip("Today's P&L", today_change["text"], GREEN if today_change["class_name"] == "positive" else RED if today_change["class_name"] == "negative" else TEXT_FAINT, value_class=today_change["class_name"], tile_note="Current session")}
        </div>
        <div class="tile metric-tile soft-sky">
          {_build_chip("QQQM P&L", qqqm_change["text"], GREEN if qqqm_change["class_name"] == "positive" else RED if qqqm_change["class_name"] == "negative" else TEXT_FAINT, value_class=qqqm_change["class_name"], tile_note="If fully invested")}
        </div>
      </section>

      <section class="tile list-tile gainers">
        <div class="panel-head">
          <div>
            <div class="panel-kicker">Portfolio winners</div>
            <div class="panel-title">Top Gainers</div>
            <div class="panel-subtitle">All time vs average cost</div>
          </div>
          <div class="panel-badge">Lifetime</div>
        </div>
        <div class="rows">{gainers_html}</div>
      </section>

      <section class="tile list-tile movers">
        <div class="panel-head">
          <div>
            <div class="panel-kicker">Market momentum</div>
            <div class="panel-title">Top Movers Today</div>
            <div class="panel-subtitle">Current day change vs prior close</div>
          </div>
          <div class="panel-badge">Today</div>
        </div>
        <div class="rows">{movers_html}</div>
      </section>
    </div>
  </div>
</body>
</html>"""


def _build_chip(label: str, value: str, accent: str, *, value_class: str = "", tile_note: str = "") -> str:
    suffix = f" {value_class}" if value_class else ""
    note_html = f'<div class="chip-note">{_e(tile_note)}</div>' if tile_note else ""
    return (
        '<div class="chip">'
        f'<div class="chip-label"><span class="chip-dot" style="background:{accent};"></span>{_e(label)}</div>'
        f'<div class="chip-value{suffix}">{_e(value)}</div>'
        f"{note_html}"
        "</div>"
    )


def _build_hero_visual(snapshot: PortfolioSnapshot, ring: dict[str, float | str], variant: str) -> str:
    if variant == "comparison_pillars":
        benchmark_value = _benchmark_portfolio_value(snapshot)
        actual = max(snapshot.net_liquidation, 1.0)
        benchmark = max(benchmark_value or actual * 0.76, 1.0)
        scale = max(actual, benchmark, 1.0)
        actual_height = max(34.0, min(100.0, (actual / scale) * 100.0))
        benchmark_height = max(34.0, min(100.0, (benchmark / scale) * 100.0))
        return (
            '<div class="hero-visual hero-pillars">'
            '<div class="hero-pillars-grid">'
            f'<div class="hero-pillar"><div class="hero-pillar-fill actual" style="height:{actual_height:.2f}%;"></div><div class="hero-pillar-tag">ME</div></div>'
            f'<div class="hero-pillar"><div class="hero-pillar-fill benchmark" style="height:{benchmark_height:.2f}%;"></div><div class="hero-pillar-tag">QQQM</div></div>'
            "</div>"
            "</div>"
        )

    if variant == "stacked_account_bar":
        invested = max(0.0, sum(max(position.market_value, 0.0) for position in snapshot.positions))
        cash = max(snapshot.cash_value, 0.0)
        total = max(snapshot.net_liquidation, invested + cash, 1.0)
        invested_pct = max(8.0, min(100.0, (invested / total) * 100.0))
        cash_pct = max(0.0, min(100.0 - invested_pct, (cash / total) * 100.0))
        benchmark_value = _benchmark_portfolio_value(snapshot)
        benchmark_pct = 50.0 if benchmark_value is None else max(0.0, min(100.0, (benchmark_value / total) * 100.0))
        return (
            '<div class="hero-visual hero-stack">'
            '<div class="hero-stack-track">'
            f'<div class="hero-stack-segment invested" style="left:0;width:{invested_pct:.2f}%;"></div>'
            f'<div class="hero-stack-segment cash" style="left:{invested_pct:.2f}%;width:{cash_pct:.2f}%;"></div>'
            f'<div class="hero-stack-marker" style="left:calc({benchmark_pct:.2f}% - 3px);"></div>'
            '</div>'
            '<div class="hero-stack-legend">'
            '<div class="hero-stack-key"><span class="hero-stack-swatch" style="background:rgba(255,182,46,0.96);"></span>Invested</div>'
            '<div class="hero-stack-key"><span class="hero-stack-swatch" style="background:rgba(102,143,255,0.96);"></span>Cash</div>'
            '<div class="hero-stack-key"><span class="hero-stack-swatch" style="background:rgba(45,218,135,0.96);"></span>QQQM mark</div>'
            '</div>'
            '</div>'
        )

    if variant == "portfolio_skyline":
        top_positions = sorted(
            (position for position in snapshot.positions if position.market_value > 0),
            key=lambda position: position.market_value,
            reverse=True,
        )[:5]
        if not top_positions:
            top_positions = list(snapshot.positions[:5])
        if not top_positions:
            top_positions = []
        max_value = max((abs(position.market_value) for position in top_positions), default=1.0)
        bars: list[str] = []
        for index in range(5):
            if index < len(top_positions):
                position = top_positions[index]
                fill = max(28.0, min(100.0, (abs(position.market_value) / max_value) * 100.0))
                alt_class = " alt" if index % 2 else ""
                bars.append(
                    f'<div class="hero-skyline-bar{alt_class}" style="--bar-fill:{fill:.2f}%;">'
                    f'<div class="hero-skyline-cap">{_e(position.symbol[:4])}</div>'
                    "</div>"
                )
            else:
                fill = 34.0 + (index * 8.0)
                alt_class = " alt" if index % 2 else ""
                bars.append(
                    f'<div class="hero-skyline-bar{alt_class}" style="--bar-fill:{fill:.2f}%;">'
                    '<div class="hero-skyline-cap">-</div>'
                    "</div>"
                )
        return '<div class="hero-visual hero-skyline">' + "".join(bars) + "</div>"

    if variant == "comparison_arcs":
        benchmark_value = _benchmark_portfolio_value(snapshot)
        actual = max(snapshot.net_liquidation, 1.0)
        benchmark = max(benchmark_value or actual * 0.76, 1.0)
        scale = max(actual, benchmark, 1.0)
        actual_end = 180.0 + (actual / scale) * 180.0
        benchmark_end = 180.0 + (benchmark / scale) * 180.0
        return (
            '<div class="hero-visual hero-arcs">'
            f'<div class="hero-arc outer"></div><div class="hero-arc outer-fill" style="--actual-end:{actual_end:.2f}deg;"></div>'
            f'<div class="hero-arc inner"></div><div class="hero-arc inner-fill" style="--benchmark-end:{benchmark_end:.2f}deg;"></div>'
            '<div class="hero-arc-labels">'
            '<div class="hero-arc-pill"><span class="hero-stack-swatch" style="background:rgba(255,182,46,0.96);"></span>Portfolio</div>'
            '<div class="hero-arc-pill"><span class="hero-stack-swatch" style="background:rgba(93,135,255,0.96);"></span>QQQM</div>'
            '</div>'
            '</div>'
        )

    return (
        '<div class="hero-visual hero-orbit">'
        '<div class="hero-orbit-ring track"></div>'
        '<div class="hero-orbit-ring accent"></div>'
        "</div>"
    )


def _benchmark_portfolio_value(snapshot: PortfolioSnapshot) -> Optional[float]:
    if snapshot.qqqm_total_diff is None:
        return None
    return snapshot.net_liquidation - snapshot.qqqm_total_diff


def _build_rank_rows(rows: list[RankedPosition]) -> str:
    markup: list[str] = []
    for index in range(5):
        item = rows[index] if index < len(rows) else None
        rank_label = str(index + 1)
        if item is None:
            markup.append(
                '<div class="row">'
                f'<div class="row-left"><div class="row-rank">{rank_label}</div><div class="row-copy"><div class="row-symbol empty">-</div><div class="row-weight">No data</div></div></div>'
                '<div class="row-value neutral">No data</div>'
                "</div>"
            )
            continue

        value_class = "positive" if item.percent >= 0 else "negative"
        markup.append(
            '<div class="row">'
            f'<div class="row-left"><div class="row-rank">{rank_label}</div><div class="row-copy"><div class="row-symbol">{_e(item.symbol)}</div><div class="row-weight">Daily move</div></div></div>'
            f'<div class="row-value {value_class}">{_e(_format_percent(item.percent))}</div>'
            "</div>"
        )
    return "".join(markup)


def _build_gainer_rows(snapshot: PortfolioSnapshot, rows: list[RankedPosition]) -> str:
    positions_by_symbol = {position.symbol: position for position in snapshot.positions}
    markup: list[str] = []
    for index in range(5):
        item = rows[index] if index < len(rows) else None
        rank_label = str(index + 1)
        if item is None:
            markup.append(
                '<div class="row">'
                f'<div class="row-left"><div class="row-rank">{rank_label}</div><div class="row-copy"><div class="row-symbol empty">-</div><div class="row-weight">No data</div></div></div>'
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
            f'<div class="row-left"><div class="row-rank">{rank_label}</div><div class="row-copy"><div class="row-symbol">{_e(item.symbol)}</div><div class="row-weight">{_e(_format_portfolio_weight(weight))} of portfolio</div></div></div>'
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


def initialize_qqqm_benchmark_baseline(snapshot: PortfolioSnapshot) -> Optional[dict[str, float | str]]:
    if snapshot.net_liquidation <= 0 or not snapshot.trade_date:
        return None
    close_history = _fetch_qqqm_close_history(snapshot.trade_date, snapshot.trade_date)
    if close_history is None:
        return None
    start_close = _first_close_on_or_after(close_history.close_by_date, snapshot.trade_date)
    if start_close is None or start_close <= 0:
        return None
    return {
        "trade_date": snapshot.trade_date,
        "net_liquidation": snapshot.net_liquidation,
        "qqqm_start_close": start_close,
        "started_at": snapshot.fetched_at,
    }


def compute_qqqm_total_diff(
    snapshot: PortfolioSnapshot,
    *,
    baseline_trade_date: str,
    baseline_net_liquidation: float,
    baseline_qqqm_start_close: float,
) -> Optional[float]:
    if baseline_net_liquidation <= 0 or baseline_qqqm_start_close <= 0:
        return None
    if not baseline_trade_date or not snapshot.trade_date:
        return None

    benchmark_history = _fetch_qqqm_close_history(baseline_trade_date, snapshot.trade_date)
    if benchmark_history is None:
        return None

    current_close = _last_close_on_or_before(benchmark_history.close_by_date, snapshot.trade_date)
    if current_close is None or current_close <= 0:
        return None

    synthetic_shares = baseline_net_liquidation / baseline_qqqm_start_close
    synthetic_cash = 0.0

    for trade in snapshot.trades:
        if trade.trade_date < baseline_trade_date:
            continue
        benchmark_close = _first_close_on_or_after(benchmark_history.close_by_date, trade.trade_date)
        if benchmark_close is None or benchmark_close <= 0:
            return None

        if trade.is_equity_buy:
            amount = trade.cash_spent
            if amount is None or amount <= 0:
                return None
            synthetic_shares += amount / benchmark_close
            synthetic_cash -= amount
        elif trade.is_equity_sell:
            amount = trade.cash_received
            if amount is None or amount <= 0:
                return None
            synthetic_shares -= amount / benchmark_close
            synthetic_cash += amount

    for cash_event in snapshot.cash_events:
        event_date = str(cash_event.get("event_date", "") or "")
        if not event_date or event_date < baseline_trade_date:
            continue
        amount = coerce_optional_float(cash_event.get("amount"))
        if amount is None or abs(amount) < 1e-9:
            continue
        benchmark_close = _first_close_on_or_after(benchmark_history.close_by_date, event_date)
        if benchmark_close is None or benchmark_close <= 0:
            return None
        synthetic_shares += amount / benchmark_close

    synthetic_value = synthetic_cash + (synthetic_shares * current_close)
    return snapshot.net_liquidation - synthetic_value


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
        ("SF Pro Display", SFPRO_BOLD, 700),
        ("SF Pro Display", SFPRO_HEAVY, 800),
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
