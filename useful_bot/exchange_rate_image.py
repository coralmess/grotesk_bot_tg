"""Glassmorphism-styled exchange-rate card renderer.

Produces a PNG image with three frosted-glass panels on top of a
background photograph:

  ┌──────────┬──────────┐
  │   USD    │   EUR    │  buy / sell + Δ vs yesterday
  │          │          │
  ├──────────┴──────────┤
  │ USD spread │ cross  │  spread & cross vs historical avg
  └─────────────────────┘
"""

from __future__ import annotations

import io
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from PIL import Image, ImageDraw, ImageFilter, ImageFont

from helpers.image_pipeline import load_font
from helpers.runtime_paths import PROJECT_ROOT

logger = logging.getLogger(__name__)


RENDER_SCALE = 2


def _px(value: int) -> int:
    return int(round(value * RENDER_SCALE))

# ── paths ─────────────────────────────────────────────────────────────────
FONTS_DIR = PROJECT_ROOT / "fonts"
BG_IMAGE_PATH = (
    PROJECT_ROOT / "runtime_data" / "image" / "glassmorphism_background.jpg"
)

# ── canvas ────────────────────────────────────────────────────────────────
CANVAS_W = _px(1200)
CANVAS_H = _px(936)

# ── layout ────────────────────────────────────────────────────────────────
MARGIN = _px(28)
GAP = _px(18)
CORNER_RADIUS = _px(24)
DATE_PANEL_H = _px(56)  # height of the date glass panel

# ── glass effect ──────────────────────────────────────────────────────────
BLUR_RADIUS = _px(25)
GLASS_ALPHA = 22  # white overlay opacity  (0 = clear, 255 = opaque)
BORDER_ALPHA = 100
BORDER_WIDTH = _px(2)

# ── colours ───────────────────────────────────────────────────────────────
WHITE = (255, 255, 255)
TEXT_SECONDARY = (200, 200, 220)
STAT_MUTED = (150, 150, 170)       # min / max numbers & labels
STAT_LINE = (110, 110, 135)        # connecting line between min–max
GREEN = (52, 211, 153)
RED = (248, 113, 113)
TODAY_MARKER_COLOR = TEXT_SECONDARY  # matches avg text colour (200, 200, 220)


# ── public API ────────────────────────────────────────────────────────────


def render_exchange_rate_card(
    *,
    usd_buy: float,
    usd_sell: float,
    eur_buy: float,
    eur_sell: float,
    prev_usd_buy: Optional[float] = None,
    prev_usd_sell: Optional[float] = None,
    prev_eur_buy: Optional[float] = None,
    prev_eur_sell: Optional[float] = None,
    usd_spread: float,
    eur_sell_minus_usd_buy: float,
    usd_spread_avg: Optional[float] = None,
    cross_avg: Optional[float] = None,
    usd_spread_min: Optional[float] = None,
    usd_spread_max: Optional[float] = None,
    cross_min: Optional[float] = None,
    cross_max: Optional[float] = None,
    usd_spread_current: Optional[float] = None,
    cross_current: Optional[float] = None,
    usd_buy_avg: Optional[float] = None,
    usd_buy_min: Optional[float] = None,
    usd_buy_max: Optional[float] = None,
    eur_buy_avg: Optional[float] = None,
    eur_buy_min: Optional[float] = None,
    eur_buy_max: Optional[float] = None,
    background_path: Optional[Path] = None,
    date_label: Optional[str] = None,
) -> io.BytesIO:
    """Return a PNG *BytesIO* of a glassmorphism exchange-rate card."""

    bg_path = background_path or BG_IMAGE_PATH

    # 1. Background ───────────────────────────────────────────────────
    if bg_path.exists():
        bg = Image.open(bg_path).convert("RGBA")
        bg = _cover_resize(bg, CANVAS_W, CANVAS_H)
    else:
        logger.warning("Background image not found at %s – using fallback", bg_path)
        bg = Image.new("RGBA", (CANVAS_W, CANVAS_H), (25, 25, 50, 255))

    bg_blurred = bg.filter(ImageFilter.GaussianBlur(radius=BLUR_RADIUS))
    canvas = bg.copy()

    # 2. Panel geometry ───────────────────────────────────────────────
    date_rect = (MARGIN, MARGIN, CANVAS_W - MARGIN, MARGIN + DATE_PANEL_H)

    panel_w = (CANVAS_W - 2 * MARGIN - GAP) // 2
    panel_top = MARGIN + DATE_PANEL_H + GAP
    top_h = _px(470)
    usd_rect = (MARGIN, panel_top, MARGIN + panel_w, panel_top + top_h)
    eur_rect = (
        MARGIN + panel_w + GAP,
        panel_top,
        CANVAS_W - MARGIN,
        panel_top + top_h,
    )
    bot_rect = (
        MARGIN,
        panel_top + top_h + GAP,
        CANVAS_W - MARGIN,
        CANVAS_H - MARGIN,
    )

    # 3. Glass panels ─────────────────────────────────────────────────
    for rect in (date_rect, usd_rect, eur_rect, bot_rect):
        _apply_glass(canvas, bg_blurred, rect)

    # 4. Semi-transparent borders & separator ─────────────────────────
    deco = Image.new("RGBA", (CANVAS_W, CANVAS_H), (0, 0, 0, 0))
    dd = ImageDraw.Draw(deco)
    for rect in (date_rect, usd_rect, eur_rect, bot_rect):
        dd.rounded_rectangle(
            [rect[:2], (rect[2] - 1, rect[3] - 1)],
            radius=CORNER_RADIUS,
            outline=(*WHITE, BORDER_ALPHA),
            width=BORDER_WIDTH,
        )
    # vertical separator inside the bottom panel
    sep_x = (bot_rect[0] + bot_rect[2]) // 2
    dd.line(
        [(sep_x, bot_rect[1] + _px(24)), (sep_x, bot_rect[3] - _px(24))],
        fill=(*WHITE, 50),
        width=_px(1),
    )
    canvas = Image.alpha_composite(canvas, deco)

    # 5. Date panel text ──────────────────────────────────────────────
    draw = ImageDraw.Draw(canvas)
    ft = _load_fonts()

    if date_label is None:
        date_label = datetime.now(ZoneInfo("Europe/Kyiv")).strftime("%d.%m.%Y")
    title_text = f"Monobank Exchange Rate  ·  {date_label}"
    dcx = (date_rect[0] + date_rect[2]) // 2
    dcy = (date_rect[1] + date_rect[3]) // 2
    draw.text(
        (dcx, dcy),
        title_text,
        font=ft["date"],
        fill=WHITE,
        anchor="mm",
    )

    # 6. Text & indicators ────────────────────────────────────────────
    _draw_currency_panel(
        draw,
        usd_rect,
        "USD",
        buy=usd_buy,
        sell=usd_sell,
        prev_buy=prev_usd_buy,
        prev_sell=prev_usd_sell,
        buy_avg=usd_buy_avg,
        buy_min=usd_buy_min,
        buy_max=usd_buy_max,
        ft=ft,
    )
    _draw_currency_panel(
        draw,
        eur_rect,
        "EUR",
        buy=eur_buy,
        sell=eur_sell,
        prev_buy=prev_eur_buy,
        prev_sell=prev_eur_sell,
        buy_avg=eur_buy_avg,
        buy_min=eur_buy_min,
        buy_max=eur_buy_max,
        ft=ft,
    )
    _draw_metrics_panel(
        draw,
        bot_rect,
        usd_spread=usd_spread,
        cross=eur_sell_minus_usd_buy,
        usd_spread_avg=usd_spread_avg,
        cross_avg=cross_avg,
        usd_spread_min=usd_spread_min,
        usd_spread_max=usd_spread_max,
        cross_min=cross_min,
        cross_max=cross_max,
        usd_spread_current=usd_spread_current,
        cross_current=cross_current,
        ft=ft,
    )

    # 7. Encode ───────────────────────────────────────────────────────
    buf = io.BytesIO()
    canvas.convert("RGB").save(buf, format="PNG")
    buf.seek(0)
    return buf


# ── internal helpers ──────────────────────────────────────────────────────


def _load_fonts() -> dict:
    return {
        "title": load_font(_px(52), fonts_dir=FONTS_DIR, prefer_heavy=True),
        "value": load_font(_px(46), fonts_dir=FONTS_DIR, prefer_heavy=False),
        "label": load_font(_px(30), fonts_dir=FONTS_DIR, prefer_heavy=False),
        "delta": load_font(_px(28), fonts_dir=FONTS_DIR, prefer_heavy=False),
        "mtitle": load_font(_px(38), fonts_dir=FONTS_DIR, prefer_heavy=True),
        "mval": load_font(_px(44), fonts_dir=FONTS_DIR, prefer_heavy=False),
        "mdetail": load_font(_px(28), fonts_dir=FONTS_DIR, prefer_heavy=False),
        "date": load_font(_px(28), fonts_dir=FONTS_DIR, prefer_heavy=False),
        "stat": load_font(_px(26), fonts_dir=FONTS_DIR, prefer_heavy=False),
        "stat_label": load_font(_px(19), fonts_dir=FONTS_DIR, prefer_heavy=False),
    }


def _cover_resize(img: Image.Image, tw: int, th: int) -> Image.Image:
    """Resize + centre-crop to *exactly* fill (*tw*, *th*) (CSS cover)."""
    iw, ih = img.size
    scale = max(tw / iw, th / ih)
    nw, nh = int(iw * scale), int(ih * scale)
    img = img.resize((nw, nh), Image.LANCZOS)
    left, top = (nw - tw) // 2, (nh - th) // 2
    return img.crop((left, top, left + tw, top + th))


def _apply_glass(
    canvas: Image.Image,
    bg_blurred: Image.Image,
    rect: tuple[int, int, int, int],
) -> None:
    """Paste a frosted-glass region onto *canvas* **in-place**."""
    x1, y1, x2, y2 = rect
    w, h = x2 - x1, y2 - y1

    # rounded-rectangle mask
    mask = Image.new("L", (w, h), 0)
    ImageDraw.Draw(mask).rounded_rectangle(
        [(0, 0), (w - 1, h - 1)],
        radius=CORNER_RADIUS,
        fill=255,
    )

    # blurred crop + semi-transparent white tint
    crop = bg_blurred.crop((x1, y1, x2, y2)).convert("RGBA")
    tint = Image.new("RGBA", (w, h), (255, 255, 255, GLASS_ALPHA))
    glass = Image.alpha_composite(crop, tint)

    canvas.paste(glass, (x1, y1), mask)


# ── triangle drawing ─────────────────────────────────────────────────────


def _draw_triangle(
    draw: ImageDraw.Draw,
    cx: int,
    cy: int,
    size: int,
    direction: str,
    color: tuple,
) -> None:
    """Draw a small filled equilateral-ish triangle centred at (*cx*, *cy*).

    *direction*: ``'up'`` or ``'down'``.
    """
    hw = size // 2
    hh = int(size * 0.45)
    if direction == "up":
        pts = [(cx, cy - hh), (cx - hw, cy + hh), (cx + hw, cy + hh)]
    else:
        pts = [(cx - hw, cy - hh), (cx + hw, cy - hh), (cx, cy + hh)]
    draw.polygon(pts, fill=color)


# ── delta indicator ──────────────────────────────────────────────────────


def _draw_delta(
    draw: ImageDraw.Draw,
    cx: int,
    cy: int,
    delta: float,
    font: ImageFont.FreeTypeFont,
    *,
    is_spread: bool,
) -> None:
    """Render a coloured triangle + value centred at (*cx*, *cy*).

    * **is_spread=False** (price context): up → green ▲,  down → red ▼
    * **is_spread=True**  (spread context): lower → green ▼,  higher → red ▲
    """
    if abs(delta) < 0.005:
        draw.text((cx, cy), "—", font=font, fill=TEXT_SECONDARY, anchor="mm")
        return

    if is_spread:
        color = GREEN if delta < 0 else RED
        tri_dir = "down" if delta < 0 else "up"
    else:
        color = GREEN if delta > 0 else RED
        tri_dir = "up" if delta > 0 else "down"

    value_str = f"{abs(delta):.2f}"

    # measure text so we can centre the [▲ value] group
    bbox = draw.textbbox((0, 0), value_str, font=font, anchor="lm")
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]

    tri_size = max(_px(12), int(th * 0.70))
    gap = _px(8)
    total_w = tri_size + gap + tw
    sx = cx - total_w // 2

    _draw_triangle(draw, sx + tri_size // 2, cy, tri_size, tri_dir, color)
    draw.text(
        (sx + tri_size + gap, cy),
        value_str,
        font=font,
        fill=color,
        anchor="lm",
    )


# ── currency panel (top-left / top-right) ────────────────────────────────


def _draw_currency_panel(
    draw: ImageDraw.Draw,
    rect: tuple[int, int, int, int],
    currency: str,
    *,
    buy: float,
    sell: float,
    prev_buy: Optional[float],
    prev_sell: Optional[float],
    buy_avg: Optional[float] = None,
    buy_min: Optional[float] = None,
    buy_max: Optional[float] = None,
    ft: dict,
) -> None:
    x1, y1, x2, y2 = rect
    cx = (x1 + x2) // 2
    h = y2 - y1

    # Spacing: small=8px, medium=18px, big=32px
    # All positions computed cumulatively from top
    S, M, B = _px(8), _px(18), _px(32)

    y_title = y1 + _px(14) + B
    y_buy_lbl = y_title + _px(42) + B
    y_buy_val = y_buy_lbl + _px(24) + M
    y_buy_delta = y_buy_val + _px(38) + S + _px(11)
    y_buy_stats = y_buy_delta + _px(11) + M + _px(22)
    y_sell_lbl = y_buy_stats + _px(23) + B
    y_sell_val = y_sell_lbl + _px(24) + M
    y_sell_delta = y_sell_val + _px(38) + S + _px(11)

    # currency title
    draw.text(
        (cx, y_title),
        currency,
        font=ft["title"],
        fill=WHITE,
        anchor="mt",
    )

    # ── Buy ──
    draw.text(
        (cx, y_buy_lbl),
        "Buy",
        font=ft["label"],
        fill=TEXT_SECONDARY,
        anchor="mt",
    )
    draw.text(
        (cx, y_buy_val),
        f"{buy:.2f}",
        font=ft["value"],
        fill=WHITE,
        anchor="mt",
    )
    if prev_buy is not None:
        _draw_delta(
            draw,
            cx,
            y_buy_delta,
            buy - prev_buy,
            ft["delta"],
            is_spread=False,
        )

    # ── Buy stats (min / avg / max) ──
    if buy_avg is not None:
        _draw_stats_row(
            draw, cx, y_buy_stats,
            avg_val=buy_avg,
            min_val=buy_min,
            max_val=buy_max,
            font=ft["stat"],
            label_font=ft["stat_label"],
            current_val=buy,
        )

    # ── Sell ──
    draw.text(
        (cx, y_sell_lbl),
        "Sell",
        font=ft["label"],
        fill=TEXT_SECONDARY,
        anchor="mt",
    )
    draw.text(
        (cx, y_sell_val),
        f"{sell:.2f}",
        font=ft["value"],
        fill=WHITE,
        anchor="mt",
    )
    if prev_sell is not None:
        _draw_delta(
            draw,
            cx,
            y_sell_delta,
            sell - prev_sell,
            ft["delta"],
            is_spread=False,
        )


# ── metrics panel (bottom) ───────────────────────────────────────────────


def _draw_stats_row(
    draw: ImageDraw.Draw,
    cx: int,
    y: int,
    avg_val: Optional[float],
    min_val: Optional[float],
    max_val: Optional[float],
    font: ImageFont.FreeTypeFont,
    label_font: ImageFont.FreeTypeFont,
    current_val: Optional[float] = None,
) -> None:
    """Draw min / avg / max with a connecting line, dot markers, and a
    white vertical tick + dot for today's *current_val*."""
    items: list[tuple[str, float]] = []
    if min_val is not None:
        items.append(("min", min_val))
    if avg_val is not None:
        items.append(("avg", avg_val))
    if max_val is not None:
        items.append(("max", max_val))

    if not items:
        draw.text((cx, y), "no data", font=font, fill=TEXT_SECONDARY, anchor="mm")
        return

    spacing = _px(130)
    total_w = spacing * (len(items) - 1) if len(items) > 1 else 0
    start_x = cx - total_w // 2

    # ── 1. Draw numbers ──────────────────────────────────────────────
    positions: list[int] = []  # x-centres for each item
    bottom_y = 0
    for i, (label, val) in enumerate(items):
        px = start_x + i * spacing
        positions.append(px)
        text = f"{val:.2f}"
        color = TEXT_SECONDARY if label == "avg" else STAT_MUTED
        draw.text((px, y), text, font=font, fill=color, anchor="mm")
        bbox = draw.textbbox((px, y), text, font=font, anchor="mm")
        bottom_y = max(bottom_y, bbox[3])

    line_y = bottom_y + _px(11)

    # ── 2. Single connecting line (min → max) ────────────────────────
    if len(positions) >= 2:
        draw.line(
            [(positions[0], line_y), (positions[-1], line_y)],
            fill=STAT_LINE,
            width=1,
        )

    # ── 3. Dot at each position + label underneath ───────────────────
    dot_r = _px(3)
    for i, (label, _) in enumerate(items):
        px = positions[i]
        dot_fill = TEXT_SECONDARY if label == "avg" else STAT_MUTED
        draw.ellipse(
            [(px - dot_r, line_y - dot_r), (px + dot_r, line_y + dot_r)],
            fill=dot_fill,
        )
        lbl_color = TEXT_SECONDARY if label == "avg" else STAT_MUTED
        draw.text(
            (px, line_y + _px(10)),
            label,
            font=label_font,
            fill=lbl_color,
            anchor="mt",
        )

    # ── 4. White "today" marker on the min–avg–max line ──────────────
    if (
        current_val is not None
        and min_val is not None
        and max_val is not None
        and len(positions) >= 2
    ):
        mark_x = _compute_stats_marker_x(
            min_val=min_val,
            avg_val=avg_val,
            max_val=max_val,
            current_val=current_val,
            positions=positions,
        )

        tick_half = _px(8)
        draw.line(
            [(mark_x, line_y - tick_half), (mark_x, line_y + tick_half)],
            fill=TODAY_MARKER_COLOR,
            width=2,
        )


def _compute_stats_marker_x(
    *,
    min_val: float,
    avg_val: Optional[float],
    max_val: float,
    current_val: float,
    positions: list[int],
) -> int:
    # The chart gives avg its own visual midpoint, so marker placement must be
    # piecewise: min→avg for values on the left half, avg→max for values on the
    # right half. A single min→max interpolation can put a value that is above
    # avg to the left of the avg dot when the numeric range is skewed.
    if avg_val is not None and len(positions) >= 3:
        min_x = positions[0]
        avg_x = positions[1]
        max_x = positions[-1]

        if current_val <= avg_val:
            span = avg_val - min_val
            if span > 0:
                t = (current_val - min_val) / span
            else:
                t = 1.0
            t = max(0.0, min(1.0, t))
            return round(min_x + t * (avg_x - min_x))

        span = max_val - avg_val
        if span > 0:
            t = (current_val - avg_val) / span
        else:
            t = 0.0
        t = max(0.0, min(1.0, t))
        return round(avg_x + t * (max_x - avg_x))

    lo_x = positions[0]
    hi_x = positions[-1]
    span = max_val - min_val
    if span > 0:
        t = (current_val - min_val) / span
    else:
        t = 0.5
    t = max(0.0, min(1.0, t))
    return round(lo_x + t * (hi_x - lo_x))


def _draw_metrics_panel(
    draw: ImageDraw.Draw,
    rect: tuple[int, int, int, int],
    *,
    usd_spread: float,
    cross: float,
    usd_spread_avg: Optional[float],
    cross_avg: Optional[float],
    usd_spread_min: Optional[float],
    usd_spread_max: Optional[float],
    cross_min: Optional[float],
    cross_max: Optional[float],
    usd_spread_current: Optional[float] = None,
    cross_current: Optional[float] = None,
    ft: dict,
) -> None:
    x1, y1, x2, y2 = rect
    pw = x2 - x1
    ph = y2 - y1
    lcx = x1 + pw // 4
    rcx = x1 + 3 * pw // 4

    # Spacing: small=8px, medium=18px, big=32px (same system as currency panels)
    S, M, B = _px(8), _px(18), _px(32)

    row_title = y1 + _px(14) + B
    row_sub = row_title + _px(32) + S
    row_value = row_sub + _px(22) + M
    row_delta = row_value + _px(36) + S + _px(11)
    row_stats = row_delta + _px(11) + M + _px(22)

    # ── Left: USD Spread ──
    draw.text(
        (lcx, row_title),
        "USD Spread",
        font=ft["mtitle"],
        fill=WHITE,
        anchor="mt",
    )
    draw.text(
        (lcx, row_sub),
        "sell − buy",
        font=ft["mdetail"],
        fill=TEXT_SECONDARY,
        anchor="mt",
    )
    draw.text(
        (lcx, row_value),
        f"{usd_spread:.2f}",
        font=ft["mval"],
        fill=WHITE,
        anchor="mt",
    )
    if usd_spread_avg is not None:
        _draw_delta(
            draw,
            lcx,
            row_delta,
            usd_spread - usd_spread_avg,
            ft["mdetail"],
            is_spread=True,
        )
        _draw_stats_row(
            draw, lcx, row_stats,
            avg_val=usd_spread_avg,
            min_val=usd_spread_min,
            max_val=usd_spread_max,
            font=ft["stat"],
            label_font=ft["stat_label"],
            current_val=usd_spread_current if usd_spread_current is not None else usd_spread,
        )
    else:
        draw.text(
            (lcx, row_delta),
            "no avg data",
            font=ft["mdetail"],
            fill=TEXT_SECONDARY,
            anchor="mt",
        )

    # ── Right: EUR to USD ──
    draw.text(
        (rcx, row_title),
        "EUR to USD",
        font=ft["mtitle"],
        fill=WHITE,
        anchor="mt",
    )
    draw.text(
        (rcx, row_sub),
        "sell − buy",
        font=ft["mdetail"],
        fill=TEXT_SECONDARY,
        anchor="mt",
    )
    draw.text(
        (rcx, row_value),
        f"{cross:.2f}",
        font=ft["mval"],
        fill=WHITE,
        anchor="mt",
    )
    if cross_avg is not None:
        _draw_delta(
            draw,
            rcx,
            row_delta,
            cross - cross_avg,
            ft["mdetail"],
            is_spread=True,
        )
        _draw_stats_row(
            draw, rcx, row_stats,
            avg_val=cross_avg,
            min_val=cross_min,
            max_val=cross_max,
            font=ft["stat"],
            label_font=ft["stat_label"],
            current_val=cross_current if cross_current is not None else cross,
        )
    else:
        draw.text(
            (rcx, row_delta),
            "no avg data",
            font=ft["mdetail"],
            fill=TEXT_SECONDARY,
            anchor="mt",
        )
