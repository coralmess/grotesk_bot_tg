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

# ── paths ─────────────────────────────────────────────────────────────────
FONTS_DIR = PROJECT_ROOT / "fonts"
BG_IMAGE_PATH = (
    PROJECT_ROOT / "runtime_data" / "image" / "glassmorphism_background.jpg"
)

# ── canvas ────────────────────────────────────────────────────────────────
CANVAS_W = 1200
CANVAS_H = 850

# ── layout ────────────────────────────────────────────────────────────────
MARGIN = 28
GAP = 18
CORNER_RADIUS = 24
DATE_PANEL_H = 56  # height of the date glass panel

# ── glass effect ──────────────────────────────────────────────────────────
BLUR_RADIUS = 25
GLASS_ALPHA = 22  # white overlay opacity  (0 = clear, 255 = opaque)
BORDER_ALPHA = 100
BORDER_WIDTH = 2

# ── colours ───────────────────────────────────────────────────────────────
WHITE = (255, 255, 255)
TEXT_SECONDARY = (200, 200, 220)
GREEN = (52, 211, 153)
RED = (248, 113, 113)


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
    eur_buy_minus_usd_sell: float,
    usd_spread_avg: Optional[float] = None,
    cross_avg: Optional[float] = None,
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
    top_h = 400
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
        [(sep_x, bot_rect[1] + 24), (sep_x, bot_rect[3] - 24)],
        fill=(*WHITE, 50),
        width=1,
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
        ft=ft,
    )
    _draw_metrics_panel(
        draw,
        bot_rect,
        usd_spread=usd_spread,
        cross=eur_buy_minus_usd_sell,
        usd_spread_avg=usd_spread_avg,
        cross_avg=cross_avg,
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
        "title": load_font(52, fonts_dir=FONTS_DIR, prefer_heavy=True),
        "value": load_font(46, fonts_dir=FONTS_DIR, prefer_heavy=False),
        "label": load_font(30, fonts_dir=FONTS_DIR, prefer_heavy=False),
        "delta": load_font(28, fonts_dir=FONTS_DIR, prefer_heavy=False),
        "mtitle": load_font(34, fonts_dir=FONTS_DIR, prefer_heavy=True),
        "mval": load_font(40, fonts_dir=FONTS_DIR, prefer_heavy=False),
        "mdetail": load_font(26, fonts_dir=FONTS_DIR, prefer_heavy=False),
        "date": load_font(28, fonts_dir=FONTS_DIR, prefer_heavy=False),
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

    tri_size = max(12, int(th * 0.70))
    gap = 8
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
    ft: dict,
) -> None:
    x1, y1, x2, y2 = rect
    cx = (x1 + x2) // 2
    h = y2 - y1

    # currency title
    draw.text(
        (cx, y1 + int(h * 0.10)),
        currency,
        font=ft["title"],
        fill=WHITE,
        anchor="mt",
    )

    # ── Buy ──
    draw.text(
        (cx, y1 + int(h * 0.26)),
        "Buy",
        font=ft["label"],
        fill=TEXT_SECONDARY,
        anchor="mt",
    )
    draw.text(
        (cx, y1 + int(h * 0.36)),
        f"{buy:.2f}",
        font=ft["value"],
        fill=WHITE,
        anchor="mt",
    )
    if prev_buy is not None:
        _draw_delta(
            draw,
            cx,
            y1 + int(h * 0.50),
            buy - prev_buy,
            ft["delta"],
            is_spread=False,
        )

    # ── Sell ──
    draw.text(
        (cx, y1 + int(h * 0.60)),
        "Sell",
        font=ft["label"],
        fill=TEXT_SECONDARY,
        anchor="mt",
    )
    draw.text(
        (cx, y1 + int(h * 0.70)),
        f"{sell:.2f}",
        font=ft["value"],
        fill=WHITE,
        anchor="mt",
    )
    if prev_sell is not None:
        _draw_delta(
            draw,
            cx,
            y1 + int(h * 0.84),
            sell - prev_sell,
            ft["delta"],
            is_spread=False,
        )


# ── metrics panel (bottom) ───────────────────────────────────────────────


def _draw_metrics_panel(
    draw: ImageDraw.Draw,
    rect: tuple[int, int, int, int],
    *,
    usd_spread: float,
    cross: float,
    usd_spread_avg: Optional[float],
    cross_avg: Optional[float],
    ft: dict,
) -> None:
    x1, y1, x2, y2 = rect
    pw = x2 - x1
    ph = y2 - y1
    lcx = x1 + pw // 4
    rcx = x1 + 3 * pw // 4

    row_title = y1 + int(ph * 0.12)
    row_sub = y1 + int(ph * 0.28)
    row_value = y1 + int(ph * 0.46)
    row_delta = y1 + int(ph * 0.66)
    row_avg = y1 + int(ph * 0.84)

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
        draw.text(
            (lcx, row_avg),
            f"avg {usd_spread_avg:.2f}",
            font=ft["mdetail"],
            fill=TEXT_SECONDARY,
            anchor="mt",
        )
    else:
        draw.text(
            (lcx, row_delta),
            "no avg data",
            font=ft["mdetail"],
            fill=TEXT_SECONDARY,
            anchor="mt",
        )

    # ── Right: EUR buy − USD sell ──
    draw.text(
        (rcx, row_title),
        "EUR buy − USD sell",
        font=ft["mtitle"],
        fill=WHITE,
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
        draw.text(
            (rcx, row_avg),
            f"avg {cross_avg:.2f}",
            font=ft["mdetail"],
            fill=TEXT_SECONDARY,
            anchor="mt",
        )
    else:
        draw.text(
            (rcx, row_delta),
            "no avg data",
            font=ft["mdetail"],
            fill=TEXT_SECONDARY,
            anchor="mt",
        )
