from io import BytesIO
from pathlib import Path
from typing import Dict, List, Tuple

from PIL import Image, ImageDraw, ImageFont

MINUTES_PER_DAY = 24 * 60

FONT_DIR = Path(__file__).resolve().parents[1] / "fonts"
FONT_BOLD = FONT_DIR / "SFPro-Bold.ttf"
FONT_HEAVY = FONT_DIR / "SFPro-Heavy.ttf"


def slot_to_time(minutes: int) -> str:
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours:02d}:{mins:02d}"


def format_interval_image(start: int, end: int) -> str:
    start_text = slot_to_time(start % MINUTES_PER_DAY)
    end_mod = end % MINUTES_PER_DAY
    end_text = "00:00" if end_mod == 0 else slot_to_time(end_mod)
    return f"{start_text} - {end_text}"


def duration_text(start: int, end: int) -> str:
    minutes = end - start
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours} год. {mins:02d} хв."


def load_font(path: Path, size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    try_paths = [path]
    windows_fonts = Path("C:/Windows/Fonts")
    try_paths.extend(
        [
            windows_fonts / "arial.ttf",
            windows_fonts / "segoeui.ttf",
            windows_fonts / "times.ttf",
        ]
    )
    for candidate in try_paths:
        try:
            return ImageFont.truetype(str(candidate), size)
        except Exception:
            continue
    return ImageFont.load_default()


def render_schedule_image(
    intervals_by_group: Dict[str, List[Tuple[str, int, int]]],
    groups: List[str],
) -> BytesIO:
    width, height = 1597, 1733
    img = Image.new("RGB", (width, height), (0, 15, 69))
    draw = ImageDraw.Draw(img)

    # Background gradient
    top_color = (0, 15, 69)
    bottom_color = (2, 35, 101)
    for y in range(height):
        t = y / max(1, height - 1)
        r = int(top_color[0] * (1 - t) + bottom_color[0] * t)
        g = int(top_color[1] * (1 - t) + bottom_color[1] * t)
        b = int(top_color[2] * (1 - t) + bottom_color[2] * t)
        draw.line([(0, y), (width, y)], fill=(r, g, b))

    # Fonts
    header_font = load_font(FONT_HEAVY, 38)
    row_font = load_font(FONT_HEAVY, 36)
    box_time_font = load_font(FONT_BOLD, 26)
    box_dur_font = load_font(FONT_BOLD, 18)

    # Layout
    margin_top = 20
    margin_bottom = 20
    margin_left = 20
    margin_right = 20
    header_height = 110
    rows = len(groups)
    available = height - margin_top - margin_bottom - header_height
    row_height = max(1, available // rows)
    extra = available - row_height * rows
    margin_bottom += extra

    left_header_text = "Підчерга"
    left_header_width = draw.textbbox((0, 0), left_header_text, font=header_font)[2]
    left_col_width = max(140, left_header_width + 30)

    right_start = margin_left + left_col_width + 20
    right_width = width - margin_right - right_start
    right_inner_margin = 16

    grid_color = (90, 150, 220)
    draw.rectangle(
        [margin_left, margin_top, width - margin_right, height - margin_bottom],
        outline=grid_color,
        width=2,
    )

    # Header
    header_y = margin_top + (header_height - 34) // 2
    draw.text((margin_left + 10, header_y), left_header_text, fill=(255, 255, 255), font=header_font)
    draw.text(
        (right_start + 20, header_y),
        "Діапазони відключень",
        fill=(255, 255, 255),
        font=header_font,
    )
    draw.line(
        [(margin_left, margin_top + header_height), (width - margin_right, margin_top + header_height)],
        fill=grid_color,
        width=2,
    )
    draw.line(
        [(margin_left + left_col_width, margin_top), (margin_left + left_col_width, height - margin_bottom)],
        fill=grid_color,
        width=2,
    )

    # Determine max boxes per row for layout
    max_boxes = 1
    for group in groups:
        intervals = intervals_by_group.get(group, [])
        max_boxes = max(max_boxes, len(intervals))

    box_gap = 12
    usable_width = right_width - right_inner_margin * 2
    box_width = (usable_width - (max_boxes - 1) * box_gap) / max_boxes
    box_height = row_height - 26
    box_y_pad = (row_height - box_height) // 2
    text_pad_x = 10
    text_gap_y = 6

    # Rows
    for idx, group in enumerate(groups):
        row_top = margin_top + header_height + idx * row_height
        row_bottom = row_top + row_height
        draw.line([(margin_left, row_bottom), (width - margin_right, row_bottom)], fill=grid_color, width=1)

        # Row label
        label_x = margin_left + 25
        label_bbox = draw.textbbox((0, 0), group, font=row_font)
        label_h = label_bbox[3] - label_bbox[1]
        label_y = row_top + (row_height - label_h) // 2
        draw.text((label_x, label_y), group, fill=(255, 255, 255), font=row_font)

        # Boxes
        intervals = intervals_by_group.get(group, [])
        box_start = right_start + right_inner_margin
        for idx_box, (kind, start, end) in enumerate(intervals):
            box_x = box_start + idx_box * (box_width + box_gap)
            box_y = row_top + box_y_pad
            radius = 12
            if kind == "light":
                fill_color = (120, 225, 140)
                text_color = (15, 35, 75)
            else:
                fill_color = (225, 235, 255)
                text_color = (20, 30, 70)

            draw.rounded_rectangle(
                [box_x, box_y, box_x + box_width, box_y + box_height],
                radius=radius,
                fill=fill_color,
                outline=(200, 210, 240),
                width=2,
            )

            time_text = format_interval_image(start, end)
            dur_text = duration_text(start, end)

            time_bbox = draw.textbbox((0, 0), time_text, font=box_time_font)
            time_w = time_bbox[2] - time_bbox[0]
            time_h = time_bbox[3] - time_bbox[1]

            dur_bbox = draw.textbbox((0, 0), dur_text, font=box_dur_font)
            dur_w = dur_bbox[2] - dur_bbox[0]
            dur_h = dur_bbox[3] - dur_bbox[1]

            total_text_h = time_h + text_gap_y + dur_h
            time_y = box_y + (box_height - total_text_h) / 2
            dur_y = time_y + time_h + text_gap_y

            time_x = box_x + (box_width - time_w) / 2
            time_x = max(box_x + text_pad_x, min(time_x, box_x + box_width - text_pad_x - time_w))
            draw.text((time_x, time_y), time_text, font=box_time_font, fill=text_color)

            dur_x = box_x + (box_width - dur_w) / 2
            dur_x = max(box_x + text_pad_x, min(dur_x, box_x + box_width - text_pad_x - dur_w))
            draw.text((dur_x, dur_y), dur_text, font=box_dur_font, fill=text_color)

    buffer = BytesIO()
    img.save(buffer, format="PNG")
    buffer.seek(0)
    return buffer
