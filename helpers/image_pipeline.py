from __future__ import annotations

import io
import threading
from pathlib import Path
from typing import Awaitable, Callable, Iterable, Optional

import requests
from PIL import Image, ImageDraw, ImageFont

_EDSR_LOCK = threading.Lock()
_EDSR_SUPERRES = None
_EDSR_MODEL_PATH_LOADED: Optional[str] = None


def load_font(font_size: int, *, fonts_dir: Optional[Path] = None, prefer_heavy: bool = False):
    font_dir = fonts_dir
    if prefer_heavy:
        font_candidates = [
            (font_dir / "SFPro-Heavy.ttf") if font_dir else "SFPro-Heavy.ttf",
            (font_dir / "SFPro-Bold.ttf") if font_dir else "SFPro-Bold.ttf",
        ]
    else:
        font_candidates = [
            (font_dir / "SFPro-Bold.ttf") if font_dir else "SFPro-Bold.ttf",
            (font_dir / "SFPro-Heavy.ttf") if font_dir else "SFPro-Heavy.ttf",
        ]
    font_candidates += [
        "SFPro-Heavy.ttf",
        "SFPro-Bold.ttf",
        "arialbd.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    ]
    for font_file in font_candidates:
        try:
            return ImageFont.truetype(str(font_file), font_size)
        except IOError:
            continue
    return ImageFont.load_default()


def _ensure_edsr_weights(*, model_path: Path, model_url: str, logger) -> bool:
    if model_path.exists():
        return True
    try:
        model_path.parent.mkdir(exist_ok=True)
        resp = requests.get(model_url, stream=True, timeout=60)
        if resp.status_code != 200:
            logger.warning(f"EDSR weights download failed: HTTP {resp.status_code}")
            return False
        with open(model_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                if chunk:
                    f.write(chunk)
        return True
    except Exception as exc:
        logger.warning(f"EDSR weights download failed: {exc}")
        return False


def _get_edsr_superres(*, cv2_module, model_path: Path, model_url: str, logger):
    global _EDSR_SUPERRES, _EDSR_MODEL_PATH_LOADED
    model_path_str = str(model_path.resolve())
    if _EDSR_SUPERRES is not None and _EDSR_MODEL_PATH_LOADED == model_path_str:
        return _EDSR_SUPERRES
    if cv2_module is None or not hasattr(cv2_module, "dnn_superres"):
        raise RuntimeError("opencv-contrib (dnn_superres) not available")
    if not _ensure_edsr_weights(model_path=model_path, model_url=model_url, logger=logger):
        raise RuntimeError("EDSR weights unavailable")
    sr = cv2_module.dnn_superres.DnnSuperResImpl_create()
    sr.readModel(model_path_str)
    sr.setModel("edsr", 2)
    _EDSR_SUPERRES = sr
    _EDSR_MODEL_PATH_LOADED = model_path_str
    return _EDSR_SUPERRES


def _upscale_with_edsr(
    pil_img: Image.Image,
    *,
    cv2_module,
    np_module,
    model_path: Path,
    model_url: str,
    logger,
) -> Image.Image:
    if cv2_module is None or np_module is None:
        raise RuntimeError("opencv/numpy not available for EDSR")
    sr = _get_edsr_superres(
        cv2_module=cv2_module,
        model_path=model_path,
        model_url=model_url,
        logger=logger,
    )
    img_rgb = np_module.array(pil_img.convert("RGB"))
    img_bgr = cv2_module.cvtColor(img_rgb, cv2_module.COLOR_RGB2BGR)
    with _EDSR_LOCK:
        up_bgr = sr.upsample(img_bgr)
    up_rgb = cv2_module.cvtColor(up_bgr, cv2_module.COLOR_BGR2RGB)
    return Image.fromarray(up_rgb)


def _fetch_image_bytes(
    image_url: str,
    *,
    image_url_candidates_fn: Callable[[str | None], Iterable[str]],
) -> bytes:
    last_exc = None
    for url in image_url_candidates_fn(image_url):
        try:
            resp = requests.get(url, timeout=30)
            if not resp.ok or not resp.content:
                last_exc = RuntimeError(f"Image HTTP {resp.status_code} for {url}")
                continue
            return resp.content
        except Exception as exc:
            last_exc = exc
    if last_exc:
        raise last_exc
    raise RuntimeError("No image candidates available")


def process_image(
    image_url,
    uah_price,
    sale_percentage,
    *,
    upscale_images: bool,
    upscale_method: str,
    image_url_candidates_fn: Callable[[str | None], Iterable[str]],
    logger,
    fonts_dir: Optional[Path] = None,
    cv2_module=None,
    np_module=None,
    edsr_model_path: Optional[Path] = None,
    edsr_model_url: str = "",
):
    response_bytes = _fetch_image_bytes(image_url, image_url_candidates_fn=image_url_candidates_fn)
    img = Image.open(io.BytesIO(response_bytes))
    # If upscaling is disabled, downscale large sources to keep file size under Telegram limit.
    if not upscale_images:
        max_edge = 1280
        w, h = img.size
        scale = min(1.0, max_edge / max(w, h)) if max(w, h) else 1.0
        if scale < 1.0:
            new_size = (int(w * scale), int(h * scale))
            img = img.resize(new_size, Image.LANCZOS)
    width, height = img.size
    should_upscale = upscale_images and max(width, height) < 720
    if should_upscale:
        if upscale_method == "edsr":
            try:
                if edsr_model_path is None:
                    raise RuntimeError("edsr_model_path is required for EDSR")
                img = _upscale_with_edsr(
                    img,
                    cv2_module=cv2_module,
                    np_module=np_module,
                    model_path=edsr_model_path,
                    model_url=edsr_model_url,
                    logger=logger,
                )
                width, height = img.size
            except Exception as exc:
                logger.warning(f"EDSR upscale failed, falling back to LANCZOS: {exc}")
                width, height = [dim * 2 for dim in img.size]
                img = img.resize((width, height), Image.LANCZOS)
        else:
            width, height = [dim * 2 for dim in img.size]
            img = img.resize((width, height), Image.LANCZOS)

    price_text, sale_text = f"{uah_price} UAH", f"-{sale_percentage}%"
    padding = max(12, int(width * 0.03))
    text_margin = max(20, int(width * 0.1))
    text_margin = min(text_margin, int(width * 0.14))

    # Choose base font size and adjust if needed to fit width.
    base_scale = 0.064 if width > height else 0.06
    font_size = max(24, int(width * base_scale))
    font = load_font(font_size, fonts_dir=fonts_dir, prefer_heavy=False)

    def _fit_font(font_size_value):
        while font_size_value > 12:
            local_font = load_font(font_size_value, fonts_dir=fonts_dir, prefer_heavy=False)
            dummy = Image.new("RGB", (width, width), (255, 255, 255))
            draw = ImageDraw.Draw(dummy)
            price_bbox = draw.textbbox((0, 0), price_text, font=local_font)
            sale_bbox = draw.textbbox((0, 0), sale_text, font=local_font)
            price_w = price_bbox[2] - price_bbox[0]
            sale_w = sale_bbox[2] - sale_bbox[0]
            if max(price_w, sale_w) <= (width - (text_margin * 2)):
                return local_font
            font_size_value -= 2
        return load_font(font_size_value, fonts_dir=fonts_dir, prefer_heavy=False)

    font = _fit_font(font_size)
    ascent, descent = font.getmetrics()
    text_height = ascent + descent
    line_padding = max(2, int(font_size * 0.15))

    if width > height:
        # Make square by adding white space ABOVE the image (not below the prices).
        top_pad = width - height
        square_img = Image.new("RGB", (width, width), (255, 255, 255))
        square_img.paste(img, (0, top_pad))

        bottom_area = text_height + (padding * 2) + line_padding
        new_img = Image.new("RGB", (width, width + bottom_area), (255, 255, 255))
        new_img.paste(square_img, (0, 0))
        draw = ImageDraw.Draw(new_img)
        text_y = width + padding + ascent + (line_padding // 2)
        draw.text((text_margin, text_y), price_text, font=font, fill=(22, 22, 24), anchor="ls")
        draw.text((width - text_margin, text_y), sale_text, font=font, fill=(255, 59, 48), anchor="rs")
    elif height > width and (height / width) > 1.56:
        # Add side padding for very tall images to reach target portrait ratio (1:1.56).
        target_ratio = 1.56
        target_width = int(round(height / target_ratio))
        if target_width > width:
            side_pad_total = target_width - width
            left_pad = side_pad_total // 2
            padded_img = Image.new("RGB", (target_width, height), (255, 255, 255))
            padded_img.paste(img, (left_pad, 0))
        else:
            padded_img = img
            target_width = width

        bottom_area = text_height + (padding * 2) + line_padding
        new_img = Image.new("RGB", (target_width, height + bottom_area), (255, 255, 255))
        new_img.paste(padded_img, (0, 0))
        draw = ImageDraw.Draw(new_img)
        text_y = height + padding + ascent + (line_padding // 2)
        draw.text((text_margin, text_y), price_text, font=font, fill=(22, 22, 24), anchor="ls")
        draw.text((target_width - text_margin, text_y), sale_text, font=font, fill=(255, 59, 48), anchor="rs")
    else:
        # Default: add a bottom bar for text.
        bottom_area = text_height + (padding * 2) + line_padding
        new_img = Image.new("RGB", (width, height + bottom_area), (255, 255, 255))
        new_img.paste(img, (0, 0))
        draw = ImageDraw.Draw(new_img)
        text_y = height + padding + ascent + (line_padding // 2)
        draw.text((text_margin, text_y), price_text, font=font, fill=(22, 22, 24), anchor="ls")
        draw.text((width - text_margin, text_y), sale_text, font=font, fill=(255, 59, 48), anchor="rs")

    img_byte_arr = io.BytesIO()
    if upscale_images:
        new_img.save(img_byte_arr, format="PNG", quality=95)
    else:
        # JPEG is smaller and avoids Telegram size limits for large images.
        if new_img.mode != "RGB":
            new_img = new_img.convert("RGB")
        new_img.save(img_byte_arr, format="JPEG", quality=85, optimize=True, subsampling=0)
    img_byte_arr.seek(0)
    return img_byte_arr


def fits_telegram_photo(width: int, height: int) -> bool:
    if width <= 0 or height <= 0:
        return False
    if width + height > 10000:
        return False
    ratio = max(width / float(height), height / float(width))
    return ratio <= 20.0


def encode_jpeg_for_telegram(image: Image.Image) -> Optional[bytes]:
    max_bytes = 10 * 1024 * 1024
    for quality in (98, 95, 92, 88, 84, 80):
        out = io.BytesIO()
        image.save(out, format="JPEG", quality=quality, subsampling=0, optimize=False)
        data = out.getvalue()
        if len(data) <= max_bytes:
            return data
    return None


def upscale_image_bytes_for_telegram_sync(
    img_bytes: bytes,
    *,
    max_dim: int = 5000,
    min_upscale_dim: int = 1280,
    upscale_factors: Iterable[float] = (3.0, 2.5, 2.0),
    logger=None,
) -> Optional[bytes]:
    try:
        im = Image.open(io.BytesIO(img_bytes))
        if im.mode not in ("RGB", "L"):
            im = im.convert("RGB")
        w, h = im.size
        if min(w, h) >= min_upscale_dim:
            return None

        for factor in upscale_factors:
            new_w, new_h = int(w * factor), int(h * factor)
            longer = max(new_w, new_h)
            if longer > max_dim:
                ratio = max_dim / float(longer)
                new_w, new_h = int(new_w * ratio), int(new_h * ratio)
            if not fits_telegram_photo(new_w, new_h):
                continue

            im_up = im.resize((new_w, new_h), resample=Image.Resampling.LANCZOS)
            data = encode_jpeg_for_telegram(im_up)
            if data is not None:
                return data

            trial = im_up
            for _ in range(6):
                dw = max(1, int(trial.width * 0.9))
                dh = max(1, int(trial.height * 0.9))
                if (dw, dh) == trial.size:
                    break
                trial = trial.resize((dw, dh), resample=Image.Resampling.LANCZOS)
                if not fits_telegram_photo(dw, dh):
                    continue
                data = encode_jpeg_for_telegram(trial)
                if data is not None:
                    return data
        return None
    except Exception as exc:
        if logger is not None:
            logger.error(f"Image upscale failed: {exc}")
        return None


async def send_remote_photo_with_fallback(
    *,
    bot,
    chat_id,
    caption: str,
    image_url: Optional[str],
    is_valid_image_url: Callable[[Optional[str]], bool],
    download_bytes: Callable[[str], Awaitable[Optional[bytes]]],
    send_message: Callable[[object, str, str], Awaitable[bool]],
    send_photo_by_bytes: Callable[[object, str, bytes, str], Awaitable[bool]],
    run_cpu_bound_fn: Callable[..., Awaitable[Optional[bytes]]],
    logger,
    min_upscale_dim: int = 1280,
    max_dim: int = 5000,
) -> bool:
    if not image_url or not is_valid_image_url(image_url):
        result = await send_message(bot, chat_id, caption)
        return bool(result)

    raw = await download_bytes(image_url)
    if not raw:
        logger.warning("Photo not downloaded; sending text")
        result = await send_message(bot, chat_id, caption)
        return bool(result)

    photo_bytes = await run_cpu_bound_fn(
        upscale_image_bytes_for_telegram_sync,
        raw,
        max_dim=max_dim,
        min_upscale_dim=min_upscale_dim,
        logger=logger,
    )
    photo_bytes = photo_bytes or raw

    result = await send_photo_by_bytes(bot, chat_id, photo_bytes, caption)
    if result is not None:
        return bool(result)

    logger.warning("Photo not sent; sending text")
    result = await send_message(bot, chat_id, caption)
    return bool(result)
