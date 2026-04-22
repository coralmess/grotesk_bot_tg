from __future__ import annotations

from pathlib import Path

from helpers import image_pipeline as image_pipeline_helpers


def load_font(font_size, *, fonts_dir: Path, prefer_heavy=False):
    # The Lyst renderer now goes through one media boundary so caption sending
    # does not need to know where fonts live or how they are selected.
    return image_pipeline_helpers.load_font(
        font_size,
        fonts_dir=fonts_dir,
        prefer_heavy=prefer_heavy,
    )


def ensure_edsr_weights(*, model_path: Path, model_url: str, logger):
    return image_pipeline_helpers._ensure_edsr_weights(
        model_path=model_path,
        model_url=model_url,
        logger=logger,
    )


def get_edsr_superres(*, cv2_module, model_path: Path, model_url: str, logger):
    return image_pipeline_helpers._get_edsr_superres(
        cv2_module=cv2_module,
        model_path=model_path,
        model_url=model_url,
        logger=logger,
    )


def upscale_with_edsr(pil_img, *, cv2_module, np_module, model_path: Path, model_url: str, logger):
    return image_pipeline_helpers._upscale_with_edsr(
        pil_img,
        cv2_module=cv2_module,
        np_module=np_module,
        model_path=model_path,
        model_url=model_url,
        logger=logger,
    )


def fetch_image_bytes(image_url: str, *, image_url_candidates_fn):
    return image_pipeline_helpers._fetch_image_bytes(
        image_url,
        image_url_candidates_fn=image_url_candidates_fn,
    )


def process_image(
    image_url,
    uah_price,
    sale_percentage,
    *,
    upscale_images: bool,
    upscale_method: str,
    image_url_candidates_fn,
    logger,
    fonts_dir: Path,
    cv2_module,
    np_module,
    edsr_model_path: Path,
    edsr_model_url: str,
):
    # This adapter keeps the monolith from reaching into the shared image
    # pipeline directly, which makes later renderer changes cheaper and safer.
    return image_pipeline_helpers.process_image(
        image_url,
        uah_price,
        sale_percentage,
        upscale_images=upscale_images,
        upscale_method=upscale_method,
        image_url_candidates_fn=image_url_candidates_fn,
        logger=logger,
        fonts_dir=fonts_dir,
        cv2_module=cv2_module,
        np_module=np_module,
        edsr_model_path=edsr_model_path,
        edsr_model_url=edsr_model_url,
    )
