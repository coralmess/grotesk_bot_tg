from __future__ import annotations

from urllib.parse import quote


def build_vin_decoder_url(vin: str) -> str:
    # Keeping the decoder URL builder separate makes VIN enrichment a replaceable provider
    # boundary instead of hard-wiring one third-party site throughout the scraper runtime.
    return f"https://www.vindecoderz.com/EN/check-lookup/{quote(vin)}"

