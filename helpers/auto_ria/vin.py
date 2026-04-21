from __future__ import annotations

from urllib.parse import quote


def build_vin_decoder_url(vin: str, *, model_year: int | None = None) -> str:
    # VIN enrichment now points at NHTSA vPIC because it is free, official, and returned
    # stable machine-readable data in testing, unlike the scraped third-party decoder.
    suffix = f"&modelyear={model_year}" if model_year else ""
    return f"https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVinValues/{quote(vin)}?format=json{suffix}"
