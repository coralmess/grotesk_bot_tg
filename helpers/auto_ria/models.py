from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class AutoRiaListing:
    # A dedicated listing model keeps Auto RIA parsing separate from the generic
    # marketplace item shape because cars need VIN enrichment fields that OLX/SHAFA do not.
    id: str
    url: str
    title: str
    subtitle: str
    price_usd: int
    price_text: str
    mileage_text: str
    fuel_engine_text: str
    image_url: Optional[str]


@dataclass(frozen=True)
class VinDecoderDetails:
    trim: Optional[str] = None
    transmission: Optional[str] = None

