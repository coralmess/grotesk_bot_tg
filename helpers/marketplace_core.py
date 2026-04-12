from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class MarketplaceItem:
    # Shared item shape keeps OLX and SHAFA adapters thin so dedupe/send/persist logic
    # can live in one place instead of drifting across two scraper implementations.
    id: str
    name: str
    link: str
    price_text: str
    price_int: int
    first_image_url: Optional[str] = None


@dataclass(frozen=True)
class SourceStats:
    streak: int = 0
    cycle_count: int = 0


@dataclass(frozen=True)
class SourceDecision:
    should_process: bool
    next_streak: int
    next_cycle_count: int
    level: int
    divisor: int


def normalize_duplicate_name(name: str) -> str:
    # Duplicate detection is based on the human-facing title we send to Telegram, so
    # whitespace/case normalization has to be shared across both marketplace scrapers.
    return " ".join((name or "").split()).casefold()


def duplicate_key(name: str, price_int: int) -> Optional[Tuple[str, int]]:
    normalized_name = normalize_duplicate_name(name)
    if not normalized_name or price_int <= 0:
        return None
    return normalized_name, int(price_int)


def notification_storage_key(key: Tuple[str, int]) -> str:
    return f"{key[0]}\x1f{key[1]}"


def make_source_decision(stats: SourceStats) -> SourceDecision:
    cycle_count = stats.cycle_count + 1
    # This lightweight throttle was extracted into the shared core so OLX and SHAFA
    # cool down stale sources with the same policy instead of diverging over time.
    level = min(stats.streak // 365, 23)
    divisor = level + 1
    should_process = (cycle_count % divisor) == 0
    if should_process:
        return SourceDecision(
            should_process=True,
            next_streak=stats.streak,
            next_cycle_count=cycle_count,
            level=level,
            divisor=divisor,
        )
    return SourceDecision(
        should_process=False,
        next_streak=stats.streak,
        next_cycle_count=cycle_count,
        level=level,
        divisor=divisor,
    )


def finished_source_decision(current_streak: int, item_count: int) -> tuple[int, int]:
    if item_count > 0:
        return 0, 0
    return current_streak + 1, 0
