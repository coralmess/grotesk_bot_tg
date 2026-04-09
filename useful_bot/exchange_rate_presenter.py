from __future__ import annotations

"""Pure presentation helpers for the exchange-rate bot.

The fetch helper owns network/state concerns. This module owns the derived values
that feed ``render_exchange_rate_card`` so rendering math can be tested without
running Telegram or Minfin fetch logic.
"""

from statistics import mean
from typing import Any, Optional


def mean_from_history(history: list[dict[str, Any]], key: str) -> Optional[float]:
    values = []
    for item in history:
        try:
            values.append(float(item[key]))
        except (KeyError, TypeError, ValueError):
            continue
    return mean(values) if values else None


def min_from_history(history: list[dict[str, Any]], key: str) -> Optional[float]:
    values = []
    for item in history:
        try:
            values.append(float(item[key]))
        except (KeyError, TypeError, ValueError):
            continue
    return min(values) if values else None


def max_from_history(history: list[dict[str, Any]], key: str) -> Optional[float]:
    values = []
    for item in history:
        try:
            values.append(float(item[key]))
        except (KeyError, TypeError, ValueError):
            continue
    return max(values) if values else None


def build_exchange_rate_render_kwargs(snapshot, last_snapshot, history: list[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    usd_spread = snapshot.usd_sell - snapshot.usd_buy
    eur_sell_minus_usd_buy = snapshot.eur_sell - snapshot.usd_buy

    # Include the current scrape in the render stats so marker placement and
    # min/max bounds always reflect the value that will be drawn right now.
    history_plus_today = history + [
        {
            "usd_spread": usd_spread,
            "eur_sell_minus_usd_buy": eur_sell_minus_usd_buy,
            "usd_buy": snapshot.usd_buy,
            "eur_buy": snapshot.eur_buy,
        },
    ]

    recent_30 = history_plus_today[-30:]
    kwargs = {
        "usd_buy": snapshot.usd_buy,
        "usd_sell": snapshot.usd_sell,
        "eur_buy": snapshot.eur_buy,
        "eur_sell": snapshot.eur_sell,
        "prev_usd_buy": last_snapshot.usd_buy if last_snapshot else None,
        "prev_usd_sell": last_snapshot.usd_sell if last_snapshot else None,
        "prev_eur_buy": last_snapshot.eur_buy if last_snapshot else None,
        "prev_eur_sell": last_snapshot.eur_sell if last_snapshot else None,
        "usd_spread": usd_spread,
        "eur_sell_minus_usd_buy": eur_sell_minus_usd_buy,
        "usd_spread_avg": mean_from_history(history_plus_today, "usd_spread"),
        "cross_avg": mean_from_history(history_plus_today, "eur_sell_minus_usd_buy"),
        "usd_spread_min": min_from_history(history_plus_today, "usd_spread"),
        "usd_spread_max": max_from_history(history_plus_today, "usd_spread"),
        "cross_min": min_from_history(history_plus_today, "eur_sell_minus_usd_buy"),
        "cross_max": max_from_history(history_plus_today, "eur_sell_minus_usd_buy"),
        "usd_spread_current": usd_spread,
        "cross_current": eur_sell_minus_usd_buy,
        "usd_buy_avg": mean_from_history(recent_30, "usd_buy"),
        "usd_buy_min": min_from_history(recent_30, "usd_buy"),
        "usd_buy_max": max_from_history(recent_30, "usd_buy"),
        "eur_buy_avg": mean_from_history(recent_30, "eur_buy"),
        "eur_buy_min": min_from_history(recent_30, "eur_buy"),
        "eur_buy_max": max_from_history(recent_30, "eur_buy"),
    }
    return kwargs, history_plus_today
