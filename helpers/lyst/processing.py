from __future__ import annotations

import asyncio
import traceback
from collections import defaultdict
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class LystProcessingStats:
    new_total: int = 0
    removed_total: int = 0


def merge_base_url_into_shoes(result, base_url, country, *, logger):
    merged = []
    for shoe in result:
        if isinstance(shoe, dict):
            shoe["base_url"] = base_url
            merged.append(shoe)
        else:
            logger.error("Unexpected item data type for %s: %s", country, type(shoe))
    return merged


def shoe_key(shoe: dict[str, Any]) -> str:
    return f"{shoe['name']}_{shoe['unique_id']}"


def filter_duplicates(shoes, exchange_rates, *, country_priority, convert_to_uah):
    filtered_shoes = []
    grouped_shoes = defaultdict(list)
    for shoe in shoes:
        grouped_shoes[shoe_key(shoe)].append(shoe)

    for group in grouped_shoes.values():
        country_map = {}
        for shoe in group:
            country = shoe["country"]
            if country not in country_map:
                country_map[country] = shoe
            else:
                existing = country_map[country]
                existing_has_img = bool(existing.get("image_url")) and existing["image_url"].startswith(("http", "https"))
                new_has_img = bool(shoe.get("image_url")) and shoe["image_url"].startswith(("http", "https"))
                if not existing_has_img and new_has_img:
                    country_map[country] = shoe

        group = list(country_map.values())
        if len(group) == 1:
            filtered_shoes.append(group[0])
            continue

        group.sort(
            key=lambda item: country_priority.index(item["country"])
            if item["country"] in country_priority
            else len(country_priority)
        )
        for shoe in group:
            shoe["uah_price"] = convert_to_uah(shoe["sale_price"], shoe["country"], exchange_rates, shoe["name"]).uah_amount
        base = group[0]
        replacement = next((shoe for shoe in group[1:] if base["uah_price"] - shoe["uah_price"] >= 200), None)
        filtered_shoes.append(replacement or base)
    return filtered_shoes


def apply_new_shoe_state(shoe, uah_sale):
    shoe.update(
        {
            "lowest_price": shoe["sale_price"],
            "lowest_price_uah": uah_sale,
            "uah_price": uah_sale,
            "active": True,
        }
    )


def apply_existing_shoe_state(shoe, old_shoe, uah_sale, exchange_rates, *, convert_to_uah):
    old_sale_price = old_shoe["sale_price"]
    old_sale_country = old_shoe["country"]
    old_uah = old_shoe.get("uah_price") or convert_to_uah(
        old_sale_price,
        old_sale_country,
        exchange_rates,
        shoe["name"],
    ).uah_amount
    lowest_price_uah = old_shoe.get("lowest_price_uah") or old_uah

    shoe["uah_price"] = uah_sale
    if uah_sale < lowest_price_uah:
        shoe["lowest_price"], shoe["lowest_price_uah"] = shoe["sale_price"], uah_sale
    else:
        shoe["lowest_price"], shoe["lowest_price_uah"] = old_shoe["lowest_price"], lowest_price_uah
    shoe["active"] = True


async def save_single_shoe(key, shoe, *, save_shoe_data_bulk):
    await save_shoe_data_bulk([dict(shoe, key=key)])


async def process_shoe(
    shoe,
    old_data,
    message_queue,
    exchange_rates,
    *,
    is_shoe_processed,
    mark_shoe_processed,
    save_shoe_data_bulk,
    build_shoe_message,
    calculate_sale_percentage,
    convert_to_uah,
):
    key = shoe_key(shoe)
    is_new_item = key not in old_data
    was_processed = await is_shoe_processed(key) if is_new_item else False

    sale_percentage = calculate_sale_percentage(shoe["original_price"], shoe["sale_price"], shoe["country"])
    sale_exchange_data = convert_to_uah(shoe["sale_price"], shoe["country"], exchange_rates, shoe["name"])
    kurs = sale_exchange_data.exchange_rate
    uah_sale = sale_exchange_data.uah_amount
    kurs_symbol = sale_exchange_data.currency_symbol

    if is_new_item:
        apply_new_shoe_state(shoe, uah_sale)
        # Processed markers only suppress duplicate "new item" alerts. They must not
        # suppress future state updates for already-known items.
        if not was_processed:
            message = build_shoe_message(shoe, sale_percentage, uah_sale, kurs, kurs_symbol)
            await message_queue.add_message(
                shoe["base_url"]["telegram_chat_id"],
                message,
                shoe["image_url"],
                uah_sale,
                sale_percentage,
            )
            await mark_shoe_processed(key)
        old_data[key] = shoe
        await save_single_shoe(key, shoe, save_shoe_data_bulk=save_shoe_data_bulk)
        return

    old_shoe = old_data[key]
    apply_existing_shoe_state(
        shoe,
        old_shoe,
        uah_sale,
        exchange_rates,
        convert_to_uah=convert_to_uah,
    )
    old_data[key] = shoe
    await save_single_shoe(key, shoe, save_shoe_data_bulk=save_shoe_data_bulk)


async def process_all_shoes(
    all_shoes,
    old_data,
    message_queue,
    exchange_rates,
    *,
    shoe_concurrency,
    resolve_redirects,
    run_failed,
    logger,
    touch_progress,
    calculate_sale_percentage,
    convert_to_uah,
    build_shoe_message,
    is_shoe_processed,
    mark_shoe_processed,
    save_shoe_data_bulk,
    get_final_clear_link,
):
    new_shoe_count = 0
    semaphore = asyncio.Semaphore(shoe_concurrency)
    total_items = len(all_shoes)
    touch_progress("process_shoes_start", total_items=total_items)

    async def process_single_shoe(i, shoe):
        nonlocal new_shoe_count
        async with semaphore:
            try:
                touch_progress("process_shoe", index=i, total_items=total_items)
                country = shoe["country"]
                name = shoe["name"]
                key = shoe_key(shoe)
                sale_percentage = calculate_sale_percentage(shoe["original_price"], shoe["sale_price"], country)

                if sale_percentage < shoe["base_url"]["min_sale"]:
                    return

                if key not in old_data:
                    if resolve_redirects:
                        shoe["shoe_link"] = await get_final_clear_link(
                            shoe["shoe_link"],
                            semaphore,
                            name,
                            country,
                            i,
                            total_items,
                        )
                    new_shoe_count += 1
                else:
                    shoe["shoe_link"] = old_data[key]["shoe_link"]

                await process_shoe(
                    shoe,
                    old_data,
                    message_queue,
                    exchange_rates,
                    is_shoe_processed=is_shoe_processed,
                    mark_shoe_processed=mark_shoe_processed,
                    save_shoe_data_bulk=save_shoe_data_bulk,
                    build_shoe_message=build_shoe_message,
                    calculate_sale_percentage=calculate_sale_percentage,
                    convert_to_uah=convert_to_uah,
                )
            except Exception as exc:
                logger.error("Error processing shoe %s: %s", shoe.get("name", "unknown"), exc)
                logger.error(traceback.format_exc())

    batch_size = 10
    for i in range(0, len(all_shoes), batch_size):
        batch = all_shoes[i : i + batch_size]
        await asyncio.gather(*[process_single_shoe(i + j, shoe) for j, shoe in enumerate(batch)])
        touch_progress("process_shoes_batch", batch_start=i, batch_size=len(batch))
        await asyncio.sleep(0.1)

    logger.info("Processed %s new shoes in total", new_shoe_count)

    removed_shoes = []
    if not run_failed:
        current_shoes = {shoe_key(shoe) for shoe in all_shoes}
        removed_shoes = [
            dict(shoe, key=key, active=False)
            for key, shoe in old_data.items()
            if key not in current_shoes and shoe.get("active", True)
        ]
        for shoe in removed_shoes:
            old_data[shoe["key"]]["active"] = False
        if removed_shoes:
            logger.info("Marking %s removed shoes inactive", len(removed_shoes))
            chunk_size = 500
            for i in range(0, len(removed_shoes), chunk_size):
                chunk = removed_shoes[i : i + chunk_size]
                await save_shoe_data_bulk(chunk)
                touch_progress(
                    "removed_shoes_batch",
                    batch_start=i,
                    batch_size=len(chunk),
                    total_removed=len(removed_shoes),
                )
                await asyncio.sleep(0)

    touch_progress("process_shoes_done", removed_total=len(removed_shoes), new_total=new_shoe_count)
    # Returning stats keeps the final run status truthful; otherwise a healthy run that
    # found new shoes looked like it found zero new items in machine-readable status.
    return LystProcessingStats(new_total=new_shoe_count, removed_total=len(removed_shoes))
