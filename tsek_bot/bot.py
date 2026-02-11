import logging
import math
import random
import re
import sys
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Tuple

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    ConversationHandler,
    filters,
)
from config import TELEGRAM_TSEK_BOT_TOKEN

try:
    from tsek_bot.image_renderer import render_schedule_image
except ImportError:
    from image_renderer import render_schedule_image

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("tsek_schedule_bot")
logging.getLogger("httpx").setLevel(logging.WARNING)

MINUTES_PER_DAY = 24 * 60
GROUPS = [f"{i}.{j}" for i in range(1, 7) for j in (1, 2)]
GROUP_RE = re.compile(r"Черга\s+([1-6]\.[12])", re.IGNORECASE)
TIME_RE = re.compile(r"(\d{1,2}:\d{2})\s*[-–]\s*(\d{1,2}:\d{2})")

SHOW_LIGHT_WINDOWS_ON_IMAGE = True


@dataclass(frozen=True)
class Rule:
    queues: float
    light_hours: float
    dark_hours: float
    min_break_hours: float | None
    max_consecutive_off_hours: float | None
    max_on_windows: int | None = None


RULES: Dict[float, Rule] = {
    6.0: Rule(queues=6.0, light_hours=0, dark_hours=24, min_break_hours=0, max_consecutive_off_hours=None),
    5.5: Rule(queues=5.5, light_hours=2, dark_hours=22, min_break_hours=1, max_consecutive_off_hours=11, max_on_windows=2),
    5.0: Rule(queues=5.0, light_hours=4, dark_hours=20, min_break_hours=2, max_consecutive_off_hours=10, max_on_windows=2),
    4.5: Rule(queues=4.5, light_hours=6, dark_hours=18, min_break_hours=2, max_consecutive_off_hours=9, max_on_windows=3),
    4.0: Rule(queues=4.0, light_hours=8, dark_hours=16, min_break_hours=2, max_consecutive_off_hours=8, max_on_windows=3),
    3.5: Rule(queues=3.5, light_hours=10, dark_hours=14, min_break_hours=4, max_consecutive_off_hours=7, max_on_windows=3),
    3.0: Rule(queues=3.0, light_hours=12, dark_hours=12, min_break_hours=4, max_consecutive_off_hours=6, max_on_windows=3),
    2.5: Rule(queues=2.5, light_hours=14, dark_hours=10, min_break_hours=4, max_consecutive_off_hours=5, max_on_windows=4),
    2.0: Rule(queues=2.0, light_hours=16, dark_hours=8, min_break_hours=4, max_consecutive_off_hours=4, max_on_windows=3),
    1.5: Rule(queues=1.5, light_hours=18, dark_hours=6, min_break_hours=None, max_consecutive_off_hours=3, max_on_windows=2),
    1.0: Rule(queues=1.0, light_hours=20, dark_hours=4, min_break_hours=None, max_consecutive_off_hours=2, max_on_windows=2),
}

VALID_QUEUES = sorted(RULES.keys())

GEN_RULE, YEST_RULE, YEST_SCHEDULE = range(3)


def format_queue_value(value: float) -> str:
    return str(value).rstrip("0").rstrip(".")


def min_windows_needed_for_rule(q: float) -> int:
    rule = RULES[q]
    min_window_minutes = min_light_window_minutes(q)
    if rule.light_hours <= 0 or min_window_minutes <= 0:
        return 0
    min_window_hours = min_window_minutes / 60
    return int(math.ceil(rule.light_hours / min_window_hours))


def humanize_error(
    exc: Exception,
    *,
    q: float | None = None,
    from_yesterday: bool = False,
) -> str:
    msg = str(exc)
    schedule_error_msgs = {
        "Invalid slot size for the configured light window.",
        "Invalid block size.",
        "Cannot distribute light evenly with these settings.",
        "Cannot build schedule with equal light windows.",
        "Неможливо побудувати розклад з цим max_on_windows.",
        "Перевищено max_on_windows.",
        "Max on-windows менше за мінімально дозволені 2 вікна.",
        "Неможливо забезпечити мінімум 2 вікна світла.",
        "Не вдалося побудувати розклад.",
    }
    if msg in schedule_error_msgs and q is not None and q in RULES:
        rule = RULES[q]
        min_window_minutes = min_light_window_minutes(q)
        if (
            rule.max_on_windows is not None
            and min_window_minutes > 0
            and min_windows_needed_for_rule(q) > rule.max_on_windows
        ):
            min_windows_needed = min_windows_needed_for_rule(q)
            min_window_hours = int(min_window_minutes // 60)
            light_hours = (
                int(rule.light_hours)
                if float(rule.light_hours).is_integer()
                else rule.light_hours
            )
            return (
                "Не виходить скласти рівний графік для значення "
                f"{format_queue_value(q)}. За правилом світло має бути "
                f"{light_hours} год на добу, і кожне увімкнення повинно тривати "
                f"не менше {min_window_hours} год. Тому потрібно щонайменше "
                f"{min_windows_needed} увімкнення, а правило дозволяє лише "
                f"{rule.max_on_windows}. Через це рівний графік скласти неможливо."
            )

    if msg in (
        "Invalid time format.",
        "Invalid time value.",
    ):
        return (
            "Не можу прочитати час. Пишіть так: 08:00 - 12:00. "
            "Хвилини можуть бути лише 00 або 30."
        )

    if msg == "No valid intervals found in yesterday schedule.":
        return (
            "У повідомленні не знайшла жодного діапазону часу. "
            "Надішліть розклад у форматі, як бот надсилає."
        )

    if msg in (
        "Invalid slot size for the configured light window.",
        "Invalid block size.",
    ):
        return (
            "Не можу скласти графік з таким кроком часу. "
            "Спробуйте інше значення або інший розклад."
        )

    if msg == "Cannot distribute light evenly with these settings.":
        return (
            "Не виходить поділити світло порівну між усіма чергами "
            "за цими правилами."
        )

    if msg in (
        "Cannot build schedule with equal light windows.",
        "Неможливо побудувати розклад з цим max_on_windows.",
        "Перевищено max_on_windows.",
        "Max on-windows менше за мінімально дозволені 2 вікна.",
        "Неможливо забезпечити мінімум 2 вікна світла.",
    ):
        if from_yesterday:
            return (
                "Не вийшло скласти новий графік на основі вчорашнього. "
                "У цьому розкладі не можна рівно розподілити світло між усіма "
                "чергами так, щоб виконати всі правила. Спробуйте інший "
                "вчорашній розклад або інше значення."
            )
        return (
            "Не вийшло скласти рівний графік за цими правилами. "
            "Спробуйте інше значення."
        )

    if msg in (
        "Перевищено max_consecutive_off_hours.",
    ):
        if q is not None and q in RULES and RULES[q].max_consecutive_off_hours:
            max_hours = RULES[q].max_consecutive_off_hours
            return (
                "Виходить занадто довге відключення для однієї черги. "
                f"За правилом максимум {max_hours} год поспіль."
            )
        return "Виходить занадто довге відключення для однієї черги."

    if msg == "Не вдалося побудувати розклад.":
        return (
            "На жаль, не вдалося скласти графік. "
            "Спробуйте інше значення або інший розклад."
        )

    return (
        "На жаль, не вдалося скласти графік. "
        "Спробуйте інше значення або інший розклад."
    )


def example_yesterday_format_message() -> str:
    return (
        "*Приклад правильного формату:*\n"
        "Графік відключень:\n"
        "🔹 Черга 1.1\n"
        "00:00 - 04:00\n"
        "06:00 - 10:00\n"
        "\n"
        "🔹 Черга 1.2\n"
        "02:00 - 06:00\n"
        "08:00 - 12:00\n"
        "\n"
        "🔹 Черга 2.1\n"
        "04:00 - 08:00\n"
        "10:00 - 14:00\n"
        "\n"
        "_Можна надсилати текст із повідомлення, яке переслане з каналу. "
        "Головне, щоб були рядки з 'Черга X.Y' і час у форматі 00:00 - 00:00 "
        "(хвилини тільки 00 або 30)._"
    )


def normalize_queue_value(raw: str) -> float | None:
    try:
        value = float(raw.replace(",", "."))
    except ValueError:
        return None
    rounded = round(value * 2) / 2
    if abs(rounded - value) > 1e-6:
        return None
    if rounded not in RULES:
        return None
    return rounded


def off_queue_count(q: float) -> int:
    return int(round(q * 2))


def on_queue_count(q: float) -> int:
    return max(0, len(GROUPS) - off_queue_count(q))


def min_light_window_minutes(q: float) -> int:
    if q >= 6.0:
        return 0
    if q >= 5.5:
        return 60
    return 120


def rotate_groups(start_group: str) -> List[str]:
    if start_group not in GROUPS:
        return GROUPS[:]
    idx = GROUPS.index(start_group)
    return GROUPS[idx:] + GROUPS[:idx]


def off_before_day_end_minutes(off_intervals: List[Tuple[int, int]]) -> int:
    intervals = normalize_for_display(off_intervals)
    if intervals and intervals[-1][1] == MINUTES_PER_DAY:
        return intervals[-1][1] - intervals[-1][0]
    return 0


def choose_start_group_from_yesterday(
    schedule: Dict[str, List[Tuple[int, int]]]
) -> str:
    order = rotate_groups("6.1")
    order_index = {group: idx for idx, group in enumerate(order)}

    best_group = order[0]
    best_duration = -1
    for group in GROUPS:
        duration = off_before_day_end_minutes(schedule.get(group, []))
        if duration > best_duration:
            best_duration = duration
            best_group = group
        elif duration == best_duration and order_index[group] < order_index[best_group]:
            best_group = group
    return best_group


def slot_to_time(minutes: int) -> str:
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours:02d}:{mins:02d}"


def build_pattern(
    *,
    light_hours: float,
    dark_hours: float,
    min_break_hours: float,
    max_consecutive_off_hours: float | None,
    rng: random.Random,
) -> List[Tuple[str, int]]:
    light_hours_int = int(round(light_hours))
    dark_hours_int = int(round(dark_hours))
    if light_hours_int + dark_hours_int != 24:
        dark_hours_int = 24 - light_hours_int

    max_consecutive_hours = (
        int(round(max_consecutive_off_hours))
        if max_consecutive_off_hours is not None
        else None
    )
    min_break_hours_int = int(round(min_break_hours))

    if light_hours_int == 0:
        return [("off", dark_hours_int * 60)]

    if max_consecutive_hours:
        off_blocks = (dark_hours_int + max_consecutive_hours - 1) // max_consecutive_hours
    else:
        off_blocks = 1

    light_blocks = max(1, off_blocks - 1)

    min_light_needed = light_blocks * min_break_hours_int
    if light_hours_int < min_light_needed:
        raise ValueError(
            f"Недостатньо годин світла: мінімальна перерва {min_break_hours_int} годин, "
            f"але всього світла {light_hours_int} годин."
        )

    # Distribute light hours across light blocks.
    light_lengths = [min_break_hours_int for _ in range(light_blocks)]
    remaining = light_hours_int - min_light_needed
    for _ in range(remaining):
        light_lengths[rng.randrange(light_blocks)] += 1

    # Distribute dark hours across off blocks, capped by max consecutive if present.
    off_lengths: List[int] = []
    if max_consecutive_hours:
        base = dark_hours_int // off_blocks
        rem = dark_hours_int % off_blocks
        for i in range(off_blocks):
            length = base + (1 if i < rem else 0)
            if length > max_consecutive_hours:
                length = max_consecutive_hours
            off_lengths.append(length)
    else:
        off_lengths = [dark_hours_int]

    rng.shuffle(off_lengths)
    if light_blocks > 1:
        rng.shuffle(light_lengths)

    pattern: List[Tuple[str, int]] = []
    for i in range(off_blocks):
        if off_lengths[i] > 0:
            pattern.append(("off", off_lengths[i] * 60))
        if i < light_blocks and light_lengths[i] > 0:
            pattern.append(("light", light_lengths[i] * 60))

    total = sum(length for _, length in pattern)
    if total != MINUTES_PER_DAY:
        diff = MINUTES_PER_DAY - total
        if pattern:
            kind, length = pattern[-1]
            pattern[-1] = (kind, length + diff)
    return pattern


def pattern_to_off_intervals(pattern: List[Tuple[str, int]]) -> List[Tuple[int, int]]:
    intervals: List[Tuple[int, int]] = []
    t = 0
    for kind, length in pattern:
        if length <= 0:
            continue
        if kind == "off":
            intervals.append((t, t + length))
        t += length
    return intervals


def shift_intervals(intervals: List[Tuple[int, int]], offset: int) -> List[Tuple[int, int]]:
    shifted: List[Tuple[int, int]] = []
    for start, end in intervals:
        length = end - start
        if length <= 0:
            continue
        new_start = start + offset
        new_end = new_start + length
        shifted.append((new_start, new_end))
    return normalize_intervals(shifted)


def normalize_intervals(intervals: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    cleaned = []
    for start, end in intervals:
        if end <= start:
            continue
        cleaned.append((start, end))
    cleaned.sort(key=lambda x: x[0])
    merged: List[Tuple[int, int]] = []
    for start, end in cleaned:
        if not merged:
            merged.append((start, end))
            continue
        last_start, last_end = merged[-1]
        if start <= last_end:
            merged[-1] = (last_start, max(last_end, end))
        else:
            merged.append((start, end))
    return merged


def max_consecutive_for_offset(
    base_off_intervals: List[Tuple[int, int]],
    offset: int,
) -> int:
    intervals = shift_intervals(base_off_intervals, offset)
    intervals = normalize_for_display(intervals)
    if not intervals:
        return 0
    return max(end - start for start, end in intervals)


def build_constant_schedule(
    *,
    q: float,
    slot_minutes: int,
    start_group: str,
    anchor_offset: int | None,
    initial_age_by_group: Dict[str, int] | None = None,
    priority_minutes_by_group: Dict[str, int] | None = None,
    max_on_windows: int | None = None,
    max_consecutive_off_hours: float | None = None,
    rng: random.Random,
) -> Dict[str, List[Tuple[int, int]]]:
    on_count = on_queue_count(q)
    if on_count <= 0:
        return {group: [(0, MINUTES_PER_DAY)] for group in GROUPS}

    block_minutes = min_light_window_minutes(q)
    if block_minutes <= 0:
        return {group: [(0, MINUTES_PER_DAY)] for group in GROUPS}

    if block_minutes % slot_minutes != 0:
        raise ValueError("Invalid slot size for the configured light window.")

    blocks_per_day = MINUTES_PER_DAY // block_minutes
    if blocks_per_day <= 0:
        raise ValueError("Invalid block size.")

    if (on_count * blocks_per_day) % len(GROUPS) != 0:
        raise ValueError("Cannot distribute light evenly with these settings.")

    blocks_per_group = (on_count * blocks_per_day) // len(GROUPS)
    min_windows = 0 if q >= 6.0 else 2
    if max_on_windows is not None and max_on_windows < min_windows:
        raise ValueError("Max on-windows менше за мінімально дозволені 2 вікна.")
    max_windows = max_on_windows if max_on_windows is not None else blocks_per_group
    max_off_blocks = None
    if max_consecutive_off_hours is not None:
        max_off_blocks = int((max_consecutive_off_hours * 60) // block_minutes)
        if max_off_blocks < 0:
            max_off_blocks = None
    ordered_groups = rotate_groups(start_group)
    order_index = {group: idx for idx, group in enumerate(ordered_groups)}

    if anchor_offset is None:
        anchor_offset = rng.randrange(0, block_minutes, slot_minutes)
    anchor_offset = anchor_offset % block_minutes
    if min_windows > 0 and anchor_offset != 0:
        anchor_offset = 0

    last_on: Dict[str, int] = {}
    off_run: Dict[str, int] = {group: 0 for group in GROUPS}
    if initial_age_by_group:
        for group in GROUPS:
            age = initial_age_by_group.get(group, 0)
            last_on[group] = -age
    else:
        for group in GROUPS:
            age = len(GROUPS) - order_index[group]
            last_on[group] = -age

    if on_count == 1:
        if priority_minutes_by_group is None:
            priority_minutes_by_group = {group: 0 for group in GROUPS}
        priority_order = rotate_groups("6.1")
        priority_index = {group: idx for idx, group in enumerate(priority_order)}
        base_order = sorted(
            GROUPS,
            key=lambda g: (-priority_minutes_by_group.get(g, 0), priority_index[g]),
        )
        if base_order[0] != start_group:
            start_idx = base_order.index(start_group) if start_group in base_order else 0
            base_order = base_order[start_idx:] + base_order[:start_idx]

        block_on_groups: List[List[str]] = []
        for _ in range(blocks_per_group):
            for group in base_order:
                block_on_groups.append([group])

        on_intervals: Dict[str, List[Tuple[int, int]]] = {group: [] for group in GROUPS}
        for block_idx, groups in enumerate(block_on_groups):
            start = (anchor_offset + block_idx * block_minutes) % MINUTES_PER_DAY
            end = start + block_minutes
            for group in groups:
                if end <= MINUTES_PER_DAY:
                    on_intervals[group].append((start, end))
                else:
                    on_intervals[group].append((start, MINUTES_PER_DAY))
                    on_intervals[group].append((0, end - MINUTES_PER_DAY))

        schedule: Dict[str, List[Tuple[int, int]]] = {}
        for group in GROUPS:
            on_times = normalize_intervals(on_intervals[group])
            off_times: List[Tuple[int, int]] = []
            cursor = 0
            for start, end in on_times:
                if start > cursor:
                    off_times.append((cursor, start))
                cursor = end
            if cursor < MINUTES_PER_DAY:
                off_times.append((cursor, MINUTES_PER_DAY))
            schedule[group] = off_times

        for group in GROUPS:
            if max_on_windows is not None and blocks_per_group > max_on_windows:
                raise ValueError("Перевищено max_on_windows.")
        if max_consecutive_off_hours is not None:
            max_off = max(end - start for group in GROUPS for start, end in schedule[group])
            if max_off > max_consecutive_off_hours * 60:
                raise ValueError("Перевищено max_consecutive_off_hours.")
        return schedule

    remaining: Dict[str, int] = {group: blocks_per_group for group in GROUPS}
    run_len: Dict[str, int] = {group: 0 for group in GROUPS}
    windows_used: Dict[str, int] = {group: 0 for group in GROUPS}
    block_on_groups: List[List[str]] = []

    for block_idx in range(blocks_per_day):
        blocks_left = blocks_per_day - block_idx
        mandatory = [group for group in GROUPS if remaining[group] == blocks_left]
        mandatory.sort(
            key=lambda g: (
                run_len[g] == 0,
                -run_len[g],
                -(block_idx - last_on[g]),
                order_index[g],
            )
        )

        selected: List[str] = []
        if block_idx == 0 and remaining.get(start_group, 0) > 0:
            selected.append(start_group)
        if max_off_blocks is not None:
            must_on = [
                group
                for group in GROUPS
                if remaining[group] > 0 and off_run.get(group, 0) >= max_off_blocks
            ]
            must_on.sort(key=lambda g: (-(block_idx - last_on[g]), order_index[g]))
            for group in must_on:
                if group not in selected:
                    selected.append(group)
        for group in mandatory:
            if run_len[group] == 0 and windows_used[group] >= max_windows:
                raise ValueError("Неможливо побудувати розклад з цим max_on_windows.")
            if group not in selected:
                selected.append(group)

        if len(selected) > on_count:
            selected = selected[:on_count]

        candidates = [
            group for group in GROUPS if remaining[group] > 0 and group not in selected
        ]
        must_start = [
            group
            for group in candidates
            if run_len[group] == 0
            and windows_used[group] < min_windows
            and windows_used[group] < max_windows
        ]
        must_start.sort(key=lambda g: (-(block_idx - last_on[g]), order_index[g]))

        continuing = [group for group in candidates if run_len[group] > 0]
        continuing.sort(
            key=lambda g: (
                -run_len[g],
                -(block_idx - last_on[g]),
                order_index[g],
            )
        )

        starters = [
            group
            for group in candidates
            if run_len[group] == 0
            and group not in must_start
            and windows_used[group] < max_windows
        ]
        starters.sort(key=lambda g: (-(block_idx - last_on[g]), order_index[g]))
        needed = on_count - len(selected)
        if needed > 0:
            for group in must_start:
                if needed <= 0:
                    break
                selected.append(group)
                needed -= 1
            for group in continuing:
                if needed <= 0:
                    break
                if group not in selected:
                    selected.append(group)
                    needed -= 1
            for group in starters:
                if needed <= 0:
                    break
                if group not in selected:
                    selected.append(group)
                    needed -= 1

        if len(selected) != on_count:
            raise ValueError("Cannot build schedule with equal light windows.")

        selected_set = set(selected)
        for group in GROUPS:
            if group in selected_set:
                if run_len[group] == 0:
                    windows_used[group] += 1
                remaining[group] -= 1
                last_on[group] = block_idx
                run_len[group] = run_len[group] + 1
                off_run[group] = 0
            else:
                run_len[group] = 0
                off_run[group] = off_run.get(group, 0) + 1
                if max_off_blocks is not None and off_run[group] > max_off_blocks:
                    raise ValueError("Перевищено max_consecutive_off_hours.")

        block_on_groups.append(selected)

    on_intervals: Dict[str, List[Tuple[int, int]]] = {group: [] for group in GROUPS}
    for block_idx, groups in enumerate(block_on_groups):
        start = (anchor_offset + block_idx * block_minutes) % MINUTES_PER_DAY
        end = start + block_minutes
        for group in groups:
            if end <= MINUTES_PER_DAY:
                on_intervals[group].append((start, end))
            else:
                on_intervals[group].append((start, MINUTES_PER_DAY))
                on_intervals[group].append((0, end - MINUTES_PER_DAY))

    schedule: Dict[str, List[Tuple[int, int]]] = {}
    for group in GROUPS:
        on_times = normalize_intervals(on_intervals[group])
        off_times: List[Tuple[int, int]] = []
        cursor = 0
        for start, end in on_times:
            if start > cursor:
                off_times.append((cursor, start))
            cursor = end
        if cursor < MINUTES_PER_DAY:
            off_times.append((cursor, MINUTES_PER_DAY))
        schedule[group] = off_times

    for group in GROUPS:
        if windows_used[group] < min_windows:
            raise ValueError("Неможливо забезпечити мінімум 2 вікна світла.")
        if max_on_windows is not None and windows_used[group] > max_windows:
            raise ValueError("Перевищено max_on_windows.")

    return schedule


def build_schedule(
    *,
    q: float,
    slot_minutes: int,
    rng: random.Random | None = None,
) -> Dict[str, List[Tuple[int, int]]]:
    rng = rng or random.Random()
    rule = RULES[q]
    last_error: Exception | None = None
    for _ in range(20):
        start_group = rng.choice(GROUPS)
        try:
            return build_constant_schedule(
                q=q,
                slot_minutes=slot_minutes,
                start_group=start_group,
                anchor_offset=None,
                max_on_windows=rule.max_on_windows,
                max_consecutive_off_hours=rule.max_consecutive_off_hours,
                rng=rng,
            )
        except ValueError as exc:
            last_error = exc
            continue
    if last_error:
        raise last_error
    raise ValueError("Не вдалося побудувати розклад.")


def normalize_for_display(intervals: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    # Split any cross-midnight intervals into same-day segments [0, 24)
    normalized: List[Tuple[int, int]] = []
    for start, end in intervals:
        if end <= start:
            continue
        t = start
        while t < end:
            day_start = (t // MINUTES_PER_DAY) * MINUTES_PER_DAY
            seg_start = t
            seg_end = min(end, day_start + MINUTES_PER_DAY)
            normalized.append((seg_start - day_start, seg_end - day_start))
            t = seg_end

    normalized.sort(key=lambda x: x[0])
    merged: List[Tuple[int, int]] = []
    for start, end in normalized:
        if not merged:
            merged.append((start, end))
            continue
        last_start, last_end = merged[-1]
        if start <= last_end:
            merged[-1] = (last_start, max(last_end, end))
        else:
            merged.append((start, end))
    return merged


def format_interval(start: int, end: int) -> str:
    start_mod = start % MINUTES_PER_DAY
    end_mod = end % MINUTES_PER_DAY
    start_text = slot_to_time(start_mod)
    end_text = "00:00" if end_mod == 0 else slot_to_time(end_mod)
    return f"{start_text} - {end_text}"


def parse_time_to_minutes(token: str) -> int:
    parts = token.split(":")
    if len(parts) != 2:
        raise ValueError("Invalid time format.")
    hour = int(parts[0])
    minute = int(parts[1])
    if hour == 24 and minute == 0:
        return MINUTES_PER_DAY
    if hour < 0 or hour > 23 or minute not in (0, 30):
        raise ValueError("Invalid time value.")
    return hour * 60 + minute


def parse_yesterday_schedule(lines: List[str]) -> Tuple[Dict[str, List[Tuple[int, int]]], int]:
    schedule: Dict[str, List[Tuple[int, int]]] = {group: [] for group in GROUPS}
    current_group: str | None = None
    has_half_hour = False

    for line in lines:
        match_group = GROUP_RE.search(line)
        if match_group:
            current_group = match_group.group(1)
            continue

        match_time = TIME_RE.search(line)
        if match_time and current_group:
            start_raw, end_raw = match_time.groups()
            start = parse_time_to_minutes(start_raw)
            end = parse_time_to_minutes(end_raw)
            if start % 60 or end % 60:
                has_half_hour = True
            if end <= start:
                end += MINUTES_PER_DAY
            schedule[current_group].append((start, end))

    slot_minutes = 30 if has_half_hour else 60
    if not any(schedule[group] for group in GROUPS):
        raise ValueError("No valid intervals found in yesterday schedule.")
    return schedule, slot_minutes


def choose_offset_for_yesterday(
    schedule: Dict[str, List[Tuple[int, int]]],
    slot_minutes: int,
    max_consecutive_off_hours: float | None,
    rng: random.Random,
) -> int:
    max_allowed = (
        int(round(max_consecutive_off_hours * 60))
        if max_consecutive_off_hours is not None
        else None
    )
    offsets = list(range(0, MINUTES_PER_DAY, slot_minutes))
    offset_scores: List[Tuple[int, int]] = []
    for offset in offsets:
        max_len = 0
        for intervals in schedule.values():
            if not intervals:
                continue
            shifted = shift_intervals(intervals, offset)
            display = normalize_for_display(shifted)
            for start, end in display:
                max_len = max(max_len, end - start)
        offset_scores.append((offset, max_len))

    if max_allowed is None:
        non_zero = [offset for offset, _ in offset_scores if offset != 0]
        return rng.choice(non_zero) if non_zero else 0

    valid = [offset for offset, max_len in offset_scores if max_len <= max_allowed]
    non_zero_valid = [offset for offset in valid if offset != 0]
    if non_zero_valid:
        return rng.choice(non_zero_valid)
    if valid:
        return rng.choice(valid)

    best_max = min(max_len for _, max_len in offset_scores)
    best_offsets = [offset for offset, max_len in offset_scores if max_len == best_max]
    non_zero_best = [offset for offset in best_offsets if offset != 0]
    chosen = rng.choice(non_zero_best) if non_zero_best else rng.choice(best_offsets)
    logger.warning(
        "Yesterday schedule cannot satisfy max consecutive off-hours constraint; "
        "using best available offset (max=%s, allowed=%s).",
        best_max / 60,
        max_allowed / 60,
    )
    return chosen


def max_consecutive_in_schedule(schedule: Dict[str, List[Tuple[int, int]]]) -> int:
    max_len = 0
    for intervals in schedule.values():
        display = normalize_for_display(intervals)
        for start, end in display:
            max_len = max(max_len, end - start)
    return max_len


def max_interval_example(
    schedule: Dict[str, List[Tuple[int, int]]]
) -> Tuple[int, str | None]:
    best_len = 0
    best_example = None
    for group in GROUPS:
        intervals = normalize_for_display(schedule.get(group, []))
        for start, end in intervals:
            length = end - start
            if length > best_len:
                best_len = length
                best_example = f"Черга {group}: {format_interval(start, end)}"
    return best_len, best_example


def extract_light_windows(off_intervals: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    off = normalize_for_display(off_intervals)
    lights: List[Tuple[int, int]] = []
    cursor = 0
    for start, end in off:
        if start > cursor:
            lights.append((cursor, start))
        cursor = end
    if cursor < MINUTES_PER_DAY:
        lights.append((cursor, MINUTES_PER_DAY))
    return lights


def allocate_slots_proportional(
    lengths: List[int],
    total_slots: int,
    min_slot: int,
) -> List[int]:
    if not lengths:
        return []
    count = len(lengths)
    if total_slots <= 0:
        return [0] * count

    if total_slots < count * min_slot:
        order = sorted(range(count), key=lambda i: lengths[i], reverse=True)
        slots = [0] * count
        for i in range(total_slots):
            slots[order[i]] = min_slot
        return slots

    slots = [min_slot] * count
    remaining = total_slots - count * min_slot
    weights = [max(1, length) for length in lengths]
    total_weight = sum(weights)
    if total_weight <= 0:
        return slots

    fractional = []
    for i, weight in enumerate(weights):
        share = remaining * weight / total_weight
        add = int(share)
        slots[i] += add
        fractional.append((share - add, i))

    leftover = total_slots - sum(slots)
    fractional.sort(reverse=True)
    for i in range(leftover):
        slots[fractional[i][1]] += 1

    return slots


def place_light_windows(
    centers: List[float],
    lengths: List[int],
    slots_per_day: int,
) -> List[int]:
    starts = []
    for center, length in zip(centers, lengths):
        start = int(round(center - length / 2))
        starts.append(start)

    # Forward pass to prevent overlap and keep within bounds.
    min_start = 0
    for i, length in enumerate(lengths):
        start = max(starts[i], min_start)
        if start + length > slots_per_day:
            start = slots_per_day - length
        if start < 0:
            start = 0
        starts[i] = start
        min_start = start + length

    # Backward pass to keep within day end.
    max_end = slots_per_day
    for i in range(len(lengths) - 1, -1, -1):
        length = lengths[i]
        end = starts[i] + length
        if end > max_end:
            start = max_end - length
            if start < 0:
                start = 0
            starts[i] = start
        max_end = starts[i]

    # Final forward pass for safety.
    min_start = 0
    for i, length in enumerate(lengths):
        start = max(starts[i], min_start)
        starts[i] = start
        min_start = start + length

    return starts


def adjust_off_intervals_to_rule(
    off_intervals: List[Tuple[int, int]],
    slot_minutes: int,
    light_hours: float,
) -> List[Tuple[int, int]]:
    target_light_minutes = int(round(light_hours * 60))
    total_light_slots = max(0, target_light_minutes // slot_minutes)
    if not off_intervals:
        return []

    light_windows = extract_light_windows(off_intervals)
    if not light_windows:
        if total_light_slots <= 0:
            return [(0, MINUTES_PER_DAY)]
        light_windows = [(0, min(MINUTES_PER_DAY, total_light_slots * slot_minutes))]

    lengths_slots = [
        max(0, (end - start) // slot_minutes) for start, end in light_windows
    ]
    slots = allocate_slots_proportional(lengths_slots, total_light_slots, 1)

    centers = [((start + end) / 2) / slot_minutes for start, end in light_windows]
    lengths = [slots_count for slots_count in slots if slots_count > 0]
    centers = [center for center, slots_count in zip(centers, slots) if slots_count > 0]

    if not lengths:
        return [(0, MINUTES_PER_DAY)]

    slots_per_day = MINUTES_PER_DAY // slot_minutes
    starts_slots = place_light_windows(centers, lengths, slots_per_day)

    new_lights: List[Tuple[int, int]] = []
    for start_slot, length_slots in zip(starts_slots, lengths):
        start = start_slot * slot_minutes
        end = start + length_slots * slot_minutes
        if end > start:
            new_lights.append((start, end))

    new_lights.sort(key=lambda x: x[0])
    new_off: List[Tuple[int, int]] = []
    cursor = 0
    for start, end in new_lights:
        if start > cursor:
            new_off.append((cursor, start))
        cursor = end
    if cursor < MINUTES_PER_DAY:
        new_off.append((cursor, MINUTES_PER_DAY))

    return new_off


def build_even_base_off_intervals(
    *,
    light_hours: float,
    light_window_count: int,
    slot_minutes: int,
    anchor_start: int | None,
    rng: random.Random,
) -> List[Tuple[int, int]]:
    total_light_minutes = int(round(light_hours * 60))
    slots_per_day = MINUTES_PER_DAY // slot_minutes
    total_light_slots = max(0, total_light_minutes // slot_minutes)

    if light_window_count <= 0:
        light_window_count = 1

    base_len = total_light_slots // light_window_count
    remainder = total_light_slots % light_window_count
    lengths = [
        base_len + (1 if i < remainder else 0) for i in range(light_window_count)
    ]

    if all(length == 0 for length in lengths):
        return [(0, MINUTES_PER_DAY)]

    spacing = slots_per_day / light_window_count
    starts_slots: List[int] = []
    for i, length in enumerate(lengths):
        bucket_start = int(round(i * spacing))
        bucket_end = int(round((i + 1) * spacing))
        if length <= 0:
            starts_slots.append(bucket_start)
            continue
        available = max(0, (bucket_end - bucket_start) - length)
        start = bucket_start + available // 2
        starts_slots.append(start)

    if anchor_start is not None:
        anchor_slot = (anchor_start // slot_minutes) % slots_per_day
        shift_slots = (anchor_slot - starts_slots[0]) % slots_per_day
    else:
        shift_slots = rng.randrange(0, slots_per_day) if slots_per_day > 0 else 0

    lights: List[Tuple[int, int]] = []
    for start_slot, length in zip(starts_slots, lengths):
        if length <= 0:
            continue
        start = ((start_slot + shift_slots) % slots_per_day) * slot_minutes
        end = start + length * slot_minutes
        if end <= MINUTES_PER_DAY:
            lights.append((start, end))
        else:
            lights.append((start, MINUTES_PER_DAY))
            lights.append((0, end - MINUTES_PER_DAY))

    lights = normalize_intervals(lights)
    off: List[Tuple[int, int]] = []
    cursor = 0
    for start, end in lights:
        if start > cursor:
            off.append((cursor, start))
        cursor = end
    if cursor < MINUTES_PER_DAY:
        off.append((cursor, MINUTES_PER_DAY))
    return off


def build_rule_based_off_intervals(
    *,
    light_hours: float,
    dark_hours: float,
    max_consecutive_off_hours: float | None,
    slot_minutes: int,
    min_light_window_hours: float | None,
    anchor_start: int | None,
    rng: random.Random,
) -> List[Tuple[int, int]]:
    slots_per_day = MINUTES_PER_DAY // slot_minutes
    light_slots = int(round(light_hours * 60 / slot_minutes))
    dark_slots = slots_per_day - light_slots
    if light_slots <= 0:
        return [(0, MINUTES_PER_DAY)]

    max_off_slots = (
        int(round(max_consecutive_off_hours * 60 / slot_minutes))
        if max_consecutive_off_hours is not None
        else dark_slots
    )
    if max_off_slots <= 0:
        max_off_slots = dark_slots

    off_blocks = (dark_slots + max_off_slots - 1) // max_off_slots
    if off_blocks <= 0:
        off_blocks = 1
    light_blocks = max(1, off_blocks - 1)

    base_light = light_slots // light_blocks
    rem_light = light_slots % light_blocks
    light_lengths = [base_light + (1 if i < rem_light else 0) for i in range(light_blocks)]

    min_light_slots = 0
    if min_light_window_hours is not None and min_light_window_hours > 0:
        min_light_slots = int(round(min_light_window_hours * 60 / slot_minutes))

    if min_light_slots > 0 and light_slots >= min_light_slots * light_blocks:
        remaining = light_slots - min_light_slots * light_blocks
        light_lengths = [min_light_slots for _ in range(light_blocks)]
        for i in range(remaining):
            light_lengths[i % light_blocks] += 1

    base_off = dark_slots // off_blocks
    rem_off = dark_slots % off_blocks
    off_lengths = [base_off + (1 if i < rem_off else 0) for i in range(off_blocks)]

    pattern: List[Tuple[str, int]] = []
    for i in range(off_blocks):
        if off_lengths[i] > 0:
            pattern.append(("off", off_lengths[i] * slot_minutes))
        if i < light_blocks and light_lengths[i] > 0:
            pattern.append(("light", light_lengths[i] * slot_minutes))

    base_off_intervals = pattern_to_off_intervals(pattern)
    if anchor_start is None:
        return base_off_intervals

    first_light = None
    t = 0
    for kind, length in pattern:
        if kind == "light":
            first_light = t
            break
        t += length
    if first_light is None:
        return base_off_intervals

    anchor = int(round(anchor_start / slot_minutes)) * slot_minutes % MINUTES_PER_DAY
    shift = (anchor - first_light) % MINUTES_PER_DAY
    return shift_intervals(base_off_intervals, shift)


def adjust_schedule_to_rule(
    schedule: Dict[str, List[Tuple[int, int]]],
    slot_minutes: int,
    light_hours: float,
) -> Dict[str, List[Tuple[int, int]]]:
    new_schedule: Dict[str, List[Tuple[int, int]]] = {}
    for group in GROUPS:
        new_schedule[group] = adjust_off_intervals_to_rule(
            schedule.get(group, []),
            slot_minutes=slot_minutes,
            light_hours=light_hours,
        )
    return new_schedule


def build_display_intervals(
    off_intervals: List[Tuple[int, int]],
    include_light: bool,
) -> List[Tuple[str, int, int]]:
    off = normalize_for_display(off_intervals)
    if not include_light:
        return [("off", start, end) for start, end in off]

    items: List[Tuple[str, int, int]] = []
    cursor = 0
    for start, end in off:
        if start > cursor:
            items.append(("light", cursor, start))
        items.append(("off", start, end))
        cursor = end
    if cursor < MINUTES_PER_DAY:
        items.append(("light", cursor, MINUTES_PER_DAY))
    return items


def first_light_start(off_intervals: List[Tuple[int, int]]) -> int | None:
    lights = extract_light_windows(off_intervals)
    if not lights:
        return None
    return lights[0][0]


def build_on_counts(
    schedule: Dict[str, List[Tuple[int, int]]],
    slot_minutes: int,
) -> List[Tuple[int, int, int]]:
    slots_per_day = MINUTES_PER_DAY // slot_minutes
    counts = [0] * slots_per_day
    for group in GROUPS:
        off_intervals = schedule.get(group, [])
        light_intervals = extract_light_windows(off_intervals)
        for start, end in light_intervals:
            start_slot = start // slot_minutes
            end_slot = end // slot_minutes
            for slot in range(start_slot, end_slot):
                if 0 <= slot < slots_per_day:
                    counts[slot] += 1

    result: List[Tuple[int, int, int]] = []
    for slot, count in enumerate(counts):
        start = slot * slot_minutes
        end = start + slot_minutes
        result.append((start, end, count))
    return result


def build_on_groups(
    schedule: Dict[str, List[Tuple[int, int]]],
    slot_minutes: int,
) -> List[Tuple[int, int, List[str]]]:
    slots_per_day = MINUTES_PER_DAY // slot_minutes
    groups_by_slot: List[List[str]] = [[] for _ in range(slots_per_day)]

    for group in GROUPS:
        off_intervals = schedule.get(group, [])
        light_intervals = extract_light_windows(off_intervals)
        for start, end in light_intervals:
            start_slot = start // slot_minutes
            end_slot = end // slot_minutes
            for slot in range(start_slot, end_slot):
                if 0 <= slot < slots_per_day:
                    groups_by_slot[slot].append(group)

    result: List[Tuple[int, int, List[str]]] = []
    for slot, groups in enumerate(groups_by_slot):
        start = slot * slot_minutes
        end = start + slot_minutes
        result.append((start, end, groups))
    return result


def choose_best_step_slots(
    base_off_intervals: List[Tuple[int, int]],
    slot_minutes: int,
    group_count: int,
) -> int:
    slots_per_day = MINUTES_PER_DAY // slot_minutes
    if slots_per_day <= 0:
        return 1

    light_intervals = extract_light_windows(base_off_intervals)
    light_slots = [(start // slot_minutes, end // slot_minutes) for start, end in light_intervals]

    best_score = None
    best_step = 1
    for step_slots in range(1, slots_per_day + 1):
        counts = [0] * slots_per_day
        for idx in range(group_count):
            offset = (idx * step_slots) % slots_per_day
            for start, end in light_slots:
                for slot in range(start + offset, end + offset):
                    counts[slot % slots_per_day] += 1

        max_count = max(counts) if counts else 0
        min_count = min(counts) if counts else 0
        zeros = sum(1 for value in counts if value == 0)
        spread = max_count - min_count
        avg = sum(counts) / slots_per_day if slots_per_day else 0
        variance = sum((value - avg) ** 2 for value in counts) / slots_per_day if slots_per_day else 0

        score = (max_count, zeros, spread, variance, step_slots)
        if best_score is None or score < best_score:
            best_score = score
            best_step = step_slots

    return best_step


def longest_run(values: List[int], target: int) -> int:
    if not values:
        return 0
    max_run = 0
    run = 0
    doubled = values + values
    for value in doubled:
        if value == target:
            run += 1
            if run > max_run:
                max_run = run
        else:
            run = 0
    return min(max_run, len(values))


def score_counts(counts: List[int]) -> Tuple[int, int, int, float]:
    if not counts:
        return 0, 0, 0, 0.0
    max_count = max(counts)
    zeros = sum(1 for value in counts if value == 0)
    longest_zero = longest_run(counts, 0)
    avg = sum(counts) / len(counts)
    variance = sum((value - avg) ** 2 for value in counts) / len(counts)
    return max_count, zeros, longest_zero, variance


def circular_distance(a: int, b: int, mod: int) -> int:
    if mod <= 0:
        return 0
    diff = abs(a - b) % mod
    return min(diff, mod - diff)


def light_slots_from_off_intervals(
    base_off_intervals: List[Tuple[int, int]],
    slot_minutes: int,
) -> List[int]:
    slots_per_day = MINUTES_PER_DAY // slot_minutes
    if slots_per_day <= 0:
        return []
    light_intervals = extract_light_windows(base_off_intervals)
    slots: List[int] = []
    for start, end in light_intervals:
        start_slot = start // slot_minutes
        end_slot = end // slot_minutes
        for slot in range(start_slot, end_slot):
            slots.append(slot % slots_per_day)
    return slots


def choose_balanced_offsets(
    base_off_intervals: List[Tuple[int, int]],
    slot_minutes: int,
    group_count: int,
    rng: random.Random,
    attempts: int = 40,
) -> Tuple[List[int], Tuple[int, int, int, float]]:
    slots_per_day = MINUTES_PER_DAY // slot_minutes
    if slots_per_day <= 0 or group_count <= 0:
        return [0 for _ in range(group_count)], (0, 0, 0, 0.0)

    light_slots = light_slots_from_off_intervals(base_off_intervals, slot_minutes)
    if not light_slots:
        return [0 for _ in range(group_count)], (0, slots_per_day, slots_per_day, 0.0)

    offset_slots: List[List[int]] = []
    for offset in range(slots_per_day):
        offset_slots.append([(slot + offset) % slots_per_day for slot in light_slots])

    ideal_offsets = [
        int(round(idx * slots_per_day / group_count)) % slots_per_day
        for idx in range(group_count)
    ]

    best_offsets: List[int] | None = None
    best_score: Tuple[int, int, int, float] | None = None
    best_order_penalty = None

    for _ in range(max(1, attempts)):
        counts = [0] * slots_per_day
        offsets: List[int] = []
        used_offsets: set[int] = set()
        for idx in range(group_count):
            candidates = list(range(slots_per_day))
            rng.shuffle(candidates)
            local_best_score = None
            local_best_off = 0
            for off in candidates:
                for slot in offset_slots[off]:
                    counts[slot] += 1
                score = score_counts(counts)
                reuse_penalty = 1 if off in used_offsets else 0
                order_penalty = circular_distance(off, ideal_offsets[idx], slots_per_day)
                ranked = (*score, reuse_penalty, order_penalty)
                if local_best_score is None or ranked < local_best_score:
                    local_best_score = ranked
                    local_best_off = off
                for slot in offset_slots[off]:
                    counts[slot] -= 1

            offsets.append(local_best_off)
            used_offsets.add(local_best_off)
            for slot in offset_slots[local_best_off]:
                counts[slot] += 1

        score = score_counts(counts)
        order_penalty = sum(
            circular_distance(offsets[i], ideal_offsets[i], slots_per_day)
            for i in range(group_count)
        )
        if best_score is None or score < best_score or (
            score == best_score and (best_order_penalty is None or order_penalty < best_order_penalty)
        ):
            best_score = score
            best_offsets = offsets
            best_order_penalty = order_penalty

    if best_offsets is None:
        best_offsets = [0 for _ in range(group_count)]
        best_score = score_counts([0] * slots_per_day)

    shift = rng.randrange(slots_per_day) if slots_per_day > 0 else 0
    best_offsets = [(off + shift) % slots_per_day for off in best_offsets]
    return [off * slot_minutes for off in best_offsets], best_score


async def send_off_counts(
    update: Update,
    schedule: Dict[str, List[Tuple[int, int]]],
    slot_minutes: int,
) -> None:
    lines = ["Без світла по годинах:"]
    for start, end, on_groups in build_on_groups(schedule, slot_minutes):
        off_groups = [group for group in GROUPS if group not in on_groups]
        if off_groups:
            group_text = ", ".join(off_groups)
        else:
            group_text = "немає"
        lines.append(f"{format_interval(start, end)} — {group_text} ({len(off_groups)})")
    await update.message.reply_text("\n".join(lines))


def build_intervals_for_image(
    schedule: Dict[str, List[Tuple[int, int]]],
) -> Dict[str, List[Tuple[str, int, int]]]:
    intervals_by_group: Dict[str, List[Tuple[str, int, int]]] = {}
    for group in GROUPS:
        intervals_by_group[group] = build_display_intervals(
            schedule.get(group, []),
            include_light=SHOW_LIGHT_WINDOWS_ON_IMAGE,
        )
    return intervals_by_group


def format_schedule(schedule: Dict[str, List[Tuple[int, int]]]) -> str:
    lines = ["Графік відключень:"]
    for group in GROUPS:
        lines.append(f"🔹 Черга {group}")
        intervals = normalize_for_display(schedule.get(group, []))
        for start, end in intervals:
            lines.append(format_interval(start, end))
        lines.append("")
    return "\n".join(lines).strip()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    values = ", ".join(str(v).rstrip("0").rstrip(".") for v in VALID_QUEUES)
    keyboard = ReplyKeyboardMarkup(
        [[KeyboardButton("Новий"), KeyboardButton("З урахуванням вчорашнього")]],
        resize_keyboard=True,
    )
    text = (
        "Вітаю! Я генерую графік відключень.\n"
        "Команда: /generate <кількість_відключених_черг>\n"
        "Команда: /yesterday <кількість_відключених_черг> (розклад наступним повідомленням)\n"
        f"Доступні значення: {values}"
    )
    await update.message.reply_text(text, reply_markup=keyboard)


async def ask_generate_rule(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Вкажіть кількість відключених черг (наприклад, 5.5).")
    return GEN_RULE


async def ask_yesterday_rule(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Вкажіть кількість відключених черг (наприклад, 4).")
    return YEST_RULE


async def start_yesterday_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if context.args:
        q = normalize_queue_value(context.args[0])
        if q is None:
            values = ", ".join(str(v).rstrip("0").rstrip(".") for v in VALID_QUEUES)
            await update.message.reply_text(
                "Невірне значення. Доступні: " + values
            )
            return ConversationHandler.END
        context.user_data["yesterday_q"] = q
        await update.message.reply_text("Надішліть вчорашній розклад у наступному повідомленні.")
        return YEST_SCHEDULE

    await update.message.reply_text("Вкажіть кількість відключених черг (наприклад, 3).")
    return YEST_RULE


async def handle_generate_rule(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = normalize_queue_value(update.message.text.strip())
    if q is None:
        values = ", ".join(str(v).rstrip("0").rstrip(".") for v in VALID_QUEUES)
        await update.message.reply_text(
            "Невірне значення. Доступні: " + values
        )
        return GEN_RULE

    try:
        schedule = build_schedule(
            q=q,
            slot_minutes=60,
            rng=random.Random(),
        )
    except ValueError as exc:
        await update.message.reply_text(humanize_error(exc, q=q, from_yesterday=False))
        return ConversationHandler.END

    await update.message.reply_text(format_schedule(schedule))
    await send_off_counts(update, schedule, 60)
    try:
        image = render_schedule_image(build_intervals_for_image(schedule), GROUPS)
        await update.message.reply_photo(photo=image)
    except Exception as exc:
        logger.exception("Failed to render schedule image: %s", exc)

    return ConversationHandler.END


async def handle_yesterday_rule(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = normalize_queue_value(update.message.text.strip())
    if q is None:
        values = ", ".join(str(v).rstrip("0").rstrip(".") for v in VALID_QUEUES)
        await update.message.reply_text(
            "Невірне значення. Доступні: " + values
        )
        return YEST_RULE

    context.user_data["yesterday_q"] = q
    await update.message.reply_text("Надішліть вчорашній розклад у такому ж форматі, як бот надсилає.")
    return YEST_SCHEDULE


async def handle_yesterday_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text or ""
    if text.strip() == "З урахуванням вчорашнього":
        return await ask_yesterday_rule(update, context)
    if text.strip() == "Новий":
        return await ask_generate_rule(update, context)
    if context.user_data.get("yesterday_q") is None:
        if normalize_queue_value(text.strip()) is not None:
            return await handle_yesterday_rule(update, context)
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        await update.message.reply_text("Немає розкладу. Спробуйте ще раз.")
        return YEST_SCHEDULE

    q = context.user_data.get("yesterday_q")
    try:
        schedule, slot_minutes = parse_yesterday_schedule(lines)
    except ValueError as exc:
        await update.message.reply_text(humanize_error(exc, q=q, from_yesterday=True))
        if str(exc) == "No valid intervals found in yesterday schedule.":
            await update.message.reply_text(
                example_yesterday_format_message(),
                parse_mode="Markdown",
            )
        return YEST_SCHEDULE

    if q is None:
        await update.message.reply_text("Не знайдено кількість черг. Почніть спочатку.")
        return ConversationHandler.END

    rng = random.Random()

    base_group = choose_start_group_from_yesterday(schedule)
    min_window = min_light_window_minutes(q)
    anchor_offset = 0 if min_window > 0 else None

    initial_age_by_group: Dict[str, int] | None = None
    if min_window > 0:
        initial_age_by_group = {}
        priority_minutes_by_group: Dict[str, int] = {}
        for group in GROUPS:
            off_minutes = off_before_day_end_minutes(schedule.get(group, []))
            initial_age_by_group[group] = int(round(off_minutes / min_window))
            priority_minutes_by_group[group] = off_minutes

    rule = RULES[q]
    try:
        new_schedule = build_constant_schedule(
            q=q,
            slot_minutes=slot_minutes,
            start_group=base_group,
            anchor_offset=anchor_offset,
            initial_age_by_group=initial_age_by_group,
            priority_minutes_by_group=priority_minutes_by_group if min_window > 0 else None,
            max_on_windows=rule.max_on_windows,
            max_consecutive_off_hours=rule.max_consecutive_off_hours,
            rng=rng,
        )
    except ValueError as exc:
        await update.message.reply_text(humanize_error(exc, q=q, from_yesterday=True))
        return ConversationHandler.END

    await update.message.reply_text(format_schedule(new_schedule))
    await send_off_counts(update, new_schedule, slot_minutes)
    try:
        image = render_schedule_image(build_intervals_for_image(new_schedule), GROUPS)
        await update.message.reply_photo(photo=image)
    except Exception as exc:
        logger.exception("Failed to render schedule image: %s", exc)

    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Скасовано.")
    return ConversationHandler.END


async def generate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Використання: /generate 5.5")
        return

    q = normalize_queue_value(context.args[0])
    if q is None:
        values = ", ".join(str(v).rstrip("0").rstrip(".") for v in VALID_QUEUES)
        await update.message.reply_text(
            "Невірне значення. Доступні: " + values
        )
        return

    try:
        schedule = build_schedule(
            q=q,
            slot_minutes=60,
            rng=random.Random(),
        )
    except ValueError as exc:
        await update.message.reply_text(humanize_error(exc, q=q, from_yesterday=False))
        return

    await update.message.reply_text(format_schedule(schedule))
    await send_off_counts(update, schedule, 60)
    try:
        image = render_schedule_image(build_intervals_for_image(schedule), GROUPS)
        await update.message.reply_photo(photo=image)
    except Exception as exc:
        logger.exception("Failed to render schedule image: %s", exc)


def main() -> None:
    if not TELEGRAM_TSEK_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_TSEK_BOT_TOKEN is not set in the environment.")

    app = ApplicationBuilder().token(TELEGRAM_TSEK_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("generate", generate))
    app.add_handler(CommandHandler("cancel", cancel))

    conversation = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex("^Новий$"), ask_generate_rule),
            MessageHandler(filters.Regex("^З урахуванням вчорашнього$"), ask_yesterday_rule),
            CommandHandler("yesterday", start_yesterday_command),
        ],
        states={
            GEN_RULE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_generate_rule)],
            YEST_RULE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_yesterday_rule)],
            YEST_SCHEDULE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_yesterday_schedule)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    app.add_handler(conversation)

    logger.info("TSEK schedule bot started.")
    app.run_polling()


if __name__ == "__main__":
    main()
