import asyncio
import logging
import math
import random
import re
import sys
from pathlib import Path
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
from tsek_bot.constants import (
    MINUTES_PER_DAY,
    GROUPS,
    SHOW_LIGHT_WINDOWS_ON_IMAGE,
    RULES,
    VALID_QUEUES,
    LIGHT_PATTERNS_BY_QUEUE,
    MIN_LIGHT_WINDOW_BY_QUEUE,
)

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

GROUP_RE = re.compile(r"Черга\s+([1-6]\.[12])", re.IGNORECASE)
TIME_RE = re.compile(r"(\d{1,2}:\d{2})\s*[-–]\s*(\d{1,2}:\d{2})")

GEN_RULE, YEST_RULE, YEST_SCHEDULE, YEST_PATTERN, SHIFT_CHOICE = range(5)


def format_queue_value(value: float) -> str:
    return str(value).rstrip("0").rstrip(".")


def format_pattern_value(value: float) -> str:
    return str(value).rstrip("0").rstrip(".")


def format_light_pattern(pattern: Tuple[float, ...]) -> str:
    return "+".join(format_pattern_value(x) for x in pattern)


def parse_light_pattern_input(text: str) -> Tuple[float, ...] | None:
    raw = text.strip().replace(" ", "").replace(",", ".")
    if not raw:
        return None
    chunks = raw.split("+")
    if not chunks:
        return None
    parsed: List[float] = []
    for chunk in chunks:
        if not chunk:
            return None
        try:
            value = float(chunk)
        except ValueError:
            return None
        if value <= 0:
            return None
        rounded = round(value * 2) / 2
        if abs(rounded - value) > 1e-6:
            return None
        parsed.append(rounded)
    return tuple(parsed)


def min_windows_needed_for_rule(q: float) -> int:
    rule = RULES[q]
    if rule.light_hours <= 0:
        return 0
    if q >= 6.0:
        return 0
    return 2


def humanize_error(
    exc: Exception,
    *,
    q: float | None = None,
    from_yesterday: bool = False,
) -> str:
    msg = str(exc)

    if msg in (
        "Invalid time format.",
        "Invalid time value.",
    ):
        return (
            "Неправильний формат часу. Приклад: 08:00 - 12:00. "
            "Хвилини можуть бути лише 00 або 30."
        )

    if msg == "No valid intervals found in yesterday schedule.":
        return (
            "У повідомленні не знайшла жодного діапазону часу. "
            "Надішліть розклад у форматі, як бот надсилає."
        )

    if "Invalid slot size" in msg or "Invalid block size" in msg:
        return (
            "У розкладі є час, який не підходить під крок 30 хвилин або 1 година. "
            "Перевірте, щоб часи були кратні 30 хвилинам."
        )

    if "Cannot distribute light evenly" in msg:
        return "Не можу рівномірно розподілити години світла для всіх груп."

    if q is not None and q in RULES:
        rule = RULES[q]
        min_window_minutes = min_light_window_minutes(q)
        if (
            rule.max_on_windows is not None
            and min_window_minutes > 0
            and min_windows_needed_for_rule(q) > rule.max_on_windows
            and (
                "max_on_windows" in msg
                or "Cannot build schedule" in msg
            )
        ):
            min_windows_needed = min_windows_needed_for_rule(q)
            min_window_hours = int(min_window_minutes // 60)
            light_hours = (
                int(rule.light_hours)
                if float(rule.light_hours).is_integer()
                else rule.light_hours
            )
            return (
                "Не можу побудувати розклад для правила "
                f"{format_queue_value(q)}. За цим правилом світла має бути "
                f"{light_hours} год на добу, а мінімальна тривалість одного "
                f"вікна світла — {min_window_hours} год. Це означає, що потрібно "
                f"щонайменше {min_windows_needed} вікна, але дозволено лише "
                f"{rule.max_on_windows}. Спробуйте інше правило або формат."
            )

    if (
        "Cannot build schedule" in msg
        or "max_on_windows" in msg
    ):
        if from_yesterday:
            return (
                "Не можу побудувати графік за вчорашнім розкладом. "
                "Ліміт «максимум без світла» рахується тільки в межах нового дня, "
                "але з цими вікнами все одно не виходить скласти рівний графік. "
                "Спробуйте інший розклад або інше правило."
            )
        return (
            "Не можу побудувати графік за цими параметрами. "
            "Спробуйте інше правило."
        )

    if "max_consecutive_off_hours" in msg:
        if q is not None and q in RULES and RULES[q].max_consecutive_off_hours:
            max_hours = RULES[q].max_consecutive_off_hours
            return (
                "Перевищено максимальну тривалість відключення. "
                f"За правилом не можна більше ніж {max_hours} год підряд."
            )
        return "Перевищено максимальну тривалість відключення."

    return (
        "Не знаю, як побудувати графік для цих умов. "
        "Спробуйте інше правило або розклад."
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


def normalize_schedule(
    schedule: Dict[str, List[Tuple[int, int]]],
) -> Dict[str, List[Tuple[int, int]]]:
    normalized: Dict[str, List[Tuple[int, int]]] = {}
    for group in GROUPS:
        intervals = schedule.get(group, [])
        normalized[group] = normalize_for_display(intervals)
    return normalized


def detect_slot_minutes(schedule: Dict[str, List[Tuple[int, int]]]) -> int:
    for intervals in schedule.values():
        for start, end in intervals:
            if start % 60 != 0 or end % 60 != 0:
                return 30
    return 60


def schedules_equal(
    left: Dict[str, List[Tuple[int, int]]],
    right: Dict[str, List[Tuple[int, int]]],
) -> bool:
    left_norm = normalize_schedule(left)
    right_norm = normalize_schedule(right)
    for group in GROUPS:
        if left_norm.get(group, []) != right_norm.get(group, []):
            return False
    return True


def shift_schedule(
    schedule: Dict[str, List[Tuple[int, int]]],
    offset_minutes: int,
) -> Dict[str, List[Tuple[int, int]]]:
    shifted: Dict[str, List[Tuple[int, int]]] = {}
    for group in GROUPS:
        intervals = schedule.get(group, [])
        shifted[group] = shift_intervals(intervals, offset_minutes)
    return shifted


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
    hours = MIN_LIGHT_WINDOW_BY_QUEUE.get(q, 0.0)
    return int(round(hours * 60))


def needs_half_hour_slot(q: float) -> bool:
    if min_light_window_minutes(q) % 60 != 0:
        return True
    for pattern in LIGHT_PATTERNS_BY_QUEUE.get(q, []):
        if any(abs(length - round(length)) > 1e-6 for length in pattern):
            return True
    return False


def pattern_slot_minutes(pattern: Tuple[float, ...] | None) -> int:
    if not pattern:
        return 60
    for length in pattern:
        if abs(length - round(length)) > 1e-6:
            return 30
    return 60


def light_pattern_candidates(
    *,
    q: float,
    allowed_window_counts: set[int] | None = None,
    slot_minutes: int | None = None,
) -> List[Tuple[float, ...]]:
    patterns = LIGHT_PATTERNS_BY_QUEUE.get(q, [])
    if not patterns:
        return []
    rule = RULES[q]
    min_hours = MIN_LIGHT_WINDOW_BY_QUEUE.get(q, 0.0)
    filtered: List[Tuple[float, ...]] = []
    for pattern in patterns:
        if allowed_window_counts is not None and len(pattern) not in allowed_window_counts:
            continue
        if abs(sum(pattern) - rule.light_hours) > 1e-6:
            continue
        if any(length + 1e-6 < min_hours for length in pattern):
            continue
        if slot_minutes is not None:
            if any(int(round(length * 60)) % slot_minutes != 0 for length in pattern):
                continue
        filtered.append(pattern)
    return filtered


def pick_light_pattern(
    *,
    q: float,
    rng: random.Random,
    allowed_window_counts: set[int] | None = None,
    slot_minutes: int | None = None,
) -> Tuple[float, ...] | None:
    filtered = light_pattern_candidates(
        q=q,
        allowed_window_counts=allowed_window_counts,
        slot_minutes=slot_minutes,
    )
    if not filtered:
        return None
    return rng.choice(filtered)


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
    force_windows_count: int | None = None,
    light_pattern: Tuple[float, ...] | None = None,
    rng: random.Random,
) -> Dict[str, List[Tuple[int, int]]]:
    on_count = on_queue_count(q)
    if on_count <= 0:
        return {group: [(0, MINUTES_PER_DAY)] for group in GROUPS}

    min_window_minutes = min_light_window_minutes(q)
    block_minutes = slot_minutes
    if block_minutes <= 0:
        return {group: [(0, MINUTES_PER_DAY)] for group in GROUPS}

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
    min_run_blocks = 1
    if min_window_minutes > 0:
        min_run_blocks = max(1, int(math.ceil(min_window_minutes / block_minutes)))
    if max_windows <= 0:
        max_windows = blocks_per_group

    if force_windows_count is not None:
        min_windows = force_windows_count
        max_windows = force_windows_count

    run_plans: Dict[str, List[int]] = {}
    if light_pattern is not None:
        pattern_blocks = [int(round(length * 60 / block_minutes)) for length in light_pattern]
        if any(length <= 0 for length in pattern_blocks):
            raise ValueError("Invalid slot size for the configured light window.")
        if sum(pattern_blocks) != blocks_per_group:
            raise ValueError("Cannot distribute light evenly with these settings.")
        if any(length < min_run_blocks for length in pattern_blocks):
            raise ValueError("Неможливо забезпечити мінімальну тривалість вікна світла.")
        windows_count = len(pattern_blocks)
        if max_on_windows is not None and windows_count > max_on_windows:
            raise ValueError("Перевищено max_on_windows.")
        min_windows = windows_count
        max_windows = windows_count
        if force_windows_count is not None and force_windows_count != windows_count:
            raise ValueError("Cannot build schedule with equal light windows.")
        for group in GROUPS:
            lengths = list(pattern_blocks)
            rng.shuffle(lengths)
            run_plans[group] = lengths
    else:
        feasible_windows = [
            w
            for w in range(min_windows, max_windows + 1)
            if w * min_run_blocks <= blocks_per_group
        ]
        if not feasible_windows:
            raise ValueError("Неможливо забезпечити мінімальну тривалість вікна світла.")
        windows_count = rng.choice(feasible_windows)

        for group in GROUPS:
            lengths = [min_run_blocks for _ in range(windows_count)]
            remaining_blocks = blocks_per_group - min_run_blocks * windows_count
            if remaining_blocks > 0:
                indices = list(range(windows_count))
                rng.shuffle(indices)
                for i in range(remaining_blocks):
                    lengths[indices[i % windows_count]] += 1
            rng.shuffle(lengths)
            run_plans[group] = lengths

    max_off_blocks = None
    if max_consecutive_off_hours is not None:
        max_off_blocks = int((max_consecutive_off_hours * 60) // block_minutes)
        if max_off_blocks < 0:
            max_off_blocks = None
    ordered_groups = rotate_groups(start_group)
    order_index = {group: idx for idx, group in enumerate(ordered_groups)}

    if anchor_offset is None:
        anchor_offset = rng.randrange(0, block_minutes, block_minutes)
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

        if blocks_per_group < min_windows * min_run_blocks:
            raise ValueError("Неможливо забезпечити мінімальну тривалість вікна світла.")
        if light_pattern is not None:
            pattern_blocks = [int(round(length * 60 / block_minutes)) for length in light_pattern]
            if any(length <= 0 for length in pattern_blocks):
                raise ValueError("Invalid slot size for the configured light window.")
            if any(length < min_run_blocks for length in pattern_blocks):
                raise ValueError("Неможливо забезпечити мінімальну тривалість вікна світла.")
            windows_per_group = len(pattern_blocks)
            if windows_per_group < min_windows:
                raise ValueError("Неможливо забезпечити мінімальну тривалість вікна світла.")
            if max_windows is not None and windows_per_group > max_windows:
                raise ValueError("Перевищено max_on_windows.")
            run_lengths = list(pattern_blocks)
            rng.shuffle(run_lengths)
        else:
            windows_per_group = min(
                max_windows,
                blocks_per_group // min_run_blocks if min_run_blocks > 0 else blocks_per_group,
            )
            if windows_per_group < min_windows:
                raise ValueError("Неможливо забезпечити мінімальну тривалість вікна світла.")
            base_len = blocks_per_group // windows_per_group
            remainder = blocks_per_group % windows_per_group
            run_lengths = [base_len + 1] * remainder + [base_len] * (windows_per_group - remainder)
            if any(length < min_run_blocks for length in run_lengths):
                raise ValueError("Неможливо забезпечити мінімальну тривалість вікна світла.")

        block_on_groups: List[List[str]] = []
        for run_len in run_lengths:
            for group in base_order:
                for _ in range(run_len):
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
        for group in GROUPS:
            for start, end in extract_light_windows(schedule[group]):
                if end - start < min_window_minutes:
                    raise ValueError("Неможливо забезпечити мінімум 2 вікна світла.")
        return schedule

    remaining: Dict[str, int] = {group: blocks_per_group for group in GROUPS}
    run_len: Dict[str, int] = {group: 0 for group in GROUPS}
    windows_used: Dict[str, int] = {group: 0 for group in GROUPS}
    run_plan_index: Dict[str, int] = {group: 0 for group in GROUPS}
    run_goal: Dict[str, int] = {group: 0 for group in GROUPS}
    block_on_groups: List[List[str]] = []

    def next_run_len(group: str) -> int:
        idx = run_plan_index.get(group, 0)
        plan = run_plans.get(group, [])
        if idx >= len(plan):
            return 0
        return plan[idx]

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

        if max_windows is not None:
            for group in GROUPS:
                if (
                    run_len[group] == 0
                    and windows_used[group] >= max_windows
                    and remaining.get(group, 0) > 0
                ):
                    raise ValueError("Перевищено max_on_windows.")
        must_keep = []
        if max_windows is not None:
            must_keep = [
                group
                for group in GROUPS
                if run_len[group] > 0
                and windows_used[group] >= max_windows
                and remaining.get(group, 0) > 0
            ]
        must_keep.sort(
            key=lambda g: (-(block_idx - last_on[g]), order_index[g], rng.random())
        )
        for group in must_keep:
            if group not in selected:
                selected.append(group)

        must_continue = [
            group
            for group in GROUPS
            if run_len[group] > 0
            and run_len[group] < run_goal[group]
            and remaining.get(group, 0) > 0
        ]
        must_continue.sort(
            key=lambda g: (
                -off_run.get(g, 0),
                -(block_idx - last_on[g]),
                order_index[g],
                rng.random(),
            )
        )
        for group in must_continue:
            if group not in selected:
                selected.append(group)

        if max_off_blocks is not None:
            must_on = [
                group
                for group in GROUPS
                if remaining[group] > 0 and off_run.get(group, 0) >= max_off_blocks
            ]
            must_on.sort(
                key=lambda g: (
                    -off_run.get(g, 0),
                    -(block_idx - last_on[g]),
                    order_index[g],
                    rng.random(),
                )
            )
            for group in must_on:
                if group not in selected:
                    selected.append(group)
            urgent_off = [
                group
                for group in GROUPS
                if remaining.get(group, 0) > 0
                and off_run.get(group, 0) >= max_off_blocks - 1
                and group not in selected
            ]
            urgent_off.sort(
                key=lambda g: (-off_run.get(g, 0), order_index[g], rng.random())
            )
            for group in urgent_off:
                if len(selected) >= on_count:
                    break
                if group not in selected:
                    selected.append(group)
        for group in mandatory:
            if run_len[group] == 0 and windows_used[group] >= max_windows:
                raise ValueError("Перевищено max_on_windows.")
            if group not in selected:
                selected.append(group)

        if len(selected) > on_count:
            raise ValueError("Cannot build schedule with equal light windows.")

        candidates = [
            group for group in GROUPS if remaining[group] > 0 and group not in selected
        ]
        for group in GROUPS:
            if (
                run_len[group] == 0
                and windows_used[group] < min_windows
                and blocks_left < next_run_len(group)
            ):
                raise ValueError("Неможливо забезпечити мінімальну тривалість вікна світла.")
        must_start = [
            group
            for group in candidates
            if run_len[group] == 0
            and windows_used[group] < min_windows
            and windows_used[group] < max_windows
            and blocks_left >= next_run_len(group)
        ]
        must_start.sort(
            key=lambda g: (
                -off_run.get(g, 0),
                -(block_idx - last_on[g]),
                order_index[g],
                rng.random(),
            )
        )

        continuing = [
            group
            for group in candidates
            if run_len[group] > 0 and run_len[group] < run_goal[group]
        ]
        continuing.sort(
            key=lambda g: (
                -(run_goal[g] - run_len[g]),
                -off_run.get(g, 0),
                -(block_idx - last_on[g]),
                order_index[g],
                rng.random(),
            )
        )

        overrun = [
            group
            for group in candidates
            if run_len[group] > 0 and run_len[group] >= run_goal[group]
        ]
        overrun.sort(
            key=lambda g: (
                -off_run.get(g, 0),
                -(block_idx - last_on[g]),
                order_index[g],
                rng.random(),
            )
        )

        starters = [
            group
            for group in candidates
            if run_len[group] == 0
            and group not in must_start
            and windows_used[group] < max_windows
            and blocks_left >= next_run_len(group)
        ]
        starters.sort(
            key=lambda g: (
                -off_run.get(g, 0),
                -(block_idx - last_on[g]),
                order_index[g],
                rng.random(),
            )
        )
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
            for group in overrun:
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
                    goal = next_run_len(group)
                    if goal <= 0:
                        raise ValueError("Cannot build schedule with equal light windows.")
                    run_goal[group] = goal
                    run_plan_index[group] += 1
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
        if min_window_minutes > 0:
            for start, end in extract_light_windows(schedule[group]):
                if end - start < min_window_minutes:
                    raise ValueError("Неможливо забезпечити мінімум 2 вікна світла.")

    return schedule


def build_schedule(
    *,
    q: float,
    slot_minutes: int | None,
    rng: random.Random | None = None,
) -> Dict[str, List[Tuple[int, int]]]:
    rng = rng or random.Random()
    rule = RULES[q]
    preferred_slot = slot_minutes
    if preferred_slot is None and needs_half_hour_slot(q):
        preferred_slot = 30
    last_error: Exception | None = None
    patterns = light_pattern_candidates(
        q=q,
        slot_minutes=preferred_slot,
    )
    if not patterns and q < 6.0 and LIGHT_PATTERNS_BY_QUEUE.get(q):
        raise ValueError("Cannot build schedule with equal light windows.")
    if not patterns:
        patterns = [None]

    pattern_order = patterns[:]
    rng.shuffle(pattern_order)
    attempts_per_pattern = 160
    for pattern in pattern_order:
        resolved_slot = preferred_slot
        if resolved_slot is None:
            resolved_slot = pattern_slot_minutes(pattern)
        if resolved_slot == 60 and needs_half_hour_slot(q):
            resolved_slot = 30
        for _ in range(attempts_per_pattern):
            local_rng = random.Random(rng.random())
            start_group = local_rng.choice(GROUPS)
            try:
                return build_constant_schedule(
                    q=q,
                    slot_minutes=resolved_slot,
                    start_group=start_group,
                    anchor_offset=None,
                    max_on_windows=rule.max_on_windows,
                    max_consecutive_off_hours=rule.max_consecutive_off_hours,
                    light_pattern=pattern,
                    rng=local_rng,
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


def build_schedule_from_yesterday(
    schedule: Dict[str, List[Tuple[int, int]]],
    q: float,
    slot_minutes: int,
    rng: random.Random,
    max_attempts: int = 8,
    allowed_patterns: List[Tuple[float, ...]] | None = None,
) -> Dict[str, List[Tuple[int, int]]]:
    if slot_minutes == 60 and needs_half_hour_slot(q):
        slot_minutes = 30
    base_group = choose_start_group_from_yesterday(schedule)
    min_window = min_light_window_minutes(q)
    anchor_offset = 0 if min_window > 0 else None

    initial_age_by_group: Dict[str, int] | None = None
    priority_minutes_by_group: Dict[str, int] | None = None
    if min_window > 0:
        initial_age_by_group = {}
        priority_minutes_by_group = {}
        for group in GROUPS:
            off_minutes = off_before_day_end_minutes(schedule.get(group, []))
            initial_age_by_group[group] = int(round(off_minutes / min_window))
            priority_minutes_by_group[group] = off_minutes

    max_off = max(
        off_before_day_end_minutes(schedule.get(group, [])) for group in GROUPS
    )
    order = rotate_groups("6.1")
    candidates = [group for group in order if off_before_day_end_minutes(schedule.get(group, [])) == max_off]
    if base_group in candidates:
        start_idx = candidates.index(base_group)
        candidates = candidates[start_idx:] + candidates[:start_idx]

    rule = RULES[q]
    preferred_windows: int | None = None
    fallback_windows: int | None = None
    most_common: Tuple[int, ...] | None = None
    if q in (4.0, 4.5):
        pattern_counts: Dict[Tuple[int, ...], int] = {}
        for group in GROUPS:
            light_lengths = [
                end - start
                for start, end in extract_light_windows(schedule.get(group, []))
            ]
            if not light_lengths:
                continue
            key = tuple(sorted(light_lengths))
            pattern_counts[key] = pattern_counts.get(key, 0) + 1
        if pattern_counts:
            most_common = max(pattern_counts.items(), key=lambda item: item[1])[0]
        if most_common is not None:
            if len(most_common) == 2:
                preferred_windows = 3
                fallback_windows = 2
            elif len(most_common) == 3:
                preferred_windows = 2
                fallback_windows = 3
            else:
                preferred_windows = rng.choice([2, 3])
                fallback_windows = 2 if preferred_windows == 3 else 3
        else:
            preferred_windows = rng.choice([2, 3])
            fallback_windows = 2 if preferred_windows == 3 else 3

    if allowed_patterns is not None:
        possible = light_pattern_candidates(
            q=q,
            allowed_window_counts=None,
            slot_minutes=slot_minutes,
        )
        possible_set = {tuple(p) for p in possible}
        patterns = [tuple(p) for p in allowed_patterns if tuple(p) in possible_set]
    elif preferred_windows is not None:
        primary_patterns = light_pattern_candidates(
            q=q,
            allowed_window_counts={preferred_windows},
            slot_minutes=slot_minutes,
        )
        secondary_patterns = light_pattern_candidates(
            q=q,
            allowed_window_counts={fallback_windows} if fallback_windows is not None else None,
            slot_minutes=slot_minutes,
        )
        # If preferred bucket has only one possible shape, keep randomness by allowing fallback shapes too.
        if len(primary_patterns) <= 1 and secondary_patterns:
            patterns = primary_patterns + [
                pat for pat in secondary_patterns if pat not in primary_patterns
            ]
        else:
            patterns = primary_patterns
    else:
        patterns = light_pattern_candidates(
            q=q,
            allowed_window_counts=None,
            slot_minutes=slot_minutes,
        )
    if not patterns and q < 6.0 and LIGHT_PATTERNS_BY_QUEUE.get(q):
        raise ValueError("Cannot build schedule with equal light windows.")
    if not patterns:
        patterns = [None]

    pattern_order = patterns[:]
    rng.shuffle(pattern_order)
    attempts_per_pattern = max(40, max_attempts * 20)
    last_schedule: Dict[str, List[Tuple[int, int]]] | None = None
    last_error: Exception | None = None
    for pattern in pattern_order:
        for attempt in range(attempts_per_pattern):
            start_group = candidates[attempt % len(candidates)] if candidates else base_group
            local_rng = random.Random(rng.random())
            try:
                new_schedule = build_constant_schedule(
                    q=q,
                    slot_minutes=slot_minutes,
                    start_group=start_group,
                    anchor_offset=anchor_offset,
                    initial_age_by_group=initial_age_by_group,
                    priority_minutes_by_group=priority_minutes_by_group if min_window > 0 else None,
                    max_on_windows=rule.max_on_windows,
                    max_consecutive_off_hours=rule.max_consecutive_off_hours,
                    light_pattern=pattern,
                    rng=local_rng,
                )
            except ValueError as exc:
                last_error = exc
                continue
            last_schedule = new_schedule
            if not schedules_equal(schedule, new_schedule):
                return new_schedule
    if last_schedule is not None:
        return last_schedule
    if last_error:
        raise last_error
    return schedule


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
        "Вітаю! Я генерую графіки відключень.\n"
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
            slot_minutes=None,
            rng=random.Random(),
        )
    except ValueError as exc:
        await update.message.reply_text(humanize_error(exc, q=q, from_yesterday=False))
        return ConversationHandler.END

    await update.message.reply_text(format_schedule(schedule))
    slot_minutes = detect_slot_minutes(schedule)
    await send_off_counts(update, schedule, slot_minutes)
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
    await update.message.reply_text("Надішліть вчорашній розклад у такому ж форматі, як надсилає бот.")
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

    if slot_minutes == 60 and needs_half_hour_slot(q):
        slot_minutes = 30

    available_patterns = light_pattern_candidates(
        q=q,
        allowed_window_counts=None,
        slot_minutes=slot_minutes,
    )
    if not available_patterns and q < 6.0 and LIGHT_PATTERNS_BY_QUEUE.get(q):
        await update.message.reply_text("Для цього правила не знайдено жодного валідного шаблону світла.")
        return ConversationHandler.END

    context.user_data["yesterday_schedule"] = schedule
    context.user_data["yesterday_slot_minutes"] = slot_minutes
    context.user_data["yesterday_available_patterns"] = available_patterns

    if available_patterns:
        variants = ", ".join(format_light_pattern(p) for p in available_patterns)
        await update.message.reply_text(
            "Оберіть шаблон вікон світла.\n"
            f"Варіанти: {variants}, Навмання\n"
            "Надішліть один варіант у форматі 2+2+2 або слово Навмання."
        )
        return YEST_PATTERN

    # For rules with no pattern table (e.g. 6.0), continue with default generation.
    try:
        new_schedule = build_schedule_from_yesterday(
            schedule=schedule,
            q=q,
            slot_minutes=slot_minutes,
            rng=random.Random(),
            allowed_patterns=None,
        )
    except ValueError as exc:
        await update.message.reply_text(humanize_error(exc, q=q, from_yesterday=True))
        return ConversationHandler.END

    new_text = format_schedule(new_schedule)
    old_text = format_schedule(schedule)
    if q in (5.0, 5.5) and new_text == old_text:
        max_shift = 5 if q == 5.5 else 4
        context.user_data["shift_schedule"] = new_schedule
        context.user_data["shift_slot_minutes"] = slot_minutes
        context.user_data["shift_max"] = max_shift
        title = "⚠️ Увага ⚠️"
        await update.message.reply_text(
            f"<b>     {title}</b>\n"
            "Щоб люди не сиділи без світла завтра в той самий час що і вчора, графік можна змістити.\n\n"
            "Напишіть 0, якщо хочете залишити все точно так, як було вчора.\n\n"
            f"Напишіть цифру від 1 до {max_shift}, якщо хочете змістити час вимкнень на кілька годин. "
            "Це зробить для деяких підгруп графік з більш довгими відключеннями, враховуючи ті години відключень що вони вже мають до кінця вчорашнього дня, але новий графік буде відрізнятись від учорашнього і кількість годин зі світлом все ще буде рівною для всіх.",
            parse_mode="HTML",
        )
        return SHIFT_CHOICE

    await update.message.reply_text(new_text)
    await send_off_counts(update, new_schedule, slot_minutes)
    try:
        image = render_schedule_image(build_intervals_for_image(new_schedule), GROUPS)
        await update.message.reply_photo(photo=image)
    except Exception as exc:
        logger.exception("Failed to render schedule image: %s", exc)

    return ConversationHandler.END


async def handle_yesterday_pattern(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    q = context.user_data.get("yesterday_q")
    schedule = context.user_data.get("yesterday_schedule")
    slot_minutes = context.user_data.get("yesterday_slot_minutes")
    available_patterns = context.user_data.get("yesterday_available_patterns", [])
    if q is None or schedule is None or slot_minutes is None:
        await update.message.reply_text("Не знайдено дані для генерації. Почніть спочатку.")
        return ConversationHandler.END

    pattern_text = text.casefold()
    if pattern_text == "навмання":
        allowed_patterns = available_patterns
    else:
        parsed = parse_light_pattern_input(text)
        if parsed is None:
            variants = ", ".join(format_light_pattern(p) for p in available_patterns)
            await update.message.reply_text(
                "Невірний формат шаблону.\n"
                f"Варіанти: {variants}, Навмання"
            )
            return YEST_PATTERN
        normalized = tuple(round(v * 2) / 2 for v in parsed)
        allowed_set = {tuple(p) for p in available_patterns}
        if normalized not in allowed_set:
            variants = ", ".join(format_light_pattern(p) for p in available_patterns)
            await update.message.reply_text(
                "Такого шаблону немає для цього правила.\n"
                f"Варіанти: {variants}, Навмання"
            )
            return YEST_PATTERN
        allowed_patterns = [normalized]

    try:
        new_schedule = build_schedule_from_yesterday(
            schedule=schedule,
            q=q,
            slot_minutes=slot_minutes,
            rng=random.Random(),
            max_attempts=30,
            allowed_patterns=allowed_patterns,
        )
    except ValueError as exc:
        await update.message.reply_text(humanize_error(exc, q=q, from_yesterday=True))
        return ConversationHandler.END

    new_text = format_schedule(new_schedule)
    old_text = format_schedule(schedule)
    if q in (5.0, 5.5) and new_text == old_text:
        max_shift = 5 if q == 5.5 else 4
        context.user_data["shift_schedule"] = new_schedule
        context.user_data["shift_slot_minutes"] = slot_minutes
        context.user_data["shift_max"] = max_shift
        title = "⚠️ Увага ⚠️"
        await update.message.reply_text(
            f"<b>     {title}</b>\n"
            "Щоб люди не сиділи без світла завтра в той самий час що і вчора, графік можна змістити.\n\n"
            "Напишіть 0, якщо хочете залишити все точно так, як було вчора.\n\n"
            f"Напишіть цифру від 1 до {max_shift}, якщо хочете змістити час вимкнень на кілька годин. "
            "Це зробить для деяких підгруп графік з більш довгими відключеннями, враховуючи ті години відключень що вони вже мають до кінця вчорашнього дня, але новий графік буде відрізнятись від учорашнього і кількість годин зі світлом все ще буде рівною для всіх.",
            parse_mode="HTML",
        )
        return SHIFT_CHOICE

    await update.message.reply_text(new_text)
    await send_off_counts(update, new_schedule, slot_minutes)
    try:
        image = render_schedule_image(build_intervals_for_image(new_schedule), GROUPS)
        await update.message.reply_photo(photo=image)
    except Exception as exc:
        logger.exception("Failed to render schedule image: %s", exc)

    context.user_data.pop("yesterday_schedule", None)
    context.user_data.pop("yesterday_slot_minutes", None)
    context.user_data.pop("yesterday_available_patterns", None)
    return ConversationHandler.END


async def handle_shift_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip().replace(",", ".")
    max_shift = context.user_data.get("shift_max")
    schedule = context.user_data.get("shift_schedule")
    slot_minutes = context.user_data.get("shift_slot_minutes")
    if schedule is None or slot_minutes is None or max_shift is None:
        await update.message.reply_text("Не знайдено очікуваного розкладу. Почніть спочатку.")
        return ConversationHandler.END

    try:
        value = float(text)
    except ValueError:
        await update.message.reply_text(
            f"Вкажіть число 0 або від 1 до {int(max_shift)}."
        )
        return SHIFT_CHOICE

    if abs(value - round(value)) > 1e-6:
        await update.message.reply_text(
            f"Вкажіть ціле число 0 або від 1 до {int(max_shift)}."
        )
        return SHIFT_CHOICE

    hours = int(round(value))
    if hours < 0 or hours > int(max_shift):
        await update.message.reply_text(
            f"Вкажіть число 0 або від 1 до {int(max_shift)}."
        )
        return SHIFT_CHOICE

    if hours == 0:
        new_schedule = schedule
    else:
        new_schedule = shift_schedule(schedule, hours * 60)

    await update.message.reply_text(format_schedule(new_schedule))
    await send_off_counts(update, new_schedule, slot_minutes)
    try:
        image = render_schedule_image(build_intervals_for_image(new_schedule), GROUPS)
        await update.message.reply_photo(photo=image)
    except Exception as exc:
        logger.exception("Failed to render schedule image: %s", exc)

    context.user_data.pop("shift_schedule", None)
    context.user_data.pop("shift_slot_minutes", None)
    context.user_data.pop("shift_max", None)
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
            slot_minutes=None,
            rng=random.Random(),
        )
    except ValueError as exc:
        await update.message.reply_text(humanize_error(exc, q=q, from_yesterday=False))
        return

    await update.message.reply_text(format_schedule(schedule))
    slot_minutes = detect_slot_minutes(schedule)
    await send_off_counts(update, schedule, slot_minutes)
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
            YEST_PATTERN: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_yesterday_pattern)],
            SHIFT_CHOICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_shift_choice)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    app.add_handler(conversation)

    logger.info("TSEK schedule bot started.")

    async def runner() -> None:
        await app.initialize()
        await app.start()
        await app.updater.start_polling()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass
        finally:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()

    asyncio.run(runner())


if __name__ == "__main__":
    main()
