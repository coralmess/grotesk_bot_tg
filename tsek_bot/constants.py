from dataclasses import dataclass
from typing import Dict, List, Tuple

MINUTES_PER_DAY = 24 * 60
GROUPS = [f"{i}.{j}" for i in range(1, 7) for j in (1, 2)]
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
    2.5: Rule(queues=2.5, light_hours=14, dark_hours=10, min_break_hours=4, max_consecutive_off_hours=5, max_on_windows=3),
    2.0: Rule(queues=2.0, light_hours=16, dark_hours=8, min_break_hours=4, max_consecutive_off_hours=4, max_on_windows=3),
    1.5: Rule(queues=1.5, light_hours=18, dark_hours=6, min_break_hours=None, max_consecutive_off_hours=3, max_on_windows=2),
    1.0: Rule(queues=1.0, light_hours=20, dark_hours=4, min_break_hours=None, max_consecutive_off_hours=2, max_on_windows=2),
}

VALID_QUEUES = sorted(RULES.keys())

MIN_LIGHT_WINDOW_BY_QUEUE: Dict[float, float] = {
    6.0: 0.0,
    5.5: 1.0,
    5.0: 1.5,
    4.5: 2.0,
    4.0: 2.5,
    3.5: 3.0,
    3.0: 3.5,
    2.5: 4.0,
    2.0: 5.0,
    1.5: 5.0,
    1.0: 6.0,
}

LIGHT_PATTERNS_BY_QUEUE: Dict[float, List[Tuple[float, ...]]] = {
    6.0: [],
    5.5: [(1.0, 1.0)],
    5.0: [(2.0, 2.0), (1.5, 2.5)],
    4.5: [(2.0, 4.0), (2.5, 3.5), (3.0, 3.0), (2.0, 2.0, 2.0)],
    4.0: [
        (2.5, 5.5),
        (3.0, 5.0),
        (3.5, 4.5),
        (4.0, 4.0),
        (2.5, 2.5, 3.0),
    ],
    3.5: [
        (3.0, 7.0),
        (3.5, 6.5),
        (4.0, 6.0),
        (4.5, 5.5),
        (5.0, 5.0),
        (3.0, 3.0, 4.0),
        (3.0, 3.5, 3.5),
    ],
    3.0: [
        (3.5, 4.0, 4.5),
        (4.0, 4.0, 4.0),
        (3.5, 3.5, 5.0),
        (3.5, 8.5),
        (4.0, 8.0),
        (4.5, 7.5),
        (5.0, 7.0),
        (5.5, 6.5),
        (6.0, 6.0),
    ],
    2.5: [
        (4.0, 10.0),
        (4.5, 9.5),
        (5.0, 9.0),
        (5.5, 8.5),
        (6.0, 8.0),
        (6.5, 7.5),
        (7.0, 7.0),
        (4.0, 4.0, 6.0),
        (4.0, 4.5, 5.5),
        (4.0, 5.0, 5.0),
        (4.5, 4.5, 5.0),
    ],
    2.0: [
        (5.0, 11.0),
        (5.5, 10.5),
        (6.0, 10.0),
        (6.5, 9.5),
        (7.0, 9.0),
        (7.5, 8.5),
        (8.0, 8.0),
        (5.0, 5.0, 6.0),
        (5.0, 5.5, 5.5),
    ],
    1.5: [
        (5.0, 13.0),
        (5.5, 12.5),
        (6.0, 12.0),
        (6.5, 11.5),
        (7.0, 11.0),
        (7.5, 10.5),
        (8.0, 10.0),
        (8.5, 9.5),
        (9.0, 9.0),
    ],
    1.0: [
        (6.0, 14.0),
        (6.5, 13.5),
        (7.0, 13.0),
        (7.5, 12.5),
        (8.0, 12.0),
        (8.5, 11.5),
        (9.0, 11.0),
        (9.5, 10.5),
        (10.0, 10.0),
    ],
}
