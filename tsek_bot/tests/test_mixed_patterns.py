import random
import unittest

from tsek_bot import bot


YESTERDAY_Q45 = """
Графік відключень:
🔹 Черга 1.1
02:00 - 08:00
10:00 - 16:00
18:00 - 00:00

🔹 Черга 1.2
02:00 - 08:00
10:00 - 16:00
18:00 - 00:00

🔹 Черга 2.1
02:00 - 08:00
10:00 - 16:00
18:00 - 00:00

🔹 Черга 2.2
00:00 - 04:00
06:00 - 12:00
14:00 - 20:00
22:00 - 00:00

🔹 Черга 3.1
00:00 - 04:00
06:00 - 12:00
14:00 - 20:00
22:00 - 00:00

🔹 Черга 3.2
00:00 - 06:00
08:00 - 14:00
16:00 - 22:00

🔹 Черга 4.1
00:00 - 04:00
06:00 - 12:00
14:00 - 20:00
22:00 - 00:00

🔹 Черга 4.2
00:00 - 06:00
08:00 - 14:00
16:00 - 22:00

🔹 Черга 5.1
00:00 - 02:00
04:00 - 10:00
12:00 - 18:00
20:00 - 00:00

🔹 Черга 5.2
00:00 - 06:00
08:00 - 14:00
16:00 - 22:00

🔹 Черга 6.1
00:00 - 02:00
04:00 - 10:00
12:00 - 18:00
20:00 - 00:00

🔹 Черга 6.2
00:00 - 02:00
04:00 - 10:00
12:00 - 18:00
20:00 - 00:00
"""


YESTERDAY_Q5 = """
Графік відключень:
🔹 Черга 1.1
00:00 - 04:30
08:00 - 16:00
20:30 - 00:00

🔹 Черга 1.2
00:00 - 08:00
11:30 - 19:30

🔹 Черга 2.1
00:00 - 08:00
11:30 - 19:30

🔹 Черга 2.2
04:30 - 12:30
16:00 - 00:00

🔹 Черга 3.1
03:30 - 11:30
16:00 - 00:00

🔹 Черга 3.2
04:30 - 12:30
16:00 - 00:00

🔹 Черга 4.1
00:00 - 08:00
12:30 - 20:30

🔹 Черга 4.2
03:30 - 11:30
16:00 - 00:00

🔹 Черга 5.1
00:00 - 03:30
08:00 - 16:00
19:30 - 00:00

🔹 Черга 5.2
00:00 - 08:00
12:30 - 20:30

🔹 Черга 6.1
00:00 - 03:30
08:00 - 16:00
19:30 - 00:00

🔹 Черга 6.2
00:00 - 04:30
08:00 - 16:00
20:30 - 00:00
"""


class MixedPatternsTest(unittest.TestCase):
    def assert_schedule_matches_rules(self, schedule, q: float, slot_minutes: int) -> None:
        expected_off = bot.off_queue_count(q)
        for _, _, on_groups in bot.build_on_groups(schedule, slot_minutes):
            self.assertEqual(len(bot.GROUPS) - len(on_groups), expected_off)

        min_window = bot.min_light_window_minutes(q)
        for group in bot.GROUPS:
            lights = bot.extract_light_windows(schedule[group])
            if q < 6.0:
                self.assertGreaterEqual(len(lights), 2)
            max_windows = bot.RULES[q].max_on_windows
            if max_windows is not None:
                self.assertLessEqual(len(lights), max_windows)
            for start, end in lights:
                self.assertGreaterEqual(end - start, min_window)

        max_allowed = bot.RULES[q].max_consecutive_off_hours
        if max_allowed is not None:
            max_off = max(
                (end - start)
                for group in bot.GROUPS
                for start, end in schedule[group]
            )
            self.assertLessEqual(max_off, int(max_allowed * 60))

    def test_q45_selected_25_35_uses_mixed_windows(self):
        lines = [line.strip() for line in YESTERDAY_Q45.splitlines() if line.strip()]
        schedule, slot_minutes = bot.parse_yesterday_schedule(lines)

        new_schedule = bot.build_schedule_from_yesterday(
            schedule=schedule,
            q=4.5,
            slot_minutes=slot_minutes,
            rng=random.Random(17),
            max_attempts=40,
            allowed_patterns=[(2.5, 3.5)],
        )

        self.assertFalse(bot.schedules_equal(schedule, new_schedule))
        self.assert_schedule_matches_rules(new_schedule, 4.5, 30)
        self.assertGreaterEqual(bot.count_groups_with_pattern(new_schedule, (2.5, 3.5)), 1)

    def test_q5_selected_15_25_uses_mixed_windows(self):
        lines = [line.strip() for line in YESTERDAY_Q5.splitlines() if line.strip()]
        schedule, slot_minutes = bot.parse_yesterday_schedule(lines)

        new_schedule = bot.build_schedule_from_yesterday(
            schedule=schedule,
            q=5.0,
            slot_minutes=slot_minutes,
            rng=random.Random(29),
            max_attempts=40,
            allowed_patterns=[(1.5, 2.5)],
        )

        self.assertFalse(bot.schedules_equal(schedule, new_schedule))
        self.assert_schedule_matches_rules(new_schedule, 5.0, 30)
        self.assertGreaterEqual(bot.count_groups_with_pattern(new_schedule, (1.5, 2.5)), 1)

    def test_q5_shift_optimization_keeps_min_display_light_window(self):
        lines = [line.strip() for line in YESTERDAY_Q5.splitlines() if line.strip()]
        yesterday, slot_minutes = bot.parse_yesterday_schedule(lines)

        generated = bot.build_schedule_from_yesterday(
            schedule=yesterday,
            q=5.0,
            slot_minutes=slot_minutes,
            rng=random.Random(33),
            max_attempts=40,
            allowed_patterns=[(1.5, 2.5)],
        )
        optimized, _, _ = bot.optimize_shift_against_yesterday(
            yesterday_schedule=yesterday,
            new_schedule=generated,
            q=5.0,
            slot_minutes=slot_minutes,
            max_consecutive_off_hours=bot.RULES[5.0].max_consecutive_off_hours,
        )

        self.assertTrue(bot.schedule_has_valid_display_light_windows(optimized, 5.0))
        min_window = bot.min_light_window_minutes(5.0)
        max_windows = bot.RULES[5.0].max_on_windows
        for group in bot.GROUPS:
            lights = bot.extract_light_windows(optimized[group])
            self.assertGreaterEqual(len(lights), 2)
            self.assertLessEqual(len(lights), max_windows)
            for start, end in lights:
                self.assertGreaterEqual(end - start, min_window)


if __name__ == "__main__":
    unittest.main()
