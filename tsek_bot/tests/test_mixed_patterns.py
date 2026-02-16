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

TODAY_Q5_BOUNDARY_HIGH = """
Графік відключень:
🔹 Черга 1.1
00:00 - 05:30
07:30 - 17:00
19:00 - 00:00

🔹 Черга 1.2
00:00 - 09:00
10:30 - 20:30
23:00 - 00:00

🔹 Черга 2.1
00:00 - 09:00
11:30 - 21:30
23:00 - 00:00

🔹 Черга 2.2
00:00 - 01:30
03:00 - 13:00
15:30 - 00:00

🔹 Черга 3.1
00:30 - 10:30
13:00 - 23:00

🔹 Черга 3.2
01:30 - 11:30
13:00 - 23:00

🔹 Черга 4.1
00:00 - 07:00
09:00 - 18:30
20:30 - 00:00

🔹 Черга 4.2
00:00 - 00:30
03:00 - 13:00
14:30 - 00:00

🔹 Черга 5.1
00:00 - 03:00
05:30 - 15:30
17:00 - 00:00

🔹 Черга 5.2
00:00 - 07:30
09:00 - 19:00
21:30 - 00:00

🔹 Черга 6.1
00:00 - 03:00
05:00 - 14:30
16:30 - 00:00

🔹 Черга 6.2
00:00 - 05:00
07:00 - 16:30
18:30 - 00:00
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

    def test_q5_variant1_prefers_lower_today_peak_when_boundary_is_high(self):
        yesterday_lines = [line.strip() for line in YESTERDAY_Q5.splitlines() if line.strip()]
        yesterday, slot_minutes = bot.parse_yesterday_schedule(yesterday_lines)

        today_lines = [line.strip() for line in TODAY_Q5_BOUNDARY_HIGH.splitlines() if line.strip()]
        today_raw, _ = bot.parse_yesterday_schedule(today_lines)
        # +60 min shift creates cross-day boundary > 10h for at least one subgroup.
        today = bot.shift_schedule(today_raw, 60)

        today_rule_max = int(bot.RULES[5.0].max_consecutive_off_hours * 60)
        baseline_boundary = bot.max_cross_day_boundary_off_minutes(yesterday, today)
        self.assertGreater(baseline_boundary, today_rule_max)

        baseline_today_peak = bot.max_consecutive_in_schedule(today)
        optimized, _, _ = bot.optimize_shift_against_yesterday(
            yesterday_schedule=yesterday,
            new_schedule=today,
            q=5.0,
            slot_minutes=slot_minutes,
            max_consecutive_off_hours=bot.RULES[5.0].max_consecutive_off_hours,
        )
        optimized_today_peak = bot.max_consecutive_in_schedule(optimized)

        self.assertTrue(bot.schedule_has_valid_display_light_windows(optimized, 5.0))
        self.assertLessEqual(optimized_today_peak, baseline_today_peak)


if __name__ == "__main__":
    unittest.main()
