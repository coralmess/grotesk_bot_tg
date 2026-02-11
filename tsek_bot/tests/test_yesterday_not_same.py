import random
import unittest

from tsek_bot import bot


YESTERDAY_TEXT = """
Графік відключень:
🔹 Черга 1.1
02:00 - 06:00
08:00 - 12:00
16:00 - 00:00

🔹 Черга 1.2
02:00 - 06:00
08:00 - 12:00
16:00 - 00:00

🔹 Черга 2.1
00:00 - 02:00
04:00 - 08:00
10:00 - 16:00
20:00 - 00:00

🔹 Черга 2.2
00:00 - 04:00
06:00 - 10:00
12:00 - 20:00

🔹 Черга 3.1
00:00 - 04:00
06:00 - 10:00
12:00 - 20:00

🔹 Черга 3.2
00:00 - 04:00
06:00 - 10:00
12:00 - 20:00

🔹 Черга 4.1
00:00 - 02:00
04:00 - 08:00
10:00 - 16:00
20:00 - 00:00

🔹 Черга 4.2
00:00 - 04:00
06:00 - 10:00
12:00 - 20:00

🔹 Черга 5.1
00:00 - 02:00
04:00 - 08:00
10:00 - 16:00
20:00 - 00:00

🔹 Черга 5.2
00:00 - 02:00
04:00 - 08:00
10:00 - 16:00
20:00 - 00:00

🔹 Черга 6.1
02:00 - 06:00
08:00 - 12:00
16:00 - 00:00

🔹 Черга 6.2
02:00 - 06:00
08:00 - 12:00
16:00 - 00:00
"""


YESTERDAY_TEXT_Q45_222 = """
Графік відключень:
🔹 Черга 1.1
00:00 - 02:00
04:00 - 10:00
12:00 - 18:00
20:00 - 00:00

🔹 Черга 1.2
00:00 - 02:00
04:00 - 10:00
12:00 - 18:00
20:00 - 00:00

🔹 Черга 2.1
00:00 - 02:00
04:00 - 10:00
12:00 - 18:00
20:00 - 00:00

🔹 Черга 2.2
00:00 - 02:00
04:00 - 10:00
12:00 - 18:00
20:00 - 00:00

🔹 Черга 3.1
00:00 - 02:00
04:00 - 10:00
12:00 - 18:00
20:00 - 00:00

🔹 Черга 3.2
00:00 - 02:00
04:00 - 10:00
12:00 - 18:00
20:00 - 00:00

🔹 Черга 4.1
00:00 - 02:00
04:00 - 10:00
12:00 - 18:00
20:00 - 00:00

🔹 Черга 4.2
00:00 - 02:00
04:00 - 10:00
12:00 - 18:00
20:00 - 00:00

🔹 Черга 5.1
00:00 - 02:00
04:00 - 10:00
12:00 - 18:00
20:00 - 00:00

🔹 Черга 5.2
00:00 - 02:00
04:00 - 10:00
12:00 - 18:00
20:00 - 00:00

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


YESTERDAY_TEXT_Q45_33 = """
Графік відключень:
🔹 Черга 1.1
00:00 - 04:00
07:00 - 16:00
19:00 - 00:00

🔹 Черга 1.2
00:00 - 04:00
07:00 - 16:00
19:00 - 00:00

🔹 Черга 2.1
00:00 - 04:00
07:00 - 16:00
19:00 - 00:00

🔹 Черга 2.2
00:00 - 04:00
07:00 - 16:00
19:00 - 00:00

🔹 Черга 3.1
00:00 - 04:00
07:00 - 16:00
19:00 - 00:00

🔹 Черга 3.2
00:00 - 04:00
07:00 - 16:00
19:00 - 00:00

🔹 Черга 4.1
00:00 - 04:00
07:00 - 16:00
19:00 - 00:00

🔹 Черга 4.2
00:00 - 04:00
07:00 - 16:00
19:00 - 00:00

🔹 Черга 5.1
00:00 - 04:00
07:00 - 16:00
19:00 - 00:00

🔹 Черга 5.2
00:00 - 04:00
07:00 - 16:00
19:00 - 00:00

🔹 Черга 6.1
00:00 - 04:00
07:00 - 16:00
19:00 - 00:00

🔹 Черга 6.2
00:00 - 04:00
07:00 - 16:00
19:00 - 00:00
"""


class YesterdayNotSameTest(unittest.TestCase):
    def test_yesterday_schedule_is_not_reused_for_q4(self):
        lines = [line.strip() for line in YESTERDAY_TEXT.splitlines() if line.strip()]
        schedule, slot_minutes = bot.parse_yesterday_schedule(lines)

        q = 4.0
        new_schedule = bot.build_schedule_from_yesterday(
            schedule=schedule,
            q=q,
            slot_minutes=slot_minutes,
            rng=random.Random(42),
        )

        self.assertFalse(bot.schedules_equal(schedule, new_schedule))

        expected_lengths = [120, 180, 180]
        pattern_counts = {}
        for group in bot.GROUPS:
            light_lengths = [
                end - start for start, end in bot.extract_light_windows(schedule[group])
            ]
            key = tuple(sorted(light_lengths))
            pattern_counts[key] = pattern_counts.get(key, 0) + 1
        if pattern_counts:
            most_common = max(pattern_counts.items(), key=lambda item: item[1])[0]
            if most_common[0] == 240:
                expected_lengths = [120, 180, 180]
            else:
                expected_lengths = [240, 240]
        for group in bot.GROUPS:
            light_lengths = [
                end - start for start, end in bot.extract_light_windows(new_schedule[group])
            ]
            self.assertEqual(sorted(light_lengths), expected_lengths)

    def test_yesterday_schedule_is_not_reused_for_q45(self):
        lines = [line.strip() for line in YESTERDAY_TEXT_Q45_222.splitlines() if line.strip()]
        schedule, slot_minutes = bot.parse_yesterday_schedule(lines)

        q = 4.5
        new_schedule = bot.build_schedule_from_yesterday(
            schedule=schedule,
            q=q,
            slot_minutes=slot_minutes,
            rng=random.Random(7),
        )

        self.assertFalse(bot.schedules_equal(schedule, new_schedule))

        expected_lengths = [180, 180]
        for group in bot.GROUPS:
            light_lengths = [
                end - start for start, end in bot.extract_light_windows(new_schedule[group])
            ]
            self.assertEqual(sorted(light_lengths), expected_lengths)

    def test_yesterday_schedule_is_not_reused_for_q45_reverse(self):
        lines = [line.strip() for line in YESTERDAY_TEXT_Q45_33.splitlines() if line.strip()]
        schedule, slot_minutes = bot.parse_yesterday_schedule(lines)

        q = 4.5
        new_schedule = bot.build_schedule_from_yesterday(
            schedule=schedule,
            q=q,
            slot_minutes=slot_minutes,
            rng=random.Random(11),
        )

        self.assertFalse(bot.schedules_equal(schedule, new_schedule))

        expected_lengths = [120, 120, 120]
        for group in bot.GROUPS:
            light_lengths = [
                end - start for start, end in bot.extract_light_windows(new_schedule[group])
            ]
            self.assertEqual(sorted(light_lengths), expected_lengths)




if __name__ == "__main__":
    unittest.main()
