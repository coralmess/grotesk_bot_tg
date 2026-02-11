import unittest
import random

from tsek_bot import bot

YESTERDAY_TEXT_55 = """
\u0413\u0440\u0430\u0444\u0456\u043A \u0432\u0456\u0434\u043A\u043B\u044E\u0447\u0435\u043D\u044C:
\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 1.1
00:00 - 01:00
03:00 - 16:00
18:00 - 00:00

\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 1.2
01:00 - 08:00
10:00 - 23:00

\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 2.1
00:00 - 03:00
05:00 - 12:00
14:00 - 00:00

\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 2.2
00:00 - 05:00
07:00 - 20:00
22:00 - 00:00

\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 3.1
00:00 - 10:00
12:00 - 19:00
21:00 - 00:00

\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 3.2
00:00 - 06:00
08:00 - 15:00
17:00 - 00:00

\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 4.1
00:00 - 13:00
15:00 - 22:00

\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 4.2
00:00 - 02:00
04:00 - 17:00
19:00 - 00:00

\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 5.1
00:00 - 09:00
11:00 - 18:00
20:00 - 00:00

\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 5.2
02:00 - 15:00
17:00 - 00:00

\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 6.1
00:00 - 11:00
13:00 - 20:00
22:00 - 00:00

\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 6.2
00:00 - 07:00
09:00 - 22:00
"""


class YesterdayScheduleOrder55Test(unittest.TestCase):
    def test_priority_order_for_55(self):
        lines = [line.strip() for line in YESTERDAY_TEXT_55.splitlines() if line.strip()]
        schedule, slot_minutes = bot.parse_yesterday_schedule(lines)

        q = 5.5
        base_group = bot.choose_start_group_from_yesterday(schedule)
        self.assertEqual(base_group, "2.1")

        min_window = bot.min_light_window_minutes(q)
        initial_age_by_group = {
            group: int(round(bot.off_before_day_end_minutes(schedule.get(group, [])) / min_window))
            for group in bot.GROUPS
        }
        priority_minutes_by_group = {
            group: bot.off_before_day_end_minutes(schedule.get(group, [])) for group in bot.GROUPS
        }

        new_schedule = bot.build_constant_schedule(
            q=q,
            slot_minutes=slot_minutes,
            start_group=base_group,
            anchor_offset=0,
            initial_age_by_group=initial_age_by_group,
            priority_minutes_by_group=priority_minutes_by_group,
            max_on_windows=bot.RULES[q].max_on_windows,
            max_consecutive_off_hours=bot.RULES[q].max_consecutive_off_hours,
            rng=random.Random(1),
        )

        on_groups = {}
        for group in bot.GROUPS:
            for start, end in bot.extract_light_windows(new_schedule[group]):
                if start <= 0 < end:
                    on_groups[0] = group
                if start <= 60 < end:
                    on_groups[60] = group
                if start <= 120 < end:
                    on_groups[120] = group
                if start <= 180 < end:
                    on_groups[180] = group

        self.assertEqual(on_groups.get(0), "2.1")
        self.assertEqual(on_groups.get(60), "3.2")
        self.assertEqual(on_groups.get(120), "5.2")
        self.assertEqual(on_groups.get(180), "1.1")


if __name__ == "__main__":
    unittest.main()
