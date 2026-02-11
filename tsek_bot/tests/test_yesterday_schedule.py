import random
import unittest

from tsek_bot import bot


YESTERDAY_TEXT = """
\uD83D\uDCE2 \u0428\u0430\u043D\u043E\u0432\u043D\u0456 \u0441\u043F\u043E\u0436\u0438\u0432\u0430\u0447\u0456! \u041F\u043E\u0432\u0456\u0434\u043E\u043C\u043B\u044F\u0454\u043C\u043E, \u0449\u043E \u0437\u0430 \u0440\u043E\u0437\u043F\u043E\u0440\u044F\u0434\u0436\u0435\u043D\u043D\u044F\u043C \u0434\u0438\u0441\u043F\u0435\u0442\u0447\u0435\u0440\u0441\u044C\u043A\u043E\u0433\u043E \u0446\u0435\u043D\u0442\u0440\u0443 \u041D\u0415\u041A \"\u0423\u043A\u0440\u0435\u043D\u0435\u0440\u0433\u043E\u0022, 11 \u041B\u042E\u0422\u041E\u0413\u041E \u0437 00:00 \u0434\u043E 24:00 \u0437\u0430\u0441\u0442\u043E\u0441\u043E\u0432\u0443\u0432\u0430\u0442\u0438\u043C\u0443\u0442\u044C\u0441\u044F \u0432\u0456\u0434\u043A\u043B\u044E\u0447\u0435\u043D\u043D\u044F \u043D\u0430\u0441\u0442\u0443\u043F\u043D\u0438\u0445 \u0447\u0435\u0440\u0433:
\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 1.1
04:00-09:00
11:30-18:30
20:30-24:00

\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 1.2
01:30-09:00
11:30-18:30
20:30-24:00

\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 2.1
00:00-01:30
06:30-11:30
13:30-20:30
22:30-24:00

\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 2.2
00:00-01:30
04:00-11:00
14:00-20:30

\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 3.1
00:00-04:00
08:00-13:30
15:30-22:30

\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 3.2
00:00-04:00
06:30-13:30
15:30-22:30

\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 4.1
04:00-11:30
13:30-20:30
22:30-24:00

\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 4.2
00:00-04:00
06:30-11:30
15:30-22:30

\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 5.1
00:00-06:30
09:00-15:30
20:30-24:00

\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 5.2
01:30-09:00
11:30-19:00
22:30-24:00

\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 6.1
00:00-06:30
09:00-15:30
18:30-24:00

\uD83D\uDD39 \u0427\u0435\u0440\u0433\u0430 6.2
00:00-06:30
09:00-15:30
18:30-24:00
"""


class YesterdayScheduleTest(unittest.TestCase):
    def test_yesterday_schedule_respects_min_light_window_and_start_group(self):
        lines = [line.strip() for line in YESTERDAY_TEXT.splitlines() if line.strip()]
        schedule, slot_minutes = bot.parse_yesterday_schedule(lines)

        q = 4.0
        base_group = bot.choose_start_group_from_yesterday(schedule)
        self.assertEqual(base_group, "6.1")

        min_window = bot.min_light_window_minutes(q)
        initial_age_by_group = {
            group: int(round(bot.off_before_day_end_minutes(schedule.get(group, [])) / min_window))
            for group in bot.GROUPS
        }

        new_schedule = bot.build_constant_schedule(
            q=q,
            slot_minutes=slot_minutes,
            start_group=base_group,
            anchor_offset=0,
            initial_age_by_group=initial_age_by_group,
            max_on_windows=bot.RULES[q].max_on_windows,
            max_consecutive_off_hours=bot.RULES[q].max_consecutive_off_hours,
            rng=random.Random(1),
        )

        max_allowed = bot.RULES[q].max_consecutive_off_hours
        if max_allowed is not None:
            max_off = max(
                (end - start) for group in bot.GROUPS for start, end in new_schedule[group]
            )
            self.assertLessEqual(max_off, int(max_allowed * 60))

        for group in bot.GROUPS:
            for start, end in bot.extract_light_windows(new_schedule[group]):
                self.assertGreaterEqual(end - start, 120, f"Light window too short for {group}")

        expected_off = bot.off_queue_count(q)
        for _, _, on_groups in bot.build_on_groups(new_schedule, slot_minutes):
            off_count = len(bot.GROUPS) - len(on_groups)
            self.assertEqual(off_count, expected_off)

        on_groups = []
        for group in bot.GROUPS:
            for start, end in bot.extract_light_windows(new_schedule[group]):
                if start <= 0 < end:
                    on_groups.append(group)
                    break
        self.assertIn("6.1", on_groups)


if __name__ == "__main__":
    unittest.main()
