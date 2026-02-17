import random
import unittest

from tsek_bot import bot
from tsek_bot.tests.test_mixed_patterns import YESTERDAY_Q5


class MiddayUpdateTest(unittest.TestCase):
    def test_parse_update_time_accepts_half_hour(self):
        self.assertEqual(bot.parse_update_time("00:00"), 0)
        self.assertEqual(bot.parse_update_time("13:30"), 13 * 60 + 30)
        with self.assertRaises(ValueError):
            bot.parse_update_time("13:15")
        with self.assertRaises(ValueError):
            bot.parse_update_time("24:00")

    def test_midday_update_preserves_prefix_and_equalizes_day(self):
        lines = [line.strip() for line in YESTERDAY_Q5.splitlines() if line.strip()]
        current_schedule, _ = bot.parse_yesterday_schedule(lines)

        update_from_minutes = bot.parse_update_time("12:00")
        updated_schedule, slot_minutes = bot.build_midday_updated_schedule(
            current_schedule=current_schedule,
            q=5.0,
            update_from_minutes=update_from_minutes,
            rng=random.Random(7),
        )

        self.assertEqual(slot_minutes, 30)

        current_on = bot.build_on_slot_flags(current_schedule, slot_minutes)
        updated_on = bot.build_on_slot_flags(updated_schedule, slot_minutes)
        cut_slot = update_from_minutes // slot_minutes
        slots_per_day = bot.MINUTES_PER_DAY // slot_minutes

        # Prefix must stay exactly as provided by the user.
        for group in bot.GROUPS:
            self.assertEqual(current_on[group][:cut_slot], updated_on[group][:cut_slot])

        # In updated suffix each slot must follow selected rule.
        expected_on_count = bot.on_queue_count(5.0)
        expected_off_count = bot.off_queue_count(5.0)
        for slot in range(cut_slot, slots_per_day):
            on_count = sum(updated_on[group][slot] for group in bot.GROUPS)
            self.assertEqual(on_count, expected_on_count)
            self.assertEqual(len(bot.GROUPS) - on_count, expected_off_count)

        # Total ON time per group for the full day must be equal.
        totals = [sum(updated_on[group]) for group in bot.GROUPS]
        self.assertEqual(len(set(totals)), 1)

        # New ON windows started in suffix must respect minimum ON window duration.
        min_run_slots = int(bot.min_light_window_minutes(5.0) // slot_minutes)
        for group in bot.GROUPS:
            flags = updated_on[group]
            slot = cut_slot
            while slot < slots_per_day:
                prev = flags[slot - 1] if slot > 0 else 0
                if flags[slot] == 1 and prev == 0:
                    end = slot
                    while end < slots_per_day and flags[end] == 1:
                        end += 1
                    self.assertGreaterEqual(end - slot, min_run_slots)
                    slot = end
                else:
                    slot += 1

    def test_midday_update_requires_and_uses_extra_groups_when_not_divisible(self):
        source_text = """
Графік відключень:
🔹 Черга 1.1
00:00 - 01:30
03:00 - 09:30
11:30 - 18:00
20:30 - 00:00

🔹 Черга 1.2
00:00 - 03:00
04:30 - 11:00
13:00 - 19:00
21:30 - 00:00

🔹 Черга 2.1
00:00 - 03:00
05:30 - 11:30
13:30 - 20:30
22:00 - 00:00

🔹 Черга 2.2
00:00 - 03:00
05:00 - 11:30
13:00 - 19:30
22:00 - 00:00

🔹 Черга 3.1
00:00 - 05:30
07:00 - 13:30
16:00 - 22:00

🔹 Черга 3.2
00:00 - 04:30
06:00 - 13:00
15:30 - 22:00

🔹 Черга 4.1
01:30 - 06:30
09:00 - 15:30
17:30 - 00:00

🔹 Черга 4.2
01:30 - 06:00
08:30 - 15:00
17:00 - 00:00

🔹 Черга 5.1
01:30 - 07:00
09:30 - 16:00
18:00 - 00:00

🔹 Черга 5.2
00:00 - 05:00
06:30 - 13:00
15:00 - 21:30

🔹 Черга 6.1
00:00 - 01:30
03:00 - 08:30
11:00 - 17:00
19:00 - 00:00

🔹 Черга 6.2
00:00 - 01:30
03:00 - 09:00
11:30 - 17:30
19:30 - 00:00
"""
        lines = [line.strip() for line in source_text.splitlines() if line.strip()]
        current_schedule, _ = bot.parse_yesterday_schedule(lines)

        update_from_minutes = bot.parse_update_time("14:00")
        with self.assertRaisesRegex(ValueError, "Extra light group selection required: 8"):
            bot.build_midday_updated_schedule(
                current_schedule=current_schedule,
                q=4.0,
                update_from_minutes=update_from_minutes,
                rng=random.Random(1),
            )

        selected = ["3.1", "3.2", "5.2", "1.1", "1.2", "4.1", "4.2", "2.1"]
        updated_schedule, slot_minutes = bot.build_midday_updated_schedule(
            current_schedule=current_schedule,
            q=4.0,
            update_from_minutes=update_from_minutes,
            extra_light_groups=selected,
            rng=random.Random(1),
        )
        self.assertEqual(slot_minutes, 30)

        current_on = bot.build_on_slot_flags(current_schedule, slot_minutes)
        updated_on = bot.build_on_slot_flags(updated_schedule, slot_minutes)
        cut_slot = update_from_minutes // slot_minutes

        # Prefix remains unchanged.
        for group in bot.GROUPS:
            self.assertEqual(current_on[group][:cut_slot], updated_on[group][:cut_slot])

        # Exactly rule on/off counts in suffix.
        expected_on_count = bot.on_queue_count(4.0)
        for slot in range(cut_slot, bot.MINUTES_PER_DAY // slot_minutes):
            self.assertEqual(sum(updated_on[group][slot] for group in bot.GROUPS), expected_on_count)

        # 8 selected groups get +0.5h against non-selected groups in full-day totals.
        totals = {group: sum(updated_on[group]) for group in bot.GROUPS}
        selected_set = set(selected)
        selected_values = {totals[g] for g in selected_set}
        non_selected_values = {totals[g] for g in bot.GROUPS if g not in selected_set}
        self.assertEqual(len(selected_values), 1)
        self.assertEqual(len(non_selected_values), 1)
        self.assertEqual(next(iter(selected_values)) - next(iter(non_selected_values)), 1)

    def test_enumerate_feasible_extra_group_variants(self):
        source_text = """
Графік відключень:
🔹 Черга 1.1
00:00 - 01:30
03:00 - 09:30
11:30 - 18:00
20:30 - 00:00

🔹 Черга 1.2
00:00 - 03:00
04:30 - 11:00
13:00 - 19:00
21:30 - 00:00

🔹 Черга 2.1
00:00 - 03:00
05:30 - 11:30
13:30 - 20:30
22:00 - 00:00

🔹 Черга 2.2
00:00 - 03:00
05:00 - 11:30
13:00 - 19:30
22:00 - 00:00

🔹 Черга 3.1
00:00 - 05:30
07:00 - 13:30
16:00 - 22:00

🔹 Черга 3.2
00:00 - 04:30
06:00 - 13:00
15:30 - 22:00

🔹 Черга 4.1
01:30 - 06:30
09:00 - 15:30
17:30 - 00:00

🔹 Черга 4.2
01:30 - 06:00
08:30 - 15:00
17:00 - 00:00

🔹 Черга 5.1
01:30 - 07:00
09:30 - 16:00
18:00 - 00:00

🔹 Черга 5.2
00:00 - 05:00
06:30 - 13:00
15:00 - 21:30

🔹 Черга 6.1
00:00 - 01:30
03:00 - 08:30
11:00 - 17:00
19:00 - 00:00

🔹 Черга 6.2
00:00 - 01:30
03:00 - 09:00
11:30 - 17:30
19:30 - 00:00
"""
        lines = [line.strip() for line in source_text.splitlines() if line.strip()]
        current_schedule, _ = bot.parse_yesterday_schedule(lines)
        _, _, _, _, extra_count = bot.analyze_midday_update_distribution(
            current_schedule=current_schedule,
            q=4.0,
            update_from_minutes=bot.parse_update_time("14:00"),
        )
        self.assertEqual(extra_count, 8)

        feasible = bot.enumerate_feasible_extra_group_schedules(
            current_schedule=current_schedule,
            q=4.0,
            update_from_minutes=bot.parse_update_time("14:00"),
            extra_group_count=extra_count,
            max_attempts_per_variant=120,
        )
        self.assertGreater(len(feasible), 0)

        sample_key = bot.normalize_selected_groups_key(
            ["3.1", "3.2", "5.2", "1.1", "1.2", "4.1", "4.2", "2.1"]
        )
        self.assertIn(sample_key, feasible)

    def test_relaxed_update_has_no_30min_new_light_windows(self):
        source_text = """
Графік відключень:
🔹 Черга 1.1
00:00 - 01:30
03:00 - 09:30
11:30 - 16:30
20:00 - 00:00

🔹 Черга 1.2
00:00 - 03:00
04:30 - 11:00
13:00 - 20:30

🔹 Черга 2.1
00:00 - 03:00
05:30 - 11:30
13:30 - 21:30

🔹 Черга 2.2
00:00 - 03:00
05:00 - 11:30
13:00 - 17:00
20:30 - 00:00

🔹 Черга 3.1
00:00 - 05:30
07:00 - 13:30
19:00 - 00:00

🔹 Черга 3.2
00:00 - 04:30
06:00 - 13:00
14:00 - 19:30

🔹 Черга 4.1
01:30 - 06:30
09:00 - 14:00
17:00 - 00:00

🔹 Черга 4.2
01:30 - 06:00
08:30 - 14:00
17:00 - 00:00

🔹 Черга 5.1
01:30 - 07:00
09:30 - 14:00
16:30 - 00:00

🔹 Черга 5.2
00:00 - 05:00
06:30 - 13:00
14:00 - 20:00

🔹 Черга 6.1
00:00 - 01:30
03:00 - 08:30
11:00 - 17:00
19:30 - 00:00

🔹 Черга 6.2
00:00 - 01:30
03:00 - 09:00
11:30 - 19:00
21:30 - 00:00
"""
        lines = [line.strip() for line in source_text.splitlines() if line.strip()]
        schedule, _ = bot.parse_yesterday_schedule(lines)
        update_from = bot.parse_update_time("19:00")
        updated, slot_minutes, _ = bot.build_midday_updated_schedule_relaxed(
            current_schedule=schedule,
            q=4.5,
            update_from_minutes=update_from,
            rng=random.Random(3),
        )
        self.assertEqual(slot_minutes, 30)
        on_flags = bot.build_on_slot_flags(updated, slot_minutes)
        cut_slot = update_from // slot_minutes
        min_run_slots = 2  # 1 hour in 30-minute slots
        for group in bot.GROUPS:
            slot = cut_slot
            while slot < bot.MINUTES_PER_DAY // slot_minutes:
                prev = on_flags[group][slot - 1] if slot > 0 else 0
                if on_flags[group][slot] == 1 and prev == 0:
                    end = slot
                    while end < bot.MINUTES_PER_DAY // slot_minutes and on_flags[group][end] == 1:
                        end += 1
                    self.assertGreaterEqual(end - slot, min_run_slots)
                    slot = end
                else:
                    slot += 1


if __name__ == "__main__":
    unittest.main()
