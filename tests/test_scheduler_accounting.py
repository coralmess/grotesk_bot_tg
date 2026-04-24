import unittest

from helpers.scheduler import SchedulerRunAccountant


class SchedulerRunAccountantTests(unittest.TestCase):
    def test_run_ids_increment_and_state_transitions_are_recorded(self):
        accountant = SchedulerRunAccountant("lyst")

        run = accountant.start_run(now_ts=1000)
        accountant.finish_run(run, "success", now_ts=1015)

        self.assertEqual(run.run_id, 1)
        self.assertEqual(accountant.state, "idle")
        self.assertEqual(accountant.last_outcome, "success")
        self.assertEqual(accountant.last_duration_sec, 15)

    def test_sleep_log_is_only_needed_when_bucket_changes(self):
        accountant = SchedulerRunAccountant("lyst")

        self.assertTrue(accountant.should_log_sleep(100))
        self.assertFalse(accountant.should_log_sleep(95))
        self.assertTrue(accountant.should_log_sleep(30))


if __name__ == "__main__":
    unittest.main()
