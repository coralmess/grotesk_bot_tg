import unittest

from deploy.restart_changed_services import SERVICE_RULES, should_restart


class RestartChangedServicesTests(unittest.TestCase):
    def test_tracked_service_units_are_all_represented(self) -> None:
        self.assertIn("grotesk-market.service", SERVICE_RULES)
        self.assertIn("grotesk-lyst.service", SERVICE_RULES)
        self.assertIn("usefulbot.service", SERVICE_RULES)
        self.assertIn("svitlobot.service", SERVICE_RULES)

    def test_svitlobot_restarts_for_its_runtime_changes(self) -> None:
        self.assertTrue(should_restart("svitlobot.service", ["svitlo_bot.py"]))
        self.assertTrue(should_restart("svitlobot.service", ["svitlobot.service"]))
        self.assertFalse(should_restart("svitlobot.service", ["olx_scraper.py"]))

    def test_market_restart_rules_still_match_market_files(self) -> None:
        self.assertTrue(should_restart("grotesk-market.service", ["helpers/telegram_runtime.py"]))
        self.assertTrue(should_restart("grotesk-market.service", ["config_shafa_urls.py"]))
        self.assertFalse(should_restart("grotesk-market.service", ["useful_bot/index.py"]))

    def test_usefulbot_restart_rules_cover_shared_runtime_paths(self) -> None:
        self.assertTrue(should_restart("usefulbot.service", ["useful_bot/index.py"]))
        self.assertTrue(should_restart("usefulbot.service", ["helpers/runtime_paths.py"]))
        self.assertFalse(should_restart("usefulbot.service", ["svitlo_bot.py"]))


if __name__ == "__main__":
    unittest.main()
