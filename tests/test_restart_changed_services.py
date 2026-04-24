import unittest
from unittest.mock import call, patch

from deploy.restart_changed_services import (
    PROJECT_ROOT,
    SERVICE_RULES,
    changed_unit_files,
    install_changed_unit_files,
    should_restart,
    verify_service_active,
)


class RestartChangedServicesTests(unittest.TestCase):
    def test_tracked_service_units_are_all_represented(self) -> None:
        self.assertIn("grotesk-market.service", SERVICE_RULES)
        self.assertIn("grotesk-lyst.service", SERVICE_RULES)
        self.assertIn("auto-ria-bot.service", SERVICE_RULES)
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

    def test_auto_ria_restart_rules_cover_runtime_and_config(self) -> None:
        self.assertTrue(should_restart("auto-ria-bot.service", ["config_auto_ria_urls.py"]))
        self.assertTrue(should_restart("auto-ria-bot.service", ["helpers/auto_ria/runtime.py"]))
        self.assertTrue(should_restart("auto-ria-bot.service", ["auto-ria-bot.service"]))
        self.assertFalse(should_restart("auto-ria-bot.service", ["config_shafa_urls.py"]))

    def test_changed_unit_files_only_returns_tracked_service_units(self) -> None:
        self.assertEqual(
            changed_unit_files(["auto-ria-bot.service", "README.md", "unknown.service"]),
            ["auto-ria-bot.service"],
        )

    @patch("deploy.restart_changed_services.subprocess.run")
    def test_install_changed_unit_files_copies_units_and_reloads_systemd(self, mock_run) -> None:
        install_changed_unit_files(["auto-ria-bot.service", "config.py"])

        self.assertEqual(
            mock_run.call_args_list,
            [
                call(
                    ["sudo", "cp", str(PROJECT_ROOT / "auto-ria-bot.service"), "/etc/systemd/system/auto-ria-bot.service"],
                    check=True,
                ),
                call(["sudo", "systemctl", "daemon-reload"], check=True),
            ],
        )

    @patch("deploy.restart_changed_services.subprocess.run")
    def test_verify_service_active_checks_systemd_state(self, mock_run) -> None:
        verify_service_active("auto-ria-bot.service")

        mock_run.assert_called_once_with(
            ["systemctl", "is-active", "--quiet", "auto-ria-bot.service"],
            check=True,
        )


if __name__ == "__main__":
    unittest.main()
