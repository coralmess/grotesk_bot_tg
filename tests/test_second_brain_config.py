import tempfile
import unittest
from pathlib import Path

from helpers.analytics_events import AnalyticsSink
from second_brain_bot.bot import build_ai_orchestrator
from second_brain_bot.config import SecondBrainConfig


class SecondBrainConfigTests(unittest.TestCase):
    def test_build_ai_orchestrator_adds_gemini_provider_when_key_is_configured(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = SecondBrainConfig(
                bot_token="bot-token",
                owner_chat_id=123,
                vault_dir=Path(tmp) / "vault",
                digest_hour=13,
                digest_tz="Europe/Kyiv",
                modal_glm_api_key="",
                cerebras_api_key="",
                groq_api_key="",
                gemini_api_key="gemini-key",
            )

            orchestrator = build_ai_orchestrator(
                config,
                analytics_sink=AnalyticsSink(Path(tmp) / "analytics", now_func=lambda: "2026-05-13T00:00:00Z"),
            )

            self.assertIn("gemini", orchestrator.providers)
            self.assertEqual(orchestrator.providers["gemini"].model, "gemini-3-flash-preview")
            self.assertEqual(orchestrator.providers["gemini"].base_url, "https://generativelanguage.googleapis.com/v1beta/openai")


if __name__ == "__main__":
    unittest.main()
