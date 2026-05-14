import tempfile
import unittest
from pathlib import Path

from helpers.analytics_events import AnalyticsSink
from second_brain_bot.ai import GroqCompoundProvider
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
            self.assertIn("gemini_flash_lite", orchestrator.providers)
            self.assertIn("gemma_31b", orchestrator.providers)
            self.assertEqual(orchestrator.providers["gemini"].model, "gemini-3-flash-preview")
            self.assertEqual(orchestrator.providers["gemini_flash_lite"].model, "gemini-3.1-flash-lite")
            self.assertEqual(orchestrator.providers["gemma_31b"].model, "gemma-4-31b-it")
            self.assertEqual(orchestrator.providers["gemini"].reasoning_effort, "")
            self.assertEqual(orchestrator.providers["gemini_flash_lite"].reasoning_effort, "high")
            self.assertEqual(orchestrator.providers["gemma_31b"].reasoning_effort, "high")
            self.assertEqual(orchestrator.providers["gemini"].base_url, "https://generativelanguage.googleapis.com/v1beta/openai")
            self.assertEqual(orchestrator.providers["gemini_flash_lite"].base_url, "https://generativelanguage.googleapis.com/v1beta/openai")
            self.assertEqual(orchestrator.providers["gemma_31b"].base_url, "https://generativelanguage.googleapis.com/v1beta/openai")
            self.assertEqual(orchestrator.provider_daily_limits["gemini"], 20)
            self.assertEqual(orchestrator.provider_daily_limits["gemini_flash_lite"], 500)
            self.assertEqual(orchestrator.provider_daily_limits["gemma_31b"], 100)

    def test_build_ai_orchestrator_uses_groq_compound_provider(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = SecondBrainConfig(
                bot_token="bot-token",
                owner_chat_id=123,
                vault_dir=Path(tmp) / "vault",
                digest_hour=13,
                digest_tz="Europe/Kyiv",
                modal_glm_api_key="",
                cerebras_api_key="",
                groq_api_key="groq-key",
            )

            orchestrator = build_ai_orchestrator(
                config,
                analytics_sink=AnalyticsSink(Path(tmp) / "analytics", now_func=lambda: "2026-05-13T00:00:00Z"),
            )

            self.assertIsInstance(orchestrator.providers["groq"], GroqCompoundProvider)
            self.assertEqual(orchestrator.providers["groq"].model, "groq/compound")


if __name__ == "__main__":
    unittest.main()
