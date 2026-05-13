import unittest

from second_brain_bot.web_lookup import should_allow_public_lookup


class SecondBrainWebLookupPolicyTests(unittest.TestCase):
    def test_default_enrichment_does_not_use_web_lookup(self) -> None:
        self.assertFalse(should_allow_public_lookup("A useful knife brand", explicit=False))

    def test_explicit_command_allows_lookup(self) -> None:
        self.assertTrue(should_allow_public_lookup("A useful knife brand", explicit=True))

    def test_time_sensitive_capture_allows_lookup(self) -> None:
        self.assertTrue(should_allow_public_lookup("Check latest price and availability for this knife", explicit=False))


if __name__ == "__main__":
    unittest.main()
