import tempfile
import unittest
from pathlib import Path
from unittest import mock

from helpers import lyst_identity


class LystIdentityTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.browser_root = Path(self._tmpdir.name)
        patch_browser_dir = mock.patch.object(lyst_identity, "LYST_BROWSER_DIR", self.browser_root / "lyst")
        patch_cache_dir = mock.patch.object(lyst_identity, "LYST_BROWSER_CACHE_DIR", self.browser_root / "lyst" / "cache")
        patch_state_dir = mock.patch.object(lyst_identity, "LYST_STORAGE_STATE_DIR", self.browser_root / "lyst" / "storage_state")
        patch_browser_dir.start()
        patch_cache_dir.start()
        patch_state_dir.start()
        self.addCleanup(patch_browser_dir.stop)
        self.addCleanup(patch_cache_dir.stop)
        self.addCleanup(patch_state_dir.stop)

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_country_storage_state_path_creates_directory(self) -> None:
        state_path = lyst_identity.country_storage_state_path("pl")
        self.assertEqual(state_path.name, "PL.json")
        self.assertTrue(state_path.parent.exists())

    def test_browser_launch_args_include_disk_cache_dir(self) -> None:
        args = lyst_identity.browser_launch_args()
        self.assertTrue(any(arg.startswith("--disk-cache-dir=") for arg in args))
        self.assertIn("--disk-cache-size=1073741824", args)


if __name__ == "__main__":
    unittest.main()
