import unittest
from set_release_version import release_version


class ReleaseVersionTest(unittest.TestCase):
    def test_shared_and_incrementing(self):
        self.assertEqual(release_version("1.0.211", "20", "1", "abcdef012345"), "1.0.231-ci1-abcdef0")
        self.assertEqual(release_version("1.0.211-old-webui_123", "21", "1", "abcdef012345"), "1.0.232-ci1-abcdef0")
        self.assertEqual(release_version("1.0.211", "20", "2", "abcdef012345"), "1.0.231-ci2-abcdef0")

    def test_reject_invalid(self):
        for base, run, attempt, sha in [("bad", "1", "1", "abcdef0"), ("1.0.211", "0", "1", "abcdef0"),
                                        ("1.0.211", "1", "0", "abcdef0"), ("1.0.211", "1", "1", "bad")]:
            with self.assertRaises(ValueError):
                release_version(base, run, attempt, sha)


if __name__ == "__main__":
    unittest.main()
