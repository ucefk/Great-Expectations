import unittest
from unittest import TestCase


class TestMain(TestCase):
    def setUp(self) -> None:
        self.main = "main"

    def test_dumb_main(self) -> None:
        self.assertEqual(self.main, "main")


if __name__ == "__main__":
    unittest.main()
