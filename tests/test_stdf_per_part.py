import os
from unittest import TestCase
from stdf_utils import StdfPerPart


class TestStdfPerTd(TestCase):
    def setUp(self) -> None:
        self.f = os.path.abspath(os.path.join(__file__, os.pardir, "data", "lot2.stdf.gz"))

    def test_stdf_per_td(self):
        last_td = {}
        for td in StdfPerPart(self.f):
            last_td = td
            self.assertIn("mir", td.keys())
            self.assertIn("prr", td.keys())
            self.assertIn("ptr", td.keys())
        else:
            self.assertIn("finish_t", last_td["mir"].keys())
