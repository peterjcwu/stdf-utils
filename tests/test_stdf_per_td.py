import os
from unittest import TestCase
from stdf_utils import StdfPerTD, OpenFile


class TestStdfPerTd(TestCase):
    def setUp(self) -> None:
        self.f = os.path.abspath(os.path.join(__file__, os.pardir, "data", "lot2.stdf.gz"))

    def test_stdf_per_td(self):
        for i, td in enumerate(StdfPerTD(self.f)):
            assert "mir" in td.keys()
            assert "prr" in td.keys()
            assert "ptr" in td.keys()
