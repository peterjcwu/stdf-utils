import os
from unittest import TestCase
from stdf_utils import StdfToTxt


class TestStdfToTxt(TestCase):
    def setUp(self) -> None:
        self.f = os.path.abspath(os.path.join(__file__, os.pardir, "data", "lot3.stdf.gz"))

    def test_stdf_to_txt(self):
        txt_path = self.f.replace(".stdf", ".txt").replace(".gz", "")
        StdfToTxt(self.f, txt_path)
        os.unlink(txt_path)
