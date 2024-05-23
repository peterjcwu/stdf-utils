import os
from unittest import TestCase
from stdf_utils import StdfToTxt


class TestStdfToTxt(TestCase):
    def setUp(self) -> None:
        self.f = r"C:\log\w415_redfinch_r2gate_r2183_correlation\KN5YT12_1_KN5YT12-D1_20240502010458.stdf.gz"

    def test_stdf_to_txt(self):
        txt_path = self.f.replace(".stdf", ".txt").replace(".gz", "")
        StdfToTxt(self.f, txt_path)
        # os.unlink(txt_path)
