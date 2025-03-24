import os
from unittest import TestCase
from stdf_utils import StdfToTxt, StdfToECID


class TestStdfToTxt(TestCase):
    def setUp(self) -> None:
        # self.f = os.path.abspath(os.path.join(__file__, os.pardir, "data", "lot3.stdf.gz"))
        # self.f = r"C:\memo\FT\B230523-W1-TTT-FT-2\KT0RK_SYSD24BT1000_20240616101328.stdf.gz"
        self.f = r"C:\log\2025\w504_bb_cqs_r1543_msc\FR1_R2_PS205_KT4FR.07_SYSE02BVN000_20250130182119.stdf"

    def test_stdf_to_txt(self):
        txt_path = self.f.replace(".stdf", ".txt").replace(".gz", "")
        StdfToTxt(self.f, txt_path)
        # os.unlink(txt_path)
