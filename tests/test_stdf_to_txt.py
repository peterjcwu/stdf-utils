from unittest import TestCase
from stdf_utils import StdfRecord
from util import OpenFile


class TestStdfToTxt(TestCase):
    def setUp(self) -> None:
        # self.f = r"C:\Users\nxf79056\OneDrive - NXP\log\2025\w542_bb2_fowlp_buck_limit_study\20251031184042_fr1_LV93K_A2024250_ENG_r1827_SYSE03CP1400_B240924_TTT_86_80_28_89_81_33_87_94_swap_BUCK_LD_50_250_450mA.stdf"
        self.f = r"C:\Users\nxf79056\Downloads\FT_03112026_000234.std"
    def test_stdf_to_txt(self):
        txt_path = (self.f
                    .replace(".stdf", ".txt")
                    .replace(".std", ".txt")
                    .replace(".gz", ""))

        with open(txt_path, "w", newline="") as f_out:
            for rec_type, rec in StdfRecord(self.f):
                f_out.write(f"=== {rec_type} ===\r\n")
                for k, v in rec.items():
                    f_out.write(f"{k}: {v}\r\n")
