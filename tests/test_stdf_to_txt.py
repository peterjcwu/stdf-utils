import os
from unittest import TestCase
from stdf_utils import StdfToTxt


class TestStdfToTxt(TestCase):
    def setUp(self) -> None:
        self.f = r"C:\wireless_test\Blackbird\ATE_Data\A0_Verification\STR\char_dig_qfn\fr1\noSpecsearch_room\KT0GP.38_SYSD08AHF000_20240224101518.stdf"
        self.f = r"C:\log\w411_rf_stress_binning_wrong\KR4LF_SYPD07CBY000_20240218232154.stdf.gz"

    def test_stdf_to_txt(self):
        txt_path = self.f.replace(".stdf", ".txt").replace(".gz", "")
        StdfToTxt(self.f, txt_path)
        # os.unlink(txt_path)
