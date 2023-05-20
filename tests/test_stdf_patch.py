import os
import re
from unittest import TestCase
from stdf_utils import StdfPatch


class TestStdfPatch(TestCase):
    def setUp(self) -> None:
        self.f = os.path.abspath(os.path.join(__file__, os.pardir, "data", "lot3.stdf.gz"))

    def test_stdf_patch(self):
        stdf_patch = StdfPatch(self.f, patch_func=self.patch_func)
        os.unlink(stdf_patch.mod_stdf_path)

    @staticmethod
    def patch_func(rec_type: str, record: dict, buffer: bytes) -> bytes:
        if rec_type == "Dtr":
            text = record["TEXT_DAT"]
            if re.search(r"COND:.*(wmark|alarm)=", text) or re.search("CHARINFO", text):
                print(text)
                return b''
        return buffer
