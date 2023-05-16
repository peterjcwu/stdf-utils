import os
from unittest import TestCase
from stdf_utils import OpenFile, StdfRecord


class TestStdfRecord(TestCase):
    def setUp(self) -> None:
        self.f = os.path.abspath(os.path.join(__file__, os.pardir, "data", "lot3.stdf.gz"))

    def test_stdf_record_open_file(self):
        with OpenFile(self.f) as f_in:
            for rec_type, record in StdfRecord(f_in):
                print(rec_type, record)
