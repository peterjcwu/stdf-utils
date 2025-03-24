import os
from unittest import TestCase
from stdf_utils import StdfRecord


class TestStdfRecord(TestCase):
    def setUp(self) -> None:
        self.f = os.path.abspath(os.path.join(__file__, os.pardir, "data", "lot3.stdf.gz"))

    def test_stdf_record_open_file(self):
        for i, (rec_type, record) in enumerate(StdfRecord(self.f)):
            print(i, rec_type, record)
            if i > 100:
                break

