import os
import hashlib
from unittest import TestCase
from stdf_utils import StdfToCsv


class TestStdfToCsv(TestCase):
    def setUp(self) -> None:
        self.f = os.path.abspath(os.path.join(__file__, os.pardir, "data", "lot3.stdf.gz"))
        self.expected_csv = os.path.abspath(os.path.join(self.f, os.pardir, "expected", "lot3.csv"))

    def test_stdf_to_csv(self):
        stdf_to_csv = StdfToCsv(self.f)
        self.assertEqual(self._get_md5(self.expected_csv), self._get_md5(stdf_to_csv.csv_path))
        os.unlink(stdf_to_csv.csv_path)

    @staticmethod
    def _get_md5(f: str):
        return hashlib.md5(open(f, 'rb').read()).hexdigest()
