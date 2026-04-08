import re
import csv
from collections import defaultdict
from stdf_utils.stdf_record import StdfRecord


class StdfToCsvEcid:
    def __init__(self, stdf_path: str, csv_path: str = None):
        self.stdf_path = stdf_path
        self.csv_path = csv_path or stdf_path.replace(".gz", "").replace(".stdf", "_raw.csv")
        self.cache = defaultdict(list)
        self.handlers = {
            "Ptr": self.ptr_handler,
            "Dtr": self.dtr_handler,
        }
        # read
        for rec_type, rec in StdfRecord(self.stdf_path, set(self.handlers.keys())):
            self.handlers[rec_type](rec)

        # write
        self._to_csv()

    def ptr_handler(self, rec: dict):
        if 3680 <= rec["TEST_NUM"] <= 3889:
            self.cache[rec["TEST_NUM"]].append(rec["RESULT"])

    def dtr_handler(self, rec: dict):
        if re.search(r"^ECID", rec["TEXT_DAT"].decode()):
            print(rec["TEXT_DAT"])

    def _to_csv(self):
        pass


if __name__ == '__main__':
    StdfToCsvEcid( r"C:\log\w449_bb_a1_char_digital_svc_v11\qfn_char_dig\W1Vmax_KT0RN.36_SYSD42CIE000_20241018020624.stdf")
