import re
import csv
import statistics
from collections import defaultdict
from stdf_utils.stdf_record import StdfRecord


class StdfToCsvRaw:
    def __init__(self, stdf_path: str, csv_path: str = None):
        self.stdf_path = stdf_path
        self.tag = stdf_path.split("_")[0]
        self.csv_path = csv_path or stdf_path.replace(".gz", "").replace(".stdf", "_raw.csv")
        self.ptr_container = PTRContainer()
        self.cache = defaultdict(list)
        self.handlers = {
            "Ptr": self.ptr_handler,
            "Dtr": self.dtr_handler,
        }
        # read
        for rec_type, rec  in StdfRecord(self.stdf_path, set(self.handlers.keys())):
            self.handlers[rec_type](rec)

        # write
        self._to_csv()

    def ptr_handler(self, rec: dict):
        if rec["TEST_NUM"]  in {3232, 7100, 8652}:
            self.cache[rec["TEST_NUM"]].append(rec["RESULT"])

    def dtr_handler(self, rec: dict):
        if re.search(r"^ECID", rec["TEXT_DAT"].decode()):
            print(rec["TEXT_DAT"])

    def _to_csv(self):
        fieldnames = ["Test ID", "Site", "Name", "Execs", "Fails", "Low Lim", "High Lim", "Min", "Max", "Mean"]
        with open(self.csv_path, "w", newline="") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=fieldnames)
            writer.writeheader()
            for test_name, sites in self.ptr_container.data.items():
                for i, site_data in enumerate(sites):
                    data = [d["RESULT"] for d in site_data]
                    if len(data) == 0:
                        continue
                    writer.writerow({
                        "Test ID": site_data[0]["TEST_NUM"],
                        "Site": site_data[0]["SITE_NUM"],
                        "Name": test_name.decode(),
                        "Execs": len(data),
                        "Fails": len([d for d in data if d < site_data[0]["LO_LIMIT"] or d > site_data[0]["HI_LIMIT"]]),
                        "Low Lim": site_data[0]["LO_LIMIT"],
                        "High Lim": site_data[0]["HI_LIMIT"],
                        "Min": min(data),
                        "Max": max(data),
                        "Mean": statistics.mean(data),
                    })


class PTRContainer:
    def __init__(self, key_type: str = "name"):
        self._key_type = key_type
        self.data = defaultdict(list)

    def get_key(self, rec: dict):
        if self._key_type == "name":
            return rec['TEST_TXT']
        elif self._key_type == 'test_id':
            return rec['TEST_NUM']
        else:
            raise TypeError(f"key type {self._key_type} is not supported")

    def push(self, rec: dict):
        key = self.get_key(rec)
        site = rec['SITE_NUM']
        while len(self.data[key]) <= site:
            self.data[key].append([])
        self.data[key][site].append(rec)


if __name__ == '__main__':
    StdfToCsvRaw( r"C:\log\w449_bb_a1_char_digital_svc_v11\qfn_char_dig\W1Vmax_KT0RN.36_SYSD42CIE000_20241018020624.stdf")
