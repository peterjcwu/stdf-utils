import csv
import statistics
from collections import defaultdict
from .stdf_record import StdfRecord
from .util import OpenFile


class StdfReader:
    def __init__(self, file_path: str):
        self.ptr_container = PTRContainer()
        self.handlers = {
            "Ptr": self.ptr_handler,
        }
        with OpenFile(file_path) as f_in:
            for rec_type, rec in StdfRecord(f_in, set(self.handlers.keys())):
                self.handlers[rec_type](rec)

    def ptr_handler(self, rec: dict):
        self.ptr_container.push(rec)

    def export_csv(self, fp: str):
        fieldnames = ["Test ID", "Site", "Name", "Execs", "Fails", "Low Lim", "High Lim", "Min", "Max", "Mean"]
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for test_name, sites in self.ptr_container.data.items():
            for i, site_data in enumerate(sites):
                data = [d["RESULT"] for d in site_data]
                if len(data) == 0:
                    continue
                writer.writerow({
                    "Test ID": site_data[0]["TEST_NUM"],
                    "Site": site_data[0]["SITE_NUM"],
                    "Name": test_name,
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
