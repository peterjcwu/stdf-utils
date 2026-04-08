import os
import re
from typing import Dict
from stdf_utils.part_data import PartData
from stdf_utils.sql_conn import SqlConn
from stdf_utils.stdf_record import StdfRecord


class StdfToSql:
    def __init__(self, stdf_path: str):
        self.stdf_path: str = stdf_path
        self.stdf_id: int = 0
        self.sql_conn = SqlConn(os.path.join(os.path.dirname(stdf_path), "local.db"))
        self.part_data_site: Dict[int, PartData] = {}  # per site data {site: DieData}
        self.ptr_fact_dict: Dict[int, dict] = {}  # {test_num, ptr}
        self.handlers: dict = {
            "Mir": self.mir_handler,
            "Pir": self.pir_handler,
            "Dtr": self.dtr_handler,
            "Ptr": self.ptr_handler,
            "Prr": self.prr_handler,
            "Mrr": self.mrr_handler,
        }
        # read
        for rec_type, rec in StdfRecord(stdf_path, set(self.handlers.keys())):
            if self.handlers[rec_type](rec) is False:
                break

    def mir_handler(self, rec: dict) -> bool:
        stdf_name = re.sub(r"(\.stdf)(\.gz)?", "", os.path.basename(self.stdf_path), flags=re.I)
        if self.sql_conn.stdf_exists(stdf_name):
            print(f"{stdf_name} already exists")
            return False
        self.stdf_id = self.sql_conn.insert_stdf(rec, stdf_name)

    def pir_handler(self, rec: dict) -> bool:
        self.part_data_site[rec['SITE_NUM']] = PartData(rec['SITE_NUM'])  # reset
        return True

    def dtr_handler(self, rec: dict) -> bool:
        # parse ecid
        text = rec['TEXT_DAT'].decode()
        if text.startswith("ECID"):
            if m := re.search(r"ECID_VALID,\d+,(?P<site>\d+),(?P<valid>\d+)", text):
                self.part_data_site[int(m.group("site"))].ecid_valid = int(m.group("valid"))

            if m := re.search(r"ECID_FAB,\d+,(?P<site>\d+),(?P<fab>\w+)", text):
                self.part_data_site[int(m.group("site"))].ecid_fab = m.group("fab")

            if m := re.search(r"ECID_LOT_ID,\d+,(?P<site>\d+),(?P<lot_id>\w+)", text):
                self.part_data_site[int(m.group("site"))].ecid_lot_id = m.group("lot_id")

            if m := re.search(r"ECID_WAFER_ID,\d+,(?P<site>\d+),(?P<wafer_id>\w+)", text):
                self.part_data_site[int(m.group("site"))].ecid_wafer_id = m.group("wafer_id")

            if m := re.search(r"ECID_X_COORD,\d+,(?P<site>\d+),(?P<x_coord>\d+)", text):
                self.part_data_site[int(m.group("site"))].ecid_x_coord = int(m.group("x_coord"))

            if m := re.search(r"ECID_Y_COORD,\d+,(?P<site>\d+),(?P<y_coord>\d+)", text):
                self.part_data_site[int(m.group("site"))].ecid_y_coord = int(m.group("y_coord"))
        return True

    def ptr_handler(self, rec: dict) -> bool:
        self.part_data_site[rec['SITE_NUM']].update_ptr(rec)
        self.ptr_fact_dict[rec['TEST_NUM']] = rec  # TODO: check different test_text
        return True

    def prr_handler(self, rec: dict) -> bool:
        part_data = self.part_data_site[rec['SITE_NUM']]
        part_data.update_prr(rec, self.stdf_id)
        self.sql_conn.insert_part(part_data)
        self.sql_conn.insert_ptr(part_data)
        return True

    def mrr_handler(self, rec: dict) -> bool:
        self.sql_conn.insert_ptr_fact(self.stdf_id, self.ptr_fact_dict)
        self.sql_conn.conn.commit()  # remember to commit changes in the last record
        return True


if __name__ == '__main__':
    for cur_dir, dirs, file_names in os.walk(r"C:\log\2025\w538_bb_asecl_package_re_qual"):
        for file_name in file_names:
            if not re.search("(stdf)|(gz)$", file_name):
                print(file_name)
                continue
            file_path = os.path.join(cur_dir, file_name)
            StdfToSql(file_path)
