import re
import sqlite3
from typing import Optional, Dict

from stdf_utils.part_data import PartData


class SqlConn:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        # create table
        self._create_stdf()
        self._create_part()
        self._create_ptr()
        self._create_ptr_fact()
        self.conn.commit()

    # Stdf
    def _create_stdf(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS Stdf (
                stdf_id INTEGER PRIMARY KEY,
                stdf_name VARCHAR(256) NOT NULL UNIQUE,
                job VARCHAR(256),
                temperature INTEGER,
                bias VARCHAR(32),
                lot_id VARCHAR(128),
                tag VARCHAR(128)
            );""")

    def insert_stdf(self, mir_rec: dict, stdf_name: str) -> int:
        """ Insert into table Stdf and return stdf_id """

        job = mir_rec["JOB_NAM"].decode()
        temperature = self._parse_int(mir_rec["TST_TEMP"].decode())
        lot_id = mir_rec["LOT_ID"].decode()
        tag = "_".join(stdf_name.split("_")[:2])
        self.cursor.execute(
            "INSERT INTO Stdf (stdf_name, job, temperature, lot_id, tag) Values (?, ?, ?, ?, ?) "
            "ON CONFLICT (stdf_name) DO NOTHING;",
            (stdf_name, job, temperature, lot_id, tag))
        # query stdf_id
        return self.cursor.execute(f"SELECT stdf_id from Stdf WHERE STDF_NAME = '{stdf_name}'").fetchone()[0]

    # Part
    def _create_part(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS Part (
            stdf_id INTEGER,
            part_id INTEGER,
            site INTEGER,
            ecid VARCHAR(256) NOT NULL,
            ecid_wafer_id VARCHAR(32),
            ecid_lot_id VARCHAR(32),
            ecid_x_coord INTEGER,
            ecid_y_coord INTEGER,
            soft_bin INTEGER NOT NULL,
            hard_bin INTEGER NOT NULl,
            PRIMARY KEY (stdf_id, part_id)
        );""")

    def insert_part(self, part_data: PartData):
        self.cursor.execute(
            "INSERT INTO Part (site, stdf_id, ecid_wafer_id, ecid_lot_id, ecid, soft_bin, hard_bin, part_id)"
            " Values (?, ?, ?, ?, ?, ?, ?, ?)",
            (part_data.site, part_data.stdf_id, part_data.ecid_wafer_id, part_data.ecid_lot_id, part_data.ecid,
             part_data.soft_bin, part_data.hard_bin, part_data.part_id)
        )

    # Ptr
    def _create_ptr(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS Ptr (
            stdf_id INTEGER NOT NULL,
            part_id INTEGER NOT NULL,
            test_num INTEGER NOT NULL,
            result REAL NOT NULL,
            PRIMARY KEY (stdf_id, part_id, test_num)
        );""")

    def insert_ptr(self, part_data: PartData):
        self.cursor.executemany(
            "INSERT INTO Ptr (stdf_id, part_id, test_num, result) Values (?, ?, ?, ?)",
            ((part_data.stdf_id, part_data.part_id, ptr.test_num, ptr.result)
             for ptr in part_data.ptr_list)
        )

    # Ptr Fact
    def _create_ptr_fact(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS PtrFact (
            stdf_id INTEGER,
            test_num INTEGER,
            test_name VARCHAR(256),
            lo_lim REAL,
            hi_lim REAL,
            PRIMARY KEY (stdf_id, test_num)
        );""")

    def insert_ptr_fact(self, stdf_id: int, ptr_fact_dict: Dict[int, dict]):
        self.cursor.executemany("""
        INSERT INTO PtrFact (stdf_id, test_num, test_name, lo_lim, hi_lim)
            Values (?, ?, ?, ?, ?)""", ((stdf_id, test_num, ptr['TEST_TXT'].decode(), ptr['LO_LIMIT'], ptr['HI_LIMIT'])
                                        for (test_num, ptr) in ptr_fact_dict.items()))

    @staticmethod
    def _parse_int(value: str) -> Optional[int]:
        try:
            return int(re.sub(r"[mn]", "-", value, flags=re.I))
        except ValueError:
            return None

    def stdf_exists(self, stdf_name: str) -> bool:
        return self.cursor.execute(f"SELECT stdf_id from Stdf WHERE STDF_NAME = '{stdf_name}'").fetchone() is not None

    def get_by_test_num(self, test_num):
        pass
