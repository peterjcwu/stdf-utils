import os.path
import re
import sqlite3
from typing import Dict, List, Tuple
from stdf_utils.stdf_record import StdfRecord


class StdfToSql:
    def __init__(self, stdf_path: str):
        self.stdf_path = stdf_path
        self.sql_cache = SqlCache(os.path.join(os.path.dirname(stdf_path), "local.db"))

        # placeholder
        self._stdf_id: int = 0
        self._program_id: int = 0

        self.handlers = {
            "Mir": self.mir_handler,
            "Pir": self.pir_handler,
            "Dtr": self.dtr_handler,
            "Ptr": self.ptr_handler,
            "Prr": self.prr_handler,
        }

        # per site data
        self.part_data_site: Dict[int, PartData] = {}  # {site: DieData}

        # read
        for rec_type, rec in StdfRecord(stdf_path, set(self.handlers.keys())):
            self.handlers[rec_type](rec)

        # commit sql
        self.sql_cache.conn.commit()

    def mir_handler(self, rec: dict):
        print(rec)
        program_name = rec['JOB_NAM'].decode()
        self._program_id = self.sql_cache.insert_program(program_name)
        self._stdf_id = self.sql_cache.insert_stdf(os.path.basename(self.stdf_path).split(".")[0])
        #
        # for part_data in self.part_data_site.values():
        #     part_data.program_id = program_id
        #     part_data.stdf_id = stdf_id

    def pir_handler(self, rec: dict):
        self.part_data_site[rec['SITE_NUM']] = PartData()  # reset

    def dtr_handler(self, rec: dict):
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

    def ptr_handler(self, rec: dict):
        self.part_data_site[rec['SITE_NUM']].update_ptr(rec)

    def prr_handler(self, rec: dict):
        self.part_data_site[rec['SITE_NUM']].update_prr(rec, self._stdf_id, self._program_id)
        self.sql_cache.push(self.part_data_site[rec['SITE_NUM']])


class PartData:
    def __init__(self):
        # MIR
        self.stdf_id: int = 0
        self.program_id: int = 0
        self.program_name: str = ""

        # DTR
        self.ecid_valid: str = ""
        self.ecid_fab: str = ""
        self.ecid_lot_id: str = ""
        self.ecid_wafer_id: str = ""
        self.ecid_x_coord: int = 32767
        self.ecid_y_coord: int = 32767

        # PRR
        self.prr_x_coord: int = 32767
        self.prr_y_coord: int = 32767
        self.num_test: int = 0
        self.hard_bin: int = 0
        self.soft_bin: int = 0
        self.test_time: int = 0
        self.part_id: int = 0

        # PTR
        self.ptr_list: List[Ptr] = []
        self.ptr_fact = PtrFact()

    @property
    def ecid(self) -> str:
        return f"{self.ecid_lot_id}_W{self.ecid_wafer_id}_X{self.ecid_x_coord}Y{self.ecid_y_coord}"

    def update_ptr(self, row: dict):
        # assert unique test_num
        if self.ptr_fact.check(row):
            self.ptr_list.append(Ptr(row))

    def update_prr(self, row: dict, stdf_id: int, program_id: int):
        self.prr_x_coord = row["X_COORD"]
        self.prr_y_coord = row["Y_COORD"]
        self.num_test = row["NUM_TEST"]
        self.hard_bin = row["HARD_BIN"]
        self.soft_bin = row["SOFT_BIN"]
        self.test_time = row["TEST_T"]
        self.part_id = int(row["PART_ID"].decode())
        self.stdf_id = stdf_id
        self.program_id = program_id


class Ptr:
    def __init__(self, rec: dict):
        self.test_num: int = rec["TEST_NUM"]
        self.result: float = rec["RESULT"]


class PtrFact:
    def __init__(self):
        self._data: Dict[int, PtrFactRow] = {}

    def check(self, rec: dict) -> bool:
        return True


class PtrFactRow:
    def __init__(self):
        pass


class SqlCache:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.table_program: Dict[int, str] = {}
        self.ptr_fact: Dict[Tuple[int, int], PtrFact] = {}
        self._create_table()
        self._read_back()

    def _create_table(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS Program (
            program_id INTEGER PRIMARY KEY,
            program_name VARCHAR(256) NOT NULL UNIQUE
        );""")

        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS Stdf (
            stdf_id INTEGER PRIMARY KEY,
            stdf_name VARCHAR(256) NOT NULL,
            program_id, INTEGER,
            temperature INTEGER,
            bias VARCHAR(32)
        );""")

        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS Part (
            stdf_id INTEGER,
            part_id INTEGER,
            ecid VARCHAR(256) NOT NULL,
            ecid_wafer_id VARCHAR(32),
            ecid_lot_id VARCHAR(32),
            ecid_x_coord INTEGER,
            ecid_y_coord INTEGER,
            soft_bin INTEGER NOT NULL,
            hard_bin INTEGER NOT NULl,
            PRIMARY KEY (stdf_id, part_id)
        );""")

        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS Ptr (
            program_id INTEGER,
            ecid VARCHAR(32),
            stdf_id INTEGER,
            part_id INTEGER,
            test_num INTEGER,
            result REAL,
            PRIMARY KEY (program_id, ecid, test_num)
        );""")

        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS PtrFact (
            program_id INTEGER,
            test_num INTEGER,
            test_name VARCHAR(256),
            lo_lim REAL,
            hi_lim REAL,
            cond VARCHAR(512),
            PRIMARY KEY (program_id, test_num)
        );""")

        self.conn.commit()

    def _read_back(self):
        # self.table_program: Dict[int]
        for row in self.cursor.execute("SELECT * FROM Program").fetchall():
            pass

    def push(self, die_data: PartData):
        # table 'Part'
        self.cursor.execute(
            "INSERT INTO Part (stdf_id, ecid_wafer_id, ecid_lot_id, ecid, soft_bin, hard_bin, part_id)"
            " Values (?, ?, ?, ?, ?, ?, ?)",
            (die_data.stdf_id, die_data.ecid_wafer_id, die_data.ecid_lot_id, die_data.ecid, die_data.soft_bin,
             die_data.hard_bin, die_data.part_id)
        )

        # table 'Ptr'
        self.cursor.executemany(
            "INSERT OR REPLACE INTO Ptr (program_id, ecid, stdf_id, part_id, test_num, result)"
            " Values (?, ?, ?, ?, ?, ?)",
            ((die_data.program_id, die_data.ecid, die_data.stdf_id, die_data.part_id, ptr.test_num, ptr.result)
             for ptr in die_data.ptr_list)
        )

    def insert_program(self, program_name: str) -> int:
        self.cursor.execute(
            "INSERT INTO Program (program_name) Values (?) ON CONFLICT (program_name) DO NOTHING;",
            (program_name,))

        row = self.cursor.execute(f"SELECT program_id from Program where program_name = '{program_name}'").fetchone()
        print(row)

    def insert_stdf(self, stdf_name: str) -> int:
        return 0


if __name__ == '__main__':
    # path = r"C:\log\2025\w512_bb_a1_buck_triTemp_evaluation\qfn_cqs_1stCRD\FR1_R1_GY414.42_SYSE07DBK000_20250215065629.stdf.gz"
    path = r"C:\log\2025\w512_bb_a1_buck_triTemp_evaluation\qfn_cqs_1stCRD\FC2_GY414.42_SYSE07DBK000_20250216200533.stdf.gz"
    StdfToSql(path)
