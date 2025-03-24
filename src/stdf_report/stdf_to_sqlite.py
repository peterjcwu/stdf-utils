import re
import sqlite3

from stdf_utils import StdfRecord


class StdfToSqlite:
    def __init__(self, stdf_path: str):
        self.handlers = {
            "Ptr": self.ptr_handler
        }
        # sqlite connection
        db_path = re.sub("\.std(f)?(\.gz)?$", ".db", stdf_path)
        con = sqlite3.connect(db_path)
        self.cursor = con.cursor()
        self.create_tables()

        # read
        for rec_type, rec in StdfRecord(stdf_path, set(self.handlers.keys())):
            # self.handlers[rec_type](rec)
            pass
        pass
        con.commit()

    def create_tables(self):
        self.cursor.execute("""
        CREATE TABLE stdf(
            id           INT        PRIMARY KEY NOT NULL,
            name         CHAR(256),
            voltage      CHAR(32),
            temperature  INT
        );
        """)

        self.cursor.execute("""
        CREATE TABLE ptr_data(
            id INT PRIMARY KEY NOT NULL,
            
        );
        """)

        self.cursor.execute("""
        CREATE TABLE ptr(
            id INT PRIMARY KEY NOT NULL,
            
        );
        """)

    def dtr_handler(self, row):
        print(row)

    def ptr_handler(self, row: dict):
        print(row)


if __name__ == '__main__':
    StdfToSqlite(r"C:\log\w446_bb_a1_rf_char_evm_6g_high_stddev\20241106175704_atbk_fr1_AW693S_A1_vmax_FF_onSQU_r1393.stdf")