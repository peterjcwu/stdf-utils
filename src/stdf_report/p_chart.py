import os
from stdf_utils.sql_conn import SqlConn


class PChart:
    def __init__(self, db_path):
        self.cursor = SqlConn(db_path).cursor

    def draw(self, test_num: int):
        with open(f"C:\\log\\2025\\w537_rf_tdlog_change_limit_v01.01a\\{test_num}.csv", "w", newline="") as f_out:
            f_out.write(",".join(["stdf_id", "part_id", "test_num", "result"]) + os.linesep)
            for row in self.cursor.execute(f"""
            SELECT stdf_id, part_id, test_num, result 
            FROM Ptr 
            WHERE test_num = {test_num} 
            """):
                f_out.write(",".join(map(str, row)) + os.linesep)


if __name__ == '__main__':
    PChart(r"C:\log\2025\w537_rf_tdlog_change_limit_v01.01a\local.db").draw(10008)
