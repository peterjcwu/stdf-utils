import os
from .util import OpenFile
from .stdf_record import StdfRecord


class StdfToTxt:
    def __init__(self, stdf_path: str, txt_path: str):
        with open(txt_path, "w", newline="") as f_out:
            with OpenFile(stdf_path) as f_in:
                for rec_type, rec in StdfRecord(f_in):
                    f_out.write(f'== {rec_type} ==' + os.linesep)
                    for key, value in rec.items():
                        f_out.write(f'{key} -> {value}' + os.linesep)
