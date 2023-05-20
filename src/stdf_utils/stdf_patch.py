import logging
from .stdf_record import StdfRecord
from .util import OpenFile


class StdfPatch:
    def __init__(self, stdf_path: str, mod_stdf_path: str = None, patch_func=None):
        self.stdf_path = stdf_path
        self.mod_stdf_path = mod_stdf_path or stdf_path.replace(".gz", "").replace(".stdf", "_mod.stdf")
        self.patch_func = patch_func or (lambda x, y, z: z)

        with open(self.mod_stdf_path, "wb") as f_out:
            with OpenFile(stdf_path) as f_in:
                stdf = StdfRecord(f_in)
                while True:
                    try:
                        record = stdf.get_next_record()
                        if record is None:
                            continue
                        write_buffer = self.patch_func(stdf.rec_type, record, stdf.buffer)
                        f_out.write(write_buffer)

                    except Exception as e:
                        logging.debug(str(e))
                        break
