from typing import List, Optional
from stdf_utils.ptr import Ptr, PtrFact


class PartData:
    def __init__(self, site: int):
        self.site = site

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
    def ecid(self) -> Optional[str]:
        if self.ecid_wafer_id is None or self.ecid_x_coord is None or self.ecid_y_coord is None:
            return None
        # WaferLot
        return f"{self.ecid_lot_id}_W{self.ecid_wafer_id}_X{self.ecid_x_coord}Y{self.ecid_y_coord}"

        # TODO: Add TimeStamp

    def update_ptr(self, row: dict):
        if self.ptr_fact.check_unique_test_num(row):
            self.ptr_list.append(Ptr(row))

    def update_prr(self, row: dict, stdf_id: int):
        self.prr_x_coord = row["X_COORD"]
        self.prr_y_coord = row["Y_COORD"]
        self.num_test = row["NUM_TEST"]
        self.hard_bin = row["HARD_BIN"]
        self.soft_bin = row["SOFT_BIN"]
        self.test_time = row["TEST_T"]
        self.part_id = int(row["PART_ID"].decode())
        self.stdf_id = stdf_id
