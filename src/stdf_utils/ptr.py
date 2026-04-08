from typing import Dict


class Ptr:
    def __init__(self, rec: dict):
        self.test_num: int = rec["TEST_NUM"]
        self.result: float = rec["RESULT"]


class PtrFact:
    def __init__(self):
        self._data: Dict[int, PtrFactRow] = {}

    def check_unique_test_num(self, rec: dict) -> bool:
        return True


class PtrFactRow:
    def __init__(self):
        pass