import struct
import bz2
import gzip


def unp(endian: str, fmt: str, buf: bytes):
    r, = struct.unpack(endian + fmt, buf)
    return r


class OpenFile:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.fp: any = None

    def __enter__(self):
        if self.file_path.endswith(".gz"):
            self.fp = gzip.open(self.file_path)

        elif self.file_path.endswith(".bz2"):
            self.fp = bz2.open(self.file_path)

        else:
            self.fp = open(self.file_path, "rb")

        return self.fp

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.fp.close()
