# lstore/page.py

from lstore.config import PAGE_SIZE

class Page:
    """
    A physical page in columnar storage.
    We store an array of fixed-size slots (8 bytes) for this column's data.
    """
    RECORD_SIZE = 8  # 8 bytes per record

    def __init__(self, data=bytearray(PAGE_SIZE)):
        self.data = data

    def capacity(self):
        return PAGE_SIZE // Page.RECORD_SIZE

    def read(self, slot):
        """
        Read 8 bytes at slot index 'slot', return as an integer
        """
        start = slot * Page.RECORD_SIZE
        val_bytes = self.data[start:start+8]
        return int.from_bytes(val_bytes, byteorder='little', signed=True)

    def write(self, slot, value):
        """
        Write 8-byte integer at slot index 'slot'
        """
        start = slot * Page.RECORD_SIZE
        val_bytes = value.to_bytes(8, byteorder='little', signed=True)
        self.data[start:start+8] = val_bytes
