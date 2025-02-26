from lstore.config import PAGE_SIZE

class Page:

    RECORD_SIZE = 8 

    def __init__(self, data=bytearray(PAGE_SIZE)):
        self.data = data

    def capacity(self):
        return PAGE_SIZE // Page.RECORD_SIZE

    def read(self, slot):
        start = slot * Page.RECORD_SIZE
        val_bytes = self.data[start:start+8]
        return int.from_bytes(val_bytes, byteorder='little', signed=True)

    def write(self, slot, value):
        start = slot * Page.RECORD_SIZE
        val_bytes = value.to_bytes(8, byteorder='little', signed=True)
        self.data[start:start+8] = val_bytes
