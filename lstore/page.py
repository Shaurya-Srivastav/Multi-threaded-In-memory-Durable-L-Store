# lstore/page.py
import os
import struct
from lstore.config import PAGE_SIZE, MAX_RECORDS_PER_PAGE

PAGE_HEADER_FORMAT = "III"   # [page_id, page_type, num_records]
# page_type=0 => base, page_type=1 => tail

class Page:
    def __init__(self, page_id, page_type=0):
        self.page_id = page_id
        self.page_type = page_type  # 0=base,1=tail
        self.num_records = 0
        self.records = []
        self.dirty = False

    def has_capacity(self):
        return self.num_records < MAX_RECORDS_PER_PAGE

    def write_record(self, record):
        if self.has_capacity():
            self.records.append(record)
            self.num_records += 1
            self.dirty = True
            return True
        return False

    def get_record(self, slot):
        if slot < self.num_records:
            return self.records[slot]
        return None

    def set_record(self, slot, record):
        if slot < self.num_records:
            self.records[slot] = record
            self.dirty = True

    def to_bytes(self):
        header = struct.pack(PAGE_HEADER_FORMAT, self.page_id, self.page_type, self.num_records)
        body = b""
        for rec in self.records:
            # store # of columns
            num_cols = len(rec)
            body += struct.pack("I", num_cols)
            for val in rec:
                body += struct.pack("q", val)  # 64-bit int
        return header + body

    @staticmethod
    def from_bytes(data):
        if len(data) < 12:
            return None
        page_id, page_type, num_records = struct.unpack(PAGE_HEADER_FORMAT, data[:12])
        p = Page(page_id, page_type)
        offset = 12
        for _ in range(num_records):
            if offset+4 > len(data):
                break
            (num_cols,) = struct.unpack("I", data[offset:offset+4])
            offset += 4
            rec = []
            for __ in range(num_cols):
                if offset+8 > len(data):
                    break
                val = struct.unpack("q", data[offset:offset+8])[0]
                offset += 8
                rec.append(val)
            p.records.append(rec)
        p.num_records = len(p.records)
        return p
