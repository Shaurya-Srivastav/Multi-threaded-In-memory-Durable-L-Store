# lstore/index.py

class Index:
    def __init__(self, table):
        self.table = table
        # pk_index: pk_value -> rid
        self.pk_index = {}
        # optional secondary indexes: col -> { value -> set(rid) }
        self.secondaries = [None]*table.num_columns

    def create_index(self, column):
        if self.secondaries[column] is not None:
            return
        idx = {}
        for pk_val, rid in self.pk_index.items():
            (pid, slot) = self.table.page_directory[rid]
            page = self.table.bufferpool.get_page(pid)
            rec = page.get_record(slot)
            val = rec[column]
            idx.setdefault(val, set()).add(rid)
        self.secondaries[column] = idx

    def drop_index(self, column):
        self.secondaries[column] = None

    def locate(self, column, value):
        # if column == self.table.key => pk_index
        if column == self.table.key:
            rid = self.pk_index.get(value)
            return [rid] if rid is not None else []
        if self.secondaries[column] is not None:
            return list(self.secondaries[column].get(value, []))
        else:
            # naive scan
            res = []
            for pk, rid in self.pk_index.items():
                (pid, slot) = self.table.page_directory[rid]
                pg = self.table.bufferpool.get_page(pid)
                r = pg.get_record(slot)
                if r[column] == value:
                    res.append(rid)
            return res

    def locate_range(self, begin, end, column):
        if column == self.table.key:
            # pk range
            rids = []
            for pk_val, rid in self.pk_index.items():
                if begin <= pk_val <= end:
                    rids.append(rid)
            return rids
        if self.secondaries[column] is not None:
            res = []
            for val, ridset in self.secondaries[column].items():
                if begin <= val <= end:
                    res.extend(ridset)
            return res
        else:
            # naive
            res = []
            for pk_val, rid in self.pk_index.items():
                (pid, slot) = self.table.page_directory[rid]
                r = self.table.bufferpool.get_page(pid).get_record(slot)
                v = r[column]
                if v >= begin and v <= end:
                    res.append(rid)
            return res
