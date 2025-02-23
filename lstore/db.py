# lstore/db.py

import os
from lstore.table import Table

class Database:
    def __init__(self):
        self.tables = {}
        self.path = None

    def open(self, path):
        self.path = path
        if not os.path.exists(path):
            os.makedirs(path)
        # We do lazy loading => when create_table or get_table is used

    def close(self):
        for tbl in self.tables.values():
            tbl.shutdown()

    def create_table(self, name, num_columns, key_index):
        t = Table(name, num_columns, key_index)
        self.tables[name] = t
        return t

    def drop_table(self, name):
        if name in self.tables:
            self.tables[name].shutdown()
            del self.tables[name]

    def get_table(self, name):
        return self.tables.get(name, None)
