# lstore/table.py

from lstore.index import Index
from time import time

# Column definitions for L-Store. You can store them or skip them in milestone1.
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Record:
    """
    A convenient class to hold a record when returning from select queries.
    'columns' is the subset or full list of column values for that record.
    """
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        self.tps = ""

class Table:
    """
    :param name: string
    :param num_columns: int
    :param key: int  # index of the primary key in the columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.num_columns = num_columns
        self.key = key

        # rid -> list_of_versions (each version is a list of 'num_columns' values)
        self.rid_to_versions = {}

        # primary key index: pk -> rid
        self.index = Index(self)

        self.next_rid = 0

        # This is the 'page directory'. For milestone1, we can skip detailed usage.
        self.page_directory = {}

    def get_new_rid(self):
        rid = self.next_rid
        self.next_rid += 1
        return rid

    def __merge(self):
        # For milestone1, no merge logic needed.
        print("merge is happening (stub)")
        pass
