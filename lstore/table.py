# table.py

import threading
from lstore.index import Index

class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:
    """
    In-memory table storing:
    - name
    - num_columns
    - key (primary key column index)
    - rid_to_versions: dict of rid -> [versions], each version is a list of col values
    - index: primary key + optional secondaries
    - next_rid: generator for new record IDs
    - db: a reference to the Database for concurrency
    """

    def __init__(self, name, num_columns, key):
        self.name = name
        self.num_columns = num_columns
        self.key = key

        self.rid_to_versions = {}
        self.index = Index(self)
        self.next_rid = 0

        # for concurrency or references
        self.db = None

    def get_new_rid(self):
        rid = self.next_rid
        self.next_rid += 1
        return rid

    def insert_record(self, record_values):
        """
        Insert record_values (list of columns).
        Return the new RID.
        """
        rid = self.get_new_rid()
        self.rid_to_versions[rid] = [record_values]
        pk_val = record_values[self.key]
        self.index.pk_index[pk_val] = rid
        return rid

    def get_latest_version(self, rid):
        if rid not in self.rid_to_versions:
            return None
        return self.rid_to_versions[rid][-1]

    def merge_base_tail(self):
        """
        Stub for merging: keep only the newest version for each record.
        """
        for rid, versions in self.rid_to_versions.items():
            if len(versions) > 1:
                newest = versions[-1]
                self.rid_to_versions[rid] = [newest]

    def start_background_merge(self):
        merge_thread = threading.Thread(target=self.merge_base_tail, daemon=True)
        merge_thread.start()
