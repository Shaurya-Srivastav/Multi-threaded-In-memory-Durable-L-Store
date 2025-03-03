# lstore/table.py

import threading
from lstore.index import Index

class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:
    """
    :param name: str
    :param num_columns: int
    :param key: int
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.num_columns = num_columns
        self.key = key

        # Directory or internal structures
        self.page_directory = {}
        self.rid_to_versions = {}

        # The table's index
        self.index = Index(self)

        # For generating RIDs
        self.next_rid = 0

        # The database instance (set externally after creation, e.g. table.db = self_db)
        self.db = None

    def get_new_rid(self):
        rid = self.next_rid
        self.next_rid += 1
        return rid

    def insert_record(self, record_values):
        """
        Insert record values (list of columns).
        Return the new RID.
        """
        rid = self.get_new_rid()
        self.rid_to_versions[rid] = [record_values]
        # Update PK index
        pk_val = record_values[self.key]
        self.index.pk_index[pk_val] = rid
        return rid

    def merge_base_tail(self):
        """
        Simple example of merging: if a record has multiple versions, keep only the latest.
        This is a placeholder for a more robust logic.
        """
        for rid, versions in self.rid_to_versions.items():
            if len(versions) > 1:
                # Keep only the final version
                newest = versions[-1]
                self.rid_to_versions[rid] = [newest]

    def start_background_merge(self):
        merge_thread = threading.Thread(target=self.merge_base_tail, daemon=True)
        merge_thread.start()

    def get_latest_version(self, rid):
        if rid in self.rid_to_versions:
            return self.rid_to_versions[rid][-1]
        return None
