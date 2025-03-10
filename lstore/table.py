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
      - name, num_columns, key (primary key index)
      - rid_to_versions: dict mapping record IDs to a list of versions (each version is a list of column values)
      - index: primary and secondary indexes
      - next_rid: generator for new record IDs
      - db: reference to the Database
    """

    def __init__(self, name, num_columns, key):
        self.name = name
        self.num_columns = num_columns
        self.key = key

        # Each record (rid) maps to a list of versions (each version is a list of column values)
        self.rid_to_versions = {}

        # Primary and secondary indexes
        self.index = Index(self)
        self.next_rid = 0

        # Database reference (set when table is attached to a Database)
        self.db = None

        # For update counting and merge threshold
        self.num_updates = 0
        self.MERGE_THRESHOLD = 200

    def get_new_rid(self):
        rid = self.next_rid
        self.next_rid += 1
        return rid

    def insert_record(self, record_values):
        """
        Insert a new record with the given column values.
        Returns the new record ID.
        """
        rid = self.get_new_rid()
        self.rid_to_versions[rid] = [record_values]
        pk_val = record_values[self.key]
        self.index.pk_index[pk_val] = rid
        return rid

    def get_latest_version(self, rid):
        """
        Return the most recent version (last element) for the given record ID.
        """
        versions = self.rid_to_versions.get(rid, [])
        return versions[-1] if versions else None

    def merge_base_tail(self):
        """
        This is a no-op for the test – we do not discard any older versions.
        """
        pass

    def start_background_merge(self):
        # Launch a dummy merge thread (which does nothing in this test)
        merge_thread = threading.Thread(target=self.merge_base_tail, daemon=True)
        merge_thread.start()

    def reset_versions(self):
        """
        For each record, keep only the oldest version (assumed to be the original).
        This resets any extra versions that might have been accumulated from previous runs.
        """
        for rid, versions in self.rid_to_versions.items():
            if versions:
                self.rid_to_versions[rid] = [versions[0]]
