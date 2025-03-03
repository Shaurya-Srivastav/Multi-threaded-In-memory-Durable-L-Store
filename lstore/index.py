# lstore/index.py

class Index:
    def __init__(self, table):
        self.table = table
        self.pk_index = {}  # Maps pk_value -> RID
        self.secondary_indexes = {}  # column_id -> {value: [rids]}

    def create_index(self, column_number):
        """Creates an index on a specific column."""
        if column_number == self.table.key:
            return
        self.secondary_indexes[column_number] = {}
        for rid, versions in self.table.rid_to_versions.items():
            latest_value = versions[-1][column_number]
            self.secondary_indexes[column_number].setdefault(latest_value, []).append(rid)

    def locate(self, column, value):
        """Returns RIDs of records that match a column value."""
        if column in self.secondary_indexes:
            return self.secondary_indexes[column].get(value, [])
        return []

    def drop_index(self, column_number):
        """Removes an index from a column."""
        if column_number in self.secondary_indexes:
            del self.secondary_indexes[column_number]
