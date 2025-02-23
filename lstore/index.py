# lstore/index.py

from collections import defaultdict

class Index:
    """
    Extended to support secondary indexes on any column.
    indexes: a dict: col -> (dict mapping value->set_of_rids)
    pk_index: col==table.key -> dict mapping pk_value->rid
    """
    def __init__(self, table):
        self.table = table
        # default primary key index
        self.pk_index = {}
        # secondary indexes
        self.indexes = {}   # col -> { val -> set_of_rids }

    def locate(self, column, value):
        """
        Return a list of RIDs whose 'column' matches 'value'.
        If column is the primary key, do pk lookup. If there's a secondary index, use it.
        Otherwise, do naive scanning.
        """
        if column == self.table.key:
            rid = self.pk_index.get(value)
            if rid is None:
                return []
            return [rid]
        else:
            # if there's a secondary index
            if column in self.indexes:
                bucket = self.indexes[column].get(value, set())
                return list(bucket)
            # else naive scanning
            results = []
            for pk_val, rid in self.pk_index.items():
                # read the data for 'column'
                # skipping actual logic; in a real design, you'd read base/tail
                # For milestone2, you might do a direct approach in Query.
                pass
            return results

    def locate_range(self, begin, end, column):
        """
        Return the RIDs of all records with column in [begin, end].
        If we have an index, we can do a more direct approach.
        Otherwise naive scanning is needed.
        """
        if column in self.indexes:
            # we must iterate over keys in [begin..end]
            result = []
            for val, ridset in self.indexes[column].items():
                if val >= begin and val <= end:
                    result.extend(ridset)
            return result
        else:
            # naive
            # ...
            return []

    def create_index(self, column_number):
        """
        Build an index for the given column by scanning all records.
        """
        if column_number in self.indexes:
            return  # already built
        self.indexes[column_number] = defaultdict(set)

        # scan all pk->rid
        for pk_val, rid in self.pk_index.items():
            # read the column_number's value
            # In a real system, you'd do a table read
            # For simplicity, store in memory or do it with the table
            pass

    def drop_index(self, column_number):
        if column_number in self.indexes:
            del self.indexes[column_number]

    def insert_index_entry(self, column, value, rid):
        """
        Called by Query insert/update to keep index in sync.
        """
        if column == self.table.key:
            self.pk_index[value] = rid
        else:
            if column in self.indexes:
                self.indexes[column][value].add(rid)

    def remove_index_entry(self, column, value, rid):
        if column == self.table.key:
            if value in self.pk_index:
                del self.pk_index[value]
        else:
            if column in self.indexes:
                if value in self.indexes[column]:
                    if rid in self.indexes[column][value]:
                        self.indexes[column][value].remove(rid)
