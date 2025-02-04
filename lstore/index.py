# lstore/index.py

class Index:
    """
    A data structure holding indices for the table.
    The primary key column is always indexed by a dictionary pk_index: { primary_key_value: rid }
    For other columns, we do a naive approach for milestone1.
    """
    def __init__(self, table):
        self.table = table
        # primary key index
        self.pk_index = {}   # Maps pk_value -> rid

        # If you want to store secondary indexes: self.indices[col] = something
        self.indices = [None] * table.num_columns

    def locate(self, column, value):
        """
        Return a list of RIDs whose 'column' matches 'value'.
        For column == table.key, just do pk_index lookup.
        For other columns, do naive scanning of table.rid_to_versions.
        """
        if column == self.table.key:
            # direct dictionary lookup
            rid = self.pk_index.get(value)
            if rid is None:
                return []
            else:
                return [rid]
        else:
            # naive scanning
            results = []
            for pk_val, rid in self.pk_index.items():
                versions = self.table.rid_to_versions[rid]
                newest = versions[-1]
                if newest[column] == value:
                    results.append(rid)
            return results

    def locate_range(self, begin, end, column):
        """
        Return the RIDs of all records whose value in 'column' is in [begin, end].
        """
        results = []
        # If it's the PK column, we can do a direct approach:
        if column == self.table.key:
            # all pk_index keys are the PK. So let's just check for keys in [begin..end].
            # But we still need to confirm the record exists, so:
            for pk_val in self.pk_index.keys():
                if pk_val >= begin and pk_val <= end:
                    rid = self.pk_index[pk_val]
                    results.append(rid)
            return results
        else:
            # naive scanning
            for pk_val, rid in self.pk_index.items():
                versions = self.table.rid_to_versions[rid]
                newest = versions[-1]
                col_val = newest[column]
                if col_val >= begin and col_val <= end:
                    results.append(rid)
            return results

    def create_index(self, column_number):
        # optional
        pass

    def drop_index(self, column_number):
        # optional
        pass
