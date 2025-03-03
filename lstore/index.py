# index.py

class Index:
    def __init__(self, table):
        self.table = table
        self.pk_index = {}            # pk_value -> rid
        self.secondary_indexes = {}   # col_id -> { value -> [rids] }

    def create_index(self, column_number):
        """
        Create a secondary index on column_number, if not the primary key.
        We build it from existing data in rid_to_versions.
        """
        if column_number == self.table.key:
            return  # already have a primary key index

        self.secondary_indexes[column_number] = {}
        for rid, versions in self.table.rid_to_versions.items():
            newest = versions[-1]
            val = newest[column_number]
            self.secondary_indexes[column_number].setdefault(val, []).append(rid)

    def locate(self, column_number, value):
        """
        Return the list of RIDs that have 'value' in column_number.
        If no secondary index, return [].
        """
        if column_number in self.secondary_indexes:
            return self.secondary_indexes[column_number].get(value, [])
        return []

    def drop_index(self, column_number):
        if column_number in self.secondary_indexes:
            del self.secondary_indexes[column_number]
