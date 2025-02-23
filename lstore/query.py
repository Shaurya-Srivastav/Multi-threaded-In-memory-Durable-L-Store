# lstore/query.py

class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Query:
    def __init__(self, table):
        self.table = table

    def delete(self, primary_key):
        return self.table.delete_record(primary_key)

    def insert(self, *columns):
        return self.table.insert_record(list(columns))

    def select(self, search_key, search_key_index, projected_columns_index):
        """
        If searching by primary key => direct pk_index
        else => naive scan
        """
        results = []
        # The user’s search_key_index is 0 => first user column => actually self.table.key - 1
        if search_key_index == (self.table.key - 1):
            rid = self.table.index.pk_index.get(search_key)
            if rid is None:
                return []
            rec = self.table._get_versioned_record(rid, 0)  # newest
            # project
            out = []
            for i, flag in enumerate(projected_columns_index):
                if flag == 1:
                    out.append(rec[i])
            results.append(Record(rid, search_key, out))
        else:
            # naive
            for pk_val, rid in self.table.index.pk_index.items():
                rec = self.table._get_versioned_record(rid, 0)
                if rec[search_key_index] == search_key:
                    out = []
                    for i, flag in enumerate(projected_columns_index):
                        if flag == 1:
                            out.append(rec[i])
                    results.append(Record(rid, pk_val, out))
        return results

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        results = []
        if search_key_index == self.table.key - 1:
            rid = self.table.index.pk_index.get(search_key)
            if rid is None:
                return []
            rec = self.table._get_versioned_record(rid, relative_version)
            out = []
            for i, flag in enumerate(projected_columns_index):
                if flag == 1:
                    out.append(rec[i])
            results.append(Record(rid, search_key, out))
        else:
            # naive
            for pk_val, rid in self.table.index.pk_index.items():
                rec = self.table._get_versioned_record(rid, relative_version)
                if rec[search_key_index] == search_key:
                    out = []
                    for i, flag in enumerate(projected_columns_index):
                        if flag == 1:
                            out.append(rec[i])
                    results.append(Record(rid, pk_val, out))
        return results

    def update(self, primary_key, *columns):
        return self.table.update_record(primary_key, columns)

    def sum(self, start_range, end_range, aggregate_column_index):
        return self.table.sum_column(start_range, end_range, aggregate_column_index + 1)

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        return self.table.sum_column_version(start_range, end_range, aggregate_column_index + 1, relative_version)

    def increment(self, key, column):
        recs = self.select(key, self.table.key-1, [1]*self.table.num_columns)
        if not recs:
            return False
        rec = recs[0]
        old_val = rec.columns[column+1]
        new_val = old_val + 1
        ups = [None]*self.table.num_columns
        ups[column] = new_val
        return self.update(key, *ups)
