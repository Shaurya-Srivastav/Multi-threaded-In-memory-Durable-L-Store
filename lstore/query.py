# # lstore/query.py

# from lstore.table import Table, INDIRECTION_COLUMN, RID_COLUMN, Record
# from lstore.index import Index
# import struct

# class Query:
#     def __init__(self, table):
#         self.table = table

#     def insert(self, *columns):
#         """
#         Insert the record into base pages, update indexes.
#         columns is a list of int values. columns[self.table.key] is the PK.
#         """
#         pk_val = columns[self.table.key]
#         # Check if PK exists
#         if pk_val in self.table.index.pk_index:
#             return False

#         rid = self.table.get_new_rid()
#         # For each column, place the value in a new base slot
#         for col_id, val in enumerate(columns):
#             page_id, slot = self.table._new_base_slot(col_id)
#             self.table._write_value(page_id, slot, val)
#             # store in page_directory
#             self.table.page_directory[rid] = self.table.page_directory.get(rid, [])
#             self.table.page_directory[rid].append(("base", col_id, page_id, slot))

#         # update index
#         for col_id, val in enumerate(columns):
#             self.table.index.insert_index_entry(col_id, val, rid)

#         return True

#     def delete(self, key):
#         """
#         Remove the record from indexes, page_directory, etc.
#         """
#         rid = self.table.index.pk_index.get(key, None)
#         if rid is None:
#             return False
#         # remove from all indexes
#         # need to read each column from base pages
#         if rid in self.table.page_directory:
#             for entry in self.table.page_directory[rid]:
#                 # read the value
#                 (bt, col_id, page_id, slot) = entry
#                 val = self.table._read_value(page_id, slot)
#                 self.table.index.remove_index_entry(col_id, val, rid)
#             del self.table.page_directory[rid]

#         return True

#     def update(self, key, *new_values):
#         """
#         Create tail records for changed columns, or do in-place, etc.
#         For example, we do a tail-based approach: any changed column -> write to tail.
#         """
#         rid = self.table.index.pk_index.get(key, None)
#         if rid is None:
#             return False

#         # read current values from base or tail
#         # for simplicity, read from base
#         current_vals = [0]*self.table.num_columns
#         entries = self.table.page_directory[rid]
#         for (bt, col_id, page_id, slot) in entries:
#             val = self.table._read_value(page_id, slot)
#             current_vals[col_id] = val

#         # update any changed columns
#         updated_vals = list(current_vals)
#         for col_id, val in enumerate(new_values):
#             if val is not None:
#                 updated_vals[col_id] = val

#         # in a real system, we create tail records, set indirection, etc.
#         # for demonstration, let's do a naive approach: re-insert into base? That's unusual, but let's keep example
#         # Actually let's remove old index entries for changed columns, add new ones
#         for col_id in range(self.table.num_columns):
#             old_val = current_vals[col_id]
#             new_val = updated_vals[col_id]
#             if old_val != new_val:
#                 self.table.index.remove_index_entry(col_id, old_val, rid)
#                 self.table.index.insert_index_entry(col_id, new_val, rid)

#                 # also update the base page value
#                 (bt, col, pid, slot) = entries[col_id]
#                 self.table._write_value(pid, slot, new_val)

#         return True

#     def select(self, search_key, search_key_index, projected_columns_index):
#         """
#         If there's an index, we do fast lookup. Otherwise naive scan.
#         Return a list of Record objects.
#         """
#         rids = []
#         # see if we have an index
#         if search_key_index == self.table.key or (search_key_index in self.table.index.indexes):
#             rids = self.table.index.locate(search_key_index, search_key)
#         else:
#             # naive scanning
#             # ...
#             pass

#         results = []
#         for rid in rids:
#             # gather columns from base pages
#             rec_cols = []
#             # get page_directory info
#             if rid not in self.table.page_directory:
#                 continue
#             entries = self.table.page_directory[rid]
#             for i, flag in enumerate(projected_columns_index):
#                 if flag == 1:
#                     (bt, c, pid, slot) = entries[i]
#                     val = self.table._read_value(pid, slot)
#                     rec_cols.append(val)
#             # create Record
#             # the "key" is the primary key => read from the base or index
#             pk_val = None
#             # you can find it from index if needed:
#             # invert pk_index
#             # or we can read from the base column self.table.key
#             (bt, c, pid, slot) = entries[self.table.key]
#             pk_val = self.table._read_value(pid, slot)
#             results.append(Record(rid, pk_val, rec_cols))

#         return results

#     def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
#         """
#         For milestone2, you might skip version-based retrieval or treat it similarly.
#         We'll just do the same as select (no advanced versioning).
#         """
#         return self.select(search_key, search_key_index, projected_columns_index)

#     def sum(self, start_range, end_range, aggregate_column_index):
#         """
#         Summation over [start_range, end_range] of the PK for that column.
#         """
#         # we can do a range locate if index is built
#         rids = self.table.index.locate_range(start_range, end_range, self.table.key)
#         if not rids:
#             return False
#         total = 0
#         for rid in rids:
#             # get the page directory entry for that column
#             if rid not in self.table.page_directory:
#                 continue
#             entries = self.table.page_directory[rid]
#             (bt, c, pid, slot) = entries[aggregate_column_index]
#             val = self.table._read_value(pid, slot)
#             total += val
#         return total

#     def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
#         """
#         Same as sum, ignoring version. 
#         """
#         return self.sum(start_range, end_range, aggregate_column_index)

#     def increment(self, key, column):
#         """
#         Just read the old val, increment, then update
#         """
#         recs = self.select(key, self.table.key, [1]*self.table.num_columns)
#         if not recs:
#             return False
#         old_val = recs[0].columns[column]
#         new_val = old_val + 1
#         update_list = [None]*self.table.num_columns
#         update_list[column] = new_val
#         return self.update(key, *update_list)





from lstore.table import Table, Record
from lstore.index import Index

class Query:
    def __init__(self, table):
        self.table = table

    def insert(self, *columns):
        """Insert a record with the given column values."""
        pk_val = columns[self.table.key_index]
        if pk_val in self.table.index.pk_index:
            return False  # Primary key must be unique

        new_rid = self.table.get_new_rid()
        self.table.rid_to_versions[new_rid] = [list(columns)]
        self.table.index.pk_index[pk_val] = new_rid
        return True

    def delete(self, primary_key):
        # """Deletes a record by removing it from indexing and versions."""
        rid = self.table.index.pk_index.pop(primary_key, None)
        if rid is None:
            return False  # Record not found

        del self.table.rid_to_versions[rid]  # Remove versions
        return True


    def select(self, search_key, search_key_index, projected_columns_index):
        """Returns a list of Record objects that match the search key."""
        results = []
        if search_key_index == self.table.key_index:
            rid = self.table.index.pk_index.get(search_key, None)
            if rid is None:
                return []
            versions = self.table.rid_to_versions[rid]
            newest = versions[-1]

            projected = [newest[i] for i, flag in enumerate(projected_columns_index) if flag == 1]
            results.append(Record(rid, search_key, projected))  # ✅ Fix: Return a Record object
        else:
            rids = self.table.index.locate(search_key_index, search_key)
            if len(rids) == 0:
                for rid, versions in self.table.rid_to_versions.items():
                    if search_key == versions[-1][search_key_index]:
                        rids.append(rid)
                
            for rid in rids:
                versions = self.table.rid_to_versions[rid]
                newest = versions[-1]

                projected = [newest[i] for i, flag in enumerate(projected_columns_index) if flag == 1]
                results.append(Record(rid, search_key, projected))  # ✅ Fix: Return a Record object

        return results

    def update(self, primary_key, *columns):
        # """Updates a record while maintaining past versions."""
        rid = self.table.index.pk_index.get(primary_key, None)
        if rid is None:
            return False  # Record not found

        versions = self.table.rid_to_versions[rid]
        newest = versions[-1].copy()  # Copy last version to create new version

        for col_idx, val in enumerate(columns):
            if val is not None:
                newest[col_idx] = val  # Apply updates

        versions.append(newest)  # Add new version
        return True

    def sum(self, start_range, end_range, aggregate_column_index):
        """Sums the newest version of a column for a range of keys."""
        relevant_pks = [pk for pk in self.table.index.pk_index.keys() if start_range <= pk <= end_range]
        if not relevant_pks:
            return 0

        total = sum(self.table.rid_to_versions[self.table.index.pk_index[pk]][-1][aggregate_column_index] for pk in relevant_pks)
        return total
    
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        """Returns a list of Record objects that match the search key at a specific version."""
        results = []

        if search_key_index == self.table.key_index:
            rid = self.table.index.pk_index.get(search_key, None)
            if rid is None:
                return []
            versions = self.table.rid_to_versions[rid]

            # Compute version index (0 = newest, -1 = second newest, etc.)
            idx = max(0, len(versions) - 1 + relative_version)

            projected = [versions[idx][i] for i, flag in enumerate(projected_columns_index) if flag == 1]
            results.append(Record(rid, search_key, projected))  # ✅ Fix: Return a Record object
        else:
            rids = self.table.index.locate(search_key_index, search_key)
            for rid in rids:
                versions = self.table.rid_to_versions[rid]
                idx = max(0, len(versions) - 1 + relative_version)

                projected = [versions[idx][i] for i, flag in enumerate(projected_columns_index) if flag == 1]
                results.append(Record(rid, search_key, projected))  # ✅ Fix: Return a Record object

        return results
    
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
    # """Sums the values of a column for a specific past version."""
        relevant_pks = [pk for pk in self.table.index.pk_index.keys() if start_range <= pk <= end_range]
        if not relevant_pks:
            return 0

        total = 0
        for pk in relevant_pks:
            rid = self.table.index.pk_index[pk]
            versions = self.table.rid_to_versions[rid]
            idx = max(0, len(versions) - 1 + relative_version)  # Find the correct past version

            total += versions[idx][aggregate_column_index]

        return total



