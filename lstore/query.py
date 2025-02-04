# lstore/query.py

from lstore.table import Table, Record
from lstore.index import Index

class Query:
    """
    The Query class provides standard SQL-like operations on the table:
      - insert
      - delete
      - select / select_version
      - update
      - sum / sum_version
    """

    def __init__(self, table):
        self.table = table

    def _compute_version_index(self, versions_list, relative_version):
        """
        Maps a 'relative_version' to the correct index in versions_list.
         - If relative_version=0 => newest => index = len(versions_list) - 1
         - If relative_version>0 => 2nd newest, 3rd newest, etc.
         - If relative_version<0 => original/oldest => index=0 for milestone1 testers.
        """
        if not versions_list:
            return None
        length = len(versions_list)
        if relative_version < 0:
            # all negative versions => oldest version
            return 0
        else:
            # 0 => newest, 1 => second newest, etc.
            idx = (length - 1) - relative_version
            if idx < 0:
                idx = 0
            return idx

    def delete(self, primary_key):
        """
        Removes the record for 'primary_key' from the table.
        Returns True if successful, False if record doesn't exist.
        """
        rid = self.table.index.pk_index.get(primary_key, None)
        if rid is None:
            return False

        # remove from pk_index
        del self.table.index.pk_index[primary_key]

        # remove from rid_to_versions
        del self.table.rid_to_versions[rid]
        return True

    def insert(self, *columns):
        """
        Insert a record with the given column values. Returns True if success, False otherwise.
        """
        # the primary key is columns[self.table.key]
        pk_val = columns[self.table.key]
        # check if pk already exists
        if pk_val in self.table.index.pk_index:
            return False

        # generate a new rid
        new_rid = self.table.get_new_rid()

        # store the first (base) version
        self.table.rid_to_versions[new_rid] = [list(columns)]

        # store pk -> rid in the index
        self.table.index.pk_index[pk_val] = new_rid
        return True

    def select(self, search_key, search_key_index, projected_columns_index):
        """
        Returns a list of Record objects that match 'search_key' in column 'search_key_index'.
        We only need single-match for pk. If it's not pk, do naive scanning.
        projected_columns_index is an array of 0/1 flags for which columns to return.
        """
        results = []
        # If the search_key_index is the primary key, do direct lookup
        if search_key_index == self.table.key:
            rid = self.table.index.pk_index.get(search_key, None)
            if rid is None:
                return []
            versions = self.table.rid_to_versions[rid]
            newest = versions[-1]
            # project
            projected = []
            for i, flag in enumerate(projected_columns_index):
                if flag == 1:
                    projected.append(newest[i])
            record_obj = Record(rid, search_key, projected)
            results.append(record_obj)
        else:
            # naive scan
            for pk_val, rid in self.table.index.pk_index.items():
                versions = self.table.rid_to_versions[rid]
                newest = versions[-1]
                if newest[search_key_index] == search_key:
                    projected = []
                    for i, flag in enumerate(projected_columns_index):
                        if flag == 1:
                            projected.append(newest[i])
                    record_obj = Record(rid, pk_val, projected)
                    results.append(record_obj)
        return results

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        """
        Like select, but we pick the appropriate version based on 'relative_version'.
        For the milestone testers, negative => original version, 0 => newest, positive => second newest, etc.
        """
        results = []
        if search_key_index == self.table.key:
            rid = self.table.index.pk_index.get(search_key, None)
            if rid is None:
                return []
            versions = self.table.rid_to_versions[rid]
            idx = self._compute_version_index(versions, relative_version)
            if idx is None:
                return []
            chosen_version = versions[idx]
            # project
            projected = []
            for i, flag in enumerate(projected_columns_index):
                if flag == 1:
                    projected.append(chosen_version[i])
            record_obj = Record(rid, search_key, projected)
            results.append(record_obj)
        else:
            # naive scan
            for pk_val, rid in self.table.index.pk_index.items():
                versions = self.table.rid_to_versions[rid]
                newest = versions[-1]
                if newest[search_key_index] == search_key:
                    idx = self._compute_version_index(versions, relative_version)
                    chosen_version = versions[idx]
                    projected = []
                    for i, flag in enumerate(projected_columns_index):
                        if flag == 1:
                            projected.append(chosen_version[i])
                    record_obj = Record(rid, pk_val, projected)
                    results.append(record_obj)
        return results

    def update(self, primary_key, *columns):
        """
        Update the record with 'primary_key' by overwriting any non-None columns.
        Creates a new version in rid_to_versions[rid].
        Returns True if success, False otherwise.
        """
        rid = self.table.index.pk_index.get(primary_key, None)
        if rid is None:
            return False
        versions = self.table.rid_to_versions[rid]
        newest = versions[-1]
        new_version = list(newest)  # copy
        for col_idx, val in enumerate(columns):
            if val is not None:
                new_version[col_idx] = val
        # append the new version
        versions.append(new_version)
        return True

    def sum(self, start_range, end_range, aggregate_column_index):
        """
        Sums the newest version of 'aggregate_column_index' for all pks in [start_range .. end_range].
        Returns the sum or False if no record is in range.
        """
        # gather pk in [start_range .. end_range]
        relevant_pks = [pk for pk in self.table.index.pk_index.keys() if start_range <= pk <= end_range]
        if not relevant_pks:
            return False
        total = 0
        for pk in relevant_pks:
            rid = self.table.index.pk_index[pk]
            versions = self.table.rid_to_versions[rid]
            newest = versions[-1]
            total += newest[aggregate_column_index]
        return total

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        """
        Similar to sum, but chooses the correct version via relative_version.
        """
        relevant_pks = [pk for pk in self.table.index.pk_index.keys() if start_range <= pk <= end_range]
        if not relevant_pks:
            return False
        total = 0
        for pk in relevant_pks:
            rid = self.table.index.pk_index[pk]
            versions = self.table.rid_to_versions[rid]
            idx = self._compute_version_index(versions, relative_version)
            val = versions[idx][aggregate_column_index]
            total += val
        return total

    def increment(self, key, column):
        """
        Example helper: increments one column for the record whose pk=key
        by reading the newest value and then calling update.
        """
        result = self.select(key, self.table.key, [1]*self.table.num_columns)
        if len(result) == 0:
            return False
        record = result[0]
        old_val = record.columns[column]
        new_val = old_val + 1
        updated_cols = [None]*self.table.num_columns
        updated_cols[column] = new_val
        return self.update(key, *updated_cols)
