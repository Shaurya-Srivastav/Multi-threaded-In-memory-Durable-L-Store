# query.py

from lstore.table import Record
try:
    from lstore.lock_manager import LockMode
except ImportError:
    # fallback if lock_manager isn't there
    class LockMode:
        SHARED = "SHARED"
        EXCLUSIVE = "EXCLUSIVE"

class Query:
    """
    Provides an interface to do insert, select, update, delete, sum,
    with optional concurrency (transaction_id).
    If transaction_id = -1 or None, we skip lock acquisition.
    """

    def __init__(self, table):
        self.table = table

    def _acquire_lock_for_rid(self, transaction_id, rid, lock_mode):
        """
        If transaction_id is None or -1, skip concurrency.
        Otherwise, attempt to acquire no-wait lock from the global LockManager.
        Return True if success, False if fail => abort.
        """
        if transaction_id is None or transaction_id == -1 or not self.table.db:
            return True

        lm = self.table.db.lock_manager
        return lm.acquire_lock(transaction_id, rid, lock_mode)

    def insert(self, *columns, transaction_id=None):
        """
        Insert a record. Columns are the data. 
        transaction_id is optional for concurrency.
        Return True/False for success/fail.
        """
        # columns is a tuple => first self.table.num_columns are col values
        if len(columns) < self.table.num_columns:
            return False  # not enough columns

        col_list = list(columns)
        pk_val = col_list[self.table.key]
        # check uniqueness
        if pk_val in self.table.index.pk_index:
            return False

        # get new rid
        new_rid = self.table.get_new_rid()

        # Acquire exclusive lock if concurrency
        if not self._acquire_lock_for_rid(transaction_id, new_rid, LockMode.EXCLUSIVE):
            return False

        # store
        self.table.rid_to_versions[new_rid] = [col_list]
        self.table.index.pk_index[pk_val] = new_rid

        # update secondaries
        for col_id, val in enumerate(col_list):
            if col_id in self.table.index.secondary_indexes:
                dct = self.table.index.secondary_indexes[col_id]
                dct.setdefault(val, []).append(new_rid)

        return True

    def delete(self, primary_key, transaction_id=None):
        rid = self.table.index.pk_index.get(primary_key)
        if rid is None:
            return False

        if not self._acquire_lock_for_rid(transaction_id, rid, LockMode.EXCLUSIVE):
            return False

        del self.table.index.pk_index[primary_key]
        old_versions = self.table.rid_to_versions.pop(rid, None)

        if old_versions and len(old_versions) > 0:
            old_vals = old_versions[-1]
            for col_id, val in enumerate(old_vals):
                if col_id in self.table.index.secondary_indexes:
                    if val in self.table.index.secondary_indexes[col_id]:
                        lst = self.table.index.secondary_indexes[col_id][val]
                        if rid in lst:
                            lst.remove(rid)
        return True

    def select(self, search_key, search_key_index, projected_columns_index, transaction_id=None):
        """
        Return list of Record objects or False if concurrency abort is needed.
        """
        results = []

        if search_key_index == self.table.key:
            # direct pk lookup
            rid = self.table.index.pk_index.get(search_key, None)
            if rid is None:
                return []

            if not self._acquire_lock_for_rid(transaction_id, rid, LockMode.SHARED):
                return False

            versions = self.table.rid_to_versions[rid]
            newest = versions[-1]
            projected = [newest[i] for i, flag in enumerate(projected_columns_index) if flag == 1]
            results.append(Record(rid, search_key, projected))
        else:
            # secondary
            rids = self.table.index.locate(search_key_index, search_key)
            if not rids:
                # fallback brute force
                rids = []
                for rid, versions in self.table.rid_to_versions.items():
                    if versions[-1][search_key_index] == search_key:
                        rids.append(rid)

            for rid in rids:
                if not self._acquire_lock_for_rid(transaction_id, rid, LockMode.SHARED):
                    return False
                newest = self.table.rid_to_versions[rid][-1]
                projected = [newest[i] for i, flag in enumerate(projected_columns_index) if flag == 1]
                results.append(Record(rid, search_key, projected))

        return results

    def update(self, primary_key, *columns, transaction_id=None):
        rid = self.table.index.pk_index.get(primary_key, None)
        if rid is None:
            return False

        if not self._acquire_lock_for_rid(transaction_id, rid, LockMode.EXCLUSIVE):
            return False

        versions = self.table.rid_to_versions[rid]
        newest = versions[-1][:]  # copy

        updated = False
        for col_idx, val in enumerate(columns):
            if val is not None:
                old_val = newest[col_idx]
                newest[col_idx] = val
                updated = True

                # update secondary index
                if col_idx in self.table.index.secondary_indexes:
                    if old_val in self.table.index.secondary_indexes[col_idx]:
                        lst = self.table.index.secondary_indexes[col_idx][old_val]
                        if rid in lst:
                            lst.remove(rid)
                    self.table.index.secondary_indexes[col_idx].setdefault(val, []).append(rid)

        if updated:
            versions.append(newest)
        return True

    def sum(self, start_range, end_range, aggregate_column_index, transaction_id=None):
        """
        Sum col for pk in [start_range, end_range).
        """
        relevant_pks = [pk for pk in self.table.index.pk_index.keys()
                        if start_range <= pk < end_range]
        if not relevant_pks:
            return 0

        total = 0
        for pk in relevant_pks:
            rid = self.table.index.pk_index[pk]
            if not self._acquire_lock_for_rid(transaction_id, rid, LockMode.SHARED):
                return False
            newest = self.table.rid_to_versions[rid][-1]
            total += newest[aggregate_column_index]
        return total

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version, transaction_id=None):
        """
        Similar to select but we pick an older version:
          if relative_version = 0 => newest
          if negative => older 
        """
        results = []
        if search_key_index == self.table.key:
            rid = self.table.index.pk_index.get(search_key)
            if rid is None:
                return []
            if not self._acquire_lock_for_rid(transaction_id, rid, LockMode.SHARED):
                return False
            versions = self.table.rid_to_versions[rid]
            idx = max(0, len(versions)-1 + relative_version)
            older = versions[idx]
            projected = [older[i] for i, flag in enumerate(projected_columns_index) if flag == 1]
            results.append(Record(rid, search_key, projected))
        else:
            rids = self.table.index.locate(search_key_index, search_key)
            if not rids:
                # fallback
                rids = []
                for rid, vs in self.table.rid_to_versions.items():
                    if vs[-1][search_key_index] == search_key:
                        rids.append(rid)

            for rid in rids:
                if not self._acquire_lock_for_rid(transaction_id, rid, LockMode.SHARED):
                    return False
                versions = self.table.rid_to_versions[rid]
                idx = max(0, len(versions)-1 + relative_version)
                older = versions[idx]
                projected = [older[i] for i, flag in enumerate(projected_columns_index) if flag == 1]
                results.append(Record(rid, search_key, projected))

        return results

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version, transaction_id=None):
        relevant_pks = [pk for pk in self.table.index.pk_index.keys()
                        if start_range <= pk < end_range]
        if not relevant_pks:
            return 0

        total = 0
        for pk in relevant_pks:
            rid = self.table.index.pk_index[pk]
            if not self._acquire_lock_for_rid(transaction_id, rid, LockMode.SHARED):
                return False
            versions = self.table.rid_to_versions[rid]
            idx = max(0, len(versions)-1 + relative_version)
            older = versions[idx]
            total += older[aggregate_column_index]
        return total
