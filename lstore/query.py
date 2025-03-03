# lstore/query.py

from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock_manager import LockMode

class Query:
    """
    Provides operations on a specific table (insert, select, update, delete, sum, etc.).
    """

    def __init__(self, table):
        self.table = table

    def _acquire_lock_for_rid(self, transaction_id, rid, lock_mode):
        """
        Helper to acquire a lock from the table's DB LockManager under no-wait.
        Returns True if acquired, False otherwise.
        """
        if not self.table.db:
            return True  # In case the table is not bound to a DB or concurrency is off

        lm = self.table.db.lock_manager
        return lm.acquire_lock(transaction_id, rid, lock_mode)

    def insert(self, transaction_id, *columns):
        """
        Insert a record with specified columns.
        Return True upon success, False if insert fails.
        """
        # Create a brand-new RID
        # Typically we do an exclusive lock on that new RID
        pk_val = columns[self.table.key]
        if pk_val in self.table.index.pk_index:
            return False  # Duplicate PK

        new_rid = self.table.get_new_rid()

        # Attempt to lock
        ok = self._acquire_lock_for_rid(transaction_id, new_rid, LockMode.EXCLUSIVE)
        if not ok:
            return False

        # Actually insert
        self.table.rid_to_versions[new_rid] = [list(columns)]
        self.table.index.pk_index[pk_val] = new_rid

        # Also handle secondary indexes if needed
        for col_id, value in enumerate(columns):
            if col_id in self.table.index.secondary_indexes:
                dct = self.table.index.secondary_indexes[col_id]
                dct.setdefault(value, []).append(new_rid)
        return True

    def delete(self, transaction_id, primary_key):
        """
        Delete the record with the specified primary key.
        Return True if successful, False otherwise.
        """
        rid = self.table.index.pk_index.get(primary_key, None)
        if rid is None:
            return False

        # Acquire exclusive lock on that rid
        ok = self._acquire_lock_for_rid(transaction_id, rid, LockMode.EXCLUSIVE)
        if not ok:
            return False

        # Actually delete
        del self.table.index.pk_index[primary_key]
        old_versions = self.table.rid_to_versions.pop(rid, None)
        # If you maintain secondary indexes, remove from them
        if old_versions and len(old_versions) > 0:
            old_vals = old_versions[-1]
            for col_id, val in enumerate(old_vals):
                if col_id in self.table.index.secondary_indexes and val in self.table.index.secondary_indexes[col_id]:
                    lst = self.table.index.secondary_indexes[col_id][val]
                    if rid in lst:
                        lst.remove(rid)
        return True

    def select(self, transaction_id, search_key, search_key_index, projected_columns_index):
        """
        Return a list of matching Records.
        Acquire shared locks on any matching rid.
        """
        results = []
        if search_key_index == self.table.key:
            rid = self.table.index.pk_index.get(search_key, None)
            if rid is None:
                return []
            # Acquire shared lock
            ok = self._acquire_lock_for_rid(transaction_id, rid, LockMode.SHARED)
            if not ok:
                return False  # or return [], but typically we'd signal abort with False

            versions = self.table.rid_to_versions[rid]
            newest = versions[-1]
            projected = [
                newest[i] for i, flag in enumerate(projected_columns_index) if flag == 1
            ]
            results.append(Record(rid, search_key, projected))
        else:
            # Possibly use a secondary index
            rids = self.table.index.locate(search_key_index, search_key)
            if not rids:
                # fallback brute force
                rids = []
                for rid, versions in self.table.rid_to_versions.items():
                    if versions[-1][search_key_index] == search_key:
                        rids.append(rid)

            for rid in rids:
                ok = self._acquire_lock_for_rid(transaction_id, rid, LockMode.SHARED)
                if not ok:
                    return False

                versions = self.table.rid_to_versions[rid]
                newest = versions[-1]
                projected = [
                    newest[i] for i, flag in enumerate(projected_columns_index) if flag == 1
                ]
                results.append(Record(rid, search_key, projected))

        return results

    def update(self, transaction_id, primary_key, *columns):
        """
        Update the record for the given primary key with the non-None columns.
        Return True if successful, False if abort or no match.
        """
        rid = self.table.index.pk_index.get(primary_key, None)
        if rid is None:
            return False

        ok = self._acquire_lock_for_rid(transaction_id, rid, LockMode.EXCLUSIVE)
        if not ok:
            return False

        versions = self.table.rid_to_versions[rid]
        newest = versions[-1][:]

        updated = False
        for col_idx, val in enumerate(columns):
            if val is not None:
                # If we have a secondary index on col_idx, remove old value, insert new value
                old_val = newest[col_idx]
                newest[col_idx] = val
                updated = True
                # Update secondary index
                if col_idx in self.table.index.secondary_indexes:
                    # remove old
                    if old_val in self.table.index.secondary_indexes[col_idx]:
                        lst = self.table.index.secondary_indexes[col_idx][old_val]
                        if rid in lst:
                            lst.remove(rid)
                    # add new
                    self.table.index.secondary_indexes[col_idx].setdefault(val, []).append(rid)

        if updated:
            versions.append(newest)

        return True

    def sum(self, transaction_id, start_range, end_range, aggregate_column_index):
        """
        Sum over the range of primary keys. 
        Acquire shared locks on each matching rid.
        """
        relevant_pks = [
            pk for pk in self.table.index.pk_index.keys()
            if start_range <= pk <= end_range
        ]
        if not relevant_pks:
            return 0

        total = 0
        for pk in relevant_pks:
            rid = self.table.index.pk_index[pk]
            ok = self._acquire_lock_for_rid(transaction_id, rid, LockMode.SHARED)
            if not ok:
                return False

            versions = self.table.rid_to_versions[rid]
            newest = versions[-1]
            total += newest[aggregate_column_index]

        return total

    def select_version(self, transaction_id, search_key, search_key_index, projected_columns_index, relative_version):
        """
        Similar to select, but returns an older version.
        We'll still do shared locks. 
        """
        results = []
        if search_key_index == self.table.key:
            rid = self.table.index.pk_index.get(search_key, None)
            if rid is None:
                return []
            ok = self._acquire_lock_for_rid(transaction_id, rid, LockMode.SHARED)
            if not ok:
                return False

            versions = self.table.rid_to_versions[rid]
            idx = max(0, len(versions) - 1 + relative_version)
            older = versions[idx]
            projected = [older[i] for i, flag in enumerate(projected_columns_index) if flag == 1]
            results.append(Record(rid, search_key, projected))
        else:
            rids = self.table.index.locate(search_key_index, search_key)
            if not rids:
                # fallback brute force
                for rid, ver in self.table.rid_to_versions.items():
                    if ver[-1][search_key_index] == search_key:
                        rids.append(rid)

            for rid in rids:
                ok = self._acquire_lock_for_rid(transaction_id, rid, LockMode.SHARED)
                if not ok:
                    return False
                versions = self.table.rid_to_versions[rid]
                idx = max(0, len(versions) - 1 + relative_version)
                older = versions[idx]
                projected = [older[i] for i, flag in enumerate(projected_columns_index) if flag == 1]
                results.append(Record(rid, search_key, projected))
        return results

    def sum_version(self, transaction_id, start_range, end_range, aggregate_column_index, relative_version):
        relevant_pks = [
            pk for pk in self.table.index.pk_index.keys()
            if start_range <= pk <= end_range
        ]
        if not relevant_pks:
            return 0

        total = 0
        for pk in relevant_pks:
            rid = self.table.index.pk_index[pk]
            ok = self._acquire_lock_for_rid(transaction_id, rid, LockMode.SHARED)
            if not ok:
                return False
            versions = self.table.rid_to_versions[rid]
            idx = max(0, len(versions) - 1 + relative_version)
            older = versions[idx]
            total += older[aggregate_column_index]
        return total
