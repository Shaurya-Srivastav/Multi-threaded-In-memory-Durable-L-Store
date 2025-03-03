# lstore/transaction.py

class Transaction:
    """
    A transaction that can run multiple queries.
    If concurrency is used, we store a valid transaction ID.
    Otherwise, we store -1 or None to skip concurrency.
    """

    def __init__(self, transaction_id=None):
        """
        If transaction_id is None => single-thread or no concurrency
        Otherwise store an integer ID for lock manager usage.
        """
        if transaction_id is None:
            self.tid = -1
        else:
            self.tid = transaction_id

        self.queries = []
        self.rollback_log = []

    def add_query(self, query_fn, table, *args):
        """
        query_fn: a bound function like query.insert, query.update, ...
        table: the table instance
        args: the arguments to pass
        """
        self.queries.append((query_fn, table, args))

        # If it's an update or delete, store old columns for rollback
        if query_fn.__name__ in ["update", "delete"]:
            if args:
                key = args[0]  # the primary key
                rid = table.index.pk_index.get(key, None)
                if rid is not None and rid in table.rid_to_versions:
                    # the newest version's columns
                    old_cols = table.rid_to_versions[rid][-1]
                    # Store for rollback
                    self.rollback_log.append((table, rid, old_cols))

    def run(self):
        """
        Run each query in order. If any fails (returns False), abort.
        """
        for (query_fn, table, args) in self.queries:
            result = query_fn(*args, transaction_id=self.tid)
            if result is False:
                return self.abort()
        return self.commit()

    def abort(self):
        """
        Roll back changes if possible.
        Then release locks.
        """
        # naive rollback: restore old columns
        for (table, rid, old_cols) in reversed(self.rollback_log):
            if rid in table.rid_to_versions and table.rid_to_versions[rid]:
                # revert the newest version to old_cols
                table.rid_to_versions[rid][-1] = old_cols[:]
            # If you maintain secondary indexes or special flags, you'd revert them too.

        # release locks
        if self.queries:
            table = self.queries[0][1]
            if table.db and table.db.lock_manager and self.tid != -1:
                table.db.lock_manager.release_all(self.tid)

        self.rollback_log.clear()
        return False

    def commit(self):
        """
        Release locks at commit.
        """
        if self.queries:
            table = self.queries[0][1]
            if table.db and table.db.lock_manager and self.tid != -1:
                table.db.lock_manager.release_all(self.tid)

        self.rollback_log.clear()
        return True
