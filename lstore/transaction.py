# lstore/transaction.py

class Transaction:
    """
    Represents a transaction with multiple queries.
    """

    def __init__(self, transaction_id):
        self.tid = transaction_id
        self.queries = []
        # For rollback, we store (table, rid, old_values)
        self.rollback_log = []

    def add_query(self, query_fn, table, *args):
        """
        Each 'query_fn' is a bound function, e.g. query.update or query.delete, etc.
        'args' are the arguments to pass to that function.
        We also remember enough info so we can do rollback.
        """
        self.queries.append((query_fn, table, args))

        # For rollback, if this is update or delete, we read current state
        if query_fn.__name__ in ["update", "delete"]:
            # The first argument to update/delete is primary_key
            key = args[0]
            # SELECT the current record (shared? exclusive?):
            # Because we are just preparing for rollback, we'll do a direct read
            original_rec = table.select(
                self.tid,  # we can reuse the same transaction ID
                key,
                table.key,  # searching by PK
                [1]*table.num_columns
            )
            # original_rec might be False if we can’t lock. 
            # In real systems we’d do something more robust, but here we ignore that edge case.
            if isinstance(original_rec, list) and original_rec:
                # store full old columns
                old_cols = original_rec[0].columns
                # For rollback, we store (table, rid, old_cols)
                rid = original_rec[0].rid
                self.rollback_log.append((table, rid, old_cols))

    def run(self):
        """
        Execute each query in sequence. If any returns False, we abort.
        If all succeed, we commit.
        """
        for (query_fn, table, args) in self.queries:
            result = query_fn(self.tid, *args)
            if result is False:
                return self.abort()
        return self.commit()

    def abort(self):
        """
        Revert changes. Release all locks.
        """
        # rollback each action from the rollback_log in reverse order
        for (table, rid, old_cols) in reversed(self.rollback_log):
            # re-apply old_cols
            # We do an exclusive lock for safety, but in real systems,
            # you typically do an UNDO from log or something else.
            # We'll do the simplest approach:
            table.rid_to_versions[rid][-1] = old_cols[:]  # revert in place
            # Also re-update any secondary indexes if necessary
            # This can be fairly involved. We leave it as an exercise.

        # release all locks
        if table.db and table.db.lock_manager:
            table.db.lock_manager.release_all(self.tid)

        self.rollback_log.clear()
        return False

    def commit(self):
        """
        Commit changes, release locks.
        """
        # In real systems, we’d flush a WAL, etc.
        # For 2PL: we must release all locks.
        if self.queries:
            table = self.queries[0][1]  # any table reference is fine if single DB
            if table.db and table.db.lock_manager:
                table.db.lock_manager.release_all(self.tid)

        self.rollback_log.clear()
        return True
