# # lstore/transaction.py

# class Transaction:
#     def __init__(self):
#         self.queries = []

#     def add_query(self, query, table, *args):
#         self.queries.append((query, args))

#     def run(self):
#         for (q_func, q_args) in self.queries:
#             res = q_func(*q_args)
#             if res is False:
#                 return self.abort()
#         return self.commit()

#     def abort(self):
#         return False

#     def commit(self):
#         return True



class Transaction:
    def __init__(self):
        self.queries = []  # Stores queries to be executed as a single transaction

    def add_query(self, query_func, table, *args):
        """Adds a query function to the transaction."""
        self.queries.append((query_func, table, args))

    def run(self):
        """Executes all queries in the transaction."""
        for (query_func, table, args) in self.queries:
            result = query_func(*args)
            if result is False:  # If any query fails, abort the transaction
                return self.abort()
        return self.commit()

    def abort(self):
        """Handles transaction rollback (not implemented in Milestone 2)."""
        print("Transaction aborted.")
        return False

    def commit(self):
        """Commits all executed queries (Milestone 2 does not require rollback)."""
        print("Transaction committed.")
        return True
