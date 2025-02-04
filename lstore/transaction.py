# lstore/transaction.py

from lstore.table import Table, Record
from lstore.index import Index

class Transaction:
    def __init__(self):
        self.queries = []

    def add_query(self, query, table, *args):
        # (function, arguments)
        self.queries.append((query, args))

    def run(self):
        for (q_func, q_args) in self.queries:
            res = q_func(*q_args)
            if res is False:
                return self.abort()
        return self.commit()

    def abort(self):
        # For milestone1, do no special rollback
        return False

    def commit(self):
        # For milestone1, do no special commit logic
        return True
