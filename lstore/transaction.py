# lstore/transaction.py

class Transaction:
    def __init__(self):
        self.queries = []

    def add_query(self, query, table, *args):
        self.queries.append((query, args))

    def run(self):
        for (q_func, q_args) in self.queries:
            res = q_func(*q_args)
            if res is False:
                return self.abort()
        return self.commit()

    def abort(self):
        return False

    def commit(self):
        return True
