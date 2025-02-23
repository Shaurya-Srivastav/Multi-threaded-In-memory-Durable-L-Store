# lstore/transaction.py

class Transaction:
    def __init__(self):
        self.queries = []

    def add_query(self, func, table, *args):
        self.queries.append((func, args))

    def run(self):
        for (f, a) in self.queries:
            res = f(*a)
            if res is False:
                return self.abort()
        return self.commit()

    def abort(self):
        return False

    def commit(self):
        return True
