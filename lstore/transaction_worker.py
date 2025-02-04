from lstore.table import Table, Record
from lstore.index import Index

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions=[]):
        self.transactions = transactions
        self.stats = []
        self.result = 0

    def add_transaction(self, t):
        self.transactions.append(t)

    def run(self):
        # single-threaded for milestone1
        self.__run()

    def join(self):
        # no-op
        pass

    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

