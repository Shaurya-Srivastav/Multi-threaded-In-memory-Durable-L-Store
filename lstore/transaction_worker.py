# lstore/transaction_worker.py

class TransactionWorker:
    def __init__(self, transactions=[]):
        self.transactions = transactions
        self.stats = []
        self.result = 0

    def add_transaction(self, t):
        self.transactions.append(t)

    def run(self):
        for tx in self.transactions:
            outcome = tx.run()
            self.stats.append(outcome)
        self.result = sum(1 for x in self.stats if x is True)

    def join(self):
        pass
