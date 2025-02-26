import threading

class TransactionWorker:
    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions=[]):
        self.transactions = transactions  
        self.stats = []
        self.result = 0  

    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

    """
    Runs all transaction as a thread
    """
    def run(self):
        for transaction in self.transactions:
            success = transaction.run()
            self.stats.append(success)

        self.result = sum(1 for x in self.stats if x)

    def join(self):
        pass