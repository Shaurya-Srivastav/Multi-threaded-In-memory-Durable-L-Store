# lstore/transaction_worker.py

import threading

class TransactionWorker(threading.Thread):
    """
    A worker thread that processes a list of transactions.
    """

    def __init__(self, transactions=None):
        super().__init__()
        self.transactions = transactions if transactions else []
        self.stats = []
        self.result = 0

    def add_transaction(self, t):
        self.transactions.append(t)

    def run(self):
        """
        Process all transactions. If one aborts, we retry it until it commits.
        """
        for txn in self.transactions:
            while True:
                success = txn.run()
                if success:
                    self.stats.append(True)
                    break
                else:
                    self.stats.append(False)
                    # Retry immediately. 
                    # In a real system, you might use backoff or a queue.
                    # But here we just do a while loop.

        # Count how many eventually committed
        self.result = sum(1 for x in self.stats if x)

    def join(self):
        super().join()
