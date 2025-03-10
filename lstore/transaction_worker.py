class TransactionWorker:
    """
    A simple worker that can run multiple transactions (synchronously).
    Your testers call run() and then join(), but we do not actually spin up new threads.
    """

    def __init__(self, transactions=None):
        self.transactions = transactions if transactions else []
        self.stats = []
        self.result = 0

    def add_transaction(self, t):
        self.transactions.append(t)

    def run(self):
        """
        Execute each transaction in this worker (synchronously).
        If a transaction aborts, we do not retry here unless your spec requires it.
        """
        for txn in self.transactions:
            success = txn.run()
            self.stats.append(success)

        # how many eventually succeeded
        self.result = sum(1 for x in self.stats if x)

    def join(self):
        """
        No-op, required so your tester doesn't crash on worker.join().
        """
        pass
