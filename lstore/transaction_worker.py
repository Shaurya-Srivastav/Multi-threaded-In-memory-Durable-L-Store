# # lstore/transaction_worker.py

# class TransactionWorker:
#     def __init__(self, transactions=[]):
#         self.transactions = transactions
#         self.stats = []
#         self.result = 0

#     def add_transaction(self, t):
#         self.transactions.append(t)

#     def run(self):
#         self.__run()

#     def join(self):
#         pass

#     def __run(self):
#         for tx in self.transactions:
#             outcome = tx.run()
#             self.stats.append(outcome)
#         self.result = sum(1 for x in self.stats if x is True)



class TransactionWorker:
    def __init__(self):
        self.transactions = []  # List of transactions
        self.stats = []  # Stores transaction results
        self.result = 0  # Number of committed transactions

    def add_transaction(self, transaction):
        """Adds a transaction to the worker."""
        self.transactions.append(transaction)

    def run(self):
        """Executes all transactions sequentially (single-threaded for Milestone 2)."""
        for transaction in self.transactions:
            success = transaction.run()
            self.stats.append(success)

        self.result = sum(1 for x in self.stats if x)
        print(f"✅ {self.result}/{len(self.transactions)} transactions committed.")

    def join(self):
        """Placeholder function (multi-threading is for Milestone 3)."""
        pass
