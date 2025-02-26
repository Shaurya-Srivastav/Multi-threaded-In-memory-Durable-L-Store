class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []  
        self.rollback_log = []

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args)) 

        # Store original value for rollback if it's an update or delete
        if query.__name__ in ["update", "delete"]:
            key = args[0]
            record = table.select(key, table.key_index, [1] * table.num_columns)
            if record:
                self.rollback_log.append((table, key, record[0].columns))  

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    def abort(self):
        #TODO: do roll-back and any other necessary operations
        for table, key, original_values in reversed(self.rollback_log):
            table.update(key, *original_values) 
        self.rollback_log.clear()
        return False

    def commit(self):
        # TODO: commit to database
        self.rollback_log.clear()
        return True
