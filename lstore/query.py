from lstore.table import Table, Record
from lstore.index import Index

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        pk_val = columns[self.table.key]
        if pk_val in self.table.index.pk_index:
            return False  # Primary key must be unique

        new_rid = self.table.get_new_rid()
        self.table.rid_to_versions[new_rid] = [list(columns)]
        self.table.index.pk_index[pk_val] = new_rid
        return True

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        rid = self.table.index.pk_index.pop(primary_key, None)
        if rid is None:
            return False 

        del self.table.rid_to_versions[rid] 
        return True

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        results = []
        if search_key_index == self.table.key:
            rid = self.table.index.pk_index.get(search_key, None)
            if rid is None:
                return []
            versions = self.table.rid_to_versions[rid]
            newest = versions[-1]

            projected = [newest[i] for i, flag in enumerate(projected_columns_index) if flag == 1]
            results.append(Record(rid, search_key, projected)) 
        else:
            rids = self.table.index.locate(search_key_index, search_key)
            if len(rids) == 0:
                for rid, versions in self.table.rid_to_versions.items():
                    if search_key == versions[-1][search_key_index]:
                        rids.append(rid)
                
            for rid in rids:
                versions = self.table.rid_to_versions[rid]
                newest = versions[-1]

                projected = [newest[i] for i, flag in enumerate(projected_columns_index) if flag == 1]
                results.append(Record(rid, search_key, projected))

        return results

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        rid = self.table.index.pk_index.get(primary_key, None)
        if rid is None:
            return False  

        versions = self.table.rid_to_versions[rid]
        newest = versions[-1][:]  

        updated = False
        for col_idx, val in enumerate(columns):
            if val is not None:
                newest[col_idx] = val 
                updated = True

        if updated:
            versions.append(newest)

        return True

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        relevant_pks = [pk for pk in self.table.index.pk_index.keys() if start_range <= pk <= end_range]
        if not relevant_pks:
            return 0

        total = sum(self.table.rid_to_versions[self.table.index.pk_index[pk]][-1][aggregate_column_index] for pk in relevant_pks)
        return total
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        results = []

        if search_key_index == self.table.key:
            rid = self.table.index.pk_index.get(search_key, None)
            if rid is None:
                return []
            versions = self.table.rid_to_versions[rid]

            idx = max(0, len(versions) - 1 + relative_version)

            projected = [versions[idx][i] for i, flag in enumerate(projected_columns_index) if flag == 1]
            results.append(Record(rid, search_key, projected))  
        else:
            rids = self.table.index.locate(search_key_index, search_key)
            for rid in rids:
                versions = self.table.rid_to_versions[rid]
                idx = max(0, len(versions) - 1 + relative_version)

                projected = [versions[idx][i] for i, flag in enumerate(projected_columns_index) if flag == 1]
                results.append(Record(rid, search_key, projected)) 

        return results
    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        relevant_pks = [pk for pk in self.table.index.pk_index.keys() if start_range <= pk <= end_range]
        if not relevant_pks:
            return 0

        total = 0
        for pk in relevant_pks:
            rid = self.table.index.pk_index[pk]
            versions = self.table.rid_to_versions[rid]
            idx = max(0, len(versions) - 1 + relative_version)  

            total += versions[idx][aggregate_column_index]

        return total



