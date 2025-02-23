# lstore/db.py

from lstore.table import Table
import os

class Database:
    def __init__(self):
        # We'll keep a dict: { table_name : Table object }
        self.tables = {}
        self.folder_path = ""
        self.table_path = ""
        self.bufferpool = []

    # Not required for milestone1: do nothing
    def open(self, path):
        self.folder_path = path
        try:
            os.mkdir(path)
            print(f"Folder '{path}' created successfully.")
        except FileExistsError:
            print(f"Folder '{path}' already exists.")

    # Not required for milestone1: do nothing
    def close(self):
        pass
        # self.file_path.close()

    """
    Creates a new table
    :param name: string         # Table name
    :param num_columns: int     # Number of columns
    :param key_index: int       # Index of table's primary key
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        self.tables[name] = table
        try:
            path = os.path.join(self.folder_path, name)
            os.mkdir(path)
            self.table_path = path
            print(f"Folder '{path}' created successfully.")
        except FileExistsError:
            print(f"Folder '{path}' already exists.")
        return table

    """
    Deletes the specified table
    """
    def drop_table(self, name):
        if name in self.tables:
            del self.tables[name]

    """
    Returns table with the passed name
    """
    def get_table(self, name):
        return self.tables.get(name, None)