# # lstore/db.py

# import os
# import json
# from lstore.table import Table
# from lstore.bufferpool import BufferPool
# from lstore.config import DATA_PATH

# class Database:
#     def __init__(self):
#         self.tables = {}
#         self.bufferpool = None
#         self.db_path = DATA_PATH

#     def open(self, path):
#         """
#         Initialize bufferpool, read metadata from disk, re-construct tables.
#         """
#         self.db_path = path
#         self.bufferpool = BufferPool(self.db_path)
#         os.makedirs(self.db_path, exist_ok=True)

#         meta_file = os.path.join(self.db_path, "db_meta.json")
#         if os.path.exists(meta_file):
#             with open(meta_file, "r") as f:
#                 meta = json.load(f)
#             for tbl_name, tbl_info in meta.items():
#                 t = Table(tbl_name, tbl_info["num_columns"], tbl_info["key"], self.bufferpool)
#                 t.next_rid = tbl_info["next_rid"]
#                 # load more info if needed
#                 self.tables[tbl_name] = t
#         else:
#             # no metadata file => no existing tables
#             pass

#     def close(self):
#         """
#         Flush all pages, write metadata to disk, etc.
#         """
#         if self.bufferpool:
#             self.bufferpool.flush_all()

#         # write metadata
#         meta = {}
#         for tbl_name, tbl_obj in self.tables.items():
#             meta[tbl_name] = {
#                 "num_columns": tbl_obj.num_columns,
#                 "key": tbl_obj.key,
#                 "next_rid": tbl_obj.next_rid
#             }
#         meta_file = os.path.join(self.db_path, "db_meta.json")
#         with open(meta_file, "w") as f:
#             json.dump(meta, f, indent=2)

#     def create_table(self, name, num_columns, key_index):
#         # create table object
#         t = Table(name, num_columns, key_index, self.bufferpool)
#         self.tables[name] = t
#         return t

#     def drop_table(self, name):
#         if name in self.tables:
#             # optionally remove files from disk
#             self.tables[name].drop_files()
#             del self.tables[name]

#     def get_table(self, name):
#         return self.tables.get(name, None)



import os
import pickle
from lstore.table import Table
from lstore.bufferpool import Bufferpool

class Database:
    def __init__(self, bufferpool_size=10):
        self.tables = {}  # Stores all tables
        self.db_path = None  # Database directory for persistence
        self.bufferpool = Bufferpool(bufferpool_size)  # Bufferpool for managing pages

    def open(self, path):
    # """Loads database from disk, including persisted tables."""
        self.db_path = path
        if not os.path.exists(path):
            os.makedirs(path)
            # print("Database directory created.")
            return
        
        for file in os.listdir(path):
            if file.endswith(".tbl"):
                with open(os.path.join(path, file), "rb") as f:
                    table = pickle.load(f)
                    self.tables[table.name] = table
        # print("Database loaded from disk.")

    def close(self):
        """Saves all tables and their data to disk."""
        if not self.db_path:
            raise ValueError("Database path is not set.")
        
        for table_name, table in self.tables.items():
            with open(os.path.join(self.db_path, f"{table_name}.tbl"), "wb") as f:
                pickle.dump(table, f)
        # print("Database saved to disk.")

    def create_table(self, name, num_columns, key_index):
        """Creates a new table."""
        table = Table(name, num_columns, key_index)  # ✅ Ensure key_index is passed correctly
        self.tables[name] = table
        return table


    def drop_table(self, name):
        """Deletes a table."""
        if name in self.tables:
            del self.tables[name]
            os.remove(os.path.join(self.db_path, f"{name}.tbl"))

    def get_table(self, name):
        """Retrieves a table."""
        return self.tables.get(name, None)
