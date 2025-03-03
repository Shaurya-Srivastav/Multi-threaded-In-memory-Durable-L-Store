# lstore/db.py

import os
import msgpack
from lstore.table import Table
from lstore.bufferpool import Bufferpool
from lstore.index import Index
from lstore.page import Page
from lstore.query import Query
from lstore.lock_manager import LockManager

class Database:
    def __init__(self, bufferpool_size=10):
        self.tables = {}
        self.db_path = None
        self.bufferpool = Bufferpool(bufferpool_size)
        # Single global lock manager for the entire database
        self.lock_manager = LockManager()
        self._next_txn_id = 0

    def open(self, path):
        self.db_path = path
        if not os.path.exists(path):
            os.makedirs(path)
            return

        self.tables = {}
        for file in os.listdir(path):
            if file.endswith(".tbl"):
                file_path = os.path.join(path, file)
                with open(file_path, "rb") as f:
                    data = f.read()
                    if not data:
                        continue
                    table = msgpack.unpackb(data, raw=False, ext_hook=ext_hook, strict_map_key=False)
                    table.db = self  # ensure table can reference db for LockManager
                    self.tables[table.name] = table

    def close(self):
        if not self.db_path:
            raise ValueError("Database path is not set.")

        for table_name, table in self.tables.items():
            file_path = os.path.join(self.db_path, f"{table_name}.tbl")
            with open(file_path, "wb") as f:
                data = msgpack.packb(table, use_bin_type=True, default=custom_default)
                f.write(data)

    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        # Let the table know about the DB (for lock manager, etc.)
        table.db = self
        self.tables[name] = table
        return table

    def drop_table(self, name):
        if name in self.tables:
            del self.tables[name]
            os.remove(os.path.join(self.db_path, f"{name}.tbl"))

    def get_table(self, name):
        return self.tables.get(name, None)

    def get_next_txn_id(self):
        """
        Simple global transaction-id generator.
        """
        self._next_txn_id += 1
        return self._next_txn_id

# Serialization Functions:

EXT_CODE_INDEX  = 1
EXT_CODE_PAGE   = 2
EXT_CODE_QUERY  = 3
EXT_CODE_RECORD = 4
EXT_CODE_TABLE  = 5

def custom_default(obj):
    if isinstance(obj, Index):
        state = obj.__dict__.copy()
        # 'table' often leads to recursion, so skip or store table name only
        state.pop("table", None)
        packed_state = msgpack.packb(state, use_bin_type=True)
        return msgpack.ExtType(EXT_CODE_INDEX, packed_state)

    elif isinstance(obj, Page):
        state = {"data": obj.data}
        packed_state = msgpack.packb(state, use_bin_type=True)
        return msgpack.ExtType(EXT_CODE_PAGE, packed_state)

    elif isinstance(obj, Query):
        state = {"table_name": obj.table.name}
        packed_state = msgpack.packb(state, use_bin_type=True)
        return msgpack.ExtType(EXT_CODE_QUERY, packed_state)

    elif isinstance(obj, Record):
        state = obj.__dict__
        packed_state = msgpack.packb(state, use_bin_type=True)
        return msgpack.ExtType(EXT_CODE_RECORD, packed_state)

    elif isinstance(obj, Table):
        state = obj.__dict__.copy()
        # Remove the DB reference before packing
        state.pop("db", None)
        packed_state = msgpack.packb(state, use_bin_type=True, default=custom_default)
        return msgpack.ExtType(EXT_CODE_TABLE, packed_state)

    # Fallback
    return None

def ext_hook(code, data):
    if code == EXT_CODE_INDEX:
        state = msgpack.unpackb(data, raw=False, strict_map_key=False)
        idx = Index(None)
        idx.__dict__.update(state)
        return idx
    elif code == EXT_CODE_PAGE:
        state = msgpack.unpackb(data, raw=False, strict_map_key=False)
        page = Page()
        page.data = state.get("data", bytearray())
        return page
    elif code == EXT_CODE_QUERY:
        state = msgpack.unpackb(data, raw=False, strict_map_key=False)
        query = Query(Table(state.get("table_name", "unknown")))
        return query
    elif code == EXT_CODE_RECORD:
        state = msgpack.unpackb(data, raw=False, strict_map_key=False)
        from lstore.table import Record
        record = Record(0, None, None)
        record.__dict__.update(state)
        return record
    elif code == EXT_CODE_TABLE:
        state = msgpack.unpackb(data, raw=False, strict_map_key=False, ext_hook=ext_hook)
        table = Table(state["name"], state["num_columns"], state["key"])
        table.__dict__.update(state)
        return table
    return None
