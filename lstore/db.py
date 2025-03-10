import os
import msgpack
from lstore.table import Table
from lstore.bufferpool import Bufferpool
from lstore.index import Index
from lstore.page import Page
from lstore.query import Query
from lstore.lock_manager import LockManager

class Database:
    """
    Database interface to manage tables, transactions, and persistence.
    """

    def __init__(self, bufferpool_size=10):
        self.tables = {}
        self.db_path = None
        self.bufferpool = Bufferpool(bufferpool_size)
        # Single global lock manager for concurrency
        self.lock_manager = LockManager()
        self._next_txn_id = 0

    def open(self, path):
        """
        Open the database at 'path'. If the directory does not exist, create it.
        Then load all tables (files ending in ".tbl") and reset their versions.
        """
        self.db_path = path
        if not os.path.exists(path):
            os.makedirs(path)
        self.tables = {}
        for filename in os.listdir(path):
            if filename.endswith(".tbl"):
                file_path = os.path.join(path, filename)
                with open(file_path, "rb") as f:
                    data = f.read()
                    if not data:
                        continue
                    table = msgpack.unpackb(
                        data, raw=False, ext_hook=ext_hook, strict_map_key=False
                    )
                    table.db = self
                    # Reset the table so that each record has exactly one (original) version.
                    table.reset_versions()
                    self.tables[table.name] = table

    def close(self):
        """
        Persist all tables to disk by writing each table's data to a .tbl file.
        """
        if not self.db_path:
            raise ValueError("Database path is not set.")
        for table_name, table in self.tables.items():
            file_path = os.path.join(self.db_path, f"{table_name}.tbl")
            with open(file_path, "wb") as f:
                data = msgpack.packb(table, use_bin_type=True, default=custom_default)
                f.write(data)

    def create_table(self, name, num_columns, key_index):
        """
        Create a new table and attach it to this database.
        """
        table = Table(name, num_columns, key_index)
        table.db = self
        self.tables[name] = table
        return table

    def drop_table(self, name):
        """
        Remove a table from memory and delete its file from disk.
        """
        if name in self.tables:
            del self.tables[name]
            os.remove(os.path.join(self.db_path, f"{name}.tbl"))

    def get_table(self, name):
        """
        Retrieve the table by name. Raises an error if not found.
        """
        tbl = self.tables.get(name)
        if tbl is None:
            raise RuntimeError(f"Table '{name}' not found. Did you create it or load it from disk?")
        return tbl

    def get_next_txn_id(self):
        self._next_txn_id += 1
        return self._next_txn_id


# --- Serialization Helpers ---

EXT_CODE_INDEX  = 1
EXT_CODE_PAGE   = 2
EXT_CODE_QUERY  = 3
EXT_CODE_RECORD = 4
EXT_CODE_TABLE  = 5

def custom_default(obj):
    from lstore.index import Index
    from lstore.page import Page
    from lstore.query import Query
    from lstore.table import Table, Record

    if isinstance(obj, Index):
        state = obj.__dict__.copy()
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
        state.pop("db", None)
        packed_state = msgpack.packb(state, use_bin_type=True, default=custom_default)
        return msgpack.ExtType(EXT_CODE_TABLE, packed_state)
    return None

def ext_hook(code, data):
    from lstore.index import Index
    from lstore.page import Page
    from lstore.query import Query
    from lstore.table import Table, Record

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
        q = Query(Table(state.get("table_name", "unknown")))
        return q
    elif code == EXT_CODE_RECORD:
        state = msgpack.unpackb(data, raw=False, strict_map_key=False)
        record = Record(0, None, None)
        record.__dict__.update(state)
        return record
    elif code == EXT_CODE_TABLE:
        state = msgpack.unpackb(data, raw=False, strict_map_key=False, ext_hook=ext_hook)
        tbl = Table(state["name"], state["num_columns"], state["key"])
        tbl.__dict__.update(state)
        return tbl
    return None
