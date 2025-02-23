# lstore/db.py

import os
import json
from lstore.table import Table
from lstore.bufferpool import BufferPool
from lstore.config import DATA_PATH

class Database:
    def __init__(self):
        self.tables = {}
        self.bufferpool = None
        self.db_path = DATA_PATH

    def open(self, path):
        """
        Initialize bufferpool, read metadata from disk, re-construct tables.
        """
        self.db_path = path
        self.bufferpool = BufferPool(self.db_path)
        os.makedirs(self.db_path, exist_ok=True)

        meta_file = os.path.join(self.db_path, "db_meta.json")
        if os.path.exists(meta_file):
            with open(meta_file, "r") as f:
                meta = json.load(f)
            for tbl_name, tbl_info in meta.items():
                t = Table(tbl_name, tbl_info["num_columns"], tbl_info["key"], self.bufferpool)
                t.next_rid = tbl_info["next_rid"]
                # load more info if needed
                self.tables[tbl_name] = t
        else:
            # no metadata file => no existing tables
            pass

    def close(self):
        """
        Flush all pages, write metadata to disk, etc.
        """
        if self.bufferpool:
            self.bufferpool.flush_all()

        # write metadata
        meta = {}
        for tbl_name, tbl_obj in self.tables.items():
            meta[tbl_name] = {
                "num_columns": tbl_obj.num_columns,
                "key": tbl_obj.key,
                "next_rid": tbl_obj.next_rid
            }
        meta_file = os.path.join(self.db_path, "db_meta.json")
        with open(meta_file, "w") as f:
            json.dump(meta, f, indent=2)

    def create_table(self, name, num_columns, key_index):
        # create table object
        t = Table(name, num_columns, key_index, self.bufferpool)
        self.tables[name] = t
        return t

    def drop_table(self, name):
        if name in self.tables:
            # optionally remove files from disk
            self.tables[name].drop_files()
            del self.tables[name]

    def get_table(self, name):
        return self.tables.get(name, None)
