# # lstore/table.py

# import threading
# import time
# from lstore.index import Index
# from lstore.page import Page
# from lstore.config import BACKGROUND_MERGE

# INDIRECTION_COLUMN = 0
# RID_COLUMN = 1
# TIMESTAMP_COLUMN = 2
# SCHEMA_ENCODING_COLUMN = 3

# # ADD THIS RECORD CLASS:
# class Record:
#     """
#     A simple record class that can be returned by queries.
#     """
#     def __init__(self, rid, key, columns):
#         self.rid = rid
#         self.key = key
#         self.columns = columns

# class Table:
#     def __init__(self, name, num_columns, key, bufferpool=None):
#         self.name = name
#         self.num_columns = num_columns
#         self.key = key
#         self.bufferpool = bufferpool

#         # page_directory: rid -> list of (base/tail, col_id, page_index, slot_index)
#         self.page_directory = {}

#         self.index = Index(self)
#         self.next_rid = 0

#         # track how many base pages we have for each column
#         self.base_page_count = [0]*self.num_columns
#         # track how many tail pages we have for each column
#         self.tail_page_count = [0]*self.num_columns

#         # For merge
#         self.merge_thread = None
#         if BACKGROUND_MERGE:
#             self.merge_thread = threading.Thread(target=self._merge_daemon, daemon=True)
#             self.merge_thread.start()

#     def drop_files(self):
#         pass

#     def _page_id(self, base_or_tail, col, page_idx):
#         # Example page_id: "Grades_base_c2_p3"
#         return f"{self.name}_{base_or_tail}_c{col}_p{page_idx}"

#     def _new_base_slot(self, col):
#         page_idx = self.base_page_count[col]
#         page_id = self._page_id("base", col, page_idx)
#         frame = self.bufferpool.fetch_page(page_id)
#         p = Page(frame.data)
#         capacity = p.capacity()
#         for slot in range(capacity):
#             val = p.read(slot)
#             if val == 0:  # treat "0" as empty
#                 frame.pin_count -= 1
#                 return (page_id, slot)
#         # if full, new page
#         self.bufferpool.unpin_page(page_id, is_dirty=False)
#         self.base_page_count[col] += 1
#         new_page_id = self._page_id("base", col, self.base_page_count[col])
#         new_frame = self.bufferpool.fetch_page(new_page_id)
#         self.bufferpool.unpin_page(new_page_id, is_dirty=True)
#         return (new_page_id, 0)

#     def _new_tail_slot(self, col):
#         page_idx = self.tail_page_count[col]
#         page_id = self._page_id("tail", col, page_idx)
#         frame = self.bufferpool.fetch_page(page_id)
#         p = Page(frame.data)
#         capacity = p.capacity()
#         for slot in range(capacity):
#             val = p.read(slot)
#             if val == 0:
#                 frame.pin_count -= 1
#                 return (page_id, slot)
#         # full => new tail page
#         self.bufferpool.unpin_page(page_id, is_dirty=False)
#         self.tail_page_count[col] += 1
#         new_page_id = self._page_id("tail", col, self.tail_page_count[col])
#         new_frame = self.bufferpool.fetch_page(new_page_id)
#         self.bufferpool.unpin_page(new_page_id, is_dirty=True)
#         return (new_page_id, 0)

#     def _write_value(self, page_id, slot, value):
#         frame = self.bufferpool.fetch_page(page_id)
#         p = Page(frame.data)
#         p.write(slot, value)
#         self.bufferpool.unpin_page(page_id, is_dirty=True)

#     def _read_value(self, page_id, slot):
#         frame = self.bufferpool.fetch_page(page_id)
#         p = Page(frame.data)
#         val = p.read(slot)
#         self.bufferpool.unpin_page(page_id, is_dirty=False)
#         return val

#     def get_new_rid(self):
#         rid = self.next_rid
#         self.next_rid += 1
#         return rid

#     def _merge_daemon(self):
#         while True:
#             time.sleep(5)
#             self.__merge()

#     def __merge(self):
#         print(f"Merging table {self.name} ...")
#         # stub
#         pass


from lstore.index import Index
import threading

class Record:
    """A class to hold a record for select queries."""
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns  # ✅ Ensures that .columns attribute exists


class Table:
    def __init__(self, name, num_columns, key_index):
        self.name = name
        self.num_columns = num_columns
        self.key_index = key_index
        self.page_directory = {}  # ✅ Maps RID to page locations
        self.rid_to_versions = {}  # ✅ Stores all versions of a record
        self.index = Index(self)  # ✅ Ensure Index is initialized
        self.next_rid = 0  

    def merge_base_tail(self):
        """Merges tail pages into base pages asynchronously."""
        print("Starting merge process...")

        for rid, versions in self.rid_to_versions.items():
            if len(versions) > 1:
                self.rid_to_versions[rid][0] = versions[-1]
                versions.clear()
                versions.append(self.rid_to_versions[rid][0])

        print("Merge completed.")

    def start_background_merge(self):
        """Runs merge in a background thread."""
        merge_thread = threading.Thread(target=self.merge_base_tail)
        merge_thread.start()

    def get_new_rid(self):
        """Generates a new unique RID (Record ID)."""
        rid = self.next_rid
        self.next_rid += 1
        return rid

    def insert_record(self, record):
        """Inserts a new record into the table."""
        rid = self.get_new_rid()
        self.rid_to_versions[rid] = [record]  # Initialize with first version
        self.index.pk_index[record[self.key_index]] = rid  # Index the primary key
        return rid

    def get_latest_version(self, rid):
        """Returns the latest version of a record."""
        if rid in self.rid_to_versions:
            return self.rid_to_versions[rid][-1]  # Get most recent version
        return None