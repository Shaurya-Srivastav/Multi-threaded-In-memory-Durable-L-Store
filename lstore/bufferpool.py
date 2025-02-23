# lstore/bufferpool.py
import os
from collections import OrderedDict
from lstore.page import Page
from lstore.config import BUFFERPOOL_SIZE

class BufferPool:
    def __init__(self, data_dir, capacity=BUFFERPOOL_SIZE):
        self.data_dir = data_dir
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        self.capacity = capacity
        # LRU: page_id -> page_obj
        self.pool = OrderedDict()

    def get_page(self, page_id):
        if page_id in self.pool:
            # move to most recently used
            page = self.pool.pop(page_id)
            self.pool[page_id] = page
            return page
        else:
            # load from disk
            page = self._load_page(page_id)
            if len(self.pool) >= self.capacity:
                self._evict_page()
            self.pool[page_id] = page
            return page

    def _load_page(self, page_id):
        filepath = os.path.join(self.data_dir, f"page_{page_id}.bin")
        if not os.path.exists(filepath):
            # create new
            return Page(page_id, 0)
        with open(filepath, "rb") as f:
            data = f.read()
        page = Page.from_bytes(data)
        if page is None:
            page = Page(page_id, 0)
        return page

    def _evict_page(self):
        # evict LRU
        evict_id, evict_page = self.pool.popitem(last=False)
        if evict_page.dirty:
            self._flush_page(evict_page)

    def _flush_page(self, page):
        data = page.to_bytes()
        filepath = os.path.join(self.data_dir, f"page_{page.page_id}.bin")
        with open(filepath, "wb") as f:
            f.write(data)
        page.dirty = False

    def flush_all(self):
        for pid, pg in self.pool.items():
            if pg.dirty:
                self._flush_page(pg)

    def close(self):
        self.flush_all()
        self.pool.clear()
