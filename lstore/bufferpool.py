import collections
import threading

class Bufferpool:
    """
    Simplified bufferpool. 
    Uses an LRU eviction policy by default and 
    a dictionary {page_id -> page} in memory.
    """

    def __init__(self, size):
        self.size = size
        self.pages = {}
        self.lru_list = collections.deque()
        self.dirty_pages = set()
        self._lock = threading.Lock()

    def get_page(self, page_id):
        with self._lock:
            if page_id in self.pages:
                # Move to front of LRU
                self._touch(page_id)
                return self.pages[page_id]

            # Evict if needed
            if len(self.pages) >= self.size:
                self.evict_page()

            # "Load" page from disk (stub)
            page = self.load_from_disk(page_id)
            self.pages[page_id] = page
            self.lru_list.appendleft(page_id)
            return page

    def mark_dirty(self, page_id):
        with self._lock:
            self.dirty_pages.add(page_id)

    def evict_page(self):
        if not self.lru_list:
            return
        page_id = self.lru_list.pop()
        if page_id in self.dirty_pages:
            self.write_to_disk(page_id)
            self.dirty_pages.remove(page_id)
        del self.pages[page_id]

    def load_from_disk(self, page_id):
        # Stub
        return f"Page-{page_id}"

    def write_to_disk(self, page_id):
        print(f"Writing Page-{page_id} to disk.")

    def _touch(self, page_id):
        # Move page_id to front of LRU
        self.lru_list.remove(page_id)
        self.lru_list.appendleft(page_id)
