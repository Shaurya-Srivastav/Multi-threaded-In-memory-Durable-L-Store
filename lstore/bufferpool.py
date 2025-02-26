import collections

class Bufferpool:
    def __init__(self, size):
        self.size = size  
        self.pages = {}  
        self.lru_list = collections.deque()  
        self.dirty_pages = set()  

    def get_page(self, page_id):
        if page_id in self.pages:
            self.lru_list.remove(page_id)
            self.lru_list.appendleft(page_id)
            return self.pages[page_id]
        
        if len(self.pages) >= self.size:
            self.evict_page()

        page = self.load_from_disk(page_id)
        self.pages[page_id] = page
        self.lru_list.appendleft(page_id)
        return page

    def mark_dirty(self, page_id):
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
        return f"Page-{page_id}"

    def write_to_disk(self, page_id):
        print(f"Writing Page-{page_id} to disk.")
