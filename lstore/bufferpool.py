# lstore/bufferpool.py

import os
import collections
from lstore.config import PAGE_SIZE, FRAME_CAPACITY, REPLACEMENT_POLICY

class Frame:
    """
    A frame is an in-memory copy of one page from disk.
    """
    def __init__(self, page_id):
        self.page_id = page_id
        self.data = bytearray(PAGE_SIZE)
        self.dirty = False
        self.pin_count = 0

class BufferPool:
    """
    A simple buffer pool that can hold up to FRAME_CAPACITY pages in memory.
    Uses LRU (by default) or a chosen replacement policy.
    """
    def __init__(self, db_path):
        self.db_path = db_path
        self.frames = {}  # page_id -> Frame
        self.lru_order = collections.OrderedDict()  # track usage for LRU
        self.capacity = FRAME_CAPACITY

    def _page_filename(self, page_id):
        # Example: page_id might be "tableName_colX_base_00002"
        return os.path.join(self.db_path, f"{page_id}.pg")

    def fetch_page(self, page_id):
        """
        Return a Frame that contains the page. If not in memory, read from disk.
        If no free frames, evict based on policy.
        """
        # If page is already in frames, just pin and return
        if page_id in self.frames:
            frame = self.frames[page_id]
            frame.pin_count += 1
            # update LRU ordering
            if page_id in self.lru_order:
                self.lru_order.move_to_end(page_id, last=True)
            return frame

        # Need to evict if full
        if len(self.frames) >= self.capacity:
            self.evict()

        # Create a new frame
        new_frame = Frame(page_id)
        # read from disk
        filename = self._page_filename(page_id)
        if os.path.exists(filename):
            with open(filename, "rb") as f:
                new_frame.data = bytearray(f.read(PAGE_SIZE))

        new_frame.pin_count = 1
        self.frames[page_id] = new_frame
        self.lru_order[page_id] = True  # put it at the end
        return new_frame

    def evict(self):
        """
        Evict one page from frames, using LRU policy by default.
        Write it to disk if dirty, remove from self.frames.
        """
        # find a page_id in lru_order with pin_count=0
        # the oldest is at the front
        for pid in self.lru_order:
            frame = self.frames[pid]
            if frame.pin_count == 0:
                # evict this one
                self._flush_frame(frame)
                # remove from structures
                del self.frames[pid]
                self.lru_order.pop(pid)
                return
        # if we can't find any pinned=0, you have no choice but to fail or do something
        raise Exception("No evictable frame found (all pinned). Increase FRAME_CAPACITY?")

    def _flush_frame(self, frame):
        """
        Write frame to disk if dirty.
        """
        if frame.dirty:
            filename = self._page_filename(frame.page_id)
            # ensure directory
            os.makedirs(self.db_path, exist_ok=True)
            with open(filename, "wb") as f:
                f.write(frame.data)
            frame.dirty = False

    def unpin_page(self, page_id, is_dirty=False):
        """
        Decrement the pin_count. If is_dirty is True, mark the frame dirty.
        """
        frame = self.frames.get(page_id, None)
        if frame is None:
            return
        if is_dirty:
            frame.dirty = True
        if frame.pin_count > 0:
            frame.pin_count -= 1
        # update LRU ordering
        if page_id in self.lru_order:
            self.lru_order.move_to_end(page_id, last=True)

    def flush_page(self, page_id):
        """
        Force write the given page to disk (if dirty).
        """
        frame = self.frames.get(page_id, None)
        if frame:
            self._flush_frame(frame)

    def flush_all(self):
        """
        Flush all frames in memory to disk
        """
        for pid, frame in self.frames.items():
            self._flush_frame(frame)

    def get_data(self, page_id):
        """
        Return the data of the page in a pinned Frame (caller must unpin later).
        """
        frame = self.fetch_page(page_id)
        return frame.data

    def put_data(self, page_id, offset, data_bytes):
        """
        Write 'data_bytes' into the page's buffer at 'offset'. Mark dirty. 
        Caller must unpin after finishing.
        """
        frame = self.fetch_page(page_id)
        frame.data[offset:offset+len(data_bytes)] = data_bytes
        frame.dirty = True
        # pin_count is incremented in fetch_page

    def free_page(self, page_id):
        """
        (Optional) If you want to logically delete a page from disk and pool
        """
        if page_id in self.frames:
            self.evict_specific(page_id)
        filename = self._page_filename(page_id)
        if os.path.exists(filename):
            os.remove(filename)

    def evict_specific(self, page_id):
        """
        Force evict a specific page. Use carefully.
        """
        if page_id in self.frames:
            frame = self.frames[page_id]
            if frame.pin_count != 0:
                raise Exception("Cannot evict a pinned page")
            self._flush_frame(frame)
            del self.frames[page_id]
            if page_id in self.lru_order:
                self.lru_order.pop(page_id)
