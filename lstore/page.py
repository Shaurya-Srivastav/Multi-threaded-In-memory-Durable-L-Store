# lstore/page.py

class Page:
    """
    A minimal page. We won't do complex 4096-byte management in milestone1,
    but keep a structure that tracks how many records are in the page.
    """
    PAGE_CAPACITY = 512  # you can pick any number for milestone1

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)  # unused
        self.pin_count = 0
        self.is_dirty = False

    def has_capacity(self):
        return self.num_records < Page.PAGE_CAPACITY

    def write(self, value):
        """
        In a real system, we'd write 'value' into self.data at an offset.
        For milestone1, we just increment num_records.
        """
        if self.has_capacity():
            self.num_records += 1
            return True
        return False
