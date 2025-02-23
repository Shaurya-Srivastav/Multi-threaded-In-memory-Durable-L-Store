# lstore/table.py

import threading, time, os
from lstore.page import Page
from lstore.bufferpool import BufferPool
from lstore.index import Index
from lstore.config import MERGE_INTERVAL, BUFFERPOOL_SIZE

class Table:
    def __init__(self, name, num_columns, key):
        """
        We'll store user columns at indices 1..num_columns,
        index 0 is 'indirection' => chain pointer.
        self.key => key+1
        """
        self.name = name
        self.num_columns = num_columns
        self.key = key + 1
        self.page_directory = {}    # rid -> (page_id, slot)
        self.next_rid = 0
        self.next_page_id = 0

        data_path = os.path.join("./data", name)
        self.bufferpool = BufferPool(data_path, BUFFERPOOL_SIZE)

        self.index = Index(self)
        self.table_lock = threading.Lock()

        # For merges
        self.merge_thread_on = True
        self.merge_thread = threading.Thread(target=self._merge_loop, daemon=True)
        self.merge_thread.start()

    def _merge_loop(self):
        # For milestone2, you may do a small or no-op approach.
        while self.merge_thread_on:
            time.sleep(MERGE_INTERVAL)
            # If you want a real merge, implement it here.
            # We'll skip in this example or do a no-op
            pass

    def shutdown(self):
        self.merge_thread_on = False
        self.merge_thread.join()
        self.bufferpool.close()

    def _get_new_rid(self):
        rid = self.next_rid
        self.next_rid += 1
        return rid

    def _get_new_page_id(self, page_type=0):
        pid = self.next_page_id
        self.next_page_id += 1
        return pid

    def insert_record(self, user_values):
        """
        user_values => list of length = num_columns
        We'll store [ -1, user_val[0], user_val[1], ..., user_val[n-1] ]
        """
        record = [-1] + list(user_values)
        # We can store them in a new base page each time or reuse a last base page with capacity.
        base_pid = self._get_new_page_id(0)
        base_page = Page(base_pid, 0)
        self.bufferpool.pool[base_pid] = base_page

        slot = base_page.num_records
        base_page.write_record(record)

        rid = self._get_new_rid()
        self.page_directory[rid] = (base_pid, slot)
        # update pk index
        pk_val = record[self.key]
        self.index.pk_index[pk_val] = rid
        return rid

    def _get_latest_record(self, rid):
        """
        Follow the chain from the base's indirection pointer
        to get the newest version. This is used for partial update.
        We'll return the entire newest record as a list.
        Also return the rid of that newest record.
        """
        # if it's base => we do base page read
        (pid, slot) = self.page_directory[rid]
        page = self.bufferpool.get_page(pid)
        rec = page.get_record(slot)
        # Follow chain
        curr_rid = rid
        while True:
            ind = rec[0]
            if ind == -1:
                # newest
                return (curr_rid, rec)
            # else follow
            curr_rid = ind
            (tp, ts) = self.page_directory[curr_rid]
            tpobj = self.bufferpool.get_page(tp)
            rec = tpobj.get_record(ts)

    def update_record(self, primary_key, user_updates):
        """
        user_updates is length = num_columns, with None => no change
        We'll create a tail record that has the newest data for each column.
        The tail record's column0 => old_rid. Then set base's indirection => new tail's rid.
        """
        rid = self.index.pk_index.get(primary_key)
        if rid is None:
            return False

        # Get newest version
        newest_rid, newest_rec = self._get_latest_record(rid)

        # Build new tail record
        new_tail = list(newest_rec)
        # partial updates
        for i, val in enumerate(user_updates):
            if val is not None:
                new_tail[i+1] = val

        # tail's indirection => newest_rid
        new_tail[0] = newest_rid

        tail_pid = self._get_new_page_id(page_type=1)
        tail_page = Page(tail_pid, 1)
        self.bufferpool.pool[tail_pid] = tail_page

        slot = tail_page.num_records
        tail_page.write_record(new_tail)
        new_tail_rid = self._get_new_rid()
        self.page_directory[new_tail_rid] = (tail_pid, slot)

        # Now set the base record's indirection => new_tail_rid
        (base_pid, base_slot) = self.page_directory[rid]
        base_page = self.bufferpool.get_page(base_pid)
        base_rec = base_page.get_record(base_slot)
        base_rec[0] = new_tail_rid
        base_page.set_record(base_slot, base_rec)
        return True

    def delete_record(self, primary_key):
        rid = self.index.pk_index.get(primary_key, None)
        if rid is None:
            return False
        (pid, slot) = self.page_directory[rid]
        page = self.bufferpool.get_page(pid)
        rec = page.get_record(slot)
        # Mark the pk col as -1 => deleted
        rec[self.key] = -1
        page.set_record(slot, rec)
        del self.index.pk_index[primary_key]
        return True

    def _get_versioned_record(self, rid, version):
        """
        If version < 0 => original base record
        If version >=0 => newest minus 'version' steps in chain
           0 => newest, 1 => second newest, ...
        """
        (base_pid, base_slot) = self.page_directory[rid]
        base_page = self.bufferpool.get_page(base_pid)
        base_rec = base_page.get_record(base_slot)
        if version < 0:
            return base_rec
        # else follow chain #version times
        # base_rec[0] => newest tail rid
        curr_rid = base_rec[0]
        steps = version
        if curr_rid == -1:
            # no tail => base is newest
            return base_rec
        while steps > 0:
            # get that tail
            (tp, ts) = self.page_directory[curr_rid]
            tpage = self.bufferpool.get_page(tp)
            rec = tpage.get_record(ts)
            nxt = rec[0]
            if nxt == -1:
                # we are at newest
                if steps == 0:
                    return rec
                # if steps > 0 but no older => return rec
                return rec
            # else keep going
            curr_rid = nxt
            steps -= 1
        # done steps => rec is newest
        # but we ended up with the chain node we wanted
        (tp, ts) = self.page_directory[curr_rid]
        tpage = self.bufferpool.get_page(tp)
        rec = tpage.get_record(ts)
        return rec

    def sum_column(self, start_key, end_key, col_index):
        s = 0
        for pk, rid in self.index.pk_index.items():
            if pk >= start_key and pk <= end_key:
                (pid, slot) = self.page_directory[rid]
                # newest version => version=0
                rec = self._get_versioned_record(rid, 0)
                s += rec[col_index]
        return s

    def sum_column_version(self, start_key, end_key, col_index, version):
        s = 0
        for pk, rid in self.index.pk_index.items():
            if pk >= start_key and pk <= end_key:
                rec = self._get_versioned_record(rid, version)
                s += rec[col_index]
        return s
