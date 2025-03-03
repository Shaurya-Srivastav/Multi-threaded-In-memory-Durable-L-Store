# lock_manager.py

import threading
from collections import defaultdict

class LockMode:
    SHARED = "SHARED"
    EXCLUSIVE = "EXCLUSIVE"

class LockManager:
    """
    Simple lock manager implementing strict 2PL + no-wait.
    - rid_locks = { rid -> { "lock_mode": LockMode, "holders": set(txn_ids) } }
    """

    def __init__(self):
        self.rid_locks = {}
        self._lock = threading.Lock()

    def acquire_lock(self, transaction_id, rid, lock_mode):
        """
        Attempt to acquire a lock for `transaction_id` on `rid` with `lock_mode`.
        Return True if granted, False if not (no-wait).
        """
        with self._lock:
            lock_info = self.rid_locks.get(rid)

            # If no lock info, grant lock
            if lock_info is None:
                self.rid_locks[rid] = {
                    "lock_mode": lock_mode,
                    "holders": {transaction_id}
                }
                return True

            current_mode = lock_info["lock_mode"]
            holders = lock_info["holders"]

            # If same txn is already a holder
            if transaction_id in holders:
                # If already EXCLUSIVE, done
                if current_mode == LockMode.EXCLUSIVE:
                    return True
                # If current is SHARED but lock_mode = SHARED, also done
                if lock_mode == LockMode.SHARED:
                    return True
                # If want EXCLUSIVE but we share with only ourselves -> upgrade
                if lock_mode == LockMode.EXCLUSIVE and len(holders) == 1:
                    lock_info["lock_mode"] = LockMode.EXCLUSIVE
                    return True
                # Else cannot upgrade -> fail
                return False
            else:
                # Another transaction holds the lock
                if current_mode == LockMode.EXCLUSIVE:
                    # cannot share or re-get exclusive
                    return False
                else:
                    # current_mode = SHARED
                    if lock_mode == LockMode.SHARED:
                        # multiple shared is okay
                        holders.add(transaction_id)
                        return True
                    elif lock_mode == LockMode.EXCLUSIVE:
                        # can't upgrade if multiple holders
                        if len(holders) == 1:
                            # If there's exactly 1 holder (a different txn), no-wait => fail
                            return False
                        else:
                            # definitely fail
                            return False

    def release_lock(self, transaction_id, rid):
        """
        Release the lock on `rid` held by `transaction_id`.
        """
        with self._lock:
            if rid not in self.rid_locks:
                return
            lock_info = self.rid_locks[rid]
            if transaction_id in lock_info["holders"]:
                lock_info["holders"].remove(transaction_id)
                if not lock_info["holders"]:
                    del self.rid_locks[rid]

    def release_all(self, transaction_id):
        """
        Release all locks held by transaction_id.
        """
        with self._lock:
            rids_to_release = []
            for rid, lock_info in self.rid_locks.items():
                if transaction_id in lock_info["holders"]:
                    rids_to_release.append(rid)

            for rid in rids_to_release:
                lock_info = self.rid_locks.get(rid)
                if lock_info:
                    lock_info["holders"].discard(transaction_id)
                    if not lock_info["holders"]:
                        del self.rid_locks[rid]
