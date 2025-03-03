# lstore/lock_manager.py

import threading
from collections import defaultdict

class LockMode:
    SHARED = "SHARED"
    EXCLUSIVE = "EXCLUSIVE"

class LockManager:
    """
    A simple lock manager that implements strict two-phase locking with no-wait.
    - For SHARED locks: multiple transactions may hold a shared lock on the same RID.
    - For EXCLUSIVE locks: only one transaction can hold the exclusive lock, and no other shared or exclusive locks are allowed.
    - No-wait: if a lock cannot be granted immediately, we return False (the caller should abort).
    """

    def __init__(self):
        # rid_locks is a dict: { rid -> lock_info }
        # lock_info: {
        #   "lock_mode": LockMode.SHARED or LockMode.EXCLUSIVE,
        #   "holders": set of transaction_ids that hold the lock,
        # }
        self.rid_locks = {}
        self.lock = threading.Lock()  # Protects access to rid_locks

    def acquire_lock(self, transaction_id, rid, lock_mode):
        """
        Attempt to acquire the given lock_mode on `rid` for `transaction_id`.
        Returns True if successful, False if lock cannot be granted (no-wait).
        """
        with self.lock:
            lock_info = self.rid_locks.get(rid)
            if lock_info is None:
                # No one has locked this rid yet
                self.rid_locks[rid] = {
                    "lock_mode": lock_mode,
                    "holders": {transaction_id},
                }
                return True

            # Some lock already exists on this rid
            current_mode = lock_info["lock_mode"]
            holders = lock_info["holders"]

            if transaction_id in holders:
                # The same transaction already holds a lock on this rid
                # If we already hold an exclusive lock, we are fine
                # If we hold a shared lock but want exclusive, then we must check if we are the *only* holder
                if current_mode == LockMode.EXCLUSIVE:
                    # Already exclusive => always fine
                    return True

                if lock_mode == LockMode.SHARED:
                    # Already holding a shared => no upgrade needed
                    return True

                if lock_mode == LockMode.EXCLUSIVE:
                    # We want to upgrade from SHARED to EXCLUSIVE 
                    if len(holders) == 1:
                        # We are the only holder => upgrade
                        lock_info["lock_mode"] = LockMode.EXCLUSIVE
                        return True
                    else:
                        # Other transactions share the lock => no-wait => fail
                        return False
            else:
                # Another transaction holds the lock
                if current_mode == LockMode.EXCLUSIVE:
                    # We cannot share or get exclusive if someone else is holding exclusive => fail
                    return False

                # current_mode == LockMode.SHARED
                if lock_mode == LockMode.SHARED:
                    # We can share the lock 
                    lock_info["holders"].add(transaction_id)
                    return True
                elif lock_mode == LockMode.EXCLUSIVE:
                    # Another transaction(s) hold(s) the lock in SHARED => cannot get exclusive => fail
                    return False

        # If we somehow get here, return False
        return False

    def release_lock(self, transaction_id, rid):
        """
        Release the lock on `rid` held by `transaction_id`.
        """
        with self.lock:
            if rid not in self.rid_locks:
                return

            lock_info = self.rid_locks[rid]
            if transaction_id in lock_info["holders"]:
                lock_info["holders"].remove(transaction_id)
                # If no more holders, remove the lock record entirely
                if not lock_info["holders"]:
                    del self.rid_locks[rid]
                else:
                    # If holders remain but the lock was EXCLUSIVE, it must now remain exclusive
                    # or if it was shared, remain shared. We do not auto-downgrade from exclusive to shared in 2PL.
                    pass

    def release_all(self, transaction_id):
        """
        Release all locks held by `transaction_id` across all RIDs.
        """
        with self.lock:
            rids_to_release = []
            for rid, lock_info in self.rid_locks.items():
                if transaction_id in lock_info["holders"]:
                    rids_to_release.append(rid)

            for rid in rids_to_release:
                lock_info = self.rid_locks.get(rid)
                if lock_info:
                    lock_info["holders"].remove(transaction_id)
                    if not lock_info["holders"]:
                        del self.rid_locks[rid]
