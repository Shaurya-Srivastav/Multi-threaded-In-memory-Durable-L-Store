Below is an **updated overview** that extends your existing “Single-threaded, In-memory L-Store” design into a **multi-threaded** version with **transactional** semantics, **strict two-phase locking (2PL)**, and basic **no-wait** concurrency control. This incorporates the core ideas from Milestone 1 (in-memory, versioned records, base/tail organization, etc.) with the new requirements of Milestone 3 (transaction atomicity/isolation and concurrency).

---

## Multi-Threaded, In-Memory & Durable L-Store

### 1. Contributions
- **Shaurya Srivastav** – Implementation of core components.  
- **Vedant Patel** – Optimized core components and improved test code workability.  
- **Ayush Tripathi** – Coordination, serialization methods, assisted with bufferpool.  
- **Stella Huang** – Testing + Code Integrity, assisted in implementation of indices.  

### 2. Design Overview

Our project extends the previous single-threaded, in-memory L-Store from Milestone 1 into a **multi-threaded** database with support for **transactions**, **strict two-phase locking**, and **no-wait** lock acquisition. The main goals are:

1. **Transaction Semantics (Atomicity & Durability)**:
   - We group multiple queries (insert, select, update, delete, sum) into a single transaction.  
   - If **any** query in the transaction fails to acquire a lock or encounters an error, we **abort** the entire transaction and roll back any partial changes (atomicity).  
   - Committed data persists in memory (and can be written to disk in subsequent milestones for durability).  

2. **Concurrent Execution (Isolation)**:
   - We allow **multiple transactions** to run in parallel using Python threads.  
   - We ensure **serializable isolation** via **strict two-phase locking (2PL)**: transactions acquire locks (shared or exclusive) before reading or writing, then release them only at commit/abort.  
   - We adopt a **no-wait** policy to avoid deadlock: if a lock cannot be granted immediately, the transaction aborts right away (then may be retried).  

3. **In-Memory Organization**:
   - We continue to store records in base/tail structures, with each record’s versions kept in an in‑memory list.  
   - For **each** record ID (RID), we maintain `rid_to_versions[rid]`, an array of all versions from oldest to newest.  
   - Inserts create a new RID and an initial version of the record’s data.  
   - Updates append a new version, effectively simulating L-Store’s tail record.  
   - Deletes remove the record from the primary key index and from `rid_to_versions`.  

4. **Indexing**:
   - A **primary index** maps each record’s primary key → RID in a hash map for O(1) lookups.  
   - **Secondary indexes** (optional) map column values → list of RIDs to speed up queries on non-primary-key columns.  

5. **Bufferpool & Page Abstraction** (Carried Forward from Milestone 1):
   - We still use an in-memory “bufferpool” to cache page-like objects.  
   - For Milestone 3, we remain primarily in memory but add concurrency guards (locks) to the bufferpool so threads safely read/evict pages.  

6. **Merging**:
   - We maintain a (potentially background) process to **merge** older versions with the base version if desired, collapsing multiple tail records into the base.  
   - This step can run concurrently but must also coordinate locks to avoid interfering with in-flight transactions.  

---

### 3. Transaction & Concurrency Mechanics

1. **Strict Two-Phase Locking (2PL)**:  
   - Each record (RID) can be locked in either **shared** or **exclusive** mode.  
   - A transaction that needs to **read** a record requests a **shared** lock; a transaction that needs to **update** or **delete** a record requests an **exclusive** lock.  
   - Locks are held until transaction commit or abort (strict 2PL).  

2. **No-Wait Policy**:  
   - If a transaction tries to acquire a shared or exclusive lock that is incompatible with existing locks, we **immediately** reject the request (the transaction aborts).  
   - This eliminates deadlock by ensuring no transaction waits in a queue.  

3. **Rollback on Abort**:  
   - If any query within a transaction fails to acquire a lock or otherwise fails, the transaction rolls back.  
   - Rollback logic restores overwritten values in `rid_to_versions` or re-inserts a deleted record, depending on your design.  
   - All locks held by the aborted transaction are released.  

4. **Transaction Retry**:  
   - A **TransactionWorker** (thread) may keep retrying an aborted transaction until it commits, as suggested by the milestone specs.  

5. **Multithreading in Python**:  
   - We use Python’s `threading.Thread` to create worker threads, each of which runs one or more transactions.  
   - Due to Python’s GIL, only one thread runs Python bytecode at a time, but concurrency can still help when certain I/O or blocking operations occur.  
   - We still must guard shared data structures (the bufferpool, the record dictionary, indexes) with locks to avoid race conditions.  

---

### 4. Detailed Components

1. **Lock Manager**  
   - A dedicated lock manager tracks which RIDs are locked, by which transactions, and in which lock mode (shared or exclusive).  
   - On a lock request: 
     - If the RID is free, grant the lock.  
     - If the RID is locked in a compatible mode (e.g., multiple shared locks), possibly add the new holder if it’s a shared request.  
     - If incompatible, return **False** → transaction abort.  

2. **Transaction**  
   - A transaction object holds a **list of queries** (each is a function reference, such as `query.update`, plus arguments).  
   - `transaction.run()` executes each query in sequence. If any query returns `False`, it calls `abort()`. Otherwise, if all succeed, it calls `commit()`.  
   - On abort, the transaction reverts changes and releases locks.  
   - On commit, we simply release all locks.  

3. **Table & Record Layout**  
   - `Table` manages the in-memory dictionaries (`rid_to_versions`, primary key index, etc.).  
   - Each entry in `rid_to_versions[rid]` is a list of columns `[col0, col1, col2, ...]`.  
   - For a column that is updated, we append a new version with the changed values.  

4. **Queries**  
   - The `Query` class provides an interface: `insert`, `select`, `update`, `delete`, `sum`, etc. Each query tries to acquire the needed locks before proceeding. If it cannot, it returns `False`.  

5. **Bufferpool**  
   - Although data is in memory, the bufferpool structure can be extended for disk-based pages in later milestones.  
   - For concurrency, it uses internal locks to protect LRU or dirty lists.  

---

### 5. Example Operation

- **Insert**  
  - Generate a new RID.  
  - Acquire an **exclusive** lock for that RID (though no conflicts likely exist yet).  
  - Store `[col0, col1, ...]` in `rid_to_versions[new_rid]`.  
  - Update the primary key index if needed.  

- **Update(pk=10, new_values)**  
  - Find the record’s RID from `pk_index[10]`.  
  - Attempt an **exclusive** lock on that RID. If it fails, return `False` → transaction abort.  
  - Copy the most recent version, modify columns, append as a new version.  
  - Update secondary indexes if needed.  

- **Select(pk=10)**  
  - Acquire a **shared** lock on the RID from `pk_index[10]`.  
  - Read the newest version.  
  - Return the columns.  

- **Delete(pk=10)**  
  - Acquire **exclusive** lock for that RID.  
  - Remove from `pk_index`.  
  - Remove from `rid_to_versions`.  

- **Sum(start_pk, end_pk, col)**  
  - For each `pk` in `[start_pk ... end_pk]`, get its RID, acquire a **shared** lock, read the column, and accumulate.  
  - If any lock request fails, transaction aborts.  

---

### 6. Summary
This enhanced design incorporates **transaction management** and **concurrency control** into the original single-threaded L-Store. We rely on:

- **Strict 2PL**: ensuring isolation at the record level.  
- **No-wait**: avoiding deadlocks by aborting early if lock conflicts exist.  
- **Rollback**: reverting partial changes to maintain atomicity.  
- **Threading**: enabling parallel transaction execution under Python’s GIL with careful synchronization.  

As a result, we now have a basic yet robust transactional system that can be easily extended with more sophisticated features (e.g., advanced concurrency protocols, logging and recovery, etc.) for a truly durable L-Store.

---

**References**:  
- [L-Store Paper](https://www.researchgate.net/publication/324150481_L-Store_A_Real-time_OLTP_and_OLAP_System)  
- Additional concurrency and transaction control resources from standard database systems textbooks and CPython threading documentation.