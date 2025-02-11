# Single-threaded-In-memory-L-Store


## Contributions:

- Shaurya Srivastav - Implementation
- Vedant Patel - Implementation
- Ayush Tripathi - Coordination and Assignment, assisted with initial knowledge base
- Stella Huang - Initial research + class implementations
- Muhammad Laiq - Assisted in initial abstraction of roles



The project is organized around three core components:

Data Model (S1)
– Goal: Store table schemas and records in a columnar fashion.
– Key ideas:

Base Records vs. Tail Records:
• A base record is the original inserted record (stored in “base pages”).
• Updates are not applied in place but are appended as tail records (stored in “tail pages”).
Indirection:
• Each base record contains an indirection pointer to the most recent tail record (if any).
• Each tail record’s indirection pointer links to the previous tail record (if one exists).
Schema Encoding:
• Each record contains a bit vector (one bit per column) that indicates whether a column’s value has been updated.
Page Ranges:
• Records are partitioned into page ranges (for example, each page range might contain 16 base pages), and tail pages are managed at the page‐range granularity.


Bufferpool Management (S2)
– Goal: Keep all data “in memory” and provide fast access.
– Key idea:

Use a page directory (a mapping from Record ID “RID” to its in‑memory page and offset) so that given an RID you can quickly locate the data.
Although the “bufferpool” here is simpler (since data is not persisted on disk in Milestone 1), you still need to manage memory pages for base and tail records.


Query Interface (S3)
– Goal: Provide a simple SQL‑like interface with the following operations:

Insert: Add a new record (all columns must be non‑NULL).
Select: Retrieve a record (or a set of columns) given the key.
Update: Update one or more columns by appending a tail record and updating the indirection pointer.
Delete: “Logically” delete a record (for example, by invalidating the RID or adding a delete flag).
Sum: Aggregate operation—sum the values in a specific column over a range of keys. – Indexing:
A primary index is required (mapping the key column to the record’s RID) for performance. Secondary indexes are optional.

The overall goal of this milestone is to create a single-threaded, in-memory database
based on L-Store, capable of performing simple SQL-like operations. 

L-store Paper: https://www.researchgate.net/publication/324150481_L-Store_A_Real-time_OLTP_and_OLAP_System


## 1. Overview of the Approach
Storing Records
We keep for each record a list of versions. Each version is a list of all column values. The newest version is always the last in this list. The earliest version is always at index 0 in this list.

Indirection and Schema Encoding
While L‑Store typically keeps “tail” records for updates, for Milestone 1 we emulate that by appending each updated version to the version list. We also store minimal “indirection” info: the record’s rid (record ID) is stable, and an in‑memory dictionary maps rid → “list of versions.”

The simplest approach is: no explicit offset into pages, but you still store metadata if you wish.
For each updated version, the “schema encoding” can be used or you can store it as a separate field—this is optional so long as you can retrieve the correct column values.
Version Indexing
The testers do calls like select_version(key, col, [1,0,1,...], relative_version), where relative_version can be 0 or negative. We interpret:

relative_version = 0 → newest version (index = length_of_version_list - 1),
relative_version = -1 → original version (index = 0),
relative_version = -2 → also the original version, etc. (the testers treat negative as “some older version” but effectively the same base).
Positive relative versions (like relative_version = 1) means the second newest version, etc.
Primary Key Index
We store primary_key → rid in a dictionary inside Index.

For each inserted record:
We create a new rid (by incrementing a counter).
We store the record’s initial version in rid_to_versions[rid] = [ columns_list ].
We add pk_index[primary_key] = rid.
All queries can quickly find the rid via the pk index.
Base + Tail “Pages”
We do not do a full disk or advanced page structure for Milestone 1. Instead, we do an in‑memory approach with minimal “Page” objects (the user can remain mostly stubbed). This is enough to pass all testers but is easy to expand in later milestones.

Deletes
We remove the record from the pk index and remove its rid entry from rid_to_versions.

Merge
For Milestone 1, we can leave __merge() as a stub.
