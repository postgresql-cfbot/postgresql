Global Temporary Table(GTT)
=========================================

Feature description
-----------------------------------------

Previously, temporary tables are defined once and automatically
exist (starting with empty contents) in every session before using them.

The temporary table implementation in PostgreSQL, known as Local temp tables(LTT),
did not fully comply with the SQL standard. This version added the support of
Global Temporary Table .

The metadata of Global Temporary Table is persistent and shared among sessions.
The data stored in the Global temporary table is independent of sessions. This
means, when a session creates a Global Temporary Table and writes some data.
Other sessions cannot see those data, but they have an empty Global Temporary
Table with same schema.

Like local temporary table, Global Temporary Table supports ON COMMIT PRESERVE ROWS
or ON COMMIT DELETE ROWS clause, so that data in the temporary table can be
cleaned up or preserved automatically when a session exits or a transaction COMMITs.

Unlike Local Temporary Table, Global Temporary Table does not support
ON COMMIT DROP clauses.

In following paragraphs, we use GTT for Global Temporary Table and LTT for
local temporary table.

Main design ideas
-----------------------------------------
In general, GTT and LTT use the same storage and buffer design and
implementation. The storage files for both types of temporary tables are named
as t_backendid_relfilenode, and the local buffer is used to cache the data.

The schema of GTTs is shared among sessions while their data are not. We build
a new mechanisms to manage those non-shared data and their statistics.
Here is the summary of changes:

1) CATALOG
GTTs store session-specific data. The storage information of GTTs'data, their
transaction information, and their statistics are not stored in the catalog.

2) STORAGE INFO & STATISTICS INFO & TRANSACTION INFO
In order to maintain durability and availability of GTTs'session-specific data,
their storage information, statistics, and transaction information is managed
in a local hash table tt_storage_local_hash.

3) DDL
Currently, GTT supports almost all table'DDL except CLUSTER/VACUUM FULL.
Part of the DDL behavior is limited by shared definitions and multiple copies of
local data, and we added some structures to handle this.

A shared hash table active_gtt_shared_hash is added to track the state of the
GTT in a different session. This information is recorded in the hash table
during the DDL execution of the GTT.

The data stored in a GTT can only be modified or accessed by owning session.
The statements that only modify data in a GTT do not need a high level of
table locking. The operations making those changes include truncate GTT,
reindex GTT, and lock GTT.

4) MVCC commit log(clog) cleanup
Each GTT in a session has its own piece of data, and they have their own
transaction information. We set up data structures to track and maintain
this information. The cleaning of CLOGs also needs to consider the transaction
information of GTT.

Detailed design
-----------------------------------------

1. CATALOG
1.1 relpersistence
define RELPERSISTENCEGLOBALTEMP 'g'
Mark Global Temporary Table in pg_class relpersistence to 'g'. The relpersistence
of indexes created on the GTT, sequences on GTT and toast tables on GTT are
also set to 'g'

1.2 on commit clause
LTT's status associated with on commit DELETE ROWS and on commit PRESERVE ROWS
is not stored in catalog. Instead, GTTs need a bool value on_commit_delete_rows
in reloptions which is shared among sessions.

1.3 gram.y
GTT is already supported in syntax tree. We remove the warning message
"GLOBAL is deprecated in temporary table creation" and mark
relpersistence = RELPERSISTENCEGLOBALTEMP.

2. STORAGE INFO & STATISTICS INFO & TRANSACTION INFO
2.1. gtt_storage_local_hash
Each backend creates a local hashtable gtt_storage_local_hash to track a GTT's
storage file information, statistics, and transaction information.

2.2 GTT storage file info track
1) When one session inserts data into a GTT for the first time, record the
storage info to gtt_storage_local_hash.
2) Use beforeshmemexit to ensure that all files of session GTT are deleted when
the session exits.

2.3 statistics info
1) relpages reltuples relallvisible relfilenode
2) The statistics of each column from pg_statistic
All the above information is stored in gtt_storage_local_hash.
When doing vacuum or analyze, GTT's statistic is updated, which is used by
the SQL planner.
The statistics summarizes only data in the current session.

2.3 transaction info track
frozenxid minmulti from pg_class is stored to gtt_storage_local_hash.

3 DDL
3.1. active_gtt_shared_hash
This is the hash table created in shared memory to trace the GTT files initialized
in each session. Each hash entry contains a bitmap that records the backendid of
the initialized GTT file. With this hash table, we know which backend/session
is using this GTT. Such information is used during GTT's DDL operations.

3.2 DROP GTT
One GTT is allowed to be deleted when there is only one session using the table
and the session is the current session. After holding the lock on GTT,
active_gtt_shared_hash is checked to ensure that.

3.3 ALTER GTT/DROP INDEX ON GTT
Same as drop GTT.

3.4 CREATE INDEX ON GTT
1) create index on GTT statements build index based on local data in a session.
2) After the index is created, record the index metadata to the catalog.
3) Other sessions can enable or disable the local GTT index.

3.5 TRUNCATE/REINDEX GTT
The SQL truncate/reindex command open the GTT using AccessShareLock lock,
not AccessExclusiveLock, because this command only cleans up local data and
local buffers in current session. This allows these operations to be executed
concurrently between sessions, unlike normal tables.

3.6 LOCK GTT
A lock GTT statement does not hold any relation lock.

3.7 CLUSTER GTT/VACUUM FULL GTT
The current version does not support.

4 MVCC commit log(clog) cleanup

The GTT storage file contains transaction information. Queries for GTT data rely
on transaction information such as clog. The transaction information required by
each session may be completely different. We need to ensure that the transaction
information of the GTT data is not cleaned up during its lifetime and that
transaction resources are recycled at the instance level.

4.1 The session level GTT oldest frozenxid
1) To manage all GTT transaction information, add session level oldest frozenxid
in each session. When one GTT is created or removed, record the session level
oldest frozenxid and store it in MyProc.
2) When vacuum advances the database's frozenxid, session level oldest frozenxid
should be considered. This is acquired by searching all of MyProc. This way,
we can avoid the clog required by GTTs to be cleaned.

4.2 vacuum GTT
Users can perform vacuum over a GTT to clean up local data in the GTT.

4.3 autovacuum GTT
Autovacuum skips all GTTs, because the data in GTTs is only visible in current session.

5 OTHERS
5.1 Parallel query
Planner does not produce parallel query plans for SQL related to GTT. Because
GTT private data cannot be accessed across processes.

5.2 WAL and Logical replication
Like LTT, the DML on GTT does not record WAL and is not parsed or replay by
the logical replication.