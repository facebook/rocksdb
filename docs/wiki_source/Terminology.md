_**NOTE for future edits:** Please maintain entries in alphabetical order_

**2PC (Two-phase commit)** The pessimistic transactions could commit in two phases: first Prepare and then the actual Commit. See https://github.com/facebook/rocksdb/wiki/Two-Phase-Commit-Implementation


**Backup**: RocksDB has a backup tool to help users backup the DB state to a different location, like HDFS. See https://github.com/facebook/rocksdb/wiki/How-to-backup-RocksDB

**Base Level** The first level that is sorted, so typically it is level 1, but when [`level_compaction_dynamic_level_bytes`](https://github.com/facebook/rocksdb/blob/b52620ab0ea049cb0e6c17b09779065337662e01/include/rocksdb/advanced_options.h#L644) is enabled, it might not be true. It could be any lower level, as long as there's no data between level 0 and base level ([details](https://github.com/facebook/rocksdb/wiki/Leveled-Compaction#level_compaction_dynamic_level_bytes-is-true)).

**Block cache**: in-memory data structure that cache the hot data blocks from the SST files. See https://github.com/facebook/rocksdb/wiki/Block-Cache


**Block**: data block of SST files. In block-based table SST files, a block is always checksummed and usually compressed for storage.


**Block-based bloom filter** or **full bloom filter**: Two different approaches of storing bloom filters in SST files. Both are features of the block-based table format. See https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter#new-bloom-filter-format


**Block-Based Table**: The default SST file format. See https://github.com/facebook/rocksdb/wiki/Rocksdb-BlockBasedTable-Format


**Bloom filter**: See https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter

**Bottommost File** A SST file that doesn't overlap with any lower level files. So a bottommost file might not be the **Last Level** file, but the **Last Level** file is always a bottommost file.

**Checkpoint**: A checkpoint is a physical mirror of the database in another directory in the file system. See https://github.com/facebook/rocksdb/wiki/Checkpoints


**Column Family**: column family is a separate key space in one DB. In spite of the misleading name, it has nothing to do with the “column family” concept in other storage systems. RocksDB doesn't even have the concept of “column”. See https://github.com/facebook/rocksdb/wiki/Column-Families


**Compaction filter**: a user plug-in that can modify or drop existing keys during a compaction. See https://github.com/facebook/rocksdb/wiki/Compaction-Filter


**Compaction**: background jobs that merge some SST files into some other SST files. LevelDB's compaction also includes flush. In RocksDB, we further distinguished the two. See https://github.com/facebook/rocksdb/wiki/RocksDB-Overview#multi-threaded-compactions and https://github.com/facebook/rocksdb/wiki/Compaction


**Comparator**: A plug-in class which can define the order of keys. See https://github.com/facebook/rocksdb/blob/main/include/rocksdb/comparator.h


**DB properties**: some running status that can be returned by the function DB::GetProperty(). See https://github.com/facebook/rocksdb/blob/main/include/rocksdb/db.h


**Flush**: background jobs that write out data in mem tables into SST files.


**Forward iterator / Tailing iterator**: A special iterator option that optimizes for very specific use cases. See https://github.com/facebook/rocksdb/wiki/Tailing-Iterator


**Immutable memtable**: A closed **memtable** that is waiting to be flushed.


**Index** The index on the data blocks in a SST file. It is persisted as an index block in the SST file. The default index format is the binary search index.


**Iterator**: iterators are used by users to query keys in a range in sorted order. See https://github.com/facebook/rocksdb/wiki/Basic-Operations#iteration

**Last Level** The last level is the lowest level in LSM tree, for example, if the [`num_levels`](https://github.com/facebook/rocksdb/blob/b52620ab0ea049cb0e6c17b09779065337662e01/include/rocksdb/advanced_options.h#L527) is 7, the level 6 is the last level.

**Leveled Compaction** or **Level-Based Compaction Style**: the default compaction style of RocksDB. [[Leveled Compaction]]


**LSM level**: a logical organization of the DB physical data for maintaining desired LSM-tree shape and structure. See https://github.com/facebook/rocksdb/wiki/Compaction, particularly https://github.com/facebook/rocksdb/wiki/Compaction#lsm-terminology-and-metaphors


**LSM-tree**: See the definition in https://en.wikipedia.org/wiki/Log-structured_merge-tree RocksDB is LSM-tree-based storage engine.


**Memtable switch**: During this process, the current **active memtable** (the one current writes go to) is closed and turned into an **Immutable memtable**. At the same time, we will close the current WAL file and start a new one.


**Memtable** / **write buffer**: the in-memory data structure that stores the most recent updates of the database. Usually it is organized in sorted order and includes a binary searchable index. See https://github.com/facebook/rocksdb/wiki/Basic-Operations#memtable-and-table-factories


**Merge operator**: RocksDB supports a special operator Merge(), which is a delta record, merge operand, to the existing value. Merge operator is a user defined call-back class which can merge the merge operands.  See https://github.com/facebook/rocksdb/wiki/Merge-Operator-Implementation


**Partitioned Filters**: Partitioning a full bloom filter into multiple smaller blocks. See https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters.


**Partitioned Index** The binary search index block partitioned to multiple smaller blocks. See https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters


**perf context**: an in-memory data structure to measure thread-local stats. It is usually used to measure per-query stats. See https://github.com/facebook/rocksdb/wiki/Perf-Context-and-IO-Stats-Context


**Pessimistic Transactions** Using locks to provide isolation between multiple concurrent transactions. The default write policy is WriteCommitted.


**PlainTable**: An alternative format of SST file format, optimized for ramfs. See https://github.com/facebook/rocksdb/wiki/PlainTable-Format


**Point lookup**: In RocksDB, point lookup means reading one key using Get() or MultiGet().


**Prefix bloom filter**: a special bloom filter that can be limitly used in iterators. Some file reads are avoided if an SST file or memtable doesn't contain the prefix of the lookup key extracted by the prefix extractor. See https://github.com/facebook/rocksdb/wiki/Prefix-Seek-API-Changes


**Prefix extractor**: a callback class that can extract prefix part of a key. This is most frequently used as the prefix used in prefix bloom filter. See https://github.com/facebook/rocksdb/blob/main/include/rocksdb/slice_transform.h


**Range lookup**: Range lookup means reading a range of keys using an Iterator.


**Rate limiter**: it is used to limit the rate of bytes written to the file system by flush and compaction. See https://github.com/facebook/rocksdb/wiki/Rate-Limiter


**Recovery**: the process of restarting a database after it failed or was closed.


**Sequence number (SeqNum / Seqno)**: each write to the database will be assigned an auto-incremented ID number. The number is attached with the key-value pair in WAL file, memtable, and SST files. The sequence number is used to implement snapshot read, garbage collection in compactions, MVCC in transactions, and some other purposes.


**Single delete**: a special delete operation which only works when users never update an existing key: https://github.com/facebook/rocksdb/wiki/Single-Delete


**Snapshot**: a snapshot is a logical consistent point-in-time view, in a running DB. See https://github.com/facebook/rocksdb/wiki/RocksDB-Overview#gets-iterators-and-snapshots


**SST File** (**Data file** / **SST table**): SST stands for Sorted Sequence Table. They are persistent files storing data. In the file keys are usually organized in sorted order so that a key or iterating position can be identified through a binary search.


**Statistics**: an in-memory data structure that contains cumulative stats of live databases. https://github.com/facebook/rocksdb/wiki/Statistics


**Super Version**: An internal concept of RocksDB. A super version consists of the list of SST files and blob files (a “version”) and the list of live mem tables at a certain point in time. Either a compaction or flush, or a mem table switch will cause a new “super version” to be created. An old “super version” can continue being used by on-going read requests. Old super versions will eventually be garbage collected after they are not needed anymore.


**Table Properties**: metadata stored in each SST file(stats block). It includes system properties that are generated by RocksDB and user defined table properties calculated by user defined call-backs. See https://github.com/facebook/rocksdb/blob/main/include/rocksdb/table_properties.h


**Universal Compaction Style**: an alternative compaction algorithm. See https://github.com/facebook/rocksdb/wiki/Universal-Compaction


**Version**: An internal concept of RocksDB. A version consists of all the live SST files and blob files (when using BlobDB) at a certain point in time. Once a flush or compaction finishes, a new “version” will be created because the list of live SST/blob files has changed. An old “version” can continue being used by on-going read requests or compaction jobs. Old versions will eventually be garbage collected.


**Write stall**: When flush or compaction is backlogged, RocksDB may actively slowdown writes to make sure flush and compaction can catch up. See https://github.com/facebook/rocksdb/wiki/Write-Stalls


**Write-Ahead-Log (WAL)** or **log**: A log file used to recover data that is not yet flushed to SST files, during DB recovery. See https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format


**WriteCommitted** The default write policy in pessimistic transactions, which buffers the writes in memory and write them into the DB upon commit of the transaction.


**WritePrepared** A write policy in pessimistic transactions that buffers the writes in memory and write them into the DB upon prepare if it is a 2PC transaction or commit otherwise. See https://github.com/facebook/rocksdb/wiki/WritePrepared-Transactions


**WriteUnprepared** A write policy in pessimistic transactions that avoid the need for larger memory buffers by writing data to the DB as they are sent by the transaction. See https://github.com/facebook/rocksdb/wiki/WritePrepared-Transactions
