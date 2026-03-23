* Pluggable WAL
* Data encryption
* Warm block cache after flush and compactions in a smart way
* Queryable Backup
* Improve sub-compaction by making data partition more evenly
* Tools to collect operations to a database and replay them
* Customized bloom filter for data blocks
* Build a new compaction style optimized for time series data
* Implement YCSB benchmark scenarios in db_bench
* Improve DB recovery speed when WAL files are large (parallel replay WAL)
* use thread pools to do readahead + decompress and compress + write-behind. Igor started on this. When manual compaction is multi-threaded then we can use RocksDB as a fast external sorter -- load keys in random order with compaction disabled, then do manual compaction.
* expose merge operator in MongoDB + RocksDB
* SQLite + RocksDB
* Snappy compression for WAL writes. Maybe this is only done for large writes and maybe we add a field to WriteOptions so a user can request it.

# Research Projects
## Adaptive write-optimized algorithms
The granularity of tuning in RocksDB is a column family (CF), which is essentially a separate LSM tree. However many times the data in a column family is composed of various workloads with diverse characteristic, each requiring a different tuning. In the case of MyRocks this could be because CF stores multiple indexes – some are heavy on point operations, others heavy on range operations, some are read heavy, others are read-only, others are read+write. Even within an index there is variety – there will be hot spots that get writes, some data will be write once, some write N times and then it becomes read-only.

Tuning workloads and split indexes into different column families, would take too much resources. When there is variety within one index there is not much that can be done currently. A clever algorithm should be able do handle such cases.

## Self-tuning DBs

Each LSM tree has many configuration parameters, which makes optimizing them very difficult even for experts. The typical black-box machine learning algorithms that rely on availability on many data points would not work here since generating each new data point would require running a DB at large scale, which requires non-trivial time and resource budget. Gathering data points from existing production systems is also not practical due to security concerns of sharing such data to outside the organization.

## Sub-optimal, non-tunable LSM

There is a long tail of users with small workloads, who are ok with sub-optimal performance but do not have the engineering resources to fine tune them. A list of default value for configuration also do not always work for them since it could give unacceptably bad performance for some particular workloads. An LSM tree that does not have any tuning knobs yet provides a reasonable performance for any given workload (even the corner cases) is much practical for the silent, long-tail of users.

## Bloom filter for range queries

LSM trees have the drawback of bad read amplification. To mitigate this problem bloom filters are essential to LSM trees. A bloom filter can tell with a good probability whether a key exist in the SST file. When servicing a SQL query however many queries are not just point lookups and involve a range specifier on the key. An example is “Select * from T when 10 < c1 < 20” when the specific key (composed of c1) is not available. A bloom filter that could operate on ranges could be helpful in such cases.

## RocksDB on pure-NVM

How to optimize RocksDB when it uses non-volatile memory (NVM) as the storage device?

## RocksDB on hierarchical storage

RocksDB is usually run on two-level storage setup: RAM + SSD or RAM + HDD, where RAM is volatile and SSD, and HDD are non-volatile storage. There are interesting design choices of how to split data between the two storage. For example, RAM could be used solely as a cache for blocks, or it could preload all the indexes and filters, or given partitioned index/filters only preload the top-level to the RAM. The design choice become much more interesting when we have a multi-layer hierarchy of storage devices: RAM + NVM + SSD + HDD (or any combination of them) each with different storage characteristics. What is the optimal way to split data among the hierarchy.

## Time series DBs

How we can optimize RocksDB based on the particular characteristics of time series databases? For example can we do better encoding of keys knowing that they are integers in ascending order? Can we do better encoding of values knowing that they are floating point numbers with high correlation between adjacent numbers? What is an optimal compaction strategy given the patterns of data distribution in a time series DB? Do such data show different characteristics in different levels of the LSM tree and we can we leverage such information for more efficient data layout for each level? etc.