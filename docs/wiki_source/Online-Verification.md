RocksDB supports some online verifications while the DB is running, to catch potential software bugs and mitigate impacts of flawed hardware. Here are some of them.

## ColumnFamilyOptions::force_consistency_checks
The option does some basic consistency checks to LSM-tree. It checks files in non-0 level are not overlapping, and level 0 files follow some ordering rule, a file is not compacted twice, as well as some BlobDB related conditions. The DB will be frozen from new writes if a violation is detected. This feature is by default on.

## ColumnFamilyOptions::paranoid_file_checks
This option does some more expensive extra checking when generating a new SST file. After each SST is generated in flush or compaction, the file is re-opened an check the content of the file: (1) the keys are in comparator order (also available and enabled by default during file write via `ColumnFamilyOptions::check_flush_compaction_key_order`); (2) the hash of all the KVs is the same as calculated when we add KVs into it. These checks detect certain corruptions so we can prevent the corrupt files from being applied to the DB. We suggest users turn it on at least in shadow environments, and consider to run it in production too if you can afford the overheads.

## DBOptions::flush_verify_memtable_count
With this option on, RocksDB will check the count of entries added into memtable while flushing it into an SST file. This feature is to have some online coverage to memtable corruption, which can be caused by RocksDB bugs or hardware issues. Checking total count is the first step to have some checks to it. This feature will be released in 6.21 and by default on. In the future, we will check more counters during memtables, e.g. number of puts or number of deletes.

# Per key-value checksum
RocksDB has extensive support for per key-value checksum to detect in-memory data corruptions. There are different options for different types of in-memory data. Checksum will be compute for each internal key (including user key, op type and sequence number) and value and verified when necessary. For implementation detail, refer to [blog post](https://rocksdb.org/blog/2022/07/18/per-key-value-checksum.html). 

## [WriteOptions::protection_bytes_per_key](https://github.com/facebook/rocksdb/blob/bc0db33483d5e79b281ba3137ebf286b2d1efd8d/include/rocksdb/options.h#L1760-L1765)
This protects data in write batch. When replaying from WAL during DB open, this is enabled always (set to 8) so that WAL replaying is always protected.

## [ColumnFamilyOptions::memtable_protection_bytes_per_key](https://github.com/facebook/rocksdb/blob/bc0db33483d5e79b281ba3137ebf286b2d1efd8d/include/rocksdb/advanced_options.h#L1095-L1108)
This protects key-values that are in memtables.

## [ColumnFamilyOptions::block_protection_bytes_per_key](https://github.com/facebook/rocksdb/blob/bc0db33483d5e79b281ba3137ebf286b2d1efd8d/include/rocksdb/advanced_options.h#L1146-L1158)
This protects key-values in blocks that are in block cache, which is typically RocksDB's largest use of memory.
