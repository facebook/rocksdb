Fixed a bug where, under non direct IO, compaction read does not fall back to RocksDB internal prefetching when file system's prefetching can not proceed.
