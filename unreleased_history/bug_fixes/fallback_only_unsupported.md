Fixed a bug where compaction read under non direct IO still falls back to RocksDB internal prefetching after file system's prefetching returns non-OK status other than `Status::NotSupported()`
