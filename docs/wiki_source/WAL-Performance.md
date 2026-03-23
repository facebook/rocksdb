# Non-Sync Mode
When `WriteOptions.sync = false` (the default), WAL writes are not synchronized to disk. Unless the operating system thinks it must flush the data (e.g. too many dirty pages), users don't need to wait for any I/O for write.

Users who want to even reduce the CPU of latency introduced by writing to OS page cache, can choose `Options.manual_wal_flush = true`. With this option, WAL writes are not even flushed to the file system page cache, but kept in RocksDB. Users need to call `DB::FlushWAL()` to have buffered entries go to the file system.

Users can call DB::SyncWAL() to force fsync WAL files. The function will not block writes being executed in other threads.

In this mode, the WAL write is not crash safe.

# Sync Mode
When `WriteOptions.sync = true`, the WAL file is fsync'ed before returning to the user.

## Group Commit
As most other systems relying on logs, RocksDB supports **group commit** to improve WAL writing throughput, as well as write amplification. RocksDB's group commit is implemented in a naive way: when different threads are writing to the same DB at the same time, all outstanding writes that qualify to be combined will be combined together and write to WAL once, with one fsync. In this way, more writes can be completed by the same number of I/Os.

Writes with different write options might disqualify themselves to be combined. The maximum group size is 1MB. RocksDB won't try to increase batch size by proactive delaying the writes.

## Number of I/Os per write
If `Options.recycle_log_file_num = false` (the default). RocksDB always create new files for new WAL segments. Each WAL write will change both of data and size of the file, so every fsync will generate at least two I/Os, one for data and one for metadata. Note that RocksDB calls fallocate() to reserve enough space for the file, but it doesn't prevent the metadata I/O in fsync.

`Options.recycle_log_file_num = true` will keep a pool of WAL files and try to reuse them. When writing to an existing log file, random writes are used from size 0. Before writes hit the end of the file, the file size doesn't change, so the I/O for metadata might be avoided (also depends on file system mount options). Assuming most WAL files will have similar sizes, I/O needed for metadata will be minimal.

## Write Amplification
Note that for some use cases, synchronous WAL can introduce non-trivial write amplification. When writes are small, because complete block/page might need to be updated, we may end up with two 4KB writes (one for data and one for metadata) even if the write is very small. If write is only 40 bytes, 8KB is updated, the write amplification is 8 KB/40 bytes ~= 200. It can easily be even larger than the write amplification by LSM-tree.
