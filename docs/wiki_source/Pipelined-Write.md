The pipelined write feature added in RocksDB 5.5 is to improve concurrent write throughput in case WAL is enabled. By default, a single write thread queue is maintained for concurrent writers. The thread gets to the head of the queue becomes write batch group leader and responsible for writing to WAL and memtable for the batch group.

One observation is that WAL writes and memtable writes are sequential, and by making them run in parallel we can increase throughput. For one single writer WAL writes and memtable writes have to run sequentially. With concurrent writers, once the previous writer finishes its WAL write, the next writer waiting in the write queue can start writing to the WAL while the previous writer still has its memtable write ongoing. This is what pipelined writes do.

To enable pipelined write, simply set `Options.enable_pipelined_write=true`. db_bench benchmark shows 20% write throughput improvement with concurrent writers and WAL enabled, when DB is stored in ramfs and compaction throughput is not the bottleneck.

## Benchmarks

We run db_bench on tempfs with 8 threads writing concurrently with WAL enabled. Memtable is the default skiplist memtable of 64MB. LZ4 compression is enabled. With pipelined write we got roughly 30% improvement on write throughput. Raw result: https://gist.github.com/yiwu-arbug/3b5a5727e52f1e58d1c10f2b80cec05d