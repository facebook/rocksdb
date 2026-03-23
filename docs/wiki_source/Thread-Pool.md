A thread pool is associated with Env environment object. The client has to create a thread pool by setting the number of background threads using method <code>Env::SetBackgroundThreads()</code> defined in <code>rocksdb/env.h</code>. We use the thread pool for compactions and memtable flushes. Since memtable flushes are in critical code path (stalling memtable flush can stall writes, increasing p99 latency), we suggest having two thread pools - with priorities HIGH and LOW. Memtable flushes are by default scheduled on HIGH thread pool, while compactions on LOW thread pool.

The recommended way to configure background compaction and flush parallelism is with `Options::max_background_jobs`. For now, we still respect the options `Options::max_background_compactions` and `Options::max_background_flushes` in case users have workloads for which our automatic allocation is suboptimal.

<ul>

<li> <code>Options::max_background_jobs</code> - Maximum number of concurrent background jobs, including flushes and compactions. RocksDB will automatically decide how to allocate the available job slots to flushes and compactions. 

<li> **DEPRECATED** <code>Options::max_background_compactions</code> - Maximum number of concurrent background compactions, submitted to the LOW priority thread pool.

<li> **DEPRECATED** <code>Options::max_background_flushes</code> - Maximum number of concurrent background memtable flush jobs, submitted to the HIGH priority thread pool by default. To schedule flushes and backgrounds both in the LOW thread pool (not recommended), users can configure the HIGH priority thread pool to have zero threads. Separate non-empty thread pools are particularly important when the same Env is shared by multiple db instances. Without a HIGH pool, long running major compaction jobs could potentially block memtable flush jobs of other db instances, leading to unnecessary Put stalls.
</ul>

```cpp
  #include "rocksdb/env.h"
  #include "rocksdb/db.h"

  auto env = rocksdb::Env::Default();
  env->SetBackgroundThreads(2, rocksdb::Env::LOW);
  env->SetBackgroundThreads(1, rocksdb::Env::HIGH);
  rocksdb::DB* db;
  rocksdb::Options options;
  options.env = env;
  options.max_background_compactions = 2;
  options.max_background_flushes = 1;
  rocksdb::Status status = rocksdb::DB::Open(options, "/tmp/testdb", &db);
  assert(status.ok());
  ...
```