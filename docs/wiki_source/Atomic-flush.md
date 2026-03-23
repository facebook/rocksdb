RocksDB supports atomic flush of multiple column families if the DB option `atomic_flush` is set to `true`. The execution result of flushing **multiple** column families is written to the MANIFEST with 'all-or-nothing' guarantee (logically). With atomic flush, either all or no memtables of the column families of interest are persisted to SST files and added to the database.

This can be desirable if data in multiple column families must be consistent with each other. For example, imagine there is one metadata column family `meta_cf`, and a data column family `data_cf`. Every time we write a new record to `data_cf`, we also write its metadata to `meta_cf`. `meta_cf` and `data_cf` must be flushed atomically. Database becomes inconsistent if one of them is persisted but the other is not. Atomic flush provides a good guarantee. Suppose at a certain time, kv1 exists in the memtables of `meta_cf` and kv2 exists in the memtables of `data_cf`. After atomically flushing these two column families, both kv1 and kv2 are persistent if the flush succeeds. Otherwise neither of them exist in the database.

Since atomic flush also goes through the `write_thread`, it is guaranteed that no flush can occur in the middle of write batch.

Note that **it is not necessary to use the Atomic flush option if [WAL](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log) is always enabled**. When WAL is enabled, a single WAL file is used to capture writes to all column families; hence, the recovered database (by replaying the WAL logs in crash/recovery path) is guaranteed to be consistent across all column families.

It's easy to enable/disable atomic flush as a DB option. It can't be changed during runtime though. 
To open the DB with atomic flush enabled:
```cpp
Options options;
... // Set other options
options.atomic_flush = true;
DBOptions db_opts(options);
DB* db = nullptr;
Status s = DB::Open(db_opts, dbname, column_families, &handles, &db);
```


For auto-triggered flush, RocksDB atomically flushes **ALL** column families.

For manual flush, application has to specify the list of column families to flush atomically in DB::Flush():

```cpp
w_opts.disable_wal = true;
db->Put(w_opts, cf_handle1, key1, value1);
db->Put(w_opts, cf_handle2, key2, value2);
FlushOptions flush_opts;
Status s = db->Flush(flush_opts, {cf_handle1, cf_handle2});
```

