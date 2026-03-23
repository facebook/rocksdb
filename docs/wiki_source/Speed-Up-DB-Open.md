When users reopen a DB, here are some steps that may take longer than expected:

## Manifest replay
The [[MANIFEST]] file contains history of all file operations of the DB since the last time DB was opened, and is replayed during DB open. If there are too many updates to replay, it takes a long time. This can happen when:
* SST files were too small so file operations were too frequently. If this is the case, try to solve the small SST file problem. Maybe memtable is flushed too often, which generates small L0 files, or target file size is too small so that compaction generates small files. You can try to adjust the configuration accordingly
* DB simply runs for too long and accumulates too many historic updates.
Either way, you can try to set `options.max_manifest_file_size` to force a new manifest file to be generated when it hits the maximum size, to avoid replaying for too long.

## WAL replaying
All the WAL files are replayed if it contains any useful data.

If your memtable size is large, the replay can be long. So try to shrink the memtable size.

Another common reason that WAL files to replay is too large is that, one of the column families gets too slow writes, which holds logs from being deleted. When DB reopens, all those log files are read, just to replay updates from this column family. In this case, set a proper `options.max_total_wal_size` value. The low-traffic column families will be flushed to limit the total WAL files to replay to under this threshold. see [[Write Ahead Log]].

## Reading footer and meta blocks of all the SST files
When `options.max_open_files` is set to `-1`, during DB open, all the SST files will be opened, with their footer and metadata blocks to be read. This is random reads from disk. If you have a lot of files and a relatively high latency device, especially spinning disks, those random reads can take a long time. Two options can help mitigate the problem:
* `options.max_file_opening_threads` allows reading those files in parallel. Making this number higher usually works well on high bandwidth devices, like SSD.
* Set `options.skip_stats_update_on_db_open=true`. This allows RocksDB to do one fewer read per file.
* Tune the LSM-tree to reduce the number of SST file is also helpful.

## Opening DB with `options.paranoid_checks == true && options.skip_checking_sst_file_sizes_on_db_open == false`
By default, `options.paranoid_checks` is `true` and `options.skip_checking_sst_file_sizes_on_db_open` is `false`. With this configuration, RocksDB will query the underlying file system for the sizes of all live SST files during DB open, which can be slow if there are many SST files, especially when the SST files reside on remote storage. If this occurs, you can set `options.skip_checking_sst_files_sizes_on_db_open` to true and keep `paranoid_checks` true.

## Opening too many DBs one by one
Some users manage multiple DBs per service and open DBs one-by-one. If they have multi-core server, they can use a thread pool and open those DBs in parallel.