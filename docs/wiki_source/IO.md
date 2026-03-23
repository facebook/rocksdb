RocksDB provides a list of options for users to hint how I/Os should be executed.

## Control Write I/O
### Range Sync
RocksDB's data files are usually generated in an appending way. File system may choose to buffer the write until the dirty pages hit a threshold and write out all of those pages all together. This can create a burst of write I/O and cause the online I/Os to wait too long and cause long query latency. Rather, you can ask RocksDB to periodically hint OS to write out outstanding dirty pages by setting `options.bytes_per_sync` for SST files and `options.wal_bytes_per_sync` for WAL files. Underlying it calls sync_file_range() on Linux every time a file is appended for such size. The most recent pages are not included in the range sync.

### Rate Limiter
You can control the total rate RocksDB writes to data files through `options.rate_limiter`, in order to reserve enough I/O bandwidth to online queries. See [[Rate Limiter]] for details.

### Write Max Buffer
When appending a file, RocksDB has internal buffering of files before writing to the file system, unless an explicit fsync is needed. The max size of this buffer can be controlled by `options.writable_file_max_buffer_size`. Tuning this parameter is more critical in [[Direct IO] mode or to a file system without page cache. With non-direct I/O mode, enlarging this buffer only reduces number of write() system calls and is unlikely to change the I/O behavior, so unless this is what you want, it may be desirable to keep the default value 0 to save the memory.

### File Deletion
Deletion of obsolete DB files can be rate limited by configuring the delete scheduler. This is especially useful on flash devices to limit read latency spikes due to a burst of deletions. See [[Delete Scheduler]] for details.

## Control Read I/O
### fadvise
While opening an SST file for reads, users can decide whether RocksDB will call fadvise with FADV_RANDOM, by setting `options.advise_random_on_open = true` (default). If the value is `false`, no fadvise will be called while opening a file. Setting the option to be `true` usually works well if the dominating queries are either Get() or iterating a very short range, because read-ahead is not helpful in these cases anyway. Otherwise, `options.advise_random_on_open = false` can usually improve performance to hint the file system to do underlying read-ahead.

Unfortunately, there isn't a good setting for mixed workload. There is an ongoing project to address this, by doing read-ahead for iterators inside RocksDB.

### Compaction inputs
Compaction inputs are special. They are long sequential reads, so applying the same fadvise hint as user reads is usually not optimal. Also, usually, compaction input files are often going to be deleted soon, though there is no guarantee. RocksDB provides multiple ways to deal with that:

#### fadvise hint
RocksDB will call fadvise to any compaction input file according to `options.access_hint_on_compaction_start`. This can override the fadvise random setting since a file is picked as a compaction input.

#### Use a different file descriptor for compaction inputs
If `options.new_table_reader_for_compaction_inputs = true`, RocksDB will use different file descriptors for compaction inputs. This can avoid the mixture of fadvise setting for normal data files and compaction inputs. The limitation of the setting is that, RocksDB does not just create a new file descriptor, but read index, filter and other meta blocks again and store them in memory, so that it takes extra I/O and use more memory.

#### readahead for compaction inputs
You can do its own readahead following `options.compaction_readahead_size` if it is not 0. `options.new_table_reader_for_compaction_inputs` is automatically switched to `true` if the option is set. This setting can allow users to keep `options.access_hint_on_compaction_start` to `NONE`.

It is critical to set the option if [[Direct IO]] is on or the file system doesn't support readahead.

## Direct I/O
Rather than control the I/O through file system hints shown above, you can enable direct I/O in RocksDB to allow RocksDB to directly control I/O, using option `use_direct_reads` and/or `use_direct_io_for_flush_and_compaction`. If direct I/O is enabled, some or all of the options introduced above will not be applicable. See more details in [[Direct IO]].

## Memory Mapping
`options.allow_mmap_reads` and `options.allow_mmap_writes` make RocksDB mmap the whole data file while doing read or write, respectively. The benefit of the approach is to reduce the file system calls doing pread() and write(), and in many cases, reduce the memory copying too. `options.allow_mmap_reads` can usually significantly improve performance if the DB is run on ramfs. They can be used on file systems backed by block device too. However, based on our previous experience, file systems aren't usually doing a perfect job maintaining this kind of memory mapping, and some times cause slow queries. In this case, we advise you try out this option only when necessary and with caution. 

## Avoid Blocking IO
Cleanup on ```Iterator``` destruction and cleanup on column family destruction by default will try to delete obsolete files in the context of the thread calling the destructor, subject to deletion rate limits. This can result in an unexpected long latency in completing the operation. To avoid the long latency and defer the deletion of obsolete files to background threads, the following options are provided -
* ```ReadOptions::background_purge_on_iterator_cleanup``` - This option, when set in the call to ```DB::NewIterator()```, will schedule the deletion of obsolete files in a background thread on iterator destruction.
* ```DBOptions::avoid_unnecessary_blocking_io``` - This option is a DB wide option. When set, both iterator destructor and ```ColumnFamilyHandle``` destructors will schedule obsolete file deletion in a background thread.