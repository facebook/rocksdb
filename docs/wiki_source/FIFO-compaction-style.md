FIFO compaction style is the simplest compaction strategy. It is suited for keeping event log data with very low overhead (query log for example). It periodically deletes the old data, so it's basically a TTL compaction style.

In FIFO compaction, all files are in level 0. When total size of the data exceeds configured size (CompactionOptionsFIFO::max_table_files_size), we delete the oldest table file. This means that write amplification of data is always 1 (in addition to WAL write amplification).

Currently, CompactRange() function just manually triggers the compaction and deletes the old table files if there is need. It ignores the parameters of the function (begin and end keys).

Since we never rewrite the key-value pair, we also don't ever apply the compaction filter on the keys.

Please use FIFO compaction style with caution. Unlike other compaction style, it can drop data without informing users.

## Compaction
FIFO compactions can end up with lots of L0 files. Queries can be slow because we need to search all those files in the worst case in a query. Even Bloom filters may not be able to offset all of them. With typical 1% false positive, 1000 L0 files will cause 10 false positive cases in average, and generate 10 I/Os per query in the worst case. More Bloom bits per key, such as 20, is highly recommended to reduce the false positive rate, especially with [format_version=5 Bloom filter](https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter#full-filters-new-format), but more Bloom bits does use more memory. In some cases, even the CPU overhead of Bloom filter checking is too high.

To solve this case, users can choose to enable some lightweight compactions to happen. This will potentially double the write I/O, but can significantly reduce number of L0 files. This sometimes is the right trade-off for users.

The feature is introduced in 5.5 release. Users can enable it using `CompactionOptionsFIFO.allow_compaction = true`. And it tries to pick up at least `level0_file_num_compaction_trigger` files flushed from memtable and compact them together.

To be specific, we always start with the latest `level0_file_num_compaction_trigger` files and try to include more files in the compaction. We calculated compacted bytes per file deducted using `total_compaction_size / (number_files_in_compaction - 1)`. We always pick the files so that this number is minimized and is no more than `options.write_buffer_size`. In typical workloads, it will always compact `level0_file_num_compaction_trigger` freshly flushed files.

For example, if `level0_file_num_compaction_trigger = 8` and every flushed file is 100MB. Then as soon as there is 8 files, they are compacted to one 800MB file. And after we have 8 new 100MB files, they are compacted in the second 800MB, and so on. Eventually we'll have a list of 800MB files and no more than 8 100MB files.

Please note that, since the oldest files are compacted, the file to be deleted by FIFO is also larger, so potentially slightly less data is stored than without compaction.

## FIFO Compaction with TTL
A new feature called FIFO compaction with TTL has been introduced starting in RocksDB 5.7. ([2480](https://github.com/facebook/rocksdb/pull/2480))

So far, FIFO compaction worked by just taking the total file size into consideration, like: drop the oldest files if the db size exceeds `compaction_options_fifo.max_table_files_size`, one at a time until the total size drops below the set threshold. This sometimes makes it tricky to enable in production as use cases can have organic growth. 

A new option, `ttl`, has been introduced for this to delete SST files for which the TTL has expired. This feature enables users to drop files based on time rather than always based on size, say, drop all SST files older than a week or a month. [`ttl` option before 6.0 is part of `ColumnFamilyOptions.compaction_options_fifo` struct. It moved into `ColumnFamilyOptions` from 6.0 onwards].

Constraints:
- This currently works only for Block based table format and when max_open_files is set to -1 since it requires all table files to be open.
- FIFO with TTL still works within the bounds of the configured size, i.e. RocksDB temporarily falls back to FIFO deletion based on size if it observes that deleting expired files does not bring the total size to be less than the configured max size. 