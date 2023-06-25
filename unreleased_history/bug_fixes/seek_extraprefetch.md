Fix extra prefetching during seek in async_io when BlockBasedTableOptions.num_file_reads_for_auto_readahead_ = 1 leading to extra reads than required.
