Fix regression in issue #12038 due to `Options::compaction_readahead_size` greater than `max_sectors_kb` (i.e, largest I/O size that the OS issues to a block device defined in linux)
