Spinning disks are different for RocksDB, for some main reasons:
##### Memory / Persistent Storage ratio is usually much lower for databases on spinning disks. If the ratio of data to RAM is too large then you can reduce the memory required to keep performance critical data in RAM. Suggestions:
* Use relatively **larger block sizes** to reduce index block size. You should use at least 64KB block size. You can consider 256KB or even 512KB. The downside of using large blocks is that RAM is wasted in the block cache.
* Turn on **BlockBasedTableOptions.cache_index_and_filter_blocks=true** as it's very likely you can't fit all index and bloom filters in memory. Even if you can, it's better to set it for safety.
* **enable options.optimize_filters_for_hits** to reduce some bloom filter block size.
* Be careful about whether you have enough memory to keep all bloom filters. If you can't then bloom filters might hurt performance.
* Try to **encode keys as compact as possible**. Shorter keys can reduce index block size.  

##### Spinning disks usually provide much lower random read throughput than flash.
* Set **options.skip_stats_update_on_db_open=true** to speed up DB open time.
* This is a controversial suggestion: use **level-based compaction**, as it is more friendly to reduce reads from disks.
* If you use level-based compaction, use **options.level_compaction_dynamic_level_bytes=true**.
* Set **options.max_file_opening_threads** to a value larger than 1 if the server has multiple disks.

##### Throughput gap between random read vs. sequential read is much higher in spinning disks. Suggestions:
* Enable RocksDB-level read ahead for compaction inputs: **options.compaction_readahead_size** with **options.new_table_reader_for_compaction_inputs=true**
* Use relatively **large file sizes**. We suggest at least 256MB
* Use relatively larger block sizes

##### Spinning disks are much larger than flash:
* To avoid too many file descriptors, use larger files. We suggest at least file size of 256MB.
* If you use universal compaction style, don't make single DB size too large, because the full compaction will take a long time and impact performance. You can use more DBs but single DB size is smaller than 500GB.