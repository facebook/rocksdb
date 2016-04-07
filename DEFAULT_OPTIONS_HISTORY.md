## Unreleased
* options.write_buffer_size changes from 4MB to 64MB
* options.target_file_size_base changes from 2MB to 64MB
* options.max_bytes_for_level_base changes from 10MB to 256MB
* options.soft_pending_compaction_bytes_limit changes from 0 (disabled) to 64GB
* options.hard_pending_compaction_bytes_limit changes from 0 (disabled) to 256GB
* table_cache_numshardbits changes from 4 to 6
* max_file_opening_threads changes from 1 to 16 