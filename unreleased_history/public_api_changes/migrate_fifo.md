Using `OptionChangeMigration()` to migrate from non-FIFO to FIFO compaction
with `Options::compaction_options_fifo.max_table_files_size` > 0 can cause
the whole DB to be dropped right after migration if the migrated data is larger than
`max_table_files_size`
