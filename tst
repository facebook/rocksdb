$DEBUG_LEVEL is 1
Makefile:170: Warning: Compiling in debug mode. Don't use the resulting binary in production
$DEBUG_LEVEL is 1
Makefile:170: Warning: Compiling in debug mode. Don't use the resulting binary in production
  CC       cache/cache_entry_roles.o
  CC       cache/cache.o
  CC       cache/cache_key.o
  CC       cache/cache_reservation_manager.o
  CC       cache/clock_cache.o
  CC       cache/lru_cache.o
  CC       cache/sharded_cache.o
  CC       db/arena_wrapped_db_iter.o
  CC       db/blob/blob_fetcher.o
  CC       db/blob/blob_file_addition.o
  CC       db/blob/blob_file_builder.o
  CC       db/blob/blob_file_cache.o
  CC       db/blob/blob_file_garbage.o
  CC       db/blob/blob_file_meta.o
  CC       db/blob/blob_garbage_meter.o
  CC       db/blob/blob_file_reader.o
  CC       db/blob/blob_log_format.o
  CC       db/blob/blob_log_sequential_reader.o
  CC       db/blob/blob_log_writer.o
  CC       db/blob/prefetch_buffer_collection.o
  CC       db/builder.o
  CC       db/c.o
  CC       db/column_family.o
  CC       db/compaction/compaction.o
  CC       db/compaction/compaction_iterator.o
  CC       db/compaction/compaction_job.o
  CC       db/compaction/compaction_picker.o
  CC       db/compaction/compaction_picker_fifo.o
  CC       db/compaction/compaction_picker_level.o
  CC       db/compaction/compaction_picker_universal.o
  CC       db/compaction/sst_partitioner.o
  CC       db/convenience.o
  CC       db/db_filesnapshot.o
  CC       db/db_impl/compacted_db_impl.o
  CC       db/db_impl/db_impl.o
  CC       db/db_impl/db_impl_compaction_flush.o
  CC       db/db_impl/db_impl_debug.o
  CC       db/db_impl/db_impl_experimental.o
  CC       db/db_impl/db_impl_files.o
  CC       db/db_impl/db_impl_open.o
  CC       db/db_impl/db_impl_readonly.o
  CC       db/db_impl/db_impl_secondary.o
  CC       db/db_impl/db_impl_write.o
  CC       db/db_info_dumper.o
  CC       db/db_iter.o
  CC       db/dbformat.o
  CC       db/error_handler.o
  CC       db/event_helpers.o
  CC       db/experimental.o
  CC       db/external_sst_file_ingestion_job.o
  CC       db/file_indexer.o
  CC       db/flush_job.o
  CC       db/flush_scheduler.o
  CC       db/forward_iterator.o
  CC       db/import_column_family_job.o
  CC       db/internal_stats.o
  CC       db/logs_with_prep_tracker.o
  CC       db/log_reader.o
  CC       db/log_writer.o
  CC       db/malloc_stats.o
  CC       db/memtable.o
  CC       db/memtable_list.o
  CC       db/merge_helper.o
  CC       db/merge_operator.o
  CC       db/output_validator.o
  CC       db/periodic_work_scheduler.o
  CC       db/range_del_aggregator.o
  CC       db/range_tombstone_fragmenter.o
  CC       db/repair.o
  CC       db/snapshot_impl.o
  CC       db/table_cache.o
  CC       db/table_properties_collector.o
  CC       db/transaction_log_impl.o
  CC       db/trim_history_scheduler.o
  CC       db/version_builder.o
  CC       db/version_edit.o
  CC       db/version_edit_handler.o
  CC       db/version_set.o
  CC       db/wal_edit.o
  CC       db/wal_manager.o
  CC       db/write_batch.o
  CC       db/write_batch_base.o
  CC       db/write_controller.o
  CC       db/write_thread.o
  CC       env/composite_env.o
  CC       env/env.o
  CC       env/env_chroot.o
  CC       env/env_encryption.o
  CC       env/env_posix.o
  CC       env/file_system.o
  CC       env/fs_posix.o
  CC       env/fs_remap.o
  CC       env/file_system_tracer.o
  CC       env/io_posix.o
  CC       env/mock_env.o
  CC       env/unique_id_gen.o
  CC       file/delete_scheduler.o
  CC       file/file_prefetch_buffer.o
  CC       file/file_util.o
  CC       file/filename.o
  CC       file/line_file_reader.o
  CC       file/random_access_file_reader.o
  CC       file/read_write_util.o
  CC       file/readahead_raf.o
  CC       file/sequence_file_reader.o
  CC       file/sst_file_manager_impl.o
  CC       file/writable_file_writer.o
  CC       logging/auto_roll_logger.o
  CC       logging/event_logger.o
  CC       logging/log_buffer.o
  CC       memory/arena.o
  CC       memory/concurrent_arena.o
  CC       memory/jemalloc_nodump_allocator.o
  CC       memory/memkind_kmem_allocator.o
  CC       memory/memory_allocator.o
  CC       memtable/alloc_tracker.o
  CC       memtable/hash_linklist_rep.o
  CC       memtable/hash_skiplist_rep.o
  CC       memtable/skiplistrep.o
  CC       memtable/vectorrep.o
  CC       memtable/write_buffer_manager.o
  CC       monitoring/histogram.o
  CC       monitoring/histogram_windowing.o
  CC       monitoring/in_memory_stats_history.o
  CC       monitoring/instrumented_mutex.o
  CC       monitoring/iostats_context.o
  CC       monitoring/perf_context.o
  CC       monitoring/perf_level.o
  CC       monitoring/persistent_stats_history.o
  CC       monitoring/statistics.o
  CC       monitoring/thread_status_impl.o
  CC       monitoring/thread_status_updater.o
  CC       monitoring/thread_status_updater_debug.o
  CC       monitoring/thread_status_util.o
  CC       monitoring/thread_status_util_debug.o
  CC       options/cf_options.o
  CC       options/configurable.o
  CC       options/customizable.o
  CC       options/db_options.o
  CC       options/options.o
  CC       options/options_helper.o
  CC       options/options_parser.o
  CC       port/port_posix.o
  CC       port/win/env_default.o
  CC       port/win/env_win.o
  CC       port/win/io_win.o
  CC       port/win/port_win.o
  CC       port/win/win_logger.o
  CC       port/win/win_thread.o
  CC       port/stack_trace.o
  CC       table/adaptive/adaptive_table_factory.o
  CC       table/block_based/binary_search_index_reader.o
  CC       table/block_based/block.o
  CC       table/block_based/block_based_filter_block.o
  CC       table/block_based/block_based_table_builder.o
  CC       table/block_based/block_based_table_factory.o
  CC       table/block_based/block_based_table_iterator.o
  CC       table/block_based/block_based_table_reader.o
  CC       table/block_based/block_builder.o
  CC       table/block_based/block_prefetcher.o
  CC       table/block_based/block_prefix_index.o
  CC       table/block_based/data_block_hash_index.o
  CC       table/block_based/data_block_footer.o
  CC       table/block_based/filter_block_reader_common.o
  CC       table/block_based/filter_policy.o
  CC       table/block_based/flush_block_policy.o
  CC       table/block_based/full_filter_block.o
  CC       table/block_based/hash_index_reader.o
  CC       table/block_based/index_builder.o
  CC       table/block_based/index_reader_common.o
  CC       table/block_based/parsed_full_filter_block.o
  CC       table/block_based/partitioned_filter_block.o
  CC       table/block_based/partitioned_index_iterator.o
  CC       table/block_based/partitioned_index_reader.o
  CC       table/block_based/reader_common.o
  CC       table/block_based/uncompression_dict_reader.o
  CC       table/block_fetcher.o
  CC       table/cuckoo/cuckoo_table_builder.o
  CC       table/cuckoo/cuckoo_table_factory.o
  CC       table/cuckoo/cuckoo_table_reader.o
  CC       table/format.o
  CC       table/get_context.o
  CC       table/iterator.o
  CC       table/merging_iterator.o
  CC       table/meta_blocks.o
  CC       table/persistent_cache_helper.o
  CC       table/plain/plain_table_bloom.o
  CC       table/plain/plain_table_builder.o
  CC       table/plain/plain_table_factory.o
  CC       table/plain/plain_table_index.o
  CC       table/plain/plain_table_key_coding.o
  CC       table/plain/plain_table_reader.o
  CC       table/sst_file_dumper.o
  CC       table/sst_file_reader.o
  CC       table/sst_file_writer.o
  CC       table/table_factory.o
  CC       table/table_properties.o
  CC       table/two_level_iterator.o
  CC       table/unique_id.o
  CC       test_util/sync_point.o
  CC       test_util/sync_point_impl.o
  CC       test_util/transaction_test_util.o
  CC       tools/dump/db_dump_tool.o
  CC       trace_replay/trace_record_handler.o
  CC       trace_replay/trace_record_result.o
  CC       trace_replay/trace_record.o
  CC       trace_replay/trace_replay.o
  CC       trace_replay/block_cache_tracer.o
  CC       trace_replay/io_tracer.o
  CC       util/coding.o
  CC       util/compaction_job_stats_impl.o
  CC       util/comparator.o
  CC       util/compression_context_cache.o
  CC       util/concurrent_task_limiter_impl.o
  CC       util/crc32c.o
  CC       util/crc32c_arm64.o
  CC       util/dynamic_bloom.o
  CC       util/hash.o
  CC       util/murmurhash.o
  CC       util/random.o
  CC       util/rate_limiter.o
  CC       util/ribbon_config.o
  CC       util/slice.o
  CC       util/file_checksum_helper.o
  CC       util/status.o
  CC       util/string_util.o
  CC       util/thread_local.o
  CC       util/threadpool_imp.o
  CC       util/xxhash.o
  CC       utilities/backupable/backupable_db.o
  CC       utilities/blob_db/blob_compaction_filter.o
  CC       utilities/blob_db/blob_db.o
  CC       utilities/blob_db/blob_db_impl.o
  CC       utilities/blob_db/blob_db_impl_filesnapshot.o
  CC       utilities/blob_db/blob_file.o
  CC       utilities/cache_dump_load.o
  CC       utilities/cache_dump_load_impl.o
  CC       utilities/cassandra/cassandra_compaction_filter.o
  CC       utilities/cassandra/format.o
  CC       utilities/cassandra/merge_operator.o
  CC       utilities/checkpoint/checkpoint_impl.o
  CC       utilities/compaction_filters.o
  CC       utilities/compaction_filters/remove_emptyvalue_compactionfilter.o
  CC       utilities/convenience/info_log_finder.o
  CC       utilities/counted_fs.o
  CC       utilities/debug.o
  CC       utilities/env_mirror.o
  CC       utilities/env_timed.o
  CC       utilities/fault_injection_env.o
  CC       utilities/fault_injection_fs.o
  CC       utilities/fault_injection_secondary_cache.o
  CC       utilities/leveldb_options/leveldb_options.o
  CC       utilities/memory/memory_util.o
  CC       utilities/merge_operators.o
  CC       utilities/merge_operators/max.o
  CC       utilities/merge_operators/put.o
  CC       utilities/merge_operators/sortlist.o
  CC       utilities/merge_operators/string_append/stringappend.o
  CC       utilities/merge_operators/string_append/stringappend2.o
  CC       utilities/merge_operators/uint64add.o
  CC       utilities/merge_operators/bytesxor.o
  CC       utilities/object_registry.o
  CC       utilities/option_change_migration/option_change_migration.o
  CC       utilities/options/options_util.o
  CC       utilities/persistent_cache/block_cache_tier.o
  CC       utilities/persistent_cache/block_cache_tier_file.o
  CC       utilities/persistent_cache/block_cache_tier_metadata.o
  CC       utilities/persistent_cache/persistent_cache_tier.o
  CC       utilities/persistent_cache/volatile_tier_impl.o
  CC       utilities/simulator_cache/cache_simulator.o
  CC       utilities/simulator_cache/sim_cache.o
  CC       utilities/table_properties_collectors/compact_on_deletion_collector.o
  CC       utilities/trace/file_trace_reader_writer.o
  CC       utilities/trace/replayer_impl.o
  CC       utilities/transactions/lock/lock_manager.o
  CC       utilities/transactions/lock/point/point_lock_tracker.o
  CC       utilities/transactions/lock/point/point_lock_manager.o
  CC       utilities/transactions/optimistic_transaction.o
  CC       utilities/transactions/optimistic_transaction_db_impl.o
  CC       utilities/transactions/pessimistic_transaction.o
  CC       utilities/transactions/pessimistic_transaction_db.o
  CC       utilities/transactions/snapshot_checker.o
  CC       utilities/transactions/transaction_base.o
  CC       utilities/transactions/transaction_db_mutex_impl.o
  CC       utilities/transactions/transaction_util.o
  CC       utilities/transactions/write_prepared_txn_db.o
  CC       utilities/transactions/write_prepared_txn.o
  CC       utilities/transactions/write_unprepared_txn.o
  CC       utilities/transactions/write_unprepared_txn_db.o
  CC       utilities/ttl/db_ttl_impl.o
  CC       utilities/wal_filter.o
  CC       utilities/write_batch_with_index/write_batch_with_index.o
  CC       utilities/write_batch_with_index/write_batch_with_index_internal.o
  CC       utilities/transactions/lock/range/range_tree/lib/locktree/concurrent_tree.o
  CC       utilities/transactions/lock/range/range_tree/lib/locktree/keyrange.o
  CC       utilities/transactions/lock/range/range_tree/lib/locktree/lock_request.o
  CC       utilities/transactions/lock/range/range_tree/lib/locktree/locktree.o
  CC       utilities/transactions/lock/range/range_tree/lib/locktree/manager.o
  CC       utilities/transactions/lock/range/range_tree/lib/locktree/range_buffer.o
  CC       utilities/transactions/lock/range/range_tree/lib/locktree/treenode.o
  CC       utilities/transactions/lock/range/range_tree/lib/locktree/txnid_set.o
  CC       utilities/transactions/lock/range/range_tree/lib/locktree/wfg.o
  CC       utilities/transactions/lock/range/range_tree/lib/standalone_port.o
  CC       utilities/transactions/lock/range/range_tree/lib/util/dbt.o
  CC       utilities/transactions/lock/range/range_tree/lib/util/memarena.o
  CC       utilities/transactions/lock/range/range_tree/range_tree_lock_manager.o
  CC       utilities/transactions/lock/range/range_tree/range_tree_lock_tracker.o
  CC       cache/cache_bench.o
  CC       cache/cache_bench_tool.o
  CC       db/range_del_aggregator_bench.o
  CC       memtable/memtablerep_bench.o
  CC       table/table_reader_bench.o
  CC       db/db_test_util.o
  CC       test_util/mock_time_env.o
  CC       test_util/testharness.o
  CC       test_util/testutil.o
  CC       utilities/cassandra/test_utils.o
  CC       table/mock_table.o
  CC       third-party/gtest-1.8.1/fused-src/gtest/gtest-all.o
  CC       tools/db_bench.o
  CC       tools/db_bench_tool.o
  CC       tools/simulated_hybrid_file_system.o
  CC       util/filter_bench.o
  CC       utilities/persistent_cache/persistent_cache_bench.o
  CC       db_stress_tool/db_stress.o
  CC       tools/block_cache_analyzer/block_cache_trace_analyzer.o
  CC       tools/trace_analyzer_tool.o
  CC       db_stress_tool/batched_ops_stress.o
  CC       db_stress_tool/cf_consistency_stress.o
  CC       db_stress_tool/db_stress_common.o
  CC       db_stress_tool/db_stress_driver.o
  CC       db_stress_tool/db_stress_gflags.o
  CC       db_stress_tool/db_stress_listener.o
  CC       db_stress_tool/db_stress_shared_state.o
  CC       db_stress_tool/db_stress_stat.o
  CC       db_stress_tool/db_stress_test_base.o
  CC       db_stress_tool/db_stress_tool.o
  CC       db_stress_tool/expected_state.o
  CC       db_stress_tool/no_batched_ops_stress.o
  CC       db_stress_tool/multi_ops_txns_stress.o
  CC       tools/io_tracer_parser_tool.o
  CC       tools/ldb_cmd.o
  CC       tools/ldb_tool.o
  CC       tools/sst_dump_tool.o
  CC       utilities/blob_db/blob_dump_tool.o
  CC       tools/blob_dump.o
  CC       tools/block_cache_analyzer/block_cache_trace_analyzer_tool.o
  CC       tools/db_repl_stress.o
  CC       tools/db_sanity_test.o
  CC       tools/ldb.o
  CC       tools/io_tracer_parser.o
  CC       tools/sst_dump.o
  CC       tools/write_stress.o
  CC       tools/dump/rocksdb_dump.o
  CC       tools/dump/rocksdb_undump.o
  CC       tools/trace_analyzer.o
  CC       env/env_basic_test.o
  CC       cache/cache_test.o
  CC       cache/cache_reservation_manager_test.o
  CC       cache/lru_cache_test.o
  CC       db/blob/blob_counting_iterator_test.o
  CC       db/blob/blob_file_addition_test.o
  CC       db/blob/blob_file_builder_test.o
  CC       db/blob/blob_file_cache_test.o
  CC       db/blob/blob_file_garbage_test.o
  CC       db/blob/blob_file_reader_test.o
  CC       db/blob/blob_garbage_meter_test.o
  CC       db/blob/db_blob_basic_test.o
  CC       db/blob/db_blob_compaction_test.o
  CC       db/blob/db_blob_corruption_test.o
  CC       db/blob/db_blob_index_test.o
  CC       db/column_family_test.o
  CC       db/compact_files_test.o
  CC       db/compaction/clipping_iterator_test.o
  CC       db/compaction/compaction_iterator_test.o
  CC       db/compaction/compaction_job_test.o
  CC       db/compaction/compaction_job_stats_test.o
  CC       db/compaction/compaction_picker_test.o
  CC       db/compaction/compaction_service_test.o
  CC       db/comparator_db_test.o
  CC       db/corruption_test.o
  CC       db/cuckoo_table_db_test.o
  CC       db/db_basic_test.o
  CC       db/db_with_timestamp_basic_test.o
  CC       db/db_block_cache_test.o
  CC       db/db_bloom_filter_test.o
  CC       db/db_compaction_filter_test.o
  CC       db/db_compaction_test.o
  CC       db/db_dynamic_level_test.o
  CC       db/db_encryption_test.o
  CC       db/db_flush_test.o
  CC       db/import_column_family_test.o
  CC       db/db_inplace_update_test.o
  CC       db/db_io_failure_test.o
  CC       db/db_iter_test.o
  CC       db/db_iter_stress_test.o
  CC       db/db_iterator_test.o
  CC       db/db_kv_checksum_test.o
  CC       db/db_log_iter_test.o
  CC       db/db_memtable_test.o
  CC       db/db_merge_operator_test.o
  CC       db/db_merge_operand_test.o
  CC       db/db_options_test.o
  CC       db/db_properties_test.o
  CC       db/db_range_del_test.o
  CC       db/db_secondary_test.o
  CC       db/db_sst_test.o
  CC       db/db_statistics_test.o
  CC       db/db_table_properties_test.o
  CC       db/db_tailing_iter_test.o
  CC       db/db_test.o
  CC       db/db_test2.o
  CC       db/db_logical_block_size_cache_test.o
  CC       db/db_universal_compaction_test.o
  CC       db/db_wal_test.o
  CC       db/db_with_timestamp_compaction_test.o
  CC       db/db_write_buffer_manager_test.o
  CC       db/db_write_test.o
  CC       db/dbformat_test.o
  CC       db/deletefile_test.o
  CC       db/error_handler_fs_test.o
  CC       db/external_sst_file_basic_test.o
  CC       db/external_sst_file_test.o
  CC       db/fault_injection_test.o
  CC       db/file_indexer_test.o
  CC       db/filename_test.o
  CC       db/flush_job_test.o
  CC       db/listener_test.o
  CC       db/log_test.o
  CC       db/manual_compaction_test.o
  CC       db/memtable_list_test.o
  CC       db/merge_helper_test.o
  CC       db/merge_test.o
  CC       db/obsolete_files_test.o
  CC       db/options_file_test.o
  CC       db/perf_context_test.o
  CC       db/periodic_work_scheduler_test.o
  CC       db/plain_table_db_test.o
  CC       db/prefix_test.o
  CC       db/repair_test.o
  CC       db/range_del_aggregator_test.o
  CC       db/range_tombstone_fragmenter_test.o
  CC       db/table_properties_collector_test.o
  CC       db/version_builder_test.o
  CC       db/version_edit_test.o
  CC       db/version_set_test.o
  CC       db/wal_manager_test.o
  CC       db/write_batch_test.o
  CC       db/write_callback_test.o
  CC       db/write_controller_test.o
  CC       env/io_posix_test.o
  CC       env/mock_env_test.o
  CC       file/delete_scheduler_test.o
  CC       file/prefetch_test.o
  CC       file/random_access_file_reader_test.o
  CC       logging/auto_roll_logger_test.o
  CC       logging/env_logger_test.o
  CC       logging/event_logger_test.o
  CC       memory/arena_test.o
  CC       memory/memory_allocator_test.o
  CC       memtable/inlineskiplist_test.o
  CC       memtable/skiplist_test.o
  CC       memtable/write_buffer_manager_test.o
  CC       monitoring/histogram_test.o
  CC       monitoring/iostats_context_test.o
  CC       monitoring/statistics_test.o
  CC       monitoring/stats_history_test.o
  CC       options/configurable_test.o
  CC       options/customizable_test.o
  CC       options/options_settable_test.o
  CC       options/options_test.o
  CC       table/block_based/block_based_filter_block_test.o
  CC       table/block_based/block_based_table_reader_test.o
  CC       table/block_based/block_test.o
  CC       table/block_based/data_block_hash_index_test.o
  CC       table/block_based/full_filter_block_test.o
  CC       table/block_based/partitioned_filter_block_test.o
  CC       table/cleanable_test.o
  CC       table/cuckoo/cuckoo_table_builder_test.o
  CC       table/cuckoo/cuckoo_table_reader_test.o
  CC       table/merger_test.o
  CC       table/sst_file_reader_test.o
  CC       table/table_test.o
  CC       table/block_fetcher_test.o
  CC       tools/block_cache_analyzer/block_cache_trace_analyzer_test.o
  CC       tools/io_tracer_parser_test.o
  CC       tools/ldb_cmd_test.o
  CC       tools/reduce_levels_test.o
  CC       tools/sst_dump_test.o
  CC       tools/trace_analyzer_test.o
  CC       trace_replay/block_cache_tracer_test.o
  CC       trace_replay/io_tracer_test.o
  CC       util/autovector_test.o
  CC       util/bloom_test.o
  CC       util/coding_test.o
  CC       util/crc32c_test.o
  CC       util/defer_test.o
  CC       util/dynamic_bloom_test.o
  CC       util/filelock_test.o
  CC       util/file_reader_writer_test.o
  CC       util/hash_test.o
  CC       util/heap_test.o
  CC       util/random_test.o
  CC       util/rate_limiter_test.o
  CC       util/repeatable_thread_test.o
  CC       util/ribbon_test.o
  CC       util/slice_test.o
  CC       util/slice_transform_test.o
  CC       util/timer_queue_test.o
  CC       util/timer_test.o
  CC       util/thread_list_test.o
  CC       util/thread_local_test.o
  CC       util/work_queue_test.o
  CC       utilities/backupable/backupable_db_test.o
  CC       utilities/blob_db/blob_db_test.o
  CC       utilities/cassandra/cassandra_format_test.o
  CC       utilities/cassandra/cassandra_functional_test.o
  CC       utilities/cassandra/cassandra_row_merge_test.o
  CC       utilities/cassandra/cassandra_serialize_test.o
  CC       utilities/checkpoint/checkpoint_test.o
  CC       utilities/env_timed_test.o
  CC       utilities/memory/memory_test.o
  CC       utilities/merge_operators/string_append/stringappend_test.o
  CC       utilities/object_registry_test.o
  CC       utilities/option_change_migration/option_change_migration_test.o
  CC       utilities/options/options_util_test.o
  CC       utilities/persistent_cache/hash_table_test.o
  CC       utilities/persistent_cache/persistent_cache_test.o
  CC       utilities/simulator_cache/cache_simulator_test.o
  CC       utilities/simulator_cache/sim_cache_test.o
  CC       utilities/table_properties_collectors/compact_on_deletion_collector_test.o
  CC       utilities/transactions/optimistic_transaction_test.o
  CC       utilities/transactions/transaction_test.o
  CC       utilities/transactions/lock/point/point_lock_manager_test.o
  CC       utilities/transactions/write_prepared_transaction_test.o
  CC       utilities/transactions/write_unprepared_transaction_test.o
  CC       utilities/ttl/ttl_test.o
  CC       utilities/util_merge_operators_test.o
  CC       utilities/write_batch_with_index/write_batch_with_index_test.o
  GEN      util/build_version.cc
  AR       librocksdb_test_debug.a
/bin/ar: creating librocksdb_test_debug.a
  AR       librocksdb_stress_debug.a
/bin/ar: creating librocksdb_stress_debug.a
  AR       librocksdb_tools_debug.a
/bin/ar: creating librocksdb_tools_debug.a
  CCLD     heap_test
  CC       util/build_version.o
  AR       librocksdb_debug.a
  AR       librocksdb_env_basic_test.a
/bin/ar: creating librocksdb_debug.a
/bin/ar: creating librocksdb_env_basic_test.a
  CCLD     cache_bench
  CCLD     range_del_aggregator_bench
  CCLD     memtablerep_bench
  CCLD     db_bench
  CCLD     table_reader_bench
  CCLD     persistent_cache_bench
  CCLD     filter_bench
  CCLD     db_stress
  CCLD     blob_dump
  CCLD     block_cache_trace_analyzer
  CCLD     db_repl_stress
  CCLD     db_sanity_test
  CCLD     ldb
  CCLD     io_tracer_parser
  CCLD     sst_dump
  CCLD     write_stress
  CCLD     rocksdb_dump
  CCLD     rocksdb_undump
  CCLD     trace_analyzer
  CCLD     cache_test
  CCLD     cache_reservation_manager_test
  CCLD     lru_cache_test
  CCLD     blob_counting_iterator_test
  CCLD     blob_file_addition_test
  CCLD     blob_file_builder_test
  CCLD     blob_file_cache_test
  CCLD     blob_file_garbage_test
  CCLD     blob_file_reader_test
  CCLD     blob_garbage_meter_test
  CCLD     db_blob_basic_test
  CCLD     db_blob_compaction_test
  CCLD     db_blob_corruption_test
  CCLD     db_blob_index_test
  CCLD     column_family_test
  CCLD     compact_files_test
  CCLD     clipping_iterator_test
  CCLD     compaction_iterator_test
  CCLD     compaction_job_test
  CCLD     compaction_job_stats_test
  CCLD     compaction_picker_test
  CCLD     compaction_service_test
  CCLD     comparator_db_test
  CCLD     corruption_test
  CCLD     cuckoo_table_db_test
  CCLD     db_basic_test
  CCLD     db_with_timestamp_basic_test
  CCLD     db_block_cache_test
  CCLD     db_bloom_filter_test
  CCLD     db_compaction_test
  CCLD     db_compaction_filter_test
  CCLD     db_dynamic_level_test
  CCLD     db_encryption_test
  CCLD     db_flush_test
  CCLD     import_column_family_test
  CCLD     db_inplace_update_test
  CCLD     db_io_failure_test
  CCLD     db_iter_test
  CCLD     db_iter_stress_test
  CCLD     db_iterator_test
  CCLD     db_kv_checksum_test
  CCLD     db_log_iter_test
  CCLD     db_memtable_test
  CCLD     db_merge_operator_test
  CCLD     db_merge_operand_test
  CCLD     db_options_test
  CCLD     db_properties_test
  CCLD     db_range_del_test
  CCLD     db_secondary_test
  CCLD     db_sst_test
  CCLD     db_statistics_test
  CCLD     db_table_properties_test
  CCLD     db_tailing_iter_test
  CCLD     db_test
  CCLD     db_test2
  CCLD     db_logical_block_size_cache_test
  CCLD     db_universal_compaction_test
  CCLD     db_wal_test
  CCLD     db_with_timestamp_compaction_test
  CCLD     db_write_buffer_manager_test
  CCLD     db_write_test
  CCLD     dbformat_test
  CCLD     deletefile_test
  CCLD     error_handler_fs_test
  CCLD     external_sst_file_basic_test
  CCLD     external_sst_file_test
  CCLD     fault_injection_test
  CCLD     file_indexer_test
  CCLD     filename_test
  CCLD     flush_job_test
  CCLD     listener_test
  CCLD     log_test
  CCLD     manual_compaction_test
  CCLD     memtable_list_test
  CCLD     merge_helper_test
  CCLD     merge_test
  CCLD     obsolete_files_test
  CCLD     options_file_test
  CCLD     perf_context_test
  CCLD     periodic_work_scheduler_test
  CCLD     plain_table_db_test
  CCLD     prefix_test
  CCLD     repair_test
  CCLD     range_del_aggregator_test
  CCLD     range_tombstone_fragmenter_test
  CCLD     table_properties_collector_test
  CCLD     version_builder_test
  CCLD     version_edit_test
  CCLD     version_set_test
  CCLD     wal_manager_test
  CCLD     write_batch_test
  CCLD     write_callback_test
  CCLD     write_controller_test
  CCLD     env_basic_test
  CCLD     io_posix_test
  CCLD     mock_env_test
  CCLD     delete_scheduler_test
  CCLD     prefetch_test
  CCLD     random_access_file_reader_test
  CCLD     auto_roll_logger_test
  CCLD     env_logger_test
  CCLD     event_logger_test
  CCLD     arena_test
  CCLD     memory_allocator_test
  CCLD     inlineskiplist_test
  CCLD     skiplist_test
  CCLD     write_buffer_manager_test
  CCLD     histogram_test
  CCLD     iostats_context_test
  CCLD     statistics_test
  CCLD     stats_history_test
  CCLD     configurable_test
  CCLD     customizable_test
  CCLD     options_settable_test
  CCLD     options_test
  CCLD     block_based_filter_block_test
  CCLD     block_based_table_reader_test
  CCLD     block_test
  CCLD     data_block_hash_index_test
  CCLD     full_filter_block_test
  CCLD     partitioned_filter_block_test
  CCLD     cleanable_test
  CCLD     cuckoo_table_builder_test
  CCLD     cuckoo_table_reader_test
  CCLD     merger_test
  CCLD     sst_file_reader_test
  CCLD     table_test
  CCLD     block_fetcher_test
  CCLD     block_cache_trace_analyzer_test
  CCLD     io_tracer_parser_test
  CCLD     ldb_cmd_test
  CCLD     reduce_levels_test
  CCLD     sst_dump_test
  CCLD     trace_analyzer_test
  CCLD     block_cache_tracer_test
  CCLD     io_tracer_test
  CCLD     autovector_test
  CCLD     bloom_test
  CCLD     coding_test
  CCLD     crc32c_test
  CCLD     defer_test
  CCLD     dynamic_bloom_test
  CCLD     filelock_test
  CCLD     file_reader_writer_test
  CCLD     hash_test
  CCLD     random_test
  CCLD     rate_limiter_test
  CCLD     repeatable_thread_test
  CCLD     ribbon_test
  CCLD     slice_test
  CCLD     slice_transform_test
  CCLD     timer_queue_test
  CCLD     timer_test
  CCLD     thread_list_test
  CCLD     thread_local_test
  CCLD     work_queue_test
  CCLD     backupable_db_test
  CCLD     blob_db_test
  CCLD     cassandra_format_test
  CCLD     cassandra_functional_test
  CCLD     cassandra_row_merge_test
  CCLD     cassandra_serialize_test
  CCLD     checkpoint_test
  CCLD     env_timed_test
  CCLD     memory_test
  CCLD     stringappend_test
  CCLD     object_registry_test
  CCLD     option_change_migration_test
  CCLD     options_util_test
  CCLD     hash_table_test
  CCLD     persistent_cache_test
  CCLD     cache_simulator_test
  CCLD     sim_cache_test
  CCLD     compact_on_deletion_collector_test
  CCLD     optimistic_transaction_test
  CCLD     transaction_test
  CCLD     point_lock_manager_test
  CCLD     write_prepared_transaction_test
  CCLD     write_unprepared_transaction_test
  CCLD     ttl_test
  CCLD     util_merge_operators_test
  CCLD     write_batch_with_index_test
make gen_parallel_tests
make[1]: Entering directory '/data/users/sidroyc/rocksdb'
$DEBUG_LEVEL is 1
Makefile:170: Warning: Compiling in debug mode. Don't use the resulting binary in production
make parallel_cache_test parallel_cache_reservation_manager_test parallel_lru_cache_test parallel_blob_counting_iterator_test parallel_blob_file_addition_test parallel_blob_file_builder_test parallel_blob_file_cache_test parallel_blob_file_garbage_test parallel_blob_file_reader_test parallel_blob_garbage_meter_test parallel_db_blob_basic_test parallel_db_blob_compaction_test parallel_db_blob_corruption_test parallel_db_blob_index_test parallel_column_family_test parallel_compact_files_test parallel_clipping_iterator_test parallel_compaction_iterator_test parallel_compaction_job_test parallel_compaction_job_stats_test parallel_compaction_picker_test parallel_compaction_service_test parallel_comparator_db_test parallel_corruption_test parallel_cuckoo_table_db_test parallel_db_basic_test parallel_db_with_timestamp_basic_test parallel_db_block_cache_test parallel_db_bloom_filter_test parallel_db_compaction_filter_test parallel_db_compaction_test parallel_db_dynamic_level_test parallel_db_encryption_test parallel_db_flush_test parallel_import_column_family_test parallel_db_inplace_update_test parallel_db_io_failure_test parallel_db_iter_test parallel_db_iter_stress_test parallel_db_iterator_test parallel_db_kv_checksum_test parallel_db_log_iter_test parallel_db_memtable_test parallel_db_merge_operator_test parallel_db_merge_operand_test parallel_db_options_test parallel_db_properties_test parallel_db_range_del_test parallel_db_secondary_test parallel_db_sst_test parallel_db_statistics_test parallel_db_table_properties_test parallel_db_tailing_iter_test parallel_db_test parallel_db_test2 parallel_db_logical_block_size_cache_test parallel_db_universal_compaction_test parallel_db_wal_test parallel_db_with_timestamp_compaction_test parallel_db_write_buffer_manager_test parallel_db_write_test parallel_dbformat_test parallel_error_handler_fs_test parallel_external_sst_file_basic_test parallel_external_sst_file_test parallel_fault_injection_test parallel_file_indexer_test parallel_filename_test parallel_flush_job_test parallel_listener_test parallel_log_test parallel_manual_compaction_test parallel_memtable_list_test parallel_merge_helper_test parallel_merge_test parallel_obsolete_files_test parallel_options_file_test parallel_perf_context_test parallel_periodic_work_scheduler_test parallel_plain_table_db_test parallel_prefix_test parallel_repair_test parallel_range_del_aggregator_test parallel_range_tombstone_fragmenter_test parallel_table_properties_collector_test parallel_version_builder_test parallel_version_edit_test parallel_version_set_test parallel_wal_manager_test parallel_write_batch_test parallel_write_callback_test parallel_write_controller_test parallel_env_basic_test parallel_io_posix_test parallel_mock_env_test parallel_delete_scheduler_test parallel_prefetch_test parallel_random_access_file_reader_test parallel_auto_roll_logger_test parallel_env_logger_test parallel_event_logger_test parallel_arena_test parallel_memory_allocator_test parallel_inlineskiplist_test parallel_skiplist_test parallel_write_buffer_manager_test parallel_histogram_test parallel_iostats_context_test parallel_statistics_test parallel_stats_history_test parallel_configurable_test parallel_customizable_test parallel_options_settable_test parallel_options_test parallel_block_based_filter_block_test parallel_block_based_table_reader_test parallel_block_test parallel_data_block_hash_index_test parallel_full_filter_block_test parallel_partitioned_filter_block_test parallel_cleanable_test parallel_cuckoo_table_builder_test parallel_cuckoo_table_reader_test parallel_merger_test parallel_sst_file_reader_test parallel_table_test parallel_block_fetcher_test parallel_block_cache_trace_analyzer_test parallel_io_tracer_parser_test parallel_ldb_cmd_test parallel_reduce_levels_test parallel_sst_dump_test parallel_trace_analyzer_test parallel_block_cache_tracer_test parallel_io_tracer_test parallel_autovector_test parallel_bloom_test parallel_coding_test parallel_crc32c_test parallel_defer_test parallel_dynamic_bloom_test parallel_filelock_test parallel_file_reader_writer_test parallel_hash_test parallel_heap_test parallel_random_test parallel_rate_limiter_test parallel_repeatable_thread_test parallel_ribbon_test parallel_slice_test parallel_slice_transform_test parallel_timer_queue_test parallel_timer_test parallel_thread_list_test parallel_thread_local_test parallel_work_queue_test parallel_backupable_db_test parallel_blob_db_test parallel_cassandra_format_test parallel_cassandra_functional_test parallel_cassandra_row_merge_test parallel_cassandra_serialize_test parallel_checkpoint_test parallel_env_timed_test parallel_memory_test parallel_stringappend_test parallel_object_registry_test parallel_option_change_migration_test parallel_options_util_test parallel_hash_table_test parallel_persistent_cache_test parallel_cache_simulator_test parallel_sim_cache_test parallel_compact_on_deletion_collector_test parallel_optimistic_transaction_test parallel_transaction_test parallel_point_lock_manager_test parallel_write_prepared_transaction_test parallel_write_unprepared_transaction_test parallel_ttl_test parallel_util_merge_operators_test parallel_write_batch_with_index_test
make[2]: Entering directory '/data/users/sidroyc/rocksdb'
$DEBUG_LEVEL is 1
Makefile:170: Warning: Compiling in debug mode. Don't use the resulting binary in production
  Generating parallel test scripts for db_blob_corruption_test
  Generating parallel test scripts for db_blob_compaction_test
  Generating parallel test scripts for cache_reservation_manager_test
  Generating parallel test scripts for blob_counting_iterator_test
  Generating parallel test scripts for lru_cache_test
  Generating parallel test scripts for cache_test
  Generating parallel test scripts for compaction_iterator_test
  Generating parallel test scripts for blob_file_builder_test
  Generating parallel test scripts for blob_garbage_meter_test
  Generating parallel test scripts for db_blob_basic_test
  Generating parallel test scripts for compact_files_test
  Generating parallel test scripts for blob_file_cache_test
  Generating parallel test scripts for blob_file_addition_test
  Generating parallel test scripts for blob_file_reader_test
  Generating parallel test scripts for blob_file_garbage_test
  Generating parallel test scripts for db_blob_index_test
  Generating parallel test scripts for clipping_iterator_test
  Generating parallel test scripts for column_family_test
  Generating parallel test scripts for compaction_job_test
  Generating parallel test scripts for corruption_test
  Generating parallel test scripts for compaction_picker_test
  Generating parallel test scripts for comparator_db_test
  Generating parallel test scripts for cuckoo_table_db_test
  Generating parallel test scripts for compaction_service_test
  Generating parallel test scripts for compaction_job_stats_test
  Generating parallel test scripts for db_basic_test
  Generating parallel test scripts for db_compaction_test
  Generating parallel test scripts for db_block_cache_test
  Generating parallel test scripts for db_with_timestamp_basic_test
  Generating parallel test scripts for db_bloom_filter_test
  Generating parallel test scripts for db_dynamic_level_test
  Generating parallel test scripts for db_compaction_filter_test
  Generating parallel test scripts for db_encryption_test
  Generating parallel test scripts for db_flush_test
  Generating parallel test scripts for import_column_family_test
  Generating parallel test scripts for db_inplace_update_test
  Generating parallel test scripts for db_iter_stress_test
  Generating parallel test scripts for db_iterator_test
  Generating parallel test scripts for db_iter_test
  Generating parallel test scripts for db_io_failure_test
  Generating parallel test scripts for db_log_iter_test
  Generating parallel test scripts for db_kv_checksum_test
  Generating parallel test scripts for db_merge_operator_test
  Generating parallel test scripts for db_options_test
  Generating parallel test scripts for db_merge_operand_test
  Generating parallel test scripts for db_secondary_test
  Generating parallel test scripts for db_memtable_test
  Generating parallel test scripts for db_range_del_test
  Generating parallel test scripts for db_properties_test
  Generating parallel test scripts for db_sst_test
  Generating parallel test scripts for db_table_properties_test
  Generating parallel test scripts for db_statistics_test
  Generating parallel test scripts for db_test
  Generating parallel test scripts for db_tailing_iter_test
  Generating parallel test scripts for db_test2
  Generating parallel test scripts for db_logical_block_size_cache_test
  Generating parallel test scripts for db_universal_compaction_test
  Generating parallel test scripts for db_write_test
  Generating parallel test scripts for dbformat_test
  Generating parallel test scripts for error_handler_fs_test
  Generating parallel test scripts for db_with_timestamp_compaction_test
  Generating parallel test scripts for db_wal_test
  Generating parallel test scripts for db_write_buffer_manager_test
  Generating parallel test scripts for external_sst_file_basic_test
  Generating parallel test scripts for external_sst_file_test
  Generating parallel test scripts for fault_injection_test
  Generating parallel test scripts for filename_test
  Generating parallel test scripts for flush_job_test
  Generating parallel test scripts for log_test
  Generating parallel test scripts for file_indexer_test
  Generating parallel test scripts for manual_compaction_test
  Generating parallel test scripts for memtable_list_test
  Generating parallel test scripts for listener_test
  Generating parallel test scripts for merge_helper_test
  Generating parallel test scripts for options_file_test
  Generating parallel test scripts for perf_context_test
  Generating parallel test scripts for obsolete_files_test
  Generating parallel test scripts for periodic_work_scheduler_test
  Generating parallel test scripts for merge_test
  Generating parallel test scripts for plain_table_db_test
  Generating parallel test scripts for prefix_test
  Generating parallel test scripts for repair_test
  Generating parallel test scripts for write_callback_test
  Generating parallel test scripts for range_tombstone_fragmenter_test
  Generating parallel test scripts for wal_manager_test
  Generating parallel test scripts for table_properties_collector_test
  Generating parallel test scripts for version_edit_test
  Generating parallel test scripts for version_builder_test
  Generating parallel test scripts for write_batch_test
  Generating parallel test scripts for version_set_test
  Generating parallel test scripts for range_del_aggregator_test
  Generating parallel test scripts for write_controller_test
  Generating parallel test scripts for env_basic_test
  Generating parallel test scripts for io_posix_test
  Generating parallel test scripts for delete_scheduler_test
  Generating parallel test scripts for mock_env_test
  Generating parallel test scripts for random_access_file_reader_test
  Generating parallel test scripts for prefetch_test
  Generating parallel test scripts for auto_roll_logger_test
  Generating parallel test scripts for memory_allocator_test
  Generating parallel test scripts for event_logger_test
  Generating parallel test scripts for arena_test
  Generating parallel test scripts for env_logger_test
  Generating parallel test scripts for skiplist_test
  Generating parallel test scripts for write_buffer_manager_test
  Generating parallel test scripts for inlineskiplist_test
  Generating parallel test scripts for histogram_test
  Generating parallel test scripts for statistics_test
  Generating parallel test scripts for iostats_context_test
  Generating parallel test scripts for configurable_test
  Generating parallel test scripts for customizable_test
  Generating parallel test scripts for stats_history_test
  Generating parallel test scripts for block_based_table_reader_test
  Generating parallel test scripts for options_settable_test
  Generating parallel test scripts for block_test
  Generating parallel test scripts for block_based_filter_block_test
  Generating parallel test scripts for options_test
  Generating parallel test scripts for full_filter_block_test
  Generating parallel test scripts for data_block_hash_index_test
  Generating parallel test scripts for partitioned_filter_block_test
  Generating parallel test scripts for cleanable_test
  Generating parallel test scripts for cuckoo_table_builder_test
  Generating parallel test scripts for cuckoo_table_reader_test
  Generating parallel test scripts for merger_test
  Generating parallel test scripts for block_fetcher_test
  Generating parallel test scripts for sst_file_reader_test
  Generating parallel test scripts for block_cache_trace_analyzer_test
  Generating parallel test scripts for io_tracer_parser_test
  Generating parallel test scripts for ldb_cmd_test
  Generating parallel test scripts for sst_dump_test
  Generating parallel test scripts for reduce_levels_test
  Generating parallel test scripts for block_cache_tracer_test
  Generating parallel test scripts for table_test
  Generating parallel test scripts for trace_analyzer_test
  Generating parallel test scripts for autovector_test
  Generating parallel test scripts for io_tracer_test
  Generating parallel test scripts for bloom_test
  Generating parallel test scripts for coding_test
  Generating parallel test scripts for defer_test
  Generating parallel test scripts for dynamic_bloom_test
  Generating parallel test scripts for filelock_test
  Generating parallel test scripts for crc32c_test
  Generating parallel test scripts for rate_limiter_test
NPHash64 id: 251012e7
  Generating parallel test scripts for repeatable_thread_test
  Generating parallel test scripts for random_test
  Generating parallel test scripts for hash_test
  Generating parallel test scripts for file_reader_writer_test
  Generating parallel test scripts for slice_test
  Generating parallel test scripts for heap_test
  Generating parallel test scripts for ribbon_test
  Generating parallel test scripts for timer_test
  Generating parallel test scripts for slice_transform_test
  Generating parallel test scripts for thread_list_test
  Generating parallel test scripts for thread_local_test
  Generating parallel test scripts for timer_queue_test
  Generating parallel test scripts for cassandra_format_test
  Generating parallel test scripts for backupable_db_test
  Generating parallel test scripts for work_queue_test
  Generating parallel test scripts for blob_db_test
  Generating parallel test scripts for cassandra_functional_test
  Generating parallel test scripts for cassandra_serialize_test
  Generating parallel test scripts for cassandra_row_merge_test
  Generating parallel test scripts for env_timed_test
  Generating parallel test scripts for checkpoint_test
  Generating parallel test scripts for object_registry_test
  Generating parallel test scripts for memory_test
  Generating parallel test scripts for stringappend_test
  Generating parallel test scripts for option_change_migration_test
  Generating parallel test scripts for persistent_cache_test
  Generating parallel test scripts for hash_table_test
  Generating parallel test scripts for cache_simulator_test
  Generating parallel test scripts for options_util_test
  Generating parallel test scripts for sim_cache_test
  Generating parallel test scripts for compact_on_deletion_collector_test
  Generating parallel test scripts for transaction_test
  Generating parallel test scripts for point_lock_manager_test
  Generating parallel test scripts for optimistic_transaction_test
  Generating parallel test scripts for write_prepared_transaction_test
  Generating parallel test scripts for ttl_test
  Generating parallel test scripts for write_unprepared_transaction_test
  Generating parallel test scripts for util_merge_operators_test
  Generating parallel test scripts for write_batch_with_index_test
make[2]: Leaving directory '/data/users/sidroyc/rocksdb'
make[1]: Leaving directory '/data/users/sidroyc/rocksdb'
  GEN      check
make[1]: Entering directory '/data/users/sidroyc/rocksdb'
$DEBUG_LEVEL is 1
Makefile:170: Warning: Compiling in debug mode. Don't use the resulting binary in production
  GEN      check_0

To monitor subtest <duration,pass/fail,name>,
  run "make watch-log" in a separate window


Computers / CPU cores / Max jobs to run
1:local / 24 / 24

Computer:jobs running/jobs completed/%of started jobs/Average seconds to complete
ETA: 0s Left: 11232 AVG: 0.00s  local:24/0/100%/0.0s 
ETA: 0s Left: 11188 AVG: 0.00s  local:24/44/100%/0.7s 
ETA: 336s Left: 10312 AVG: 0.03s  local:24/920/100%/0.1s 
ETA: 326s Left: 9719 AVG: 0.04s  local:24/1513/100%/0.1s 
ETA: 313s Left: 8993 AVG: 0.04s  local:24/2239/100%/0.1s 
ETA: 324s Left: 8642 AVG: 0.05s  local:24/2590/100%/0.1s 
ETA: 358s Left: 8549 AVG: 0.06s  local:24/2683/100%/0.1s 
ETA: 383s Left: 8221 AVG: 0.06s  local:24/3011/100%/0.1s 
ETA: 334s Left: 7004 AVG: 0.05s  local:24/4228/100%/0.1s 
ETA: 285s Left: 6052 AVG: 0.05s  local:24/5180/100%/0.1s 
ETA: 268s Left: 5637 AVG: 0.05s  local:24/5595/100%/0.1s 
ETA: 198s Left: 4388 AVG: 0.04s  local:24/6844/100%/0.0s 
ETA: 51s Left: 1449 AVG: 0.03s  local:24/9783/100%/0.0s 
ETA: 21s Left: 617 AVG: 0.03s  local:24/10615/100%/0.0s 
ETA: 0s Left: 18 AVG: 0.04s  local:18/11214/100%/0.0s 
ETA: 0s Left: 6 AVG: 0.04s  local:6/11226/100%/0.0s 
ETA: 0s Left: 4 AVG: 0.04s  local:4/11228/100%/0.0s 
ETA: 0s Left: 2 AVG: 0.04s  local:2/11230/100%/0.0s 
ETA: 0s Left: 1 AVG: 0.05s  local:1/11231/100%/0.0s 
ETA: 0s Left: 0 AVG: 0.05s  local:0/11232/100%/0.1s 
make[1]: Leaving directory '/data/users/sidroyc/rocksdb'
rm -rf /dev/shm/rocksdb.T5aD
/usr/local/bin/python3 tools/check_all_python.py
No syntax errors in 34 .py files
make check-format
make[1]: Entering directory '/data/users/sidroyc/rocksdb'
$DEBUG_LEVEL is 1
Makefile:170: Warning: Compiling in debug mode. Don't use the resulting binary in production
build_tools/format-diff.sh -c
Checking format of uncommitted changes...
Nothing needs to be reformatted!
make[1]: Leaving directory '/data/users/sidroyc/rocksdb'
make check-buck-targets
make[1]: Entering directory '/data/users/sidroyc/rocksdb'
$DEBUG_LEVEL is 1
Makefile:170: Warning: Compiling in debug mode. Don't use the resulting binary in production
buckifier/check_buck_targets.sh
Backup original TARGETS file.
[94mGenerating TARGETS[0m
Extra dependencies:
{"": {"extra_deps": [], "extra_compiler_flags": []}}
{'LIB_SOURCES': ['cache/cache.cc', 'cache/cache_entry_roles.cc', 'cache/cache_key.cc', 'cache/cache_reservation_manager.cc', 'cache/clock_cache.cc', 'cache/lru_cache.cc', 'cache/sharded_cache.cc', 'db/arena_wrapped_db_iter.cc', 'db/blob/blob_fetcher.cc', 'db/blob/blob_file_addition.cc', 'db/blob/blob_file_builder.cc', 'db/blob/blob_file_cache.cc', 'db/blob/blob_file_garbage.cc', 'db/blob/blob_file_meta.cc', 'db/blob/blob_file_reader.cc', 'db/blob/blob_garbage_meter.cc', 'db/blob/blob_log_format.cc', 'db/blob/blob_log_sequential_reader.cc', 'db/blob/blob_log_writer.cc', 'db/blob/prefetch_buffer_collection.cc', 'db/builder.cc', 'db/c.cc', 'db/column_family.cc', 'db/compaction/compaction.cc', 'db/compaction/compaction_iterator.cc', 'db/compaction/compaction_job.cc', 'db/compaction/compaction_picker.cc', 'db/compaction/compaction_picker_fifo.cc', 'db/compaction/compaction_picker_level.cc', 'db/compaction/compaction_picker_universal.cc', 'db/compaction/sst_partitioner.cc', 'db/convenience.cc', 'db/db_filesnapshot.cc', 'db/db_impl/compacted_db_impl.cc', 'db/db_impl/db_impl.cc', 'db/db_impl/db_impl_compaction_flush.cc', 'db/db_impl/db_impl_debug.cc', 'db/db_impl/db_impl_experimental.cc', 'db/db_impl/db_impl_files.cc', 'db/db_impl/db_impl_open.cc', 'db/db_impl/db_impl_readonly.cc', 'db/db_impl/db_impl_secondary.cc', 'db/db_impl/db_impl_write.cc', 'db/db_info_dumper.cc', 'db/db_iter.cc', 'db/dbformat.cc', 'db/error_handler.cc', 'db/event_helpers.cc', 'db/experimental.cc', 'db/external_sst_file_ingestion_job.cc', 'db/file_indexer.cc', 'db/flush_job.cc', 'db/flush_scheduler.cc', 'db/forward_iterator.cc', 'db/import_column_family_job.cc', 'db/internal_stats.cc', 'db/logs_with_prep_tracker.cc', 'db/log_reader.cc', 'db/log_writer.cc', 'db/malloc_stats.cc', 'db/memtable.cc', 'db/memtable_list.cc', 'db/merge_helper.cc', 'db/merge_operator.cc', 'db/output_validator.cc', 'db/periodic_work_scheduler.cc', 'db/range_del_aggregator.cc', 'db/range_tombstone_fragmenter.cc', 'db/repair.cc', 'db/snapshot_impl.cc', 'db/table_cache.cc', 'db/table_properties_collector.cc', 'db/transaction_log_impl.cc', 'db/trim_history_scheduler.cc', 'db/version_builder.cc', 'db/version_edit.cc', 'db/version_edit_handler.cc', 'db/version_set.cc', 'db/wal_edit.cc', 'db/wal_manager.cc', 'db/write_batch.cc', 'db/write_batch_base.cc', 'db/write_controller.cc', 'db/write_thread.cc', 'env/composite_env.cc', 'env/env.cc', 'env/env_chroot.cc', 'env/env_encryption.cc', 'env/env_posix.cc', 'env/file_system.cc', 'env/fs_posix.cc', 'env/fs_remap.cc', 'env/file_system_tracer.cc', 'env/io_posix.cc', 'env/mock_env.cc', 'env/unique_id_gen.cc', 'file/delete_scheduler.cc', 'file/file_prefetch_buffer.cc', 'file/file_util.cc', 'file/filename.cc', 'file/line_file_reader.cc', 'file/random_access_file_reader.cc', 'file/read_write_util.cc', 'file/readahead_raf.cc', 'file/sequence_file_reader.cc', 'file/sst_file_manager_impl.cc', 'file/writable_file_writer.cc', 'logging/auto_roll_logger.cc', 'logging/event_logger.cc', 'logging/log_buffer.cc', 'memory/arena.cc', 'memory/concurrent_arena.cc', 'memory/jemalloc_nodump_allocator.cc', 'memory/memkind_kmem_allocator.cc', 'memory/memory_allocator.cc', 'memtable/alloc_tracker.cc', 'memtable/hash_linklist_rep.cc', 'memtable/hash_skiplist_rep.cc', 'memtable/skiplistrep.cc', 'memtable/vectorrep.cc', 'memtable/write_buffer_manager.cc', 'monitoring/histogram.cc', 'monitoring/histogram_windowing.cc', 'monitoring/in_memory_stats_history.cc', 'monitoring/instrumented_mutex.cc', 'monitoring/iostats_context.cc', 'monitoring/perf_context.cc', 'monitoring/perf_level.cc', 'monitoring/persistent_stats_history.cc', 'monitoring/statistics.cc', 'monitoring/thread_status_impl.cc', 'monitoring/thread_status_updater.cc', 'monitoring/thread_status_updater_debug.cc', 'monitoring/thread_status_util.cc', 'monitoring/thread_status_util_debug.cc', 'options/cf_options.cc', 'options/configurable.cc', 'options/customizable.cc', 'options/db_options.cc', 'options/options.cc', 'options/options_helper.cc', 'options/options_parser.cc', 'port/port_posix.cc', 'port/win/env_default.cc', 'port/win/env_win.cc', 'port/win/io_win.cc', 'port/win/port_win.cc', 'port/win/win_logger.cc', 'port/win/win_thread.cc', 'port/stack_trace.cc', 'table/adaptive/adaptive_table_factory.cc', 'table/block_based/binary_search_index_reader.cc', 'table/block_based/block.cc', 'table/block_based/block_based_filter_block.cc', 'table/block_based/block_based_table_builder.cc', 'table/block_based/block_based_table_factory.cc', 'table/block_based/block_based_table_iterator.cc', 'table/block_based/block_based_table_reader.cc', 'table/block_based/block_builder.cc', 'table/block_based/block_prefetcher.cc', 'table/block_based/block_prefix_index.cc', 'table/block_based/data_block_hash_index.cc', 'table/block_based/data_block_footer.cc', 'table/block_based/filter_block_reader_common.cc', 'table/block_based/filter_policy.cc', 'table/block_based/flush_block_policy.cc', 'table/block_based/full_filter_block.cc', 'table/block_based/hash_index_reader.cc', 'table/block_based/index_builder.cc', 'table/block_based/index_reader_common.cc', 'table/block_based/parsed_full_filter_block.cc', 'table/block_based/partitioned_filter_block.cc', 'table/block_based/partitioned_index_iterator.cc', 'table/block_based/partitioned_index_reader.cc', 'table/block_based/reader_common.cc', 'table/block_based/uncompression_dict_reader.cc', 'table/block_fetcher.cc', 'table/cuckoo/cuckoo_table_builder.cc', 'table/cuckoo/cuckoo_table_factory.cc', 'table/cuckoo/cuckoo_table_reader.cc', 'table/format.cc', 'table/get_context.cc', 'table/iterator.cc', 'table/merging_iterator.cc', 'table/meta_blocks.cc', 'table/persistent_cache_helper.cc', 'table/plain/plain_table_bloom.cc', 'table/plain/plain_table_builder.cc', 'table/plain/plain_table_factory.cc', 'table/plain/plain_table_index.cc', 'table/plain/plain_table_key_coding.cc', 'table/plain/plain_table_reader.cc', 'table/sst_file_dumper.cc', 'table/sst_file_reader.cc', 'table/sst_file_writer.cc', 'table/table_factory.cc', 'table/table_properties.cc', 'table/two_level_iterator.cc', 'table/unique_id.cc', 'test_util/sync_point.cc', 'test_util/sync_point_impl.cc', 'test_util/transaction_test_util.cc', 'tools/dump/db_dump_tool.cc', 'trace_replay/trace_record_handler.cc', 'trace_replay/trace_record_result.cc', 'trace_replay/trace_record.cc', 'trace_replay/trace_replay.cc', 'trace_replay/block_cache_tracer.cc', 'trace_replay/io_tracer.cc', 'util/build_version.cc', 'util/coding.cc', 'util/compaction_job_stats_impl.cc', 'util/comparator.cc', 'util/compression_context_cache.cc', 'util/concurrent_task_limiter_impl.cc', 'util/crc32c.cc', 'util/crc32c_arm64.cc', 'util/dynamic_bloom.cc', 'util/hash.cc', 'util/murmurhash.cc', 'util/random.cc', 'util/rate_limiter.cc', 'util/ribbon_config.cc', 'util/slice.cc', 'util/file_checksum_helper.cc', 'util/status.cc', 'util/string_util.cc', 'util/thread_local.cc', 'util/threadpool_imp.cc', 'util/xxhash.cc', 'utilities/backupable/backupable_db.cc', 'utilities/blob_db/blob_compaction_filter.cc', 'utilities/blob_db/blob_db.cc', 'utilities/blob_db/blob_db_impl.cc', 'utilities/blob_db/blob_db_impl_filesnapshot.cc', 'utilities/blob_db/blob_file.cc', 'utilities/cache_dump_load.cc', 'utilities/cache_dump_load_impl.cc', 'utilities/cassandra/cassandra_compaction_filter.cc', 'utilities/cassandra/format.cc', 'utilities/cassandra/merge_operator.cc', 'utilities/checkpoint/checkpoint_impl.cc', 'utilities/compaction_filters.cc', 'utilities/compaction_filters/remove_emptyvalue_compactionfilter.cc', 'utilities/convenience/info_log_finder.cc', 'utilities/counted_fs.cc', 'utilities/debug.cc', 'utilities/env_mirror.cc', 'utilities/env_timed.cc', 'utilities/fault_injection_env.cc', 'utilities/fault_injection_fs.cc', 'utilities/fault_injection_secondary_cache.cc', 'utilities/leveldb_options/leveldb_options.cc', 'utilities/memory/memory_util.cc', 'utilities/merge_operators.cc', 'utilities/merge_operators/max.cc', 'utilities/merge_operators/put.cc', 'utilities/merge_operators/sortlist.cc', 'utilities/merge_operators/string_append/stringappend.cc', 'utilities/merge_operators/string_append/stringappend2.cc', 'utilities/merge_operators/uint64add.cc', 'utilities/merge_operators/bytesxor.cc', 'utilities/object_registry.cc', 'utilities/option_change_migration/option_change_migration.cc', 'utilities/options/options_util.cc', 'utilities/persistent_cache/block_cache_tier.cc', 'utilities/persistent_cache/block_cache_tier_file.cc', 'utilities/persistent_cache/block_cache_tier_metadata.cc', 'utilities/persistent_cache/persistent_cache_tier.cc', 'utilities/persistent_cache/volatile_tier_impl.cc', 'utilities/simulator_cache/cache_simulator.cc', 'utilities/simulator_cache/sim_cache.cc', 'utilities/table_properties_collectors/compact_on_deletion_collector.cc', 'utilities/trace/file_trace_reader_writer.cc', 'utilities/trace/replayer_impl.cc', 'utilities/transactions/lock/lock_manager.cc', 'utilities/transactions/lock/point/point_lock_tracker.cc', 'utilities/transactions/lock/point/point_lock_manager.cc', 'utilities/transactions/optimistic_transaction.cc', 'utilities/transactions/optimistic_transaction_db_impl.cc', 'utilities/transactions/pessimistic_transaction.cc', 'utilities/transactions/pessimistic_transaction_db.cc', 'utilities/transactions/snapshot_checker.cc', 'utilities/transactions/transaction_base.cc', 'utilities/transactions/transaction_db_mutex_impl.cc', 'utilities/transactions/transaction_util.cc', 'utilities/transactions/write_prepared_txn.cc', 'utilities/transactions/write_prepared_txn_db.cc', 'utilities/transactions/write_unprepared_txn.cc', 'utilities/transactions/write_unprepared_txn_db.cc', 'utilities/ttl/db_ttl_impl.cc', 'utilities/wal_filter.cc', 'utilities/write_batch_with_index/write_batch_with_index.cc', 'utilities/write_batch_with_index/write_batch_with_index_internal.cc'], 'LIB_SOURCES_ASM': [], 'LIB_SOURCES_C': [], 'RANGE_TREE_SOURCES': ['utilities/transactions/lock/range/range_tree/lib/locktree/concurrent_tree.cc', 'utilities/transactions/lock/range/range_tree/lib/locktree/keyrange.cc', 'utilities/transactions/lock/range/range_tree/lib/locktree/lock_request.cc', 'utilities/transactions/lock/range/range_tree/lib/locktree/locktree.cc', 'utilities/transactions/lock/range/range_tree/lib/locktree/manager.cc', 'utilities/transactions/lock/range/range_tree/lib/locktree/range_buffer.cc', 'utilities/transactions/lock/range/range_tree/lib/locktree/treenode.cc', 'utilities/transactions/lock/range/range_tree/lib/locktree/txnid_set.cc', 'utilities/transactions/lock/range/range_tree/lib/locktree/wfg.cc', 'utilities/transactions/lock/range/range_tree/lib/standalone_port.cc', 'utilities/transactions/lock/range/range_tree/lib/util/dbt.cc', 'utilities/transactions/lock/range/range_tree/lib/util/memarena.cc', 'utilities/transactions/lock/range/range_tree/range_tree_lock_manager.cc', 'utilities/transactions/lock/range/range_tree/range_tree_lock_tracker.cc'], 'TOOL_LIB_SOURCES': ['tools/io_tracer_parser_tool.cc', 'tools/ldb_cmd.cc', 'tools/ldb_tool.cc', 'tools/sst_dump_tool.cc', 'utilities/blob_db/blob_dump_tool.cc'], 'ANALYZER_LIB_SOURCES': ['tools/block_cache_analyzer/block_cache_trace_analyzer.cc', 'tools/trace_analyzer_tool.cc'], 'MOCK_LIB_SOURCES': ['table/mock_table.cc'], 'BENCH_LIB_SOURCES': ['tools/db_bench_tool.cc', 'tools/simulated_hybrid_file_system.cc'], 'CACHE_BENCH_LIB_SOURCES': ['cache/cache_bench_tool.cc'], 'STRESS_LIB_SOURCES': ['db_stress_tool/batched_ops_stress.cc', 'db_stress_tool/cf_consistency_stress.cc', 'db_stress_tool/db_stress_common.cc', 'db_stress_tool/db_stress_driver.cc', 'db_stress_tool/db_stress_gflags.cc', 'db_stress_tool/db_stress_listener.cc', 'db_stress_tool/db_stress_shared_state.cc', 'db_stress_tool/db_stress_stat.cc', 'db_stress_tool/db_stress_test_base.cc', 'db_stress_tool/db_stress_tool.cc', 'db_stress_tool/expected_state.cc', 'db_stress_tool/no_batched_ops_stress.cc', 'db_stress_tool/multi_ops_txns_stress.cc'], 'TEST_LIB_SOURCES': ['db/db_test_util.cc', 'test_util/mock_time_env.cc', 'test_util/testharness.cc', 'test_util/testutil.cc', 'utilities/cassandra/test_utils.cc'], 'FOLLY_SOURCES': ['third-party/folly/folly/detail/Futex.cpp', 'third-party/folly/folly/synchronization/AtomicNotification.cpp', 'third-party/folly/folly/synchronization/DistributedMutex.cpp', 'third-party/folly/folly/synchronization/ParkingLot.cpp', 'third-party/folly/folly/synchronization/WaitOptions.cpp'], 'TOOLS_MAIN_SOURCES': ['db_stress_tool/db_stress.cc', 'tools/blob_dump.cc', 'tools/block_cache_analyzer/block_cache_trace_analyzer_tool.cc', 'tools/db_repl_stress.cc', 'tools/db_sanity_test.cc', 'tools/ldb.cc', 'tools/io_tracer_parser.cc', 'tools/sst_dump.cc', 'tools/write_stress.cc', 'tools/dump/rocksdb_dump.cc', 'tools/dump/rocksdb_undump.cc', 'tools/trace_analyzer.cc', 'tools/io_tracer_parser_tool.cc'], 'BENCH_MAIN_SOURCES': ['cache/cache_bench.cc', 'db/range_del_aggregator_bench.cc', 'memtable/memtablerep_bench.cc', 'table/table_reader_bench.cc', 'tools/db_bench.cc', 'util/filter_bench.cc', 'utilities/persistent_cache/persistent_cache_bench.cc'], 'TEST_MAIN_SOURCES': ['cache/cache_test.cc', 'cache/cache_reservation_manager_test.cc', 'cache/lru_cache_test.cc', 'db/blob/blob_counting_iterator_test.cc', 'db/blob/blob_file_addition_test.cc', 'db/blob/blob_file_builder_test.cc', 'db/blob/blob_file_cache_test.cc', 'db/blob/blob_file_garbage_test.cc', 'db/blob/blob_file_reader_test.cc', 'db/blob/blob_garbage_meter_test.cc', 'db/blob/db_blob_basic_test.cc', 'db/blob/db_blob_compaction_test.cc', 'db/blob/db_blob_corruption_test.cc', 'db/blob/db_blob_index_test.cc', 'db/column_family_test.cc', 'db/compact_files_test.cc', 'db/compaction/clipping_iterator_test.cc', 'db/compaction/compaction_iterator_test.cc', 'db/compaction/compaction_job_test.cc', 'db/compaction/compaction_job_stats_test.cc', 'db/compaction/compaction_picker_test.cc', 'db/compaction/compaction_service_test.cc', 'db/comparator_db_test.cc', 'db/corruption_test.cc', 'db/cuckoo_table_db_test.cc', 'db/db_basic_test.cc', 'db/db_with_timestamp_basic_test.cc', 'db/db_block_cache_test.cc', 'db/db_bloom_filter_test.cc', 'db/db_compaction_filter_test.cc', 'db/db_compaction_test.cc', 'db/db_dynamic_level_test.cc', 'db/db_encryption_test.cc', 'db/db_flush_test.cc', 'db/import_column_family_test.cc', 'db/db_inplace_update_test.cc', 'db/db_io_failure_test.cc', 'db/db_iter_test.cc', 'db/db_iter_stress_test.cc', 'db/db_iterator_test.cc', 'db/db_kv_checksum_test.cc', 'db/db_log_iter_test.cc', 'db/db_memtable_test.cc', 'db/db_merge_operator_test.cc', 'db/db_merge_operand_test.cc', 'db/db_options_test.cc', 'db/db_properties_test.cc', 'db/db_range_del_test.cc', 'db/db_secondary_test.cc', 'db/db_sst_test.cc', 'db/db_statistics_test.cc', 'db/db_table_properties_test.cc', 'db/db_tailing_iter_test.cc', 'db/db_test.cc', 'db/db_test2.cc', 'db/db_logical_block_size_cache_test.cc', 'db/db_universal_compaction_test.cc', 'db/db_wal_test.cc', 'db/db_with_timestamp_compaction_test.cc', 'db/db_write_buffer_manager_test.cc', 'db/db_write_test.cc', 'db/dbformat_test.cc', 'db/deletefile_test.cc', 'db/error_handler_fs_test.cc', 'db/external_sst_file_basic_test.cc', 'db/external_sst_file_test.cc', 'db/fault_injection_test.cc', 'db/file_indexer_test.cc', 'db/filename_test.cc', 'db/flush_job_test.cc', 'db/listener_test.cc', 'db/log_test.cc', 'db/manual_compaction_test.cc', 'db/memtable_list_test.cc', 'db/merge_helper_test.cc', 'db/merge_test.cc', 'db/obsolete_files_test.cc', 'db/options_file_test.cc', 'db/perf_context_test.cc', 'db/periodic_work_scheduler_test.cc', 'db/plain_table_db_test.cc', 'db/prefix_test.cc', 'db/repair_test.cc', 'db/range_del_aggregator_test.cc', 'db/range_tombstone_fragmenter_test.cc', 'db/table_properties_collector_test.cc', 'db/version_builder_test.cc', 'db/version_edit_test.cc', 'db/version_set_test.cc', 'db/wal_manager_test.cc', 'db/write_batch_test.cc', 'db/write_callback_test.cc', 'db/write_controller_test.cc', 'env/env_basic_test.cc', 'env/env_test.cc', 'env/io_posix_test.cc', 'env/mock_env_test.cc', 'file/delete_scheduler_test.cc', 'file/prefetch_test.cc', 'file/random_access_file_reader_test.cc', 'logging/auto_roll_logger_test.cc', 'logging/env_logger_test.cc', 'logging/event_logger_test.cc', 'memory/arena_test.cc', 'memory/memory_allocator_test.cc', 'memtable/inlineskiplist_test.cc', 'memtable/skiplist_test.cc', 'memtable/write_buffer_manager_test.cc', 'monitoring/histogram_test.cc', 'monitoring/iostats_context_test.cc', 'monitoring/statistics_test.cc', 'monitoring/stats_history_test.cc', 'options/configurable_test.cc', 'options/customizable_test.cc', 'options/options_settable_test.cc', 'options/options_test.cc', 'table/block_based/block_based_filter_block_test.cc', 'table/block_based/block_based_table_reader_test.cc', 'table/block_based/block_test.cc', 'table/block_based/data_block_hash_index_test.cc', 'table/block_based/full_filter_block_test.cc', 'table/block_based/partitioned_filter_block_test.cc', 'table/cleanable_test.cc', 'table/cuckoo/cuckoo_table_builder_test.cc', 'table/cuckoo/cuckoo_table_reader_test.cc', 'table/merger_test.cc', 'table/sst_file_reader_test.cc', 'table/table_test.cc', 'table/block_fetcher_test.cc', 'test_util/testutil_test.cc', 'tools/block_cache_analyzer/block_cache_trace_analyzer_test.cc', 'tools/io_tracer_parser_test.cc', 'tools/ldb_cmd_test.cc', 'tools/reduce_levels_test.cc', 'tools/sst_dump_test.cc', 'tools/trace_analyzer_test.cc', 'trace_replay/block_cache_tracer_test.cc', 'trace_replay/io_tracer_test.cc', 'util/autovector_test.cc', 'util/bloom_test.cc', 'util/coding_test.cc', 'util/crc32c_test.cc', 'util/defer_test.cc', 'util/dynamic_bloom_test.cc', 'util/filelock_test.cc', 'util/file_reader_writer_test.cc', 'util/hash_test.cc', 'util/heap_test.cc', 'util/random_test.cc', 'util/rate_limiter_test.cc', 'util/repeatable_thread_test.cc', 'util/ribbon_test.cc', 'util/slice_test.cc', 'util/slice_transform_test.cc', 'util/timer_queue_test.cc', 'util/timer_test.cc', 'util/thread_list_test.cc', 'util/thread_local_test.cc', 'util/work_queue_test.cc', 'utilities/backupable/backupable_db_test.cc', 'utilities/blob_db/blob_db_test.cc', 'utilities/cassandra/cassandra_format_test.cc', 'utilities/cassandra/cassandra_functional_test.cc', 'utilities/cassandra/cassandra_row_merge_test.cc', 'utilities/cassandra/cassandra_serialize_test.cc', 'utilities/checkpoint/checkpoint_test.cc', 'utilities/env_timed_test.cc', 'utilities/memory/memory_test.cc', 'utilities/merge_operators/string_append/stringappend_test.cc', 'utilities/object_registry_test.cc', 'utilities/option_change_migration/option_change_migration_test.cc', 'utilities/options/options_util_test.cc', 'utilities/persistent_cache/hash_table_test.cc', 'utilities/persistent_cache/persistent_cache_test.cc', 'utilities/simulator_cache/cache_simulator_test.cc', 'utilities/simulator_cache/sim_cache_test.cc', 'utilities/table_properties_collectors/compact_on_deletion_collector_test.cc', 'utilities/transactions/optimistic_transaction_test.cc', 'utilities/transactions/lock/range/range_locking_test.cc', 'utilities/transactions/transaction_test.cc', 'utilities/transactions/lock/point/point_lock_manager_test.cc', 'utilities/transactions/write_prepared_transaction_test.cc', 'utilities/transactions/write_unprepared_transaction_test.cc', 'utilities/ttl/ttl_test.cc', 'utilities/util_merge_operators_test.cc', 'utilities/write_batch_with_index/write_batch_with_index_test.cc'], 'TEST_MAIN_SOURCES_C': ['db/c_test.c'], 'MICROBENCH_SOURCES': ['microbench/ribbon_bench.cc'], 'JNI_NATIVE_SOURCES': ['java/rocksjni/backupenginejni.cc', 'java/rocksjni/backupablejni.cc', 'java/rocksjni/checkpoint.cc', 'java/rocksjni/clock_cache.cc', 'java/rocksjni/cache.cc', 'java/rocksjni/columnfamilyhandle.cc', 'java/rocksjni/compact_range_options.cc', 'java/rocksjni/compaction_filter.cc', 'java/rocksjni/compaction_filter_factory.cc', 'java/rocksjni/compaction_filter_factory_jnicallback.cc', 'java/rocksjni/compaction_job_info.cc', 'java/rocksjni/compaction_job_stats.cc', 'java/rocksjni/compaction_options.cc', 'java/rocksjni/compaction_options_fifo.cc', 'java/rocksjni/compaction_options_universal.cc', 'java/rocksjni/comparator.cc', 'java/rocksjni/comparatorjnicallback.cc', 'java/rocksjni/compression_options.cc', 'java/rocksjni/concurrent_task_limiter.cc', 'java/rocksjni/config_options.cc', 'java/rocksjni/env.cc', 'java/rocksjni/env_options.cc', 'java/rocksjni/event_listener.cc', 'java/rocksjni/event_listener_jnicallback.cc', 'java/rocksjni/ingest_external_file_options.cc', 'java/rocksjni/filter.cc', 'java/rocksjni/iterator.cc', 'java/rocksjni/jnicallback.cc', 'java/rocksjni/loggerjnicallback.cc', 'java/rocksjni/lru_cache.cc', 'java/rocksjni/memtablejni.cc', 'java/rocksjni/memory_util.cc', 'java/rocksjni/merge_operator.cc', 'java/rocksjni/native_comparator_wrapper_test.cc', 'java/rocksjni/optimistic_transaction_db.cc', 'java/rocksjni/optimistic_transaction_options.cc', 'java/rocksjni/options.cc', 'java/rocksjni/options_util.cc', 'java/rocksjni/persistent_cache.cc', 'java/rocksjni/ratelimiterjni.cc', 'java/rocksjni/remove_emptyvalue_compactionfilterjni.cc', 'java/rocksjni/cassandra_compactionfilterjni.cc', 'java/rocksjni/cassandra_value_operator.cc', 'java/rocksjni/restorejni.cc', 'java/rocksjni/rocks_callback_object.cc', 'java/rocksjni/rocksjni.cc', 'java/rocksjni/rocksdb_exception_test.cc', 'java/rocksjni/slice.cc', 'java/rocksjni/snapshot.cc', 'java/rocksjni/sst_file_manager.cc', 'java/rocksjni/sst_file_writerjni.cc', 'java/rocksjni/sst_file_readerjni.cc', 'java/rocksjni/sst_file_reader_iterator.cc', 'java/rocksjni/sst_partitioner.cc', 'java/rocksjni/statistics.cc', 'java/rocksjni/statisticsjni.cc', 'java/rocksjni/table.cc', 'java/rocksjni/table_filter.cc', 'java/rocksjni/table_filter_jnicallback.cc', 'java/rocksjni/thread_status.cc', 'java/rocksjni/trace_writer.cc', 'java/rocksjni/trace_writer_jnicallback.cc', 'java/rocksjni/transaction.cc', 'java/rocksjni/transaction_db.cc', 'java/rocksjni/transaction_options.cc', 'java/rocksjni/transaction_db_options.cc', 'java/rocksjni/transaction_log.cc', 'java/rocksjni/transaction_notifier.cc', 'java/rocksjni/transaction_notifier_jnicallback.cc', 'java/rocksjni/ttl.cc', 'java/rocksjni/testable_event_listener.cc', 'java/rocksjni/wal_filter.cc', 'java/rocksjni/wal_filter_jnicallback.cc', 'java/rocksjni/write_batch.cc', 'java/rocksjni/writebatchhandlerjnicallback.cc', 'java/rocksjni/write_batch_test.cc', 'java/rocksjni/write_batch_with_index.cc', 'java/rocksjni/write_buffer_manager.cc']}
cache_test cache/cache_test.cc
cache_reservation_manager_test cache/cache_reservation_manager_test.cc
lru_cache_test cache/lru_cache_test.cc
blob_counting_iterator_test db/blob/blob_counting_iterator_test.cc
blob_file_addition_test db/blob/blob_file_addition_test.cc
blob_file_builder_test db/blob/blob_file_builder_test.cc
blob_file_cache_test db/blob/blob_file_cache_test.cc
blob_file_garbage_test db/blob/blob_file_garbage_test.cc
blob_file_reader_test db/blob/blob_file_reader_test.cc
blob_garbage_meter_test db/blob/blob_garbage_meter_test.cc
db_blob_basic_test db/blob/db_blob_basic_test.cc
db_blob_compaction_test db/blob/db_blob_compaction_test.cc
db_blob_corruption_test db/blob/db_blob_corruption_test.cc
db_blob_index_test db/blob/db_blob_index_test.cc
column_family_test db/column_family_test.cc
compact_files_test db/compact_files_test.cc
clipping_iterator_test db/compaction/clipping_iterator_test.cc
compaction_iterator_test db/compaction/compaction_iterator_test.cc
compaction_job_test db/compaction/compaction_job_test.cc
compaction_job_stats_test db/compaction/compaction_job_stats_test.cc
compaction_picker_test db/compaction/compaction_picker_test.cc
compaction_service_test db/compaction/compaction_service_test.cc
comparator_db_test db/comparator_db_test.cc
corruption_test db/corruption_test.cc
cuckoo_table_db_test db/cuckoo_table_db_test.cc
db_basic_test db/db_basic_test.cc
db_with_timestamp_basic_test db/db_with_timestamp_basic_test.cc
db_block_cache_test db/db_block_cache_test.cc
db_bloom_filter_test db/db_bloom_filter_test.cc
db_compaction_filter_test db/db_compaction_filter_test.cc
db_compaction_test db/db_compaction_test.cc
db_dynamic_level_test db/db_dynamic_level_test.cc
db_encryption_test db/db_encryption_test.cc
db_flush_test db/db_flush_test.cc
import_column_family_test db/import_column_family_test.cc
db_inplace_update_test db/db_inplace_update_test.cc
db_io_failure_test db/db_io_failure_test.cc
db_iter_test db/db_iter_test.cc
db_iter_stress_test db/db_iter_stress_test.cc
db_iterator_test db/db_iterator_test.cc
db_kv_checksum_test db/db_kv_checksum_test.cc
db_log_iter_test db/db_log_iter_test.cc
db_memtable_test db/db_memtable_test.cc
db_merge_operator_test db/db_merge_operator_test.cc
db_merge_operand_test db/db_merge_operand_test.cc
db_options_test db/db_options_test.cc
db_properties_test db/db_properties_test.cc
db_range_del_test db/db_range_del_test.cc
db_secondary_test db/db_secondary_test.cc
db_sst_test db/db_sst_test.cc
db_statistics_test db/db_statistics_test.cc
db_table_properties_test db/db_table_properties_test.cc
db_tailing_iter_test db/db_tailing_iter_test.cc
db_test db/db_test.cc
db_test2 db/db_test2.cc
db_logical_block_size_cache_test db/db_logical_block_size_cache_test.cc
db_universal_compaction_test db/db_universal_compaction_test.cc
db_wal_test db/db_wal_test.cc
db_with_timestamp_compaction_test db/db_with_timestamp_compaction_test.cc
db_write_buffer_manager_test db/db_write_buffer_manager_test.cc
db_write_test db/db_write_test.cc
dbformat_test db/dbformat_test.cc
deletefile_test db/deletefile_test.cc
error_handler_fs_test db/error_handler_fs_test.cc
external_sst_file_basic_test db/external_sst_file_basic_test.cc
external_sst_file_test db/external_sst_file_test.cc
fault_injection_test db/fault_injection_test.cc
file_indexer_test db/file_indexer_test.cc
filename_test db/filename_test.cc
flush_job_test db/flush_job_test.cc
listener_test db/listener_test.cc
log_test db/log_test.cc
manual_compaction_test db/manual_compaction_test.cc
memtable_list_test db/memtable_list_test.cc
merge_helper_test db/merge_helper_test.cc
merge_test db/merge_test.cc
obsolete_files_test db/obsolete_files_test.cc
options_file_test db/options_file_test.cc
perf_context_test db/perf_context_test.cc
periodic_work_scheduler_test db/periodic_work_scheduler_test.cc
plain_table_db_test db/plain_table_db_test.cc
prefix_test db/prefix_test.cc
repair_test db/repair_test.cc
range_del_aggregator_test db/range_del_aggregator_test.cc
range_tombstone_fragmenter_test db/range_tombstone_fragmenter_test.cc
table_properties_collector_test db/table_properties_collector_test.cc
version_builder_test db/version_builder_test.cc
version_edit_test db/version_edit_test.cc
version_set_test db/version_set_test.cc
wal_manager_test db/wal_manager_test.cc
write_batch_test db/write_batch_test.cc
write_callback_test db/write_callback_test.cc
write_controller_test db/write_controller_test.cc
env_basic_test env/env_basic_test.cc
env_test env/env_test.cc
io_posix_test env/io_posix_test.cc
mock_env_test env/mock_env_test.cc
delete_scheduler_test file/delete_scheduler_test.cc
prefetch_test file/prefetch_test.cc
random_access_file_reader_test file/random_access_file_reader_test.cc
auto_roll_logger_test logging/auto_roll_logger_test.cc
env_logger_test logging/env_logger_test.cc
event_logger_test logging/event_logger_test.cc
arena_test memory/arena_test.cc
memory_allocator_test memory/memory_allocator_test.cc
inlineskiplist_test memtable/inlineskiplist_test.cc
skiplist_test memtable/skiplist_test.cc
write_buffer_manager_test memtable/write_buffer_manager_test.cc
histogram_test monitoring/histogram_test.cc
iostats_context_test monitoring/iostats_context_test.cc
statistics_test monitoring/statistics_test.cc
stats_history_test monitoring/stats_history_test.cc
configurable_test options/configurable_test.cc
customizable_test options/customizable_test.cc
options_settable_test options/options_settable_test.cc
options_test options/options_test.cc
block_based_filter_block_test table/block_based/block_based_filter_block_test.cc
block_based_table_reader_test table/block_based/block_based_table_reader_test.cc
block_test table/block_based/block_test.cc
data_block_hash_index_test table/block_based/data_block_hash_index_test.cc
full_filter_block_test table/block_based/full_filter_block_test.cc
partitioned_filter_block_test table/block_based/partitioned_filter_block_test.cc
cleanable_test table/cleanable_test.cc
cuckoo_table_builder_test table/cuckoo/cuckoo_table_builder_test.cc
cuckoo_table_reader_test table/cuckoo/cuckoo_table_reader_test.cc
merger_test table/merger_test.cc
sst_file_reader_test table/sst_file_reader_test.cc
table_test table/table_test.cc
block_fetcher_test table/block_fetcher_test.cc
testutil_test test_util/testutil_test.cc
block_cache_trace_analyzer_test tools/block_cache_analyzer/block_cache_trace_analyzer_test.cc
io_tracer_parser_test tools/io_tracer_parser_test.cc
ldb_cmd_test tools/ldb_cmd_test.cc
reduce_levels_test tools/reduce_levels_test.cc
sst_dump_test tools/sst_dump_test.cc
trace_analyzer_test tools/trace_analyzer_test.cc
block_cache_tracer_test trace_replay/block_cache_tracer_test.cc
io_tracer_test trace_replay/io_tracer_test.cc
autovector_test util/autovector_test.cc
bloom_test util/bloom_test.cc
coding_test util/coding_test.cc
crc32c_test util/crc32c_test.cc
defer_test util/defer_test.cc
dynamic_bloom_test util/dynamic_bloom_test.cc
filelock_test util/filelock_test.cc
file_reader_writer_test util/file_reader_writer_test.cc
hash_test util/hash_test.cc
heap_test util/heap_test.cc
random_test util/random_test.cc
rate_limiter_test util/rate_limiter_test.cc
repeatable_thread_test util/repeatable_thread_test.cc
ribbon_test util/ribbon_test.cc
slice_test util/slice_test.cc
slice_transform_test util/slice_transform_test.cc
timer_queue_test util/timer_queue_test.cc
timer_test util/timer_test.cc
thread_list_test util/thread_list_test.cc
thread_local_test util/thread_local_test.cc
work_queue_test util/work_queue_test.cc
backupable_db_test utilities/backupable/backupable_db_test.cc
blob_db_test utilities/blob_db/blob_db_test.cc
cassandra_format_test utilities/cassandra/cassandra_format_test.cc
cassandra_functional_test utilities/cassandra/cassandra_functional_test.cc
cassandra_row_merge_test utilities/cassandra/cassandra_row_merge_test.cc
cassandra_serialize_test utilities/cassandra/cassandra_serialize_test.cc
checkpoint_test utilities/checkpoint/checkpoint_test.cc
env_timed_test utilities/env_timed_test.cc
memory_test utilities/memory/memory_test.cc
stringappend_test utilities/merge_operators/string_append/stringappend_test.cc
object_registry_test utilities/object_registry_test.cc
option_change_migration_test utilities/option_change_migration/option_change_migration_test.cc
options_util_test utilities/options/options_util_test.cc
hash_table_test utilities/persistent_cache/hash_table_test.cc
persistent_cache_test utilities/persistent_cache/persistent_cache_test.cc
cache_simulator_test utilities/simulator_cache/cache_simulator_test.cc
sim_cache_test utilities/simulator_cache/sim_cache_test.cc
compact_on_deletion_collector_test utilities/table_properties_collectors/compact_on_deletion_collector_test.cc
optimistic_transaction_test utilities/transactions/optimistic_transaction_test.cc
range_locking_test utilities/transactions/lock/range/range_locking_test.cc
transaction_test utilities/transactions/transaction_test.cc
point_lock_manager_test utilities/transactions/lock/point/point_lock_manager_test.cc
write_prepared_transaction_test utilities/transactions/write_prepared_transaction_test.cc
write_unprepared_transaction_test utilities/transactions/write_unprepared_transaction_test.cc
ttl_test utilities/ttl/ttl_test.cc
util_merge_operators_test utilities/util_merge_operators_test.cc
write_batch_with_index_test utilities/write_batch_with_index/write_batch_with_index_test.cc
[94mGenerated TARGETS Summary:[0m
[94m- 7 libs[0m
[94m- 0 binarys[0m
[94m- 186 tests[0m
make[1]: Leaving directory '/data/users/sidroyc/rocksdb'
make check-sources
make[1]: Entering directory '/data/users/sidroyc/rocksdb'
$DEBUG_LEVEL is 1
Makefile:170: Warning: Compiling in debug mode. Don't use the resulting binary in production
build_tools/check-sources.sh
make[1]: Leaving directory '/data/users/sidroyc/rocksdb'
