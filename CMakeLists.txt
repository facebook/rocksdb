cmake_minimum_required(VERSION 2.6)
project(rocksdb)

# Windows build depends on this being included early-on
if (WIN64)
include(${CMAKE_CURRENT_SOURCE_DIR}/thirdparty.inc)
endif (WIN64)

execute_process(COMMAND
    "./build_tools/version.sh" "major"
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    OUTPUT_VARIABLE ROCKSDB_VERSION_MAJOR
)
string(STRIP "${ROCKSDB_VERSION_MAJOR}" ROCKSDB_VERSION_MAJOR)

execute_process(COMMAND
    "./build_tools/version.sh" "minor"
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    OUTPUT_VARIABLE ROCKSDB_VERSION_MINOR
)
string(STRIP "${ROCKSDB_VERSION_MINOR}" ROCKSDB_VERSION_MINOR)

execute_process(COMMAND
    "./build_tools/version.sh" "patch"
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    OUTPUT_VARIABLE ROCKSDB_VERSION_PATCH
)
string(STRIP "${ROCKSDB_VERSION_PATCH}" ROCKSDB_VERSION_PATCH)

set(ROCKSDB_VERSION "${ROCKSDB_VERSION_MAJOR}.${ROCKSDB_VERSION_MINOR}.${ROCKSDB_VERSION_PATCH}")
message(STATUS "rocksdb: ${ROCKSDB_VERSION}")

set(BUILD_VERSION_CC ${CMAKE_CURRENT_SOURCE_DIR}/util/build_version.cc)
add_custom_command(OUTPUT ${BUILD_VERSION_CC}
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/build_tools/create_version_cc.sh
      ${BUILD_VERSION_CC}
)

include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/include)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules/")

include_directories(${PROJECT_SOURCE_DIR}/third-party/gtest-1.7.0/fused-src)
add_subdirectory(third-party/gtest-1.7.0/fused-src/gtest)

find_package(Threads)
find_package(JeMalloc)

if (JEMALLOC_FOUND)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DROCKSDB_JEMALLOC")
endif (JEMALLOC_FOUND)

# These need to come before SOURCES and hence can't be moved
# to platform specific cmake

if (WIN64)
set(PLATFORM_SOURCES
        port/win/io_win.cc
        port/win/env_win.cc
        port/win/port_win.cc
        port/win/win_logger.cc
        port/win/xpress_win.cc
)
endif (WIN64)

if (UNIX)
set(PLATFORM_SOURCES
        port/port_posix.cc
        util/env_posix.cc
        util/io_posix.cc
)
endif (UNIX)

# Main library source code
set(SOURCES
        db/auto_roll_logger.cc
        db/builder.cc
        db/c.cc
        db/column_family.cc
        db/compacted_db_impl.cc
        db/compaction.cc
        db/compaction_iterator.cc
        db/compaction_job.cc
        db/compaction_picker.cc
        db/convenience.cc
        db/dbformat.cc
        db/db_filesnapshot.cc
        db/db_impl.cc
        db/db_impl_debug.cc
        db/db_impl_experimental.cc
        db/db_impl_readonly.cc
        db/db_info_dumper.cc
        db/db_iter.cc
        db/event_helpers.cc
        db/experimental.cc
        db/filename.cc
        db/file_indexer.cc
        db/flush_job.cc
        db/flush_scheduler.cc
        db/forward_iterator.cc
        db/internal_stats.cc
        db/log_reader.cc
        db/log_writer.cc
        db/managed_iterator.cc
        db/memtable.cc
        db/memtable_allocator.cc
        db/memtable_list.cc
        db/merge_helper.cc
        db/merge_operator.cc
        db/repair.cc
        db/slice.cc
        db/snapshot_impl.cc
        db/table_cache.cc
        db/table_properties_collector.cc
        db/transaction_log_impl.cc
        db/version_builder.cc
        db/version_edit.cc
        db/version_set.cc
        db/wal_manager.cc
        db/write_batch.cc
        db/write_batch_base.cc
        db/write_controller.cc
        db/write_thread.cc
        db/xfunc_test_points.cc
        memtable/hash_cuckoo_rep.cc
        memtable/hash_linklist_rep.cc
        memtable/hash_skiplist_rep.cc
        memtable/skiplistrep.cc
        memtable/vectorrep.cc
        port/stack_trace.cc
        table/adaptive_table_factory.cc
        table/block.cc
        table/block_based_filter_block.cc
        table/block_based_table_builder.cc
        table/block_based_table_factory.cc
        table/block_based_table_reader.cc
        table/block_builder.cc
        table/block_prefix_index.cc
        table/bloom_block.cc
        table/cuckoo_table_builder.cc
        table/cuckoo_table_factory.cc
        table/cuckoo_table_reader.cc
        table/flush_block_policy.cc
        table/format.cc
        table/full_filter_block.cc
        table/get_context.cc
        table/iterator.cc
        table/merger.cc
        table/sst_file_writer.cc
        table/meta_blocks.cc
        table/plain_table_builder.cc
        table/plain_table_factory.cc
        table/plain_table_index.cc
        table/plain_table_key_coding.cc
        table/plain_table_reader.cc
        table/persistent_cache_helper.cc
        table/table_properties.cc
        table/two_level_iterator.cc
        tools/sst_dump_tool.cc
        tools/db_bench_tool.cc
        tools/dump/db_dump_tool.cc
        util/arena.cc
        util/bloom.cc
        util/build_version.cc
        util/cache.cc
        util/coding.cc
        util/compaction_job_stats_impl.cc
        util/comparator.cc
        util/concurrent_arena.cc
        util/crc32c.cc
        util/delete_scheduler.cc
        util/dynamic_bloom.cc
        util/env.cc
        util/env_chroot.cc
        util/env_hdfs.cc
        util/event_logger.cc
        util/file_util.cc
        util/file_reader_writer.cc
        util/sst_file_manager_impl.cc
        util/filter_policy.cc
        util/hash.cc
        util/histogram.cc
        util/histogram_windowing.cc
        util/instrumented_mutex.cc
        util/iostats_context.cc
        tools/ldb_cmd.cc
        tools/ldb_tool.cc
        util/logging.cc
        util/log_buffer.cc
        util/memenv.cc
        util/murmurhash.cc
        util/mutable_cf_options.cc
        util/options.cc
        util/options_builder.cc
        util/options_helper.cc
        util/options_parser.cc
        util/options_sanity_check.cc
        util/perf_context.cc
        util/perf_level.cc
        util/random.cc
        util/rate_limiter.cc
        util/slice.cc
        util/statistics.cc
        util/status.cc
        util/status_message.cc
        util/string_util.cc
        util/sync_point.cc
        util/testharness.cc
        util/testutil.cc
        util/thread_local.cc
        util/threadpool.cc
        util/thread_status_impl.cc
        util/thread_status_updater.cc
        util/thread_status_util.cc
        util/thread_status_util_debug.cc
        util/transaction_test_util.cc
        util/xfunc.cc
        util/xxhash.cc
        utilities/backupable/backupable_db.cc
        utilities/checkpoint/checkpoint.cc
        utilities/compaction_filters/remove_emptyvalue_compactionfilter.cc
        utilities/document/document_db.cc
        utilities/document/json_document.cc
        utilities/document/json_document_builder.cc
        utilities/env_mirror.cc
        utilities/flashcache/flashcache.cc
        utilities/geodb/geodb_impl.cc
        utilities/leveldb_options/leveldb_options.cc
        utilities/memory/memory_util.cc
        utilities/merge_operators/string_append/stringappend.cc
        utilities/merge_operators/string_append/stringappend2.cc
        utilities/merge_operators/put.cc
        utilities/merge_operators/max.cc
        utilities/merge_operators/uint64add.cc
        utilities/options/options_util.cc
        utilities/redis/redis_lists.cc
        utilities/simulator_cache/sim_cache.cc
        utilities/spatialdb/spatial_db.cc
        utilities/table_properties_collectors/compact_on_deletion_collector.cc
        utilities/transactions/optimistic_transaction_impl.cc
        utilities/transactions/optimistic_transaction_db_impl.cc
        utilities/transactions/transaction_base.cc
        utilities/transactions/transaction_impl.cc
        utilities/transactions/transaction_db_impl.cc
        utilities/transactions/transaction_db_mutex_impl.cc
        utilities/transactions/transaction_lock_mgr.cc
        utilities/transactions/transaction_util.cc
        utilities/ttl/db_ttl_impl.cc
        utilities/write_batch_with_index/write_batch_with_index.cc
        utilities/write_batch_with_index/write_batch_with_index_internal.cc
        ${PLATFORM_SOURCES}
)

set(APPS
        tools/db_bench.cc
        db/memtablerep_bench.cc
        table/table_reader_bench.cc
        tools/db_stress.cc
        tools/write_stress.cc
        tools/db_repl_stress.cc
        tools/ldb.cc
        tools/sst_dump.cc
        tools/dump/rocksdb_dump.cc
        tools/dump/rocksdb_undump.cc
        util/cache_bench.cc
)

set(C_TESTS db/c_test.c)

set(TESTS
        db/auto_roll_logger_test.cc
        db/column_family_test.cc
        db/compact_files_test.cc
        db/compaction_iterator_test.cc
        db/compaction_job_test.cc
        db/compaction_job_stats_test.cc
        db/compaction_picker_test.cc
        db/comparator_db_test.cc
        db/corruption_test.cc
        db/cuckoo_table_db_test.cc
        db/db_compaction_filter_test.cc
        db/db_compaction_test.cc
        db/db_dynamic_level_test.cc
        db/db_inplace_update_test.cc
        db/db_iter_test.cc
        db/db_log_iter_test.cc
        db/db_properties_test.cc
        db/db_table_properties_test.cc
        db/db_tailing_iter_test.cc
        db/db_test.cc
        db/db_test2.cc
        db/db_block_cache_test.cc
        db/db_bloom_filter_test.cc
        db/db_iterator_test.cc
        db/db_sst_test.cc
        db/db_universal_compaction_test.cc
        db/db_wal_test.cc
        db/dbformat_test.cc
        db/deletefile_test.cc
        db/fault_injection_test.cc
        db/file_indexer_test.cc
        db/filename_test.cc
        db/flush_job_test.cc
        db/inlineskiplist_test.cc
        db/listener_test.cc
        db/log_test.cc
        db/manual_compaction_test.cc
        db/memtable_list_test.cc
        db/merge_test.cc
        db/merge_helper_test.cc
        db/options_file_test.cc
        db/perf_context_test.cc
        db/plain_table_db_test.cc
        db/prefix_test.cc
        db/repair_test.cc
        db/skiplist_test.cc
        db/table_properties_collector_test.cc
        db/version_builder_test.cc
        db/version_edit_test.cc
        db/version_set_test.cc
        db/wal_manager_test.cc
        db/write_batch_test.cc
        db/write_callback_test.cc
        db/write_controller_test.cc
        db/db_io_failure_test.cc
        table/block_based_filter_block_test.cc
        table/block_test.cc
        table/cuckoo_table_builder_test.cc
        table/cuckoo_table_reader_test.cc
        table/full_filter_block_test.cc
        table/merger_test.cc
        table/table_test.cc
        tools/db_sanity_test.cc
        tools/ldb_cmd_test.cc
        tools/reduce_levels_test.cc
        tools/sst_dump_test.cc
        util/arena_test.cc
        util/autovector_test.cc
        util/bloom_test.cc
        util/cache_test.cc
        util/coding_test.cc
        util/crc32c_test.cc
        util/delete_scheduler_test.cc
        util/dynamic_bloom_test.cc
        util/env_test.cc
        util/event_logger_test.cc
        util/filelock_test.cc
        util/file_reader_writer_test.cc
        util/heap_test.cc
        util/histogram_test.cc
        util/iostats_context_test.cc
        util/memenv_test.cc
        util/mock_env_test.cc
        util/options_settable_test.cc
        util/options_test.cc
        util/rate_limiter_test.cc
        util/slice_transform_test.cc
        util/thread_list_test.cc
        util/thread_local_test.cc
        utilities/backupable/backupable_db_test.cc
        utilities/checkpoint/checkpoint_test.cc
        utilities/document/document_db_test.cc
        utilities/document/json_document_test.cc
        utilities/geodb/geodb_test.cc
        utilities/memory/memory_test.cc
        utilities/merge_operators/string_append/stringappend_test.cc
        utilities/options/options_util_test.cc
        utilities/redis/redis_lists_test.cc
        utilities/spatialdb/spatial_db_test.cc
        utilities/table_properties_collectors/compact_on_deletion_collector_test.cc
        utilities/transactions/optimistic_transaction_test.cc
        utilities/transactions/transaction_test.cc
        utilities/ttl/ttl_test.cc
        utilities/write_batch_with_index/write_batch_with_index_test.cc
)

if (WIN64)
include(win64.cmake)
endif (WIN64)

if (UNIX)
include(linux.cmake)
endif (UNIX)
