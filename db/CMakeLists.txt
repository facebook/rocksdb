set(SOURCES
        builder.cc
        c.cc
        column_family.cc
        compaction.cc
        compaction_job.cc
        compaction_picker.cc
        dbformat.cc
        db_filesnapshot.cc
        db_impl.cc
        db_impl_debug.cc
        db_impl_experimental.cc
        db_impl_readonly.cc
        db_iter.cc
        event_logger_helpers.cc
        experimental.cc
        filename.cc
        file_indexer.cc
        flush_job.cc
        flush_scheduler.cc
        forward_iterator.cc
        internal_stats.cc
        log_reader.cc
        log_writer.cc
        managed_iterator.cc
        memtable.cc
        memtable_allocator.cc
        memtable_list.cc
        merge_helper.cc
        merge_operator.cc
        repair.cc
        slice.cc
        table_cache.cc
        table_properties_collector.cc
        transaction_log_impl.cc
        version_builder.cc
        version_edit.cc
        version_set.cc
        wal_manager.cc
        write_batch.cc
        write_batch_base.cc
        write_controller.cc
        write_thread.cc
)

add_library(rocksdblib ${SOURCES})

add_library(rocksdblib_je ${SOURCES})
set_target_properties(rocksdblib_je PROPERTIES COMPILE_FLAGS ${JEMALLOC_CXX_FLAGS})

set(APPS db_bench.cc memtablerep_bench.cc)

set(TESTS
        c_test.c
        column_family_test.cc
        compact_files_test.cc
        compaction_job_test.cc
        compaction_picker_test.cc
        comparator_db_test.cc
        corruption_test.cc
        cuckoo_table_db_test.cc
        db_iter_test.cc
        db_test.cc
        dbformat_test.cc
        deletefile_test.cc
        fault_injection_test.cc
        file_indexer_test.cc
        filename_test.cc
        flush_job_test.cc
        listener_test.cc
        log_test.cc
        memtable_list_test.cc
        merge_test.cc
        perf_context_test.cc
        plain_table_db_test.cc
        prefix_test.cc
        skiplist_test.cc
        table_properties_collector_test.cc
        version_builder_test.cc
        version_edit_test.cc
        version_set_test.cc
        wal_manager_test.cc
        write_batch_test.cc
        write_controller_test.cc
)

set(EXES ${APPS} ${TESTS})

foreach(sourcefile ${EXES})
    string(REPLACE ".cc" "" exename ${sourcefile})
    string(REPLACE ".c" "" exename ${exename})
    add_executable(${exename} ${sourcefile})
    target_link_libraries(${exename} ${LIBS})
    add_executable(${exename}_je ${sourcefile})
    set_target_properties(${exename}_je PROPERTIES COMPILE_FLAGS ${JEMALLOC_CXX_FLAGS})
    target_link_libraries(${exename}_je ${LIBS_JE})
endforeach(sourcefile ${EXES})
