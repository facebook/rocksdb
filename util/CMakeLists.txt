set(SOURCES
        arena.cc
        auto_roll_logger.cc
        bloom.cc
        build_version.cc
        cache.cc
        coding.cc
        comparator.cc
        crc32c.cc
        db_info_dumper.cc
        dynamic_bloom.cc
        env.cc
        env_hdfs.cc
        event_logger.cc
        file_util.cc
        filter_policy.cc
        hash.cc
        hash_cuckoo_rep.cc
        hash_linklist_rep.cc
        hash_skiplist_rep.cc
        histogram.cc
        instrumented_mutex.cc
        iostats_context.cc
        ldb_cmd.cc
        ldb_tool.cc
        logging.cc
        log_buffer.cc
        memenv.cc
        mock_env.cc
        murmurhash.cc
        mutable_cf_options.cc
        options.cc
        options_builder.cc
        options_helper.cc
        perf_context.cc
        rate_limiter.cc
        skiplistrep.cc
        slice.cc
        sst_dump_tool.cc
        statistics.cc
        status.cc
        string_util.cc
        sync_point.cc
        testharness.cc
        testutil.cc
        thread_status_impl.cc
        thread_status_updater.cc
        thread_status_updater_debug.cc
        thread_status_util.cc
        thread_status_util_debug.cc
        vectorrep.cc
        xfunc.cc
        xxhash.cc
        thread_local.cc
)

add_library(rocksdb_util ${SOURCES})

add_library(rocksdb_util_je ${SOURCES})
set_target_properties(rocksdb_util_je PROPERTIES COMPILE_FLAGS ${JEMALLOC_CXX_FLAGS})

set(APPS cache_bench.cc)

set(TESTS
        arena_test.cc
        autovector_test.cc
        auto_roll_logger_test.cc
        bloom_test.cc
        cache_test.cc
        coding_test.cc
        crc32c_test.cc
        dynamic_bloom_test.cc
        env_test.cc
        event_logger_test.cc
        filelock_test.cc
        histogram_test.cc
        manual_compaction_test.cc
        memenv_test.cc
        mock_env_test.cc
        options_test.cc
        rate_limiter_test.cc
        slice_transform_test.cc
        sst_dump_test.cc
        thread_list_test.cc
        thread_local_test.cc
)

set(EXES ${APPS} ${TESTS})

foreach(sourcefile ${EXES})
    string(REPLACE ".cc" "" exename ${sourcefile})
    add_executable(${exename} ${sourcefile})
    target_link_libraries(${exename} ${LIBS})
    add_executable(${exename}_je ${sourcefile})
    set_target_properties(${exename}_je PROPERTIES COMPILE_FLAGS ${JEMALLOC_CXX_FLAGS})
    target_link_libraries(${exename}_je ${LIBS_JE})
endforeach(sourcefile ${EXES})
