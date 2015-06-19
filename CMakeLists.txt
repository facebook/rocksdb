# This cmake build is for Windows only
# Set environment variable PACKAGES_HOME to point to your Pacman packages root (Unix style dir separators)
# Use cmake -G "Visual Studio 12 Win64" for project file generation for Windows
# Then use msbuild ALL_BUILD.vcxproj to build the debug version of the software or
# msbuild ALL_BUILD.vcxproj /p:Configuration=Release to build the release version

cmake_minimum_required(VERSION 2.6)
project(rocksdb)

execute_process(COMMAND $ENV{COMSPEC} " /C date /T" OUTPUT_VARIABLE DATE)
execute_process(COMMAND $ENV{COMSPEC} " /C time /T" OUTPUT_VARIABLE TIME)
string(REGEX REPLACE "(..)/(..)/..(..).*" "\\1/\\2/\\3" DATE ${DATE})
string(REGEX REPLACE "(..):(.....).*" " \\1:\\2" TIME ${TIME})
string(CONCAT GIT_DATE_TIME ${DATE} ${TIME})

execute_process(COMMAND $ENV{COMSPEC} " /C git rev-parse HEAD 2>nil" OUTPUT_VARIABLE GIT_SHA)
string(REGEX REPLACE "[^0-9a-f]+" "" GIT_SHA ${GIT_SHA})

set(BUILD_VERSION_CC ${CMAKE_CURRENT_SOURCE_DIR}/util/build_version.cc)

add_custom_command(OUTPUT ${BUILD_VERSION_CC}
    COMMAND echo "#include \"build_version.h\"" > ${BUILD_VERSION_CC}
    COMMAND echo "const char* rocksdb_build_git_sha = \"rocksdb_build_git_sha:${GIT_SHA}\";" >> ${BUILD_VERSION_CC}
    COMMAND echo "const char* rocksdb_build_git_datetime = \"rocksdb_build_git_datetime:${GIT_DATE_TIME}\";" >> ${BUILD_VERSION_CC}
    COMMAND echo const char* rocksdb_build_compile_date = __DATE__\; >> ${BUILD_VERSION_CC}
)

add_custom_target(GenerateBuildVersion DEPENDS ${BUILD_VERSION_CC})

set(GFLAGS_HOME $ENV{PACKAGES_HOME}/Gflags.Library)
set(GFLAGS_CXX_FLAGS -DGFLAGS=gflags)
set(GFLAGS_INCLUDE ${GFLAGS_HOME}/inc/include)
set(GFLAGS_LIBS debug ${GFLAGS_HOME}/bin/debug/amd64/gflags.lib optimized ${GFLAGS_HOME}/bin/retail/amd64/gflags.lib)

set(SNAPPY_HOME $ENV{PACKAGES_HOME}/Snappy.Library)
set(SNAPPY_CXX_FLAGS -DSNAPPY)
set(SNAPPY_INCLUDE ${SNAPPY_HOME}/inc/inc)
set(SNAPPY_LIBS debug ${SNAPPY_HOME}/bin/debug/amd64/snappy.lib optimized ${SNAPPY_HOME}/bin/retail/amd64/snappy.lib)

set(JEMALLOC_HOME $ENV{PACKAGES_HOME}/Jemalloc.Library)
set(JEMALLOC_CXX_FLAGS -DJEMALLOC)
set(JEMALLOC_INCLUDE ${JEMALLOC_HOME}/inc/include)
set(JEMALLOC_LIBS debug ${JEMALLOC_HOME}/bin/debug/amd64/jemalloc.lib optimized ${JEMALLOC_HOME}/bin/retail/amd64/jemalloc.lib)

add_definitions(${GFLAGS_CXX_FLAGS} ${SNAPPY_CXX_FLAGS})
include_directories(${GFLAGS_INCLUDE} ${SNAPPY_INCLUDE} ${JEMALLOC_INCLUDE})
set (THIRDPARTY_LIBS ${GFLAGS_LIBS} ${SNAPPY_LIBS} ${JEMALLOC_LIBS})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Zi /nologo /W3 /WX /EHsc /GS /fp:precise /Zc:wchar_t /Zc:forScope /Gd /TP /errorReport:queue")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /FC /d2Zi+ /wd4018 /wd4100 /wd4101 /wd4127 /wd4189 /wd4200 /wd4244 /wd4267 /wd4296 /wd4305 /wd4307 /wd4309 /wd4512 /wd4701 /wd4702 /wd4800 /wd4804 /wd4996" )

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /Od /RTC1 /Gm /MDd")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /O2 /Oi /Gm- /Gy /MD")

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /DEBUG")

add_definitions(-DWIN32 -DOS_WIN -D_MBCS -DWIN64)

include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/port)
include_directories(${PROJECT_SOURCE_DIR}/third-party/gtest-1.7.0/fused-src)

set(ROCKSDB_LIBS rocksdblib)
set(ROCKSDB_LIBS_JE rocksdblib_je)
set(THIRDPARTY_LIBS ${THIRDPARTY_LIBS} gtest)
set(SYSTEM_LIBS Shlwapi.lib Rpcrt4.lib)

set(LIBS ${ROCKSDB_LIBS} ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})
set(LIBS_JE ${ROCKSDB_LIBS_JE} ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})

add_subdirectory(third-party/gtest-1.7.0/fused-src/gtest)

set(SOURCES
        db/builder.cc
        db/c.cc
        db/column_family.cc
        db/compaction.cc
        db/compaction_job.cc
        db/compaction_picker.cc
        db/dbformat.cc
        db/db_filesnapshot.cc
        db/db_impl.cc
        db/db_impl_debug.cc
        db/db_impl_experimental.cc
        db/db_impl_readonly.cc
        db/db_iter.cc
        db/event_logger_helpers.cc
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
        port/stack_trace.cc
        port/win/env_win.cc
        port/win/port_win.cc
        port/win/win_logger.cc
        table/adaptive_table_factory.cc
        table/block.cc
        table/block_based_filter_block.cc
        table/block_based_table_builder.cc
        table/block_based_table_factory.cc
        table/block_based_table_reader.cc
        table/block_builder.cc
        table/block_hash_index.cc
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
        table/meta_blocks.cc
        table/mock_table.cc
        table/plain_table_builder.cc
        table/plain_table_factory.cc
        table/plain_table_index.cc
        table/plain_table_key_coding.cc
        table/plain_table_reader.cc
        table/table_properties.cc
        table/two_level_iterator.cc
        util/arena.cc
        util/auto_roll_logger.cc
        util/bloom.cc
        util/build_version.cc
        util/cache.cc
        util/coding.cc
        util/comparator.cc
        util/crc32c.cc
        util/db_info_dumper.cc
        util/dynamic_bloom.cc
        util/env.cc
        util/env_hdfs.cc
        util/event_logger.cc
        util/file_util.cc
        util/filter_policy.cc
        util/hash.cc
        util/hash_cuckoo_rep.cc
        util/hash_linklist_rep.cc
        util/hash_skiplist_rep.cc
        util/histogram.cc
        util/instrumented_mutex.cc
        util/iostats_context.cc
        util/ldb_cmd.cc
        util/ldb_tool.cc
        util/logging.cc
        util/log_buffer.cc
        util/memenv.cc
        util/mock_env.cc
        util/murmurhash.cc
        util/mutable_cf_options.cc
        util/options.cc
        util/options_builder.cc
        util/options_helper.cc
        util/perf_context.cc
        util/rate_limiter.cc
        util/skiplistrep.cc
        util/slice.cc
        util/sst_dump_tool.cc
        util/statistics.cc
        util/status.cc
        util/string_util.cc
        util/sync_point.cc
        util/testharness.cc
        util/testutil.cc
        util/thread_status_impl.cc
        util/thread_status_updater.cc
        util/thread_status_updater_debug.cc
        util/thread_status_util.cc
        util/thread_status_util_debug.cc
        util/vectorrep.cc
        util/xfunc.cc
        util/xxhash.cc
        util/thread_local.cc
        utilities/backupable/backupable_db.cc
        utilities/checkpoint/checkpoint.cc
        utilities/compacted_db/compacted_db_impl.cc
        utilities/convenience/convenience.cc
        utilities/document/document_db.cc
        utilities/document/json_document.cc
        utilities/document/json_document_builder.cc
        utilities/flashcache/flashcache.cc
        utilities/geodb/geodb_impl.cc
        utilities/leveldb_options/leveldb_options.cc
        utilities/merge_operators/string_append/stringappend.cc
        utilities/merge_operators/string_append/stringappend2.cc
        utilities/merge_operators/put.cc
        utilities/merge_operators/uint64add.cc
        utilities/redis/redis_lists.cc
        utilities/spatialdb/spatial_db.cc
        utilities/ttl/db_ttl_impl.cc
        utilities/write_batch_with_index/write_batch_with_index.cc
        utilities/write_batch_with_index/write_batch_with_index_internal.cc
)

add_library(rocksdblib ${SOURCES})
add_dependencies(rocksdblib GenerateBuildVersion)

add_library(rocksdblib_je ${SOURCES})
set_target_properties(rocksdblib_je PROPERTIES COMPILE_FLAGS ${JEMALLOC_CXX_FLAGS})
add_dependencies(rocksdblib_je GenerateBuildVersion)

add_library(rocksdb SHARED ${SOURCES})
set_target_properties(rocksdb PROPERTIES COMPILE_FLAGS "-DROCKSDB_DLL -DROCKSDB_LIBRARY_EXPORTS")
add_dependencies(rocksdb GenerateBuildVersion)
target_link_libraries(rocksdb ${LIBS})

add_library(rocksdb_je SHARED ${SOURCES})
set_target_properties(rocksdb_je PROPERTIES COMPILE_FLAGS "${JEMALLOC_CXX_FLAGS} -DROCKSDB_DLL -DROCKSDB_LIBRARY_EXPORTS")
add_dependencies(rocksdb_je GenerateBuildVersion)
target_link_libraries(rocksdb_je ${LIBS_JE})

set(APPS
        db/db_bench.cc
        db/memtablerep_bench.cc
        table/table_reader_bench.cc
        tools/db_stress.cc
        tools/db_repl_stress.cc
        tools/sst_dump.cc
        util/cache_bench.cc
)

set(TESTS
        db/c_test.c
        db/column_family_test.cc
        db/compact_files_test.cc
        db/compaction_job_test.cc
        db/compaction_picker_test.cc
        db/comparator_db_test.cc
        db/corruption_test.cc
        db/cuckoo_table_db_test.cc
        db/db_iter_test.cc
        db/db_test.cc
        db/dbformat_test.cc
        db/deletefile_test.cc
        db/fault_injection_test.cc
        db/file_indexer_test.cc
        db/filename_test.cc
        db/flush_job_test.cc
        db/listener_test.cc
        db/log_test.cc
        db/memtable_list_test.cc
        db/merge_test.cc
        db/perf_context_test.cc
        db/plain_table_db_test.cc
        db/prefix_test.cc
        db/skiplist_test.cc
        db/table_properties_collector_test.cc
        db/version_builder_test.cc
        db/version_edit_test.cc
        db/version_set_test.cc
        db/wal_manager_test.cc
        db/write_batch_test.cc
        db/write_controller_test.cc
        table/block_based_filter_block_test.cc
        table/block_hash_index_test.cc
        table/block_test.cc
        table/cuckoo_table_builder_test.cc
        table/cuckoo_table_reader_test.cc
        table/full_filter_block_test.cc
        table/merger_test.cc
        table/table_test.cc
        tools/db_sanity_test.cc
        tools/reduce_levels_test.cc
        util/arena_test.cc
        util/autovector_test.cc
        util/auto_roll_logger_test.cc
        util/bloom_test.cc
        util/cache_test.cc
        util/coding_test.cc
        util/crc32c_test.cc
        util/dynamic_bloom_test.cc
        util/env_test.cc
        util/event_logger_test.cc
        util/filelock_test.cc
        util/histogram_test.cc
        util/manual_compaction_test.cc
        util/memenv_test.cc
        util/mock_env_test.cc
        util/options_test.cc
        util/rate_limiter_test.cc
        util/slice_transform_test.cc
        util/sst_dump_test.cc
        util/thread_list_test.cc
        util/thread_local_test.cc
        utilities/backupable/backupable_db_test.cc
        utilities/document/document_db_test.cc
        utilities/document/json_document_test.cc
        utilities/geodb/geodb_test.cc
        utilities/merge_operators/string_append/stringappend_test.cc
        utilities/redis/redis_lists_test.cc
        utilities/spatialdb/spatial_db_test.cc
        utilities/ttl/ttl_test.cc
        utilities/write_batch_with_index/write_batch_with_index_test.cc
)

set(EXES ${APPS} ${TESTS})

foreach(sourcefile ${EXES})
    string(REPLACE ".cc" "" exename ${sourcefile})
    string(REPLACE ".c" "" exename ${exename})
    string(REGEX REPLACE "^((.+)/)+" "" exename ${exename})
    add_executable(${exename} ${sourcefile})
    target_link_libraries(${exename} ${LIBS})
    add_executable(${exename}_je ${sourcefile})
    set_target_properties(${exename}_je PROPERTIES COMPILE_FLAGS ${JEMALLOC_CXX_FLAGS})
    target_link_libraries(${exename}_je ${LIBS_JE})
endforeach(sourcefile ${EXES})
