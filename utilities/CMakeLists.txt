set(SOURCES
        backupable/backupable_db.cc
        checkpoint/checkpoint.cc
        compacted_db/compacted_db_impl.cc
        convenience/convenience.cc
        document/document_db.cc
        document/json_document.cc
        document/json_document_builder.cc
        flashcache/flashcache.cc
        geodb/geodb_impl.cc
        leveldb_options/leveldb_options.cc
        merge_operators/string_append/stringappend.cc
        merge_operators/string_append/stringappend2.cc
        merge_operators/put.cc
        merge_operators/uint64add.cc
        redis/redis_lists.cc
        spatialdb/spatial_db.cc
        ttl/db_ttl_impl.cc
        write_batch_with_index/write_batch_with_index.cc
        write_batch_with_index/write_batch_with_index_internal.cc
)

add_library(rocksdb_utilities ${SOURCES})

add_library(rocksdb_utilities_je ${SOURCES})
set_target_properties(rocksdb_utilities_je PROPERTIES COMPILE_FLAGS ${JEMALLOC_CXX_FLAGS})

set(APPS )

set(TESTS
        backupable/backupable_db_test.cc
        document/document_db_test.cc
        document/json_document_test.cc
        geodb/geodb_test.cc
        merge_operators/string_append/stringappend_test.cc
        redis/redis_lists_test.cc
        spatialdb/spatial_db_test.cc
        ttl/ttl_test.cc
        write_batch_with_index/write_batch_with_index_test.cc
)

set(EXES ${APPS} ${TESTS})

foreach(sourcefile ${EXES})
    string(REPLACE ".cc" "" exename ${sourcefile})
    string(REGEX REPLACE "^((.+)/)+" "" exename ${exename})
    add_executable(${exename} ${sourcefile})
    target_link_libraries(${exename} ${LIBS})
    add_executable(${exename}_je ${sourcefile})
    set_target_properties(${exename}_je PROPERTIES COMPILE_FLAGS ${JEMALLOC_CXX_FLAGS})
    target_link_libraries(${exename}_je ${LIBS_JE})
endforeach(sourcefile ${EXES})
