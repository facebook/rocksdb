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

#
# Refactor it!
#

add_executable(backupable_db_test backupable/backupable_db_test.cc)
target_link_libraries(backupable_db_test ${LIBS})
add_executable(backupable_db_test_je backupable/backupable_db_test.cc)
set_target_properties(backupable_db_test_je PROPERTIES COMPILE_FLAGS ${JEMALLOC_CXX_FLAGS})
target_link_libraries(backupable_db_test_je ${LIBS_JE})

#add_executable(document_db_test document/document_db_test.cc)
#target_link_libraries(document_db_test ${LIBS})

#add_executable(json_document_test document/json_document_test.cc)
#target_link_libraries(json_document_test ${LIBS})

add_executable(geodb_test geodb/geodb_test.cc)
target_link_libraries(geodb_test ${LIBS})

add_executable(stringappend_test merge_operators/string_append/stringappend_test.cc)
target_link_libraries(stringappend_test ${LIBS})

add_executable(redis_lists_test redis/redis_lists_test.cc)
target_link_libraries(redis_lists_test ${LIBS})

#add_executable(spatial_db_test spatialdb/spatial_db_test.cc)
#target_link_libraries(spatial_db_test ${LIBS})

add_executable(ttl_test ttl/ttl_test.cc)
target_link_libraries(ttl_test ${LIBS})

add_executable(write_batch_with_index_test write_batch_with_index/write_batch_with_index_test.cc)
target_link_libraries(write_batch_with_index_test ${LIBS})
