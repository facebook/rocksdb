add_library(rocksdb_utilities_write_batch_with_index
                write_batch_with_index.cc
                write_batch_with_index_internal.cc)

add_executable(write_batch_with_index_test write_batch_with_index_test.cc)
target_link_libraries(write_batch_with_index_test rocksdb_utilities_write_batch_with_index ${LIBS})
