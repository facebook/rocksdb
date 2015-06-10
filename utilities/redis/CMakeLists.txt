add_library(rocksdb_utilities_redis redis_lists.cc)

add_executable(redis_lists_test redis_lists_test.cc)
target_link_libraries(redis_lists_test rocksdb_utilities_redis ${LIBS})
