add_library(rocksdb_utilities_ttl db_ttl_impl.cc)

add_executable(ttl_test ttl_test.cc)
target_link_libraries(ttl_test rocksdb_utilities_ttl ${LIBS})
