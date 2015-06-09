add_library(rocksdb_utilities_geodb geodb_impl.cc)

add_executable(geodb_test geodb_test.cc)
target_link_libraries(geodb_test rocksdb_utilities_geodb ${LIBS})
