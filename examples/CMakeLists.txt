add_executable(simple_example simple_example.cc)
target_include_directories(simple_example PRIVATE ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(simple_example ${ROCKSDB_STATIC_LIB})

# cmake -D FAIL_ON_WARNINGS=off -D WITH_SNAPPY=on -D ENABLE_URING=on -D CMAKE_BUILD_TYPE=Debug ..
# make rocksdb
# make simple_example
