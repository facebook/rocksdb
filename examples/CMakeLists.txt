add_executable(simple_example simple_example.cc)
target_include_directories(simple_example PRIVATE ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(simple_example ${ROCKSDB_STATIC_LIB})
