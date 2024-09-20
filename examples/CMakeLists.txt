add_executable(perf-client perf/perf-client.cpp)
target_include_directories(perf-client PRIVATE ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(perf-client ${ROCKSDB_STATIC_LIB})

add_executable(perf-server perf/perf-server.cpp)
target_include_directories(perf-server PRIVATE ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(perf-server ${ROCKSDB_STATIC_LIB})