add_executable(simple_example simple_example.cc)
target_include_directories(simple_example PRIVATE ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(simple_example ${ROCKSDB_STATIC_LIB})

add_executable(hang-poc hang-poc.cpp)
target_include_directories(hang-poc PRIVATE ${PROJECT_SOURCE_DIR}/include ${PHOTON_INCLUDE_DIR})
target_link_libraries(hang-poc photon_static)