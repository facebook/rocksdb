add_library(rocksdb_utilities_merge_operators
                stringappend.cc
                stringappend2.cc
                ../put.cc
                ../uint64add.cc)

add_executable(stringappend_test stringappend_test.cc)
target_link_libraries(stringappend_test rocksdb_utilities_merge_operators ${LIBS})
