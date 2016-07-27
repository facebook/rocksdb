set(CMAKE_CXX_FLAGS "-DOS_LINUX -DROCKSDB_PLATFORM_POSIX -DGFLAGS=gflags")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DROCKSDB_LIB_IO_POSIX")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DZLIB -DBZIP2 -DSNAPPY -DLZ4")

if (CMAKE_COMPILER_IS_GNUCXX)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-memcmp")
endif (CMAKE_COMPILER_IS_GNUCXX)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse -msse4.2 -Woverloaded-virtual")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wnon-virtual-dtor")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-field-initializers")

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} \
    -fno-omit-frame-pointer -momit-leaf-frame-pointer -DNDEBUG")

add_library(rocksdb-static STATIC ${SOURCES})
add_library(rocksdb-shared SHARED ${SOURCES})
set_target_properties(rocksdb-shared PROPERTIES
                      VERSION ${ROCKSDB_VERSION}
                      SOVERSION ${ROCKSDB_VERSION_MAJOR})

set_target_properties(rocksdb-static PROPERTIES
                      LINKER_LANGUAGE CXX
                      OUTPUT_NAME "rocksdb")
set_target_properties(rocksdb-shared PROPERTIES
                      LINKER_LANGUAGE CXX
                      OUTPUT_NAME "rocksdb")

set_property(TARGET rocksdb-static PROPERTY CXX_STANDARD 11)
set_property(TARGET rocksdb-shared PROPERTY CXX_STANDARD 11)

install(TARGETS rocksdb-static COMPONENT devel ARCHIVE DESTINATION lib64)
install(TARGETS rocksdb-shared COMPONENT runtime DESTINATION lib64)
install(DIRECTORY "${PROJECT_SOURCE_DIR}/include/rocksdb/"
        COMPONENT devel
        DESTINATION include/rocksdb)

# TODO: Figure out how to package all binaries, not just "ldb"
install(TARGETS ${EXES} RUNTIME
        DESTINATION bin
        COMPONENT tools)

foreach(sourcefile ${APPS})
    string(REPLACE ".cc" "" exename ${sourcefile})
    string(REGEX REPLACE "^((.+)/)+" "" exename ${exename})
    add_executable(${exename}${ARTIFACT_SUFFIX} ${sourcefile})
    list(APPEND EXES ${exename}${ARTIFACT_SUFFIX})
    if (JEMALLOC_FOUND)
    target_link_libraries(${exename}${ARTIFACT_SUFFIX} ${JEMALLOC_LIBRARIES})
    endif (JEMALLOC_FOUND)
    target_link_libraries(${exename}${ARTIFACT_SUFFIX} rocksdb-shared gtest gflags
                          rt snappy bz2 lz4 z
                          ${CMAKE_THREAD_LIBS_INIT})
    set_property(TARGET ${exename}${ARTIFACT_SUFFIX} PROPERTY CXX_STANDARD 11)
endforeach(sourcefile ${EXES})


# Packaging
set(CPACK_PACKAGE_CONTACT "rocksdb@fb.com")
set(CPACK_PACKAGE_VERSION "${ROCKSDB_VERSION}")
set(CPACK_RPM_PACKAGE_RELEASE "1")
set(CPACK_RPM_COMPONENT_INSTALL ON)
include(CPack)

set(CMAKE_INSTALL_PREFIX /usr)

# Debian packaging
set(DEB_PACKAGE_COMPONENTS tools devel runtime)

set(DEB_PACKAGE_tools_NAME "${CMAKE_PROJECT_NAME}-tools")
set(DEB_PACKAGE_tools_DEPENDS "lib${CMAKE_PROJECT_NAME}")
set(DEB_PACKAGE_tools_DESRCIPTION "RocksDB tools package")

set(DEB_PACKAGE_runtime_NAME "lib${CMAKE_PROJECT_NAME}")
set(DEB_PACKAGE_runtime_DESRCIPTION "RocksDB package")

set(DEB_PACKAGE_devel_NAME "lib${CMAKE_PROJECT_NAME}-dev")
set(DEB_PACKAGE_devel_DESRCIPTION "RocksDB dev package")

include(deb_packaging.cmake)
