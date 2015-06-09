cmake_minimum_required(VERSION 2.6)
project(ROCKSDB)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /FC /d2Zi+ /wd4018 /wd4100 /wd4101 /wd4127 /wd4189 /wd4200 /wd4244 /wd4267 /wd4296 /wd4305 /wd4307 /wd4309 /wd4512 /wd4701 /wd4702 /wd4800 /wd4804 /wd4996" )

add_definitions(-DWIN32 -DOS_WIN)

include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/port)

set (ROCKSDBLIBS rocksdblib rocksdb_port_win rocksdb_table rocksdb_util)
set (THIRDPARTYLIBS )
set (LIBS ${ROCKSDBLIBS} ${THIRDPARTYLIBS})

add_subdirectory(db)
add_subdirectory(port)
add_subdirectory(table)
add_subdirectory(util)
add_subdirectory(utilities)

