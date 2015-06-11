cmake_minimum_required(VERSION 2.6)
project(ROCKSDB)

# Use cmake -G "Visual Studio 12 Win64" for project file generation for Windows

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Zi /nologo /W3 /WX /Od /Gm /EHsc /RTC1 /MDd /GS /fp:precise /Zc:wchar_t /Zc:forScope /Gd /TP /errorReport:queue")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /FC /d2Zi+ /wd4018 /wd4100 /wd4101 /wd4127 /wd4189 /wd4200 /wd4244 /wd4267 /wd4296 /wd4305 /wd4307 /wd4309 /wd4512 /wd4701 /wd4702 /wd4800 /wd4804 /wd4996" )

add_definitions(-DWIN32 -DOS_WIN -D_MBCS -DWIN64)
#add_definitions(-DSNAPPY)

include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/port)
include_directories(${PROJECT_SOURCE_DIR}/third-party/gtest-1.7.0/fused-src)

set (ROCKSDBLIBS rocksdblib rocksdb_port_win rocksdb_table rocksdb_util)
set (ROCKSDBUTILITIESLIBS
        rocksdb_utilities_compacted_db
        rocksdb_utilities_convenience
        rocksdb_utilities_merge_operators
        rocksdb_utilities_ttl
        rocksdb_utilities_write_batch_with_index)
set (THIRDPARTYLIBS gtest)
set (SYSTEMLIBS ${SDK_LIB_PATH}/Shlwapi.lib ${SDK_LIB_PATH}/Rpcrt4.lib)


set (LIBS ${ROCKSDBLIBS} ${ROCKSDBUTILITIESLIBS} ${THIRDPARTYLIBS} ${SYSTEMLIBS})

add_subdirectory(third-party/gtest-1.7.0/fused-src/gtest)
add_subdirectory(db)
add_subdirectory(port)
add_subdirectory(table)
add_subdirectory(util)
add_subdirectory(utilities)

