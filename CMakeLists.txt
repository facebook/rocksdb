cmake_minimum_required(VERSION 2.6)
project(ROCKSDB)

# This cmake build is for Windows only
# Set environment variable PACKAGES_HOME to point to your Pacman packages root (Unix style dir separators)
# Use cmake -G "Visual Studio 12 Win64" for project file generation for Windows
# Then use msbuild ALL_BUILD.vcxproj to build the debug version of the software or
# msbuild ALL_BUILD.vcxproj /p:Configuration=Release to build the release version

set(GFLAGS_HOME $ENV{PACKAGES_HOME}/Gflags.Library)
set(GFLAGS_CXX_FLAGS -DGFLAGS=gflags)
set(GFLAGS_INCLUDE ${GFLAGS_HOME}/inc/include)
set(GFLAGS_LIBS debug ${GFLAGS_HOME}/bin/debug/amd64/gflags.lib optimized ${GFLAGS_HOME}/bin/retail/amd64/gflags.lib)

set(SNAPPY_HOME $ENV{PACKAGES_HOME}/Snappy.Library)
set(SNAPPY_CXX_FLAGS -DSNAPPY)
set(SNAPPY_INCLUDE ${SNAPPY_HOME}/inc/inc)
set(SNAPPY_LIBS debug ${SNAPPY_HOME}/bin/debug/amd64/snappy.lib optimized ${SNAPPY_HOME}/bin/retail/amd64/snappy.lib)

add_definitions(${GFLAGS_CXX_FLAGS} ${SNAPPY_CXX_FLAGS})
include_directories(${GFLAGS_INCLUDE} ${SNAPPY_INCLUDE})
set (THIRDPARTY_LIBS ${GFLAGS_LIBS} ${SNAPPY_LIBS})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Zi /nologo /W3 /WX /EHsc /GS /fp:precise /Zc:wchar_t /Zc:forScope /Gd /TP /errorReport:queue")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /FC /d2Zi+ /wd4018 /wd4100 /wd4101 /wd4127 /wd4189 /wd4200 /wd4244 /wd4267 /wd4296 /wd4305 /wd4307 /wd4309 /wd4512 /wd4701 /wd4702 /wd4800 /wd4804 /wd4996" )

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /Od /RTC1 /Gm /MDd")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /O2 /Oi /GL /Gm- /Gy /MD")

add_definitions(-DWIN32 -DOS_WIN -D_MBCS -DWIN64)

include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/port)
include_directories(${PROJECT_SOURCE_DIR}/third-party/gtest-1.7.0/fused-src)

set (ROCKSDB_LIBS rocksdblib rocksdb_port_win rocksdb_table rocksdb_util rocksdb_utilities)
set (THIRDPARTY_LIBS ${THIRDPARTY_LIBS} gtest)
set (SYSTEM_LIBS Shlwapi.lib Rpcrt4.lib)

set (LIBS ${ROCKSDB_LIBS} ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})

add_subdirectory(third-party/gtest-1.7.0/fused-src/gtest)
add_subdirectory(db)
add_subdirectory(port)
add_subdirectory(table)
add_subdirectory(util)
add_subdirectory(utilities)
