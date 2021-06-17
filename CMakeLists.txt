# Prerequisites for Windows:
#     This cmake build is for Windows 64-bit only.
#
# Prerequisites:
#     You must have at least Visual Studio 2015 Update 3. Start the Developer Command Prompt window that is a part of Visual Studio installation.
#     Run the build commands from within the Developer Command Prompt window to have paths to the compiler and runtime libraries set.
#     You must have git.exe in your %PATH% environment variable.
#
# To build Rocksdb for Windows is as easy as 1-2-3-4-5:
#
# 1. Update paths to third-party libraries in thirdparty.inc file
# 2. Create a new directory for build artifacts
#        mkdir build
#        cd build
# 3. Run cmake to generate project files for Windows, add more options to enable required third-party libraries.
#    See thirdparty.inc for more information.
#        sample command: cmake -G "Visual Studio 15 Win64" -DCMAKE_BUILD_TYPE=Release -DWITH_GFLAGS=1 -DWITH_SNAPPY=1 -DWITH_JEMALLOC=1 -DWITH_JNI=1 ..
# 4. Then build the project in debug mode (you may want to add /m[:<N>] flag to run msbuild in <N> parallel threads
#                                          or simply /m to use all avail cores)
#        msbuild rocksdb.sln
#
#        rocksdb.sln build features exclusions of test only code in Release. If you build ALL_BUILD then everything
#        will be attempted but test only code does not build in Release mode.
#
# 5. And release mode (/m[:<N>] is also supported)
#        msbuild rocksdb.sln /p:Configuration=Release
#
# Linux:
#
# 1. Install a recent toolchain such as devtoolset-3 if you're on a older distro. C++11 required.
# 2. mkdir build; cd build
# 3. cmake ..
# 4. make -j

cmake_minimum_required(VERSION 3.10)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake/modules/")
include(ReadVersion)
include(GoogleTest)
get_rocksdb_version(rocksdb_VERSION)
project(rocksdb
  VERSION ${rocksdb_VERSION}
  LANGUAGES CXX C ASM)

if(POLICY CMP0042)
  cmake_policy(SET CMP0042 NEW)
endif()

if(NOT CMAKE_BUILD_TYPE)
  if(EXISTS "${CMAKE_SOURCE_DIR}/.git")
    set(default_build_type "Debug")
  else()
    set(default_build_type "RelWithDebInfo")
  endif()
  set(CMAKE_BUILD_TYPE "${default_build_type}" CACHE STRING
    "Default BUILD_TYPE is ${default_build_type}" FORCE)
endif()

find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)

option(WITH_JEMALLOC "build with JeMalloc" OFF)
option(WITH_LIBURING "build with liburing" ON)
option(WITH_SNAPPY "build with SNAPPY" OFF)
option(WITH_LZ4 "build with lz4" OFF)
option(WITH_ZLIB "build with zlib" OFF)
option(WITH_ZSTD "build with zstd" OFF)
option(WITH_WINDOWS_UTF8_FILENAMES "use UTF8 as characterset for opening files, regardles of the system code page" OFF)
if (WITH_WINDOWS_UTF8_FILENAMES)
  add_definitions(-DROCKSDB_WINDOWS_UTF8_FILENAMES)
endif()

if ($ENV{CIRCLECI})
  message(STATUS "Build for CircieCI env, a few tests may be disabled")
  add_definitions(-DCIRCLECI)
endif()

# third-party/folly is only validated to work on Linux and Windows for now.
# So only turn it on there by default.
if(CMAKE_SYSTEM_NAME MATCHES "Linux|Windows")
  if(MSVC AND MSVC_VERSION LESS 1910)
    # Folly does not compile with MSVC older than VS2017
    option(WITH_FOLLY_DISTRIBUTED_MUTEX "build with folly::DistributedMutex" OFF)
  else()
    option(WITH_FOLLY_DISTRIBUTED_MUTEX "build with folly::DistributedMutex" ON)
  endif()
else()
  option(WITH_FOLLY_DISTRIBUTED_MUTEX "build with folly::DistributedMutex" OFF)
endif()

if( NOT DEFINED CMAKE_CXX_STANDARD )
  set(CMAKE_CXX_STANDARD 11)
endif()

include(CMakeDependentOption)

if(MSVC)
  option(WITH_GFLAGS "build with GFlags" OFF)
  option(WITH_XPRESS "build with windows built in compression" OFF)
  include(${CMAKE_CURRENT_SOURCE_DIR}/thirdparty.inc)
else()
  if(CMAKE_SYSTEM_NAME MATCHES "FreeBSD" AND NOT CMAKE_SYSTEM_NAME MATCHES "kFreeBSD")
    # FreeBSD has jemalloc as default malloc
    # but it does not have all the jemalloc files in include/...
    set(WITH_JEMALLOC ON)
  else()
    if(WITH_JEMALLOC)
      find_package(JeMalloc REQUIRED)
      add_definitions(-DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE)
      list(APPEND THIRDPARTY_LIBS JeMalloc::JeMalloc)
    endif()
  endif()

  if(MINGW)
    option(WITH_GFLAGS "build with GFlags" OFF)
  else()
    option(WITH_GFLAGS "build with GFlags" ON)
  endif()
  set(GFLAGS_LIB)
  if(WITH_GFLAGS)
    # Config with namespace available since gflags 2.2.2
    option(GFLAGS_USE_TARGET_NAMESPACE "Use gflags import target with namespace." ON)
    find_package(gflags CONFIG)
    if(gflags_FOUND)
      if(TARGET ${GFLAGS_TARGET})
        # Config with GFLAGS_TARGET available since gflags 2.2.0
        set(GFLAGS_LIB ${GFLAGS_TARGET})
      else()
        # Config with GFLAGS_LIBRARIES available since gflags 2.1.0
        set(GFLAGS_LIB ${gflags_LIBRARIES})
      endif()
    else()
      find_package(gflags REQUIRED)
      set(GFLAGS_LIB gflags::gflags)
    endif()
    include_directories(${GFLAGS_INCLUDE_DIR})
    list(APPEND THIRDPARTY_LIBS ${GFLAGS_LIB})
    add_definitions(-DGFLAGS=1)
  endif()

  if(WITH_SNAPPY)
    find_package(Snappy CONFIG)
    if(NOT Snappy_FOUND)
      find_package(Snappy REQUIRED)
    endif()
    add_definitions(-DSNAPPY)
    list(APPEND THIRDPARTY_LIBS Snappy::snappy)
  endif()

  if(WITH_ZLIB)
    find_package(ZLIB REQUIRED)
    add_definitions(-DZLIB)
    list(APPEND THIRDPARTY_LIBS ZLIB::ZLIB)
  endif()

  option(WITH_BZ2 "build with bzip2" OFF)
  if(WITH_BZ2)
    find_package(BZip2 REQUIRED)
    add_definitions(-DBZIP2)
    if(BZIP2_INCLUDE_DIRS)
      include_directories(${BZIP2_INCLUDE_DIRS})
    else()
      include_directories(${BZIP2_INCLUDE_DIR})
    endif()
    list(APPEND THIRDPARTY_LIBS ${BZIP2_LIBRARIES})
  endif()

  if(WITH_LZ4)
    find_package(lz4 REQUIRED)
    add_definitions(-DLZ4)
    list(APPEND THIRDPARTY_LIBS lz4::lz4)
  endif()

  if(WITH_ZSTD)
    find_package(zstd REQUIRED)
    add_definitions(-DZSTD)
    include_directories(${ZSTD_INCLUDE_DIR})
    list(APPEND THIRDPARTY_LIBS zstd::zstd)
  endif()
endif()

string(TIMESTAMP TS "%Y-%m-%d %H:%M:%S" UTC)
set(BUILD_DATE "${TS}" CACHE STRING "the time we first built rocksdb")

find_package(Git)

if(GIT_FOUND AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/.git")
  execute_process(WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" OUTPUT_VARIABLE GIT_SHA COMMAND "${GIT_EXECUTABLE}" rev-parse HEAD )
  execute_process(WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" RESULT_VARIABLE GIT_MOD COMMAND "${GIT_EXECUTABLE}" diff-index HEAD --quiet)
  execute_process(WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" OUTPUT_VARIABLE GIT_DATE COMMAND "${GIT_EXECUTABLE}" log -1 --date=format:"%Y-%m-%d %T" --format="%ad")
  execute_process(WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" OUTPUT_VARIABLE GIT_TAG RESULT_VARIABLE rv COMMAND "${GIT_EXECUTABLE}" symbolic-ref -q --short HEAD OUTPUT_STRIP_TRAILING_WHITESPACE)
  if (rv AND NOT rv EQUAL 0)
    execute_process(WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" OUTPUT_VARIABLE GIT_TAG COMMAND "${GIT_EXECUTABLE}" describe --tags --exact-match OUTPUT_STRIP_TRAILING_WHITESPACE)
  endif()
else()
  set(GIT_SHA 0)
  set(GIT_MOD 1)
endif()
string(REGEX REPLACE "[^0-9a-fA-F]+" "" GIT_SHA "${GIT_SHA}")
string(REGEX REPLACE "[^0-9: /-]+" "" GIT_DATE "${GIT_DATE}")

option(WITH_MD_LIBRARY "build with MD" ON)
if(WIN32 AND MSVC)
  if(WITH_MD_LIBRARY)
    set(RUNTIME_LIBRARY "MD")
  else()
    set(RUNTIME_LIBRARY "MT")
  endif()
endif()

set(BUILD_VERSION_CC ${CMAKE_BINARY_DIR}/build_version.cc)
configure_file(util/build_version.cc.in ${BUILD_VERSION_CC} @ONLY)

if(MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Zi /nologo /EHsc /GS /Gd /GR /GF /fp:precise /Zc:wchar_t /Zc:forScope /errorReport:queue")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /FC /d2Zi+ /W4 /wd4127 /wd4800 /wd4996 /wd4351 /wd4100 /wd4204 /wd4324")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W -Wextra -Wall -pthread")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wsign-compare -Wshadow -Wno-unused-parameter -Wno-unused-variable -Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers -Wno-strict-aliasing")
  if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wstrict-prototypes")
  endif()
  if(MINGW)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-format -fno-asynchronous-unwind-tables")
    add_definitions(-D_POSIX_C_SOURCE=1)
  endif()
  if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
    include(CheckCXXCompilerFlag)
    CHECK_CXX_COMPILER_FLAG("-momit-leaf-frame-pointer" HAVE_OMIT_LEAF_FRAME_POINTER)
    if(HAVE_OMIT_LEAF_FRAME_POINTER)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -momit-leaf-frame-pointer")
    endif()
  endif()
endif()

include(CheckCCompilerFlag)
if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(powerpc|ppc)64")
  CHECK_C_COMPILER_FLAG("-mcpu=power9" HAS_POWER9)
  if(HAS_POWER9)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mcpu=power9 -mtune=power9")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcpu=power9 -mtune=power9")
  else()
    CHECK_C_COMPILER_FLAG("-mcpu=power8" HAS_POWER8)
    if(HAS_POWER8)
      set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mcpu=power8 -mtune=power8")
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcpu=power8 -mtune=power8")
    endif(HAS_POWER8)
  endif(HAS_POWER9)
  CHECK_C_COMPILER_FLAG("-maltivec" HAS_ALTIVEC)
  if(HAS_ALTIVEC)
    message(STATUS " HAS_ALTIVEC yes")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -maltivec")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -maltivec")
  endif(HAS_ALTIVEC)
endif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(powerpc|ppc)64")

if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm64|aarch64|AARCH64")
        CHECK_C_COMPILER_FLAG("-march=armv8-a+crc+crypto" HAS_ARMV8_CRC)
  if(HAS_ARMV8_CRC)
    message(STATUS " HAS_ARMV8_CRC yes")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=armv8-a+crc+crypto -Wno-unused-function")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=armv8-a+crc+crypto -Wno-unused-function")
  endif(HAS_ARMV8_CRC)
endif(CMAKE_SYSTEM_PROCESSOR MATCHES "arm64|aarch64|AARCH64")

option(PORTABLE "build a portable binary" OFF)
option(FORCE_SSE42 "force building with SSE4.2, even when PORTABLE=ON" OFF)
option(FORCE_AVX "force building with AVX, even when PORTABLE=ON" OFF)
option(FORCE_AVX2 "force building with AVX2, even when PORTABLE=ON" OFF)
if(PORTABLE)
  # MSVC does not need a separate compiler flag to enable SSE4.2; if nmmintrin.h
  # is available, it is available by default.
  if(FORCE_SSE42 AND NOT MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2 -mpclmul")
  endif()
  if(MSVC)
    if(FORCE_AVX)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /arch:AVX")
    endif()
    # MSVC automatically enables BMI / lzcnt with AVX2.
    if(FORCE_AVX2)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /arch:AVX2")
    endif()
  else()
    if(FORCE_AVX)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx")
    endif()
    if(FORCE_AVX2)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2 -mbmi -mlzcnt")
    endif()
  endif()
else()
  if(MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /arch:AVX2")
  else()
    if(NOT CMAKE_SYSTEM_PROCESSOR MATCHES "^(powerpc|ppc)64" AND NOT HAS_ARMV8_CRC)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
    endif()
  endif()
endif()

include(CheckCXXSourceCompiles)
set(OLD_CMAKE_REQUIRED_FLAGS ${CMAKE_REQUIRED_FLAGS})
if(NOT MSVC)
  set(CMAKE_REQUIRED_FLAGS "-msse4.2 -mpclmul")
endif()

CHECK_CXX_SOURCE_COMPILES("
#include <cstdint>
#include <nmmintrin.h>
#include <wmmintrin.h>
int main() {
  volatile uint32_t x = _mm_crc32_u32(0, 0);
  const auto a = _mm_set_epi64x(0, 0);
  const auto b = _mm_set_epi64x(0, 0);
  const auto c = _mm_clmulepi64_si128(a, b, 0x00);
  auto d = _mm_cvtsi128_si64(c);
}
" HAVE_SSE42)
if(HAVE_SSE42)
  add_definitions(-DHAVE_SSE42)
  add_definitions(-DHAVE_PCLMUL)
elseif(FORCE_SSE42)
  message(FATAL_ERROR "FORCE_SSE42=ON but unable to compile with SSE4.2 enabled")
endif()

# Check if -latomic is required or not
if (NOT MSVC)
  set(CMAKE_REQUIRED_FLAGS "--std=c++11")
  CHECK_CXX_SOURCE_COMPILES("
#include <atomic>
std::atomic<uint64_t> x(0);
int main() {
  uint64_t i = x.load(std::memory_order_relaxed);
  bool b = x.is_lock_free();
  return 0;
}
" BUILTIN_ATOMIC)
if (NOT BUILTIN_ATOMIC)
  #TODO: Check if -latomic exists
  list(APPEND THIRDPARTY_LIBS atomic)
endif()
endif()

if (WITH_LIBURING)
  set(CMAKE_REQUIRED_FLAGS "-luring")
  CHECK_CXX_SOURCE_COMPILES("
#include <liburing.h>
int main() {
  struct io_uring ring;
  io_uring_queue_init(1, &ring, 0);
  return 0;
}
" HAS_LIBURING)
  if (HAS_LIBURING)
    add_definitions(-DROCKSDB_IOURING_PRESENT)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -luring")
  endif()
endif()

# Reset the required flags
set(CMAKE_REQUIRED_FLAGS ${OLD_CMAKE_REQUIRED_FLAGS})

CHECK_CXX_SOURCE_COMPILES("
#if defined(_MSC_VER) && !defined(__thread)
#define __thread __declspec(thread)
#endif
int main() {
  static __thread int tls;
  (void)tls;
}
" HAVE_THREAD_LOCAL)
if(HAVE_THREAD_LOCAL)
  add_definitions(-DROCKSDB_SUPPORT_THREAD_LOCAL)
endif()

option(WITH_IOSTATS_CONTEXT "Enable IO stats context" ON)
if (NOT WITH_IOSTATS_CONTEXT)
  add_definitions(-DNIOSTATS_CONTEXT)
endif()

option(WITH_PERF_CONTEXT "Enable perf context" ON)
if (NOT WITH_PERF_CONTEXT)
  add_definitions(-DNPERF_CONTEXT)
endif()

option(FAIL_ON_WARNINGS "Treat compile warnings as errors" ON)
if(FAIL_ON_WARNINGS)
  if(MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /WX")
  else() # assume GCC
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
  endif()
endif()

option(WITH_ASAN "build with ASAN" OFF)
if(WITH_ASAN)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address")
  if(WITH_JEMALLOC)
    message(FATAL "ASAN does not work well with JeMalloc")
  endif()
endif()

option(WITH_TSAN "build with TSAN" OFF)
if(WITH_TSAN)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread -pie")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -fPIC")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=thread -fPIC")
  if(WITH_JEMALLOC)
    message(FATAL "TSAN does not work well with JeMalloc")
  endif()
endif()

option(WITH_UBSAN "build with UBSAN" OFF)
if(WITH_UBSAN)
  add_definitions(-DROCKSDB_UBSAN_RUN)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=undefined")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=undefined")
  if(WITH_JEMALLOC)
    message(FATAL "UBSAN does not work well with JeMalloc")
  endif()
endif()

option(WITH_NUMA "build with NUMA policy support" OFF)
if(WITH_NUMA)
  find_package(NUMA REQUIRED)
  add_definitions(-DNUMA)
  include_directories(${NUMA_INCLUDE_DIR})
  list(APPEND THIRDPARTY_LIBS NUMA::NUMA)
endif()

option(WITH_TBB "build with Threading Building Blocks (TBB)" OFF)
if(WITH_TBB)
  find_package(TBB REQUIRED)
  add_definitions(-DTBB)
  list(APPEND THIRDPARTY_LIBS TBB::TBB)
endif()

# Stall notifications eat some performance from inserts
option(DISABLE_STALL_NOTIF "Build with stall notifications" OFF)
if(DISABLE_STALL_NOTIF)
  add_definitions(-DROCKSDB_DISABLE_STALL_NOTIFICATION)
endif()

option(WITH_DYNAMIC_EXTENSION "build with dynamic extension support" OFF)
if(NOT WITH_DYNAMIC_EXTENSION)
  add_definitions(-DROCKSDB_NO_DYNAMIC_EXTENSION)
endif()

option(ASSERT_STATUS_CHECKED "build with assert status checked" OFF)
if (ASSERT_STATUS_CHECKED)
  message(STATUS "Build with assert status checked")
  add_definitions(-DROCKSDB_ASSERT_STATUS_CHECKED)
endif()

if(DEFINED USE_RTTI)
  if(USE_RTTI)
    message(STATUS "Enabling RTTI")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DROCKSDB_USE_RTTI")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DROCKSDB_USE_RTTI")
  else()
    if(MSVC)
      message(STATUS "Disabling RTTI in Release builds. Always on in Debug.")
      set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DROCKSDB_USE_RTTI")
      set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /GR-")
    else()
      message(STATUS "Disabling RTTI in Release builds")
      set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-rtti")
      set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fno-rtti")
    endif()
  endif()
else()
  message(STATUS "Enabling RTTI in Debug builds only (default)")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DROCKSDB_USE_RTTI")
  if(MSVC)
     set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /GR-")
  else()
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fno-rtti")
  endif()
endif()

# Used to run CI build and tests so we can run faster
option(OPTDBG "Build optimized debug build with MSVC" OFF)
option(WITH_RUNTIME_DEBUG "build with debug version of runtime library" ON)
if(MSVC)
  if(OPTDBG)
    message(STATUS "Debug optimization is enabled")
    set(CMAKE_CXX_FLAGS_DEBUG "/Oxt")
  else()
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /Od /RTC1")

    # Minimal Build is deprecated after MSVC 2015
    if( MSVC_VERSION GREATER 1900 )
      set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /Gm-")
    else()
      set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /Gm")
    endif()

  endif()
  if(WITH_RUNTIME_DEBUG)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /${RUNTIME_LIBRARY}d")
  else()
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /${RUNTIME_LIBRARY}")
  endif()
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /Oxt /Zp8 /Gm- /Gy /${RUNTIME_LIBRARY}")

  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} /DEBUG")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /DEBUG")
endif()

if(CMAKE_COMPILER_IS_GNUCXX)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-memcmp")
endif()

option(ROCKSDB_LITE "Build RocksDBLite version" OFF)
if(ROCKSDB_LITE)
  add_definitions(-DROCKSDB_LITE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-exceptions -Os")
endif()

if(CMAKE_SYSTEM_NAME MATCHES "Cygwin")
  add_definitions(-fno-builtin-memcmp -DCYGWIN)
elseif(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  add_definitions(-DOS_MACOSX)
elseif(CMAKE_SYSTEM_NAME MATCHES "Linux")
  add_definitions(-DOS_LINUX)
elseif(CMAKE_SYSTEM_NAME MATCHES "SunOS")
  add_definitions(-DOS_SOLARIS)
elseif(CMAKE_SYSTEM_NAME MATCHES "kFreeBSD")
  add_definitions(-DOS_GNU_KFREEBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
  add_definitions(-DOS_FREEBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "NetBSD")
  add_definitions(-DOS_NETBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "OpenBSD")
  add_definitions(-DOS_OPENBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "DragonFly")
  add_definitions(-DOS_DRAGONFLYBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "Android")
  add_definitions(-DOS_ANDROID)
elseif(CMAKE_SYSTEM_NAME MATCHES "Windows")
  add_definitions(-DWIN32 -DOS_WIN -D_MBCS -DWIN64 -DNOMINMAX)
  if(MINGW)
    add_definitions(-D_WIN32_WINNT=_WIN32_WINNT_VISTA)
  endif()
endif()

if(NOT WIN32)
  add_definitions(-DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX)
endif()

option(WITH_FALLOCATE "build with fallocate" ON)
if(WITH_FALLOCATE)
  CHECK_CXX_SOURCE_COMPILES("
#include <fcntl.h>
#include <linux/falloc.h>
int main() {
 int fd = open(\"/dev/null\", 0);
 fallocate(fd, FALLOC_FL_KEEP_SIZE, 0, 1024);
}
" HAVE_FALLOCATE)
  if(HAVE_FALLOCATE)
    add_definitions(-DROCKSDB_FALLOCATE_PRESENT)
  endif()
endif()

CHECK_CXX_SOURCE_COMPILES("
#include <fcntl.h>
int main() {
  int fd = open(\"/dev/null\", 0);
  sync_file_range(fd, 0, 1024, SYNC_FILE_RANGE_WRITE);
}
" HAVE_SYNC_FILE_RANGE_WRITE)
if(HAVE_SYNC_FILE_RANGE_WRITE)
  add_definitions(-DROCKSDB_RANGESYNC_PRESENT)
endif()

CHECK_CXX_SOURCE_COMPILES("
#include <pthread.h>
int main() {
  (void) PTHREAD_MUTEX_ADAPTIVE_NP;
}
" HAVE_PTHREAD_MUTEX_ADAPTIVE_NP)
if(HAVE_PTHREAD_MUTEX_ADAPTIVE_NP)
  add_definitions(-DROCKSDB_PTHREAD_ADAPTIVE_MUTEX)
endif()

include(CheckCXXSymbolExists)
if(CMAKE_SYSTEM_NAME MATCHES "^FreeBSD")
  check_cxx_symbol_exists(malloc_usable_size malloc_np.h HAVE_MALLOC_USABLE_SIZE)
else()
  check_cxx_symbol_exists(malloc_usable_size malloc.h HAVE_MALLOC_USABLE_SIZE)
endif()
if(HAVE_MALLOC_USABLE_SIZE)
  add_definitions(-DROCKSDB_MALLOC_USABLE_SIZE)
endif()

check_cxx_symbol_exists(sched_getcpu sched.h HAVE_SCHED_GETCPU)
if(HAVE_SCHED_GETCPU)
  add_definitions(-DROCKSDB_SCHED_GETCPU_PRESENT)
endif()

check_cxx_symbol_exists(getauxval auvx.h HAVE_AUXV_GETAUXVAL)
if(HAVE_AUXV_GETAUXVAL)
  add_definitions(-DROCKSDB_AUXV_GETAUXVAL_PRESENT)
endif()

include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/include)
if(WITH_FOLLY_DISTRIBUTED_MUTEX)
  include_directories(${PROJECT_SOURCE_DIR}/third-party/folly)
endif()
find_package(Threads REQUIRED)

# Main library source code

set(SOURCES
        cache/cache.cc
        cache/cache_entry_roles.cc
        cache/clock_cache.cc
        cache/lru_cache.cc
        cache/sharded_cache.cc
        db/arena_wrapped_db_iter.cc
        db/blob/blob_fetcher.cc
        db/blob/blob_file_addition.cc
        db/blob/blob_file_builder.cc
        db/blob/blob_file_cache.cc
        db/blob/blob_file_garbage.cc
        db/blob/blob_file_meta.cc
        db/blob/blob_file_reader.cc
        db/blob/blob_log_format.cc
        db/blob/blob_log_sequential_reader.cc
        db/blob/blob_log_writer.cc
        db/builder.cc
        db/c.cc
        db/column_family.cc
        db/compaction/compaction.cc
        db/compaction/compaction_iterator.cc
        db/compaction/compaction_picker.cc
        db/compaction/compaction_job.cc
        db/compaction/compaction_picker_fifo.cc
        db/compaction/compaction_picker_level.cc
        db/compaction/compaction_picker_universal.cc
        db/compaction/sst_partitioner.cc
        db/convenience.cc
        db/db_filesnapshot.cc
        db/db_impl/compacted_db_impl.cc
        db/db_impl/db_impl.cc
        db/db_impl/db_impl_write.cc
        db/db_impl/db_impl_compaction_flush.cc
        db/db_impl/db_impl_files.cc
        db/db_impl/db_impl_open.cc
        db/db_impl/db_impl_debug.cc
        db/db_impl/db_impl_experimental.cc
        db/db_impl/db_impl_readonly.cc
        db/db_impl/db_impl_secondary.cc
        db/db_info_dumper.cc
        db/db_iter.cc
        db/dbformat.cc
        db/error_handler.cc
        db/event_helpers.cc
        db/experimental.cc
        db/external_sst_file_ingestion_job.cc
        db/file_indexer.cc
        db/flush_job.cc
        db/flush_scheduler.cc
        db/forward_iterator.cc
        db/import_column_family_job.cc
        db/internal_stats.cc
        db/logs_with_prep_tracker.cc
        db/log_reader.cc
        db/log_writer.cc
        db/malloc_stats.cc
        db/memtable.cc
        db/memtable_list.cc
        db/merge_helper.cc
        db/merge_operator.cc
        db/output_validator.cc
        db/periodic_work_scheduler.cc
        db/range_del_aggregator.cc
        db/range_tombstone_fragmenter.cc
        db/repair.cc
        db/snapshot_impl.cc
        db/table_cache.cc
        db/table_properties_collector.cc
        db/transaction_log_impl.cc
        db/trim_history_scheduler.cc
        db/version_builder.cc
        db/version_edit.cc
        db/version_edit_handler.cc
        db/version_set.cc
        db/wal_edit.cc
        db/wal_manager.cc
        db/write_batch.cc
        db/write_batch_base.cc
        db/write_controller.cc
        db/write_thread.cc
        env/composite_env.cc
        env/env.cc
        env/env_chroot.cc
        env/env_encryption.cc
        env/env_hdfs.cc
        env/file_system.cc
        env/file_system_tracer.cc
        env/fs_remap.cc
        env/mock_env.cc
        file/delete_scheduler.cc
        file/file_prefetch_buffer.cc
        file/file_util.cc
        file/filename.cc
        file/line_file_reader.cc
        file/random_access_file_reader.cc
        file/read_write_util.cc
        file/readahead_raf.cc
        file/sequence_file_reader.cc
        file/sst_file_manager_impl.cc
        file/writable_file_writer.cc
        logging/auto_roll_logger.cc
        logging/event_logger.cc
        logging/log_buffer.cc
        memory/arena.cc
        memory/concurrent_arena.cc
        memory/jemalloc_nodump_allocator.cc
        memory/memkind_kmem_allocator.cc
        memtable/alloc_tracker.cc
        memtable/hash_linklist_rep.cc
        memtable/hash_skiplist_rep.cc
        memtable/skiplistrep.cc
        memtable/vectorrep.cc
        memtable/write_buffer_manager.cc
        monitoring/histogram.cc
        monitoring/histogram_windowing.cc
        monitoring/in_memory_stats_history.cc
        monitoring/instrumented_mutex.cc
        monitoring/iostats_context.cc
        monitoring/perf_context.cc
        monitoring/perf_level.cc
        monitoring/persistent_stats_history.cc
        monitoring/statistics.cc
        monitoring/thread_status_impl.cc
        monitoring/thread_status_updater.cc
        monitoring/thread_status_util.cc
        monitoring/thread_status_util_debug.cc
        options/cf_options.cc
        options/configurable.cc
        options/customizable.cc
        options/db_options.cc
        options/options.cc
        options/options_helper.cc
        options/options_parser.cc
        port/stack_trace.cc
        table/adaptive/adaptive_table_factory.cc
        table/block_based/binary_search_index_reader.cc
        table/block_based/block.cc
        table/block_based/block_based_filter_block.cc
        table/block_based/block_based_table_builder.cc
        table/block_based/block_based_table_factory.cc
        table/block_based/block_based_table_iterator.cc
        table/block_based/block_based_table_reader.cc
        table/block_based/block_builder.cc
        table/block_based/block_prefetcher.cc
        table/block_based/block_prefix_index.cc
        table/block_based/data_block_hash_index.cc
        table/block_based/data_block_footer.cc
        table/block_based/filter_block_reader_common.cc
        table/block_based/filter_policy.cc
        table/block_based/flush_block_policy.cc
        table/block_based/full_filter_block.cc
        table/block_based/hash_index_reader.cc
        table/block_based/index_builder.cc
        table/block_based/index_reader_common.cc
        table/block_based/parsed_full_filter_block.cc
        table/block_based/partitioned_filter_block.cc
        table/block_based/partitioned_index_iterator.cc
        table/block_based/partitioned_index_reader.cc
        table/block_based/reader_common.cc
        table/block_based/uncompression_dict_reader.cc
        table/block_fetcher.cc
        table/cuckoo/cuckoo_table_builder.cc
        table/cuckoo/cuckoo_table_factory.cc
        table/cuckoo/cuckoo_table_reader.cc
        table/format.cc
        table/get_context.cc
        table/iterator.cc
        table/merging_iterator.cc
        table/meta_blocks.cc
        table/persistent_cache_helper.cc
        table/plain/plain_table_bloom.cc
        table/plain/plain_table_builder.cc
        table/plain/plain_table_factory.cc
        table/plain/plain_table_index.cc
        table/plain/plain_table_key_coding.cc
        table/plain/plain_table_reader.cc
        table/sst_file_dumper.cc
        table/sst_file_reader.cc
        table/sst_file_writer.cc
        table/table_factory.cc
        table/table_properties.cc
        table/two_level_iterator.cc
        test_util/sync_point.cc
        test_util/sync_point_impl.cc
        test_util/testutil.cc
        test_util/transaction_test_util.cc
        tools/block_cache_analyzer/block_cache_trace_analyzer.cc
        tools/dump/db_dump_tool.cc
        tools/io_tracer_parser_tool.cc
        tools/ldb_cmd.cc
        tools/ldb_tool.cc
        tools/sst_dump_tool.cc
        tools/trace_analyzer_tool.cc
        trace_replay/trace_replay.cc
        trace_replay/block_cache_tracer.cc
        trace_replay/io_tracer.cc
        util/coding.cc
        util/compaction_job_stats_impl.cc
        util/comparator.cc
        util/compression_context_cache.cc
        util/concurrent_task_limiter_impl.cc
        util/crc32c.cc
        util/dynamic_bloom.cc
        util/hash.cc
        util/murmurhash.cc
        util/random.cc
        util/rate_limiter.cc
        util/ribbon_config.cc
        util/slice.cc
        util/file_checksum_helper.cc
        util/status.cc
        util/string_util.cc
        util/thread_local.cc
        util/threadpool_imp.cc
        util/xxhash.cc
        utilities/backupable/backupable_db.cc
        utilities/blob_db/blob_compaction_filter.cc
        utilities/blob_db/blob_db.cc
        utilities/blob_db/blob_db_impl.cc
        utilities/blob_db/blob_db_impl_filesnapshot.cc
        utilities/blob_db/blob_dump_tool.cc
        utilities/blob_db/blob_file.cc
        utilities/cassandra/cassandra_compaction_filter.cc
        utilities/cassandra/format.cc
        utilities/cassandra/merge_operator.cc
        utilities/checkpoint/checkpoint_impl.cc
        utilities/compaction_filters/remove_emptyvalue_compactionfilter.cc
        utilities/debug.cc
        utilities/env_mirror.cc
        utilities/env_timed.cc
        utilities/fault_injection_env.cc
        utilities/fault_injection_fs.cc
        utilities/leveldb_options/leveldb_options.cc
        utilities/memory/memory_util.cc
        utilities/merge_operators/bytesxor.cc
        utilities/merge_operators/max.cc
        utilities/merge_operators/put.cc
        utilities/merge_operators/sortlist.cc
        utilities/merge_operators/string_append/stringappend.cc
        utilities/merge_operators/string_append/stringappend2.cc
        utilities/merge_operators/uint64add.cc
        utilities/object_registry.cc
        utilities/option_change_migration/option_change_migration.cc
        utilities/options/options_util.cc
        utilities/persistent_cache/block_cache_tier.cc
        utilities/persistent_cache/block_cache_tier_file.cc
        utilities/persistent_cache/block_cache_tier_metadata.cc
        utilities/persistent_cache/persistent_cache_tier.cc
        utilities/persistent_cache/volatile_tier_impl.cc
        utilities/simulator_cache/cache_simulator.cc
        utilities/simulator_cache/sim_cache.cc
        utilities/table_properties_collectors/compact_on_deletion_collector.cc
        utilities/trace/file_trace_reader_writer.cc
        utilities/transactions/lock/lock_manager.cc
        utilities/transactions/lock/point/point_lock_tracker.cc
        utilities/transactions/lock/point/point_lock_manager.cc
        utilities/transactions/lock/range/range_tree/range_tree_lock_manager.cc
        utilities/transactions/lock/range/range_tree/range_tree_lock_tracker.cc
        utilities/transactions/optimistic_transaction_db_impl.cc
        utilities/transactions/optimistic_transaction.cc
        utilities/transactions/pessimistic_transaction.cc
        utilities/transactions/pessimistic_transaction_db.cc
        utilities/transactions/snapshot_checker.cc
        utilities/transactions/transaction_base.cc
        utilities/transactions/transaction_db_mutex_impl.cc
        utilities/transactions/transaction_util.cc
        utilities/transactions/write_prepared_txn.cc
        utilities/transactions/write_prepared_txn_db.cc
        utilities/transactions/write_unprepared_txn.cc
        utilities/transactions/write_unprepared_txn_db.cc
        utilities/ttl/db_ttl_impl.cc
        utilities/write_batch_with_index/write_batch_with_index.cc
        utilities/write_batch_with_index/write_batch_with_index_internal.cc)

list(APPEND SOURCES
  utilities/transactions/lock/range/range_tree/lib/locktree/concurrent_tree.cc
  utilities/transactions/lock/range/range_tree/lib/locktree/keyrange.cc
  utilities/transactions/lock/range/range_tree/lib/locktree/lock_request.cc
  utilities/transactions/lock/range/range_tree/lib/locktree/locktree.cc
  utilities/transactions/lock/range/range_tree/lib/locktree/manager.cc
  utilities/transactions/lock/range/range_tree/lib/locktree/range_buffer.cc
  utilities/transactions/lock/range/range_tree/lib/locktree/treenode.cc
  utilities/transactions/lock/range/range_tree/lib/locktree/txnid_set.cc
  utilities/transactions/lock/range/range_tree/lib/locktree/wfg.cc
  utilities/transactions/lock/range/range_tree/lib/standalone_port.cc
  utilities/transactions/lock/range/range_tree/lib/util/dbt.cc
  utilities/transactions/lock/range/range_tree/lib/util/memarena.cc)

if(HAVE_SSE42 AND NOT MSVC)
  set_source_files_properties(
    util/crc32c.cc
    PROPERTIES COMPILE_FLAGS "-msse4.2 -mpclmul")
endif()

if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(powerpc|ppc)64")
  list(APPEND SOURCES
    util/crc32c_ppc.c
    util/crc32c_ppc_asm.S)
endif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(powerpc|ppc)64")

if(HAS_ARMV8_CRC)
  list(APPEND SOURCES
    util/crc32c_arm64.cc)
endif(HAS_ARMV8_CRC)

if(WIN32)
  list(APPEND SOURCES
    port/win/io_win.cc
    port/win/env_win.cc
    port/win/env_default.cc
    port/win/port_win.cc
    port/win/win_logger.cc)
  if(NOT MINGW)
    # Mingw only supports std::thread when using
    # posix threads.
    list(APPEND SOURCES
      port/win/win_thread.cc)
  endif()
if(WITH_XPRESS)
  list(APPEND SOURCES
    port/win/xpress_win.cc)
endif()

if(WITH_JEMALLOC)
  list(APPEND SOURCES
    port/win/win_jemalloc.cc)
endif()

else()
  list(APPEND SOURCES
    port/port_posix.cc
    env/env_posix.cc
    env/fs_posix.cc
    env/io_posix.cc)
endif()

if(WITH_FOLLY_DISTRIBUTED_MUTEX)
  list(APPEND SOURCES
    third-party/folly/folly/detail/Futex.cpp
    third-party/folly/folly/synchronization/AtomicNotification.cpp
    third-party/folly/folly/synchronization/DistributedMutex.cpp
    third-party/folly/folly/synchronization/ParkingLot.cpp
    third-party/folly/folly/synchronization/WaitOptions.cpp)
endif()

set(ROCKSDB_STATIC_LIB rocksdb${ARTIFACT_SUFFIX})
set(ROCKSDB_SHARED_LIB rocksdb-shared${ARTIFACT_SUFFIX})

option(ROCKSDB_BUILD_SHARED "Build shared versions of the RocksDB libraries" ON)

option(WITH_LIBRADOS "Build with librados" OFF)
if(WITH_LIBRADOS)
  list(APPEND SOURCES
    utilities/env_librados.cc)
  list(APPEND THIRDPARTY_LIBS rados)
endif()

if(WIN32)
  set(SYSTEM_LIBS ${SYSTEM_LIBS} shlwapi.lib rpcrt4.lib)
else()
  set(SYSTEM_LIBS ${CMAKE_THREAD_LIBS_INIT})
endif()

add_library(${ROCKSDB_STATIC_LIB} STATIC ${SOURCES} ${BUILD_VERSION_CC})
target_link_libraries(${ROCKSDB_STATIC_LIB} PRIVATE
  ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})

if(ROCKSDB_BUILD_SHARED)
  add_library(${ROCKSDB_SHARED_LIB} SHARED ${SOURCES} ${BUILD_VERSION_CC})
  target_link_libraries(${ROCKSDB_SHARED_LIB} PRIVATE
    ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})

  if(WIN32)
    set_target_properties(${ROCKSDB_SHARED_LIB} PROPERTIES
      COMPILE_DEFINITIONS "ROCKSDB_DLL;ROCKSDB_LIBRARY_EXPORTS")
    if(MSVC)
      set_target_properties(${ROCKSDB_STATIC_LIB} PROPERTIES
        COMPILE_FLAGS "/Fd${CMAKE_CFG_INTDIR}/${ROCKSDB_STATIC_LIB}.pdb")
      set_target_properties(${ROCKSDB_SHARED_LIB} PROPERTIES
        COMPILE_FLAGS "/Fd${CMAKE_CFG_INTDIR}/${ROCKSDB_SHARED_LIB}.pdb")
    endif()
  else()
    set_target_properties(${ROCKSDB_SHARED_LIB} PROPERTIES
                          LINKER_LANGUAGE CXX
                          VERSION ${rocksdb_VERSION}
                          SOVERSION ${rocksdb_VERSION_MAJOR}
                          OUTPUT_NAME "rocksdb${ARTIFACT_SUFFIX}")
  endif()
endif()

if(ROCKSDB_BUILD_SHARED AND NOT WIN32)
  set(ROCKSDB_LIB ${ROCKSDB_SHARED_LIB})
else()
  set(ROCKSDB_LIB ${ROCKSDB_STATIC_LIB})
endif()

option(WITH_JNI "build with JNI" OFF)
# Tests are excluded from Release builds
CMAKE_DEPENDENT_OPTION(WITH_TESTS "build with tests" ON
  "CMAKE_BUILD_TYPE STREQUAL Debug" OFF)
option(WITH_BENCHMARK_TOOLS "build with benchmarks" ON)
option(WITH_CORE_TOOLS "build with ldb and sst_dump" ON)
option(WITH_TOOLS "build with tools" ON)

if(WITH_TESTS OR WITH_BENCHMARK_TOOLS OR WITH_TOOLS OR WITH_JNI OR JNI)
  include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/third-party/gtest-1.8.1/fused-src)
endif()
if(WITH_JNI OR JNI)
  message(STATUS "JNI library is enabled")
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/java)
else()
  message(STATUS "JNI library is disabled")
endif()

# Installation and packaging
if(WIN32)
  option(ROCKSDB_INSTALL_ON_WINDOWS "Enable install target on Windows" OFF)
endif()
if(NOT WIN32 OR ROCKSDB_INSTALL_ON_WINDOWS)
  if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
      # Change default installation prefix on Linux to /usr
      set(CMAKE_INSTALL_PREFIX /usr CACHE PATH "Install path prefix, prepended onto install directories." FORCE)
    endif()
  endif()

  include(GNUInstallDirs)
  include(CMakePackageConfigHelpers)

  set(package_config_destination ${CMAKE_INSTALL_LIBDIR}/cmake/rocksdb)

  configure_package_config_file(
    ${CMAKE_CURRENT_LIST_DIR}/cmake/RocksDBConfig.cmake.in RocksDBConfig.cmake
    INSTALL_DESTINATION ${package_config_destination}
  )

  write_basic_package_version_file(
    RocksDBConfigVersion.cmake
    VERSION ${rocksdb_VERSION}
    COMPATIBILITY SameMajorVersion
  )

  install(DIRECTORY include/rocksdb COMPONENT devel DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")

  install(DIRECTORY "${PROJECT_SOURCE_DIR}/cmake/modules" COMPONENT devel DESTINATION ${package_config_destination})

  install(
    TARGETS ${ROCKSDB_STATIC_LIB}
    EXPORT RocksDBTargets
    COMPONENT devel
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
  )

  if(ROCKSDB_BUILD_SHARED)
    install(
      TARGETS ${ROCKSDB_SHARED_LIB}
      EXPORT RocksDBTargets
      COMPONENT runtime
      ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
      RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
      LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
      INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
    )
  endif()

  install(
    EXPORT RocksDBTargets
    COMPONENT devel
    DESTINATION ${package_config_destination}
    NAMESPACE RocksDB::
  )

  install(
    FILES
    ${CMAKE_CURRENT_BINARY_DIR}/RocksDBConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/RocksDBConfigVersion.cmake
    COMPONENT devel
    DESTINATION ${package_config_destination}
  )
endif()

option(WITH_ALL_TESTS "Build all test, rather than a small subset" ON)

if(WITH_TESTS OR WITH_BENCHMARK_TOOLS)
  add_subdirectory(third-party/gtest-1.8.1/fused-src/gtest)
  add_library(testharness STATIC
  test_util/mock_time_env.cc
  test_util/testharness.cc)
  target_link_libraries(testharness gtest)
endif()

if(WITH_TESTS)
  set(TESTS
        db/db_basic_test.cc
        env/env_basic_test.cc
  )
  if(WITH_ALL_TESTS)
    list(APPEND TESTS
        cache/cache_test.cc
        cache/lru_cache_test.cc
        db/blob/blob_file_addition_test.cc
        db/blob/blob_file_builder_test.cc
        db/blob/blob_file_cache_test.cc
        db/blob/blob_file_garbage_test.cc
        db/blob/blob_file_reader_test.cc
        db/blob/db_blob_basic_test.cc
        db/blob/db_blob_compaction_test.cc
        db/blob/db_blob_corruption_test.cc
        db/blob/db_blob_index_test.cc
        db/column_family_test.cc
        db/compact_files_test.cc
        db/compaction/clipping_iterator_test.cc
        db/compaction/compaction_job_stats_test.cc
        db/compaction/compaction_job_test.cc
        db/compaction/compaction_iterator_test.cc
        db/compaction/compaction_picker_test.cc
        db/compaction/compaction_service_test.cc
        db/comparator_db_test.cc
        db/corruption_test.cc
        db/cuckoo_table_db_test.cc
        db/db_with_timestamp_basic_test.cc
        db/db_block_cache_test.cc
        db/db_bloom_filter_test.cc
        db/db_compaction_filter_test.cc
        db/db_compaction_test.cc
        db/db_dynamic_level_test.cc
        db/db_flush_test.cc
        db/db_inplace_update_test.cc
        db/db_io_failure_test.cc
        db/db_iter_test.cc
        db/db_iter_stress_test.cc
        db/db_iterator_test.cc
        db/db_kv_checksum_test.cc
        db/db_log_iter_test.cc
        db/db_memtable_test.cc
        db/db_merge_operator_test.cc
        db/db_merge_operand_test.cc
        db/db_options_test.cc
        db/db_properties_test.cc
        db/db_range_del_test.cc
        db/db_secondary_test.cc
        db/db_sst_test.cc
        db/db_statistics_test.cc
        db/db_table_properties_test.cc
        db/db_tailing_iter_test.cc
        db/db_test.cc
        db/db_test2.cc
        db/db_logical_block_size_cache_test.cc
        db/db_universal_compaction_test.cc
        db/db_wal_test.cc
        db/db_with_timestamp_compaction_test.cc
        db/db_write_test.cc
        db/dbformat_test.cc
        db/deletefile_test.cc
        db/error_handler_fs_test.cc
        db/obsolete_files_test.cc
        db/external_sst_file_basic_test.cc
        db/external_sst_file_test.cc
        db/fault_injection_test.cc
        db/file_indexer_test.cc
        db/filename_test.cc
        db/flush_job_test.cc
        db/listener_test.cc
        db/log_test.cc
        db/manual_compaction_test.cc
        db/memtable_list_test.cc
        db/merge_helper_test.cc
        db/merge_test.cc
        db/options_file_test.cc
        db/perf_context_test.cc
        db/periodic_work_scheduler_test.cc
        db/plain_table_db_test.cc
        db/prefix_test.cc
        db/range_del_aggregator_test.cc
        db/range_tombstone_fragmenter_test.cc
        db/repair_test.cc
        db/table_properties_collector_test.cc
        db/version_builder_test.cc
        db/version_edit_test.cc
        db/version_set_test.cc
        db/wal_manager_test.cc
        db/wal_edit_test.cc
        db/write_batch_test.cc
        db/write_callback_test.cc
        db/write_controller_test.cc
        env/env_test.cc
        env/io_posix_test.cc
        env/mock_env_test.cc
        file/delete_scheduler_test.cc
        file/prefetch_test.cc
        file/random_access_file_reader_test.cc
        logging/auto_roll_logger_test.cc
        logging/env_logger_test.cc
        logging/event_logger_test.cc
        memory/arena_test.cc
        memory/memkind_kmem_allocator_test.cc
        memtable/inlineskiplist_test.cc
        memtable/skiplist_test.cc
        memtable/write_buffer_manager_test.cc
        monitoring/histogram_test.cc
        monitoring/iostats_context_test.cc
        monitoring/statistics_test.cc
        monitoring/stats_history_test.cc
        options/configurable_test.cc
        options/customizable_test.cc
        options/options_settable_test.cc
        options/options_test.cc
        table/block_based/block_based_filter_block_test.cc
        table/block_based/block_based_table_reader_test.cc
        table/block_based/block_test.cc
        table/block_based/data_block_hash_index_test.cc
        table/block_based/full_filter_block_test.cc
        table/block_based/partitioned_filter_block_test.cc
        table/cleanable_test.cc
        table/cuckoo/cuckoo_table_builder_test.cc
        table/cuckoo/cuckoo_table_reader_test.cc
        table/merger_test.cc
        table/sst_file_reader_test.cc
        table/table_test.cc
        table/block_fetcher_test.cc
        test_util/testutil_test.cc
        trace_replay/block_cache_tracer_test.cc
        trace_replay/io_tracer_test.cc
        tools/block_cache_analyzer/block_cache_trace_analyzer_test.cc
        tools/io_tracer_parser_test.cc
        tools/ldb_cmd_test.cc
        tools/reduce_levels_test.cc
        tools/sst_dump_test.cc
        tools/trace_analyzer_test.cc
        util/autovector_test.cc
        util/bloom_test.cc
        util/coding_test.cc
        util/crc32c_test.cc
        util/defer_test.cc
        util/dynamic_bloom_test.cc
        util/file_reader_writer_test.cc
        util/filelock_test.cc
        util/hash_test.cc
        util/heap_test.cc
        util/random_test.cc
        util/rate_limiter_test.cc
        util/repeatable_thread_test.cc
        util/ribbon_test.cc
        util/slice_test.cc
        util/slice_transform_test.cc
        util/timer_queue_test.cc
        util/timer_test.cc
        util/thread_list_test.cc
        util/thread_local_test.cc
        util/work_queue_test.cc
        utilities/backupable/backupable_db_test.cc
        utilities/blob_db/blob_db_test.cc
        utilities/cassandra/cassandra_functional_test.cc
        utilities/cassandra/cassandra_format_test.cc
        utilities/cassandra/cassandra_row_merge_test.cc
        utilities/cassandra/cassandra_serialize_test.cc
        utilities/checkpoint/checkpoint_test.cc
        utilities/memory/memory_test.cc
        utilities/merge_operators/string_append/stringappend_test.cc
        utilities/object_registry_test.cc
        utilities/option_change_migration/option_change_migration_test.cc
        utilities/options/options_util_test.cc
        utilities/persistent_cache/hash_table_test.cc
        utilities/persistent_cache/persistent_cache_test.cc
        utilities/simulator_cache/cache_simulator_test.cc
        utilities/simulator_cache/sim_cache_test.cc
        utilities/table_properties_collectors/compact_on_deletion_collector_test.cc
        utilities/transactions/optimistic_transaction_test.cc
        utilities/transactions/transaction_test.cc
        utilities/transactions/lock/point/point_lock_manager_test.cc
        utilities/transactions/write_prepared_transaction_test.cc
        utilities/transactions/write_unprepared_transaction_test.cc
        utilities/transactions/lock/range/range_locking_test.cc
        utilities/ttl/ttl_test.cc
        utilities/write_batch_with_index/write_batch_with_index_test.cc
    )
  endif()
  if(WITH_LIBRADOS)
    list(APPEND TESTS utilities/env_librados_test.cc)
  endif()

  if(WITH_FOLLY_DISTRIBUTED_MUTEX)
    list(APPEND TESTS third-party/folly/folly/synchronization/test/DistributedMutexTest.cpp)
  endif()

  set(TESTUTIL_SOURCE
      db/db_test_util.cc
      monitoring/thread_status_updater_debug.cc
      table/mock_table.cc
      utilities/cassandra/test_utils.cc
  )
  enable_testing()
  add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND})
  set(TESTUTILLIB testutillib${ARTIFACT_SUFFIX})
  add_library(${TESTUTILLIB} STATIC ${TESTUTIL_SOURCE})
  target_link_libraries(${TESTUTILLIB} ${ROCKSDB_LIB})
  if(MSVC)
    set_target_properties(${TESTUTILLIB} PROPERTIES COMPILE_FLAGS "/Fd${CMAKE_CFG_INTDIR}/testutillib${ARTIFACT_SUFFIX}.pdb")
  endif()
  set_target_properties(${TESTUTILLIB}
        PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
        EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
        EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
  )

  foreach(sourcefile ${TESTS})
      get_filename_component(exename ${sourcefile} NAME_WE)
      add_executable(${exename}${ARTIFACT_SUFFIX} ${sourcefile})
      set_target_properties(${exename}${ARTIFACT_SUFFIX}
        PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
        EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
        EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
        OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
      )
      target_link_libraries(${exename}${ARTIFACT_SUFFIX} testutillib${ARTIFACT_SUFFIX} testharness gtest ${THIRDPARTY_LIBS} ${ROCKSDB_LIB})
      if(NOT "${exename}" MATCHES "db_sanity_test")
        gtest_discover_tests(${exename} DISCOVERY_TIMEOUT 120)
        add_dependencies(check ${exename}${ARTIFACT_SUFFIX})
      endif()
      if("${exename}" MATCHES "env_librados_test")
        # env_librados_test.cc uses librados directly
        target_link_libraries(${exename}${ARTIFACT_SUFFIX} rados)
      endif()
  endforeach(sourcefile ${TESTS})

  if(WIN32)
    # C executables must link to a shared object
    if(ROCKSDB_BUILD_SHARED)
      set(ROCKSDB_LIB_FOR_C ${ROCKSDB_SHARED_LIB})
    else()
      set(ROCKSDB_LIB_FOR_C OFF)
    endif()
  else()
    set(ROCKSDB_LIB_FOR_C ${ROCKSDB_LIB})
  endif()

  if(ROCKSDB_LIB_FOR_C)
    set(C_TESTS db/c_test.c)
    add_executable(c_test db/c_test.c)
    target_link_libraries(c_test ${ROCKSDB_LIB_FOR_C} testharness)
    add_test(NAME c_test COMMAND c_test${ARTIFACT_SUFFIX})
    add_dependencies(check c_test)
  endif()
endif()

if(WITH_BENCHMARK_TOOLS)
  add_executable(db_bench${ARTIFACT_SUFFIX}
    tools/simulated_hybrid_file_system.cc
    tools/db_bench.cc
    tools/db_bench_tool.cc)
  target_link_libraries(db_bench${ARTIFACT_SUFFIX}
    ${ROCKSDB_LIB} ${THIRDPARTY_LIBS})

  add_executable(cache_bench${ARTIFACT_SUFFIX}
    cache/cache_bench.cc
    cache/cache_bench_tool.cc)
  target_link_libraries(cache_bench${ARTIFACT_SUFFIX}
    ${ROCKSDB_LIB} ${GFLAGS_LIB})

  add_executable(memtablerep_bench${ARTIFACT_SUFFIX}
    memtable/memtablerep_bench.cc)
  target_link_libraries(memtablerep_bench${ARTIFACT_SUFFIX}
    ${ROCKSDB_LIB} ${GFLAGS_LIB})

  add_executable(range_del_aggregator_bench${ARTIFACT_SUFFIX}
    db/range_del_aggregator_bench.cc)
  target_link_libraries(range_del_aggregator_bench${ARTIFACT_SUFFIX}
    ${ROCKSDB_LIB} ${GFLAGS_LIB})

  add_executable(table_reader_bench${ARTIFACT_SUFFIX}
    table/table_reader_bench.cc)
  target_link_libraries(table_reader_bench${ARTIFACT_SUFFIX}
    ${ROCKSDB_LIB} testharness ${GFLAGS_LIB})

  add_executable(filter_bench${ARTIFACT_SUFFIX}
    util/filter_bench.cc)
  target_link_libraries(filter_bench${ARTIFACT_SUFFIX}
    ${ROCKSDB_LIB} ${GFLAGS_LIB})

  add_executable(hash_table_bench${ARTIFACT_SUFFIX}
    utilities/persistent_cache/hash_table_bench.cc)
  target_link_libraries(hash_table_bench${ARTIFACT_SUFFIX}
    ${ROCKSDB_LIB} ${GFLAGS_LIB})
endif()

if(WITH_CORE_TOOLS OR WITH_TOOLS)
  add_subdirectory(tools)
  add_custom_target(core_tools
    DEPENDS ${core_tool_deps})
endif()

if(WITH_TOOLS)
  add_subdirectory(db_stress_tool)
  add_custom_target(tools
    DEPENDS ${tool_deps})
endif()

option(WITH_EXAMPLES "build with examples" OFF)
if(WITH_EXAMPLES)
  add_subdirectory(examples)
endif()
