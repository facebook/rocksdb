# This cmake build is for Windows 64-bit only.
#
# Prerequisites:
#     You must have Visual Studio 2013 Update 4 installed. Start the Developer Command Prompt window that is a part of Visual Studio installation.
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
#        sample command: cmake -G "Visual Studio 12 Win64" -DGFLAGS=1 -DSNAPPY=1 -DJEMALLOC=1 -DJNI=1 ..
#        OR for VS Studio 15 cmake -G "Visual Studio 14 Win64" -DGFLAGS=1 -DSNAPPY=1 -DJEMALLOC=1 -DJNI=1 ..
# 4. Then build the project in debug mode (you may want to add /m[:<N>] flag to run msbuild in <N> parallel threads
#                                          or simply /m ot use all avail cores)
#        msbuild rocksdb.sln
#
#        rocksdb.sln build features exclusions of test only code in Release. If you build ALL_BUILD then everything
#        will be attempted but test only code does not build in Release mode.
#
# 5. And release mode (/m[:<N>] is also supported)
#        msbuild rocksdb.sln /p:Configuration=Release
#

execute_process(COMMAND powershell -Command "Get-Date -format MM_dd_yyyy" OUTPUT_VARIABLE DATE)
execute_process(COMMAND powershell -Command "Get-Date -format HH:mm:ss" OUTPUT_VARIABLE TIME)
string(REGEX REPLACE "(..)_(..)_..(..).*" "\\1/\\2/\\3" DATE ${DATE})
string(REGEX REPLACE "(..):(.....).*" " \\1:\\2" TIME ${TIME})
string(CONCAT GIT_DATE_TIME ${DATE} ${TIME})

find_package(Git)

if (GIT_FOUND AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/.git")
    execute_process(COMMAND $ENV{COMSPEC} /C ${GIT_EXECUTABLE} -C ${CMAKE_CURRENT_SOURCE_DIR} rev-parse HEAD OUTPUT_VARIABLE GIT_SHA)
else()
    set(GIT_SHA 0)
endif()

string(REGEX REPLACE "[^0-9a-f]+" "" GIT_SHA ${GIT_SHA})


add_custom_target(GenerateBuildVersion DEPENDS ${BUILD_VERSION_CC})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Zi /nologo  /EHsc /GS /Gd /GR /GF /fp:precise /Zc:wchar_t /Zc:forScope /errorReport:queue")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /FC /d2Zi+ /W3 /WX /wd4127 /wd4800 /wd4996 /wd4351")

# Used to run CI build and tests so we can run faster
set(OPTIMIZE_DEBUG_DEFAULT 0)        # Debug build is unoptimized by default use -DOPTDBG=1 to optimize

if(DEFINED OPTDBG)
   set(OPTIMIZE_DEBUG ${OPTDBG})
else()
   set(OPTIMIZE_DEBUG ${OPTIMIZE_DEBUG_DEFAULT})
endif()

if((${OPTIMIZE_DEBUG} EQUAL 1))
   message(STATUS "Debug optimization is enabled")
   set(CMAKE_CXX_FLAGS_DEBUG "/Oxt /MDd")
else()
   set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /Od /RTC1 /Gm /MDd")
endif()

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /Oxt /Zp8 /Gm- /Gy /MD")

set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} /DEBUG")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /DEBUG")

add_definitions(-DWIN32 -DOS_WIN -D_MBCS -DWIN64 -DNOMINMAX)

set(ROCKSDB_LIBS rocksdblib${ARTIFACT_SUFFIX})
set(THIRDPARTY_LIBS ${THIRDPARTY_LIBS} gtest)
set(SYSTEM_LIBS ${SYSTEM_LIBS} Shlwapi.lib Rpcrt4.lib)

set(LIBS ${ROCKSDB_LIBS} ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})

# For test util library that is build only in DEBUG mode
# and linked to tests. Add test only code that is not #ifdefed for Release here.
set(TESTUTIL_SOURCE
    db/db_test_util.cc
    table/mock_table.cc
    util/fault_injection_test_env.cc
    util/mock_env.cc
    util/thread_status_updater_debug.cc
)

add_library(rocksdblib${ARTIFACT_SUFFIX} ${SOURCES})
set_target_properties(rocksdblib${ARTIFACT_SUFFIX} PROPERTIES COMPILE_FLAGS "/Fd${CMAKE_CFG_INTDIR}/rocksdblib${ARTIFACT_SUFFIX}.pdb")
add_dependencies(rocksdblib${ARTIFACT_SUFFIX} GenerateBuildVersion)

add_library(rocksdb${ARTIFACT_SUFFIX} SHARED ${SOURCES})
set_target_properties(rocksdb${ARTIFACT_SUFFIX} PROPERTIES COMPILE_FLAGS "-DROCKSDB_DLL -DROCKSDB_LIBRARY_EXPORTS /Fd${CMAKE_CFG_INTDIR}/rocksdb${ARTIFACT_SUFFIX}.pdb")
add_dependencies(rocksdb${ARTIFACT_SUFFIX} GenerateBuildVersion)
target_link_libraries(rocksdb${ARTIFACT_SUFFIX} ${LIBS})

if (DEFINED JNI)
  if (${JNI} EQUAL 1)
    message(STATUS "JNI library is enabled")
    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/java)
  else()
    message(STATUS "JNI library is disabled")
  endif()
else()
  message(STATUS "JNI library is disabled")
endif()

set(EXES ${APPS})

foreach(sourcefile ${EXES})
    string(REPLACE ".cc" "" exename ${sourcefile})
    string(REGEX REPLACE "^((.+)/)+" "" exename ${exename})
    add_executable(${exename}${ARTIFACT_SUFFIX} ${sourcefile})
    target_link_libraries(${exename}${ARTIFACT_SUFFIX} ${LIBS})
endforeach(sourcefile ${EXES})

# test utilities are only build in debug
set(TESTUTILLIB testutillib${ARTIFACT_SUFFIX})
add_library(${TESTUTILLIB} STATIC ${TESTUTIL_SOURCE})
set_target_properties(${TESTUTILLIB} PROPERTIES COMPILE_FLAGS "/Fd${CMAKE_CFG_INTDIR}/testutillib${ARTIFACT_SUFFIX}.pdb")
set_target_properties(${TESTUTILLIB}
      PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
      EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
      EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
      )

# Tests are excluded from Release builds
set(TEST_EXES ${TESTS})

foreach(sourcefile ${TEST_EXES})
    string(REPLACE ".cc" "" exename ${sourcefile})
    string(REGEX REPLACE "^((.+)/)+" "" exename ${exename})
    add_executable(${exename}${ARTIFACT_SUFFIX} ${sourcefile})
    set_target_properties(${exename}${ARTIFACT_SUFFIX}
      PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
      EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
      EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
      )
    target_link_libraries(${exename}${ARTIFACT_SUFFIX} ${LIBS} testutillib${ARTIFACT_SUFFIX})
endforeach(sourcefile ${TEST_EXES})

# C executables must link to a shared object
set(C_TEST_EXES ${C_TESTS})

foreach(sourcefile ${C_TEST_EXES})
    string(REPLACE ".c" "" exename ${sourcefile})
    string(REGEX REPLACE "^((.+)/)+" "" exename ${exename})
    add_executable(${exename}${ARTIFACT_SUFFIX} ${sourcefile})
    set_target_properties(${exename}${ARTIFACT_SUFFIX}
      PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
      EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
      EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
      )
    target_link_libraries(${exename}${ARTIFACT_SUFFIX} rocksdb${ARTIFACT_SUFFIX} testutillib${ARTIFACT_SUFFIX})
endforeach(sourcefile ${C_TEST_EXES})
