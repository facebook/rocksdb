# Edit definitions below to specify paths to include files and libraries of all 3rd party libraries

# This example assumes all the libraries locate in the same directory tree under THIRDPARTY_HOME environment variable
# Set environment variable THIRDPARTY_HOME to point to your third party libraries home (Unix style dir separators)

# Edit these lines to enable/disable external libraries use
set(USE_GFLAGS 1)
set(USE_SNAPPY 1)
set(USE_JEMALLOC 1)

#
# Edit these 4 lines to define paths to GFLAGS
#
set(GFLAGS_HOME $ENV{THIRDPARTY_HOME}/Gflags.Library)
set(GFLAGS_INCLUDE ${GFLAGS_HOME}/inc/include)
set(GFLAGS_LIB_DEBUG ${GFLAGS_HOME}/bin/debug/amd64/gflags.lib)
set(GFLAGS_LIB_RELEASE ${GFLAGS_HOME}/bin/retail/amd64/gflags.lib)

# Don't touch these lines
set(GFLAGS_CXX_FLAGS -DGFLAGS=gflags)
set(GFLAGS_LIBS debug ${GFLAGS_LIB_DEBUG} optimized ${GFLAGS_LIB_RELEASE})

#
# Edit these 4 lines to define paths to Snappy
#
set(SNAPPY_HOME $ENV{THIRDPARTY_HOME}/Snappy.Library)
set(SNAPPY_INCLUDE ${SNAPPY_HOME}/inc/inc)
set(SNAPPY_LIB_DEBUG ${SNAPPY_HOME}/bin/debug/amd64/snappy.lib)
set(SNAPPY_LIB_RELEASE ${SNAPPY_HOME}/bin/retail/amd64/snappy.lib)

# Don't touch these lines
set(SNAPPY_CXX_FLAGS -DSNAPPY)
set(SNAPPY_LIBS debug ${SNAPPY_LIB_DEBUG} optimized ${SNAPPY_LIB_RELEASE})

#
# Edit these 4 lines to define paths to Jemalloc
#
set(JEMALLOC_HOME $ENV{THIRDPARTY_HOME}/Jemalloc.Library)
set(JEMALLOC_INCLUDE ${JEMALLOC_HOME}/inc/include)
set(JEMALLOC_LIB_DEBUG ${JEMALLOC_HOME}/bin/debug/amd64/jemalloc.lib)
set(JEMALLOC_LIB_RELEASE ${JEMALLOC_HOME}/bin/retail/amd64/jemalloc.lib)

# Don't touch these lines
set(JEMALLOC_CXX_FLAGS -DJEMALLOC)
set(JEMALLOC_LIBS debug ${JEMALLOC_LIB_DEBUG} optimized ${JEMALLOC_LIB_RELEASE})


# Aggregate all third-party dependencies

set (THIRDPARTY_LIBS "")
set (ARTIFACT_SUFFIX "")

if (${USE_GFLAGS} EQUAL 1)
set (THIRDPARTY_LIBS ${THIRDPARTY_LIBS} ${GFLAGS_LIBS})
add_definitions(${GFLAGS_CXX_FLAGS})
include_directories(${GFLAGS_INCLUDE})
endif (${USE_GFLAGS} EQUAL 1)

if (${USE_SNAPPY} EQUAL 1)
set (THIRDPARTY_LIBS ${THIRDPARTY_LIBS} ${SNAPPY_LIBS})
add_definitions(${SNAPPY_CXX_FLAGS})
include_directories(${SNAPPY_INCLUDE})
endif (${USE_SNAPPY} EQUAL 1)

if (${USE_JEMALLOC} EQUAL 1)
set (THIRDPARTY_LIBS ${THIRDPARTY_LIBS} ${JEMALLOC_LIBS})
add_definitions(${JEMALLOC_CXX_FLAGS})
include_directories(${JEMALLOC_INCLUDE})
set (ARTIFACT_SUFFIX "_je")
endif (${USE_JEMALLOC} EQUAL 1)
