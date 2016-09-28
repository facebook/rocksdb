# - Find JeMalloc library
# Find the native JeMalloc includes and library
# This module defines
#  JEMALLOC_INCLUDE_DIRS, where to find jemalloc.h, Set when
#                        JEMALLOC_INCLUDE_DIR is found.
#  JEMALLOC_LIBRARIES, libraries to link against to use JeMalloc.
#  JEMALLOC_FOUND, If false, do not try to use JeMalloc.
#
find_path(JEMALLOC_INCLUDE_DIR
  jemalloc/jemalloc.h)

find_library(JEMALLOC_LIBRARIES
  jemalloc)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(JeMalloc DEFAULT_MSG
    JEMALLOC_LIBRARIES JEMALLOC_INCLUDE_DIR)

MARK_AS_ADVANCED(
  JEMALLOC_INCLUDE_DIR
  JEMALLOC_LIBRARIES
)
