# - Find NUMA
# Find the NUMA library and includes
#
# NUMA_INCLUDE_DIR - where to find numa.h, etc.
# NUMA_LIBRARIES - List of libraries when using NUMA.
# NUMA_FOUND - True if NUMA found.

find_path(NUMA_INCLUDE_DIR
  NAMES numa.h numaif.h
  HINTS ${NUMA_ROOT_DIR}/include)

find_library(NUMA_LIBRARIES
  NAMES numa
  HINTS ${NUMA_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(NUMA DEFAULT_MSG NUMA_LIBRARIES NUMA_INCLUDE_DIR)

mark_as_advanced(
  NUMA_LIBRARIES
  NUMA_INCLUDE_DIR)
