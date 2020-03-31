# - Find NUMA
# Find the NUMA library and includes
#
# NUMA_INCLUDE_DIRS - where to find numa.h, etc.
# NUMA_LIBRARIES - List of libraries when using NUMA.
# NUMA_FOUND - True if NUMA found.

find_path(NUMA_INCLUDE_DIRS
  NAMES numa.h numaif.h
  HINTS ${NUMA_ROOT_DIR}/include)

find_library(NUMA_LIBRARIES
  NAMES numa
  HINTS ${NUMA_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(NUMA DEFAULT_MSG NUMA_LIBRARIES NUMA_INCLUDE_DIRS)

mark_as_advanced(
  NUMA_LIBRARIES
  NUMA_INCLUDE_DIRS)

if(NUMA_FOUND AND NOT (TARGET NUMA::NUMA))
  add_library (NUMA::NUMA UNKNOWN IMPORTED)
  set_target_properties(NUMA::NUMA
    PROPERTIES
      IMPORTED_LOCATION ${NUMA_LIBRARIES}
      INTERFACE_INCLUDE_DIRECTORIES ${NUMA_INCLUDE_DIRS})
endif()
