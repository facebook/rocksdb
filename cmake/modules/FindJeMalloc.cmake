# - Find JeMalloc library
# Find the native JeMalloc includes and library
#
# JeMalloc_INCLUDE_DIRS - where to find jemalloc.h, etc.
# JeMalloc_LIBRARIES - List of libraries when using jemalloc.
# JeMalloc_FOUND - True if jemalloc found.

find_path(JeMalloc_INCLUDE_DIRS
  NAMES jemalloc/jemalloc.h
  HINTS ${JEMALLOC_ROOT_DIR}/include)

find_library(JeMalloc_LIBRARIES
  NAMES jemalloc
  HINTS ${JEMALLOC_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(JeMalloc DEFAULT_MSG JeMalloc_LIBRARIES JeMalloc_INCLUDE_DIRS)

mark_as_advanced(
  JeMalloc_LIBRARIES
  JeMalloc_INCLUDE_DIRS)

if(JeMalloc_FOUND AND NOT (TARGET JeMalloc::JeMalloc))
  add_library (JeMalloc::JeMalloc UNKNOWN IMPORTED)
  set_target_properties(JeMalloc::JeMalloc
    PROPERTIES
      IMPORTED_LOCATION ${JeMalloc_LIBRARIES}
      INTERFACE_INCLUDE_DIRECTORIES ${JeMalloc_INCLUDE_DIRS})
endif()
